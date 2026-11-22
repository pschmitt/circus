use std::{
  collections::{HashMap, HashSet},
  future,
  path::Path,
  time::Duration,
};

use circus_common::{
  PgPool,
  models::{
    Build,
    CreateBuild,
    DEPENDENCY_JOB_PREFIX,
    EvaluationStatus,
    JobsetInput,
  },
  repo,
  systems::system_allowed,
};
use circus_config::S3SubstituterCredentials;
use circus_s3::{Credentials as S3Credentials, Presigner};
use data_encoding::HEXLOWER;
use futures::stream::{self, StreamExt};
use jiff::Timestamp;
use tokio::process::Command;
use url::Url;
use uuid::Uuid;

use crate::{memory::MemoryLimit, nix::NixJob};

async fn read_required_features(
  drv_path: &str,
  memory_limit: MemoryLimit,
) -> Vec<String> {
  let mut command =
    circus_nix::derivation::required_features_command(&[drv_path.to_owned()]);
  command.kill_on_drop(true);
  if memory_limit.apply_to(&mut command).is_err() {
    return Vec::new();
  }
  let Ok(output) = command.output().await else {
    return Vec::new();
  };
  if !output.status.success() {
    return Vec::new();
  }
  serde_json::from_slice(&output.stdout)
    .map(|value| circus_nix::derivation::union_required_features(&value))
    .unwrap_or_default()
}

#[derive(Debug, Clone)]
struct DerivationInfo {
  system:            Option<String>,
  outputs:           Option<HashMap<String, String>>,
  input_drvs:        Option<HashMap<String, serde_json::Value>>,
  required_features: Vec<String>,
}

fn parse_derivation_infos(
  store_dir: &Path,
  value: &serde_json::Value,
) -> HashMap<String, DerivationInfo> {
  let Some(derivations) = value
    .get("derivations")
    .and_then(serde_json::Value::as_object)
    .or_else(|| value.as_object())
  else {
    return HashMap::new();
  };

  derivations
    .iter()
    .map(|(drv_path, drv_val)| {
      let drv_path = circus_nix::derivation::store_path(store_dir, drv_path);
      let system = circus_nix::derivation::drv_system(drv_val);
      let outputs = circus_nix::derivation::drv_outputs(store_dir, drv_val);
      let input_drvs =
        circus_nix::derivation::drv_input_drvs(store_dir, drv_val);
      let required_features =
        circus_nix::derivation::drv_required_features(drv_val);

      (drv_path, DerivationInfo {
        system,
        outputs,
        input_drvs,
        required_features,
      })
    })
    .collect()
}

async fn show_derivations(
  drv_paths: &[String],
  memory_limit: MemoryLimit,
) -> HashMap<String, DerivationInfo> {
  let mut derivations = HashMap::new();
  let store_dir = match circus_nix::derivation::store_dir().await {
    Ok(dir) => dir,
    Err(error) => {
      tracing::warn!(%error, "failed to read the nix store directory");
      return derivations;
    },
  };

  for chunk in drv_paths.chunks(1024) {
    let mut command = Command::new("nix");
    command
      .args(["derivation", "show"])
      .args(chunk)
      .kill_on_drop(true);
    if let Err(error) = memory_limit.apply_to(&mut command) {
      tracing::warn!(%error, "failed to apply evaluator memory limit");
      return derivations;
    }
    let output = match command.output().await {
      Ok(output) => output,
      Err(error) => {
        tracing::warn!(%error, "failed to run nix derivation show");
        continue;
      },
    };
    if !output.status.success() {
      tracing::warn!(
        stderr = %String::from_utf8_lossy(&output.stderr),
        "nix derivation show failed"
      );
      continue;
    }
    if let Ok(value) =
      serde_json::from_slice::<serde_json::Value>(&output.stdout)
    {
      derivations.extend(parse_derivation_infos(store_dir, &value));
    }
  }
  derivations
}

/// Outputs absent from the configured caches and, when trusted, the local
/// store. Paths the checks
/// cannot vouch for are treated as invalid so the derivation still gets
/// enqueued.
async fn invalid_output_paths<'a>(
  derivations: impl Iterator<Item = &'a DerivationInfo>,
  trust_local_outputs: bool,
  substituters: &[Substituter],
  memory_limit: MemoryLimit,
) -> HashSet<String> {
  let paths = derivations
    .filter_map(|info| info.outputs.as_ref())
    .flat_map(|outputs| outputs.values().cloned())
    .collect::<HashSet<String>>()
    .into_iter()
    .collect::<Vec<String>>();

  let mut invalid = HashSet::new();
  if trust_local_outputs {
    for chunk in paths.chunks(1024) {
      let mut command = Command::new("nix-store");
      command
        .args(["--check-validity", "--print-invalid"])
        .args(chunk)
        .kill_on_drop(true);
      let output = if memory_limit.apply_to(&mut command).is_ok() {
        command.output().await.ok()
      } else {
        None
      };
      match output {
        Some(output) if output.status.success() => {
          invalid.extend(
            String::from_utf8_lossy(&output.stdout)
              .lines()
              .map(str::to_owned),
          );
        },
        _ => invalid.extend(chunk.iter().cloned()),
      }
    }
  } else {
    invalid.extend(paths);
  }

  for path in substitutable(&invalid, substituters).await {
    invalid.remove(&path);
  }
  invalid
}

/// A binary cache the evaluator checks for outputs that need no build.
#[derive(Clone, Debug)]
pub(crate) enum Substituter {
  /// `http(s)://` cache, checked with an anonymous `HEAD` of the narinfo.
  Http(String),
  /// `s3://` cache, checked with a presigned `HEAD` of the narinfo.
  S3(Presigner),
}

impl Substituter {
  /// Parse one entry of Nix's `substituters` setting. `s3://` caches are
  /// only checked when credentials are configured; other store types (and
  /// unparsable entries) are skipped.
  fn parse(url: &str, s3: Option<&S3SubstituterCredentials>) -> Option<Self> {
    if url.starts_with("http://") || url.starts_with("https://") {
      // Query parameters are store settings like `priority`.
      let base = url.split_once('?').map_or(url, |(base, _)| base);
      return Some(Self::Http(base.trim_end_matches('/').to_owned()));
    }
    if url.starts_with("s3://") {
      return s3_presigner(url, s3?).map(Self::S3);
    }
    None
  }

  /// URL whose `HEAD` succeeds when the cache has `hash.narinfo`.
  fn narinfo_url(&self, hash: &str, now: Timestamp) -> String {
    match self {
      Self::Http(base) => format!("{base}/{hash}.narinfo"),
      Self::S3(presigner) => {
        presigner.presign_at(
          "HEAD",
          &format!("{hash}.narinfo"),
          Duration::from_secs(300),
          now,
        )
      },
    }
  }
}

/// A presigner for a Nix `s3://bucket[/prefix]?endpoint=…&region=…` store
/// URL, signing with `credentials` (the URL's `profile` is not consulted).
fn s3_presigner(
  url: &str,
  credentials: &S3SubstituterCredentials,
) -> Option<Presigner> {
  let parsed = Url::parse(url).ok()?;
  let bucket = parsed.host_str().filter(|host| !host.is_empty())?;
  let prefix = Some(parsed.path().trim_matches('/'))
    .filter(|prefix| !prefix.is_empty())
    .map(str::to_owned);
  let param = |name: &str| {
    parsed
      .query_pairs()
      .find(|(key, _)| key == name)
      .map(|(_, value)| value.into_owned())
  };
  let scheme = param("scheme").unwrap_or_else(|| "https".to_owned());
  let endpoint_url = param("endpoint").map(|endpoint| {
    if endpoint.contains("://") {
      endpoint
    } else {
      format!("{scheme}://{endpoint}")
    }
  });
  // Nix addresses custom endpoints path-style unless told otherwise.
  let use_path_style = match param("addressing-style").as_deref() {
    Some("virtual") => false,
    Some("path") => true,
    _ => endpoint_url.is_some(),
  };
  Some(Presigner {
    credentials: S3Credentials {
      access_key:    credentials.access_key_id.clone()?,
      secret_key:    credentials.secret_access_key.clone()?,
      session_token: credentials.session_token.clone(),
    },
    region: param("region").unwrap_or_else(|| "us-east-1".to_owned()),
    bucket: bucket.to_owned(),
    prefix,
    endpoint_url,
    use_path_style,
  })
}

/// The binary caches this host's Nix substitutes from that the evaluator can
/// check: `http(s)://` ones, and `s3://` ones when `s3` credentials are set.
async fn nix_substituters(
  memory_limit: MemoryLimit,
  s3: Option<&S3SubstituterCredentials>,
) -> Vec<Substituter> {
  let mut command = Command::new("nix");
  command
    .args(["config", "show", "substituters"])
    .kill_on_drop(true);
  if memory_limit.apply_to(&mut command).is_err() {
    return Vec::new();
  }

  match command.output().await {
    Ok(output) if output.status.success() => {
      String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .filter_map(|url| Substituter::parse(url, s3))
        .collect()
    },
    Ok(output) => {
      tracing::warn!(
        stderr = %String::from_utf8_lossy(&output.stderr).trim(),
        "Failed to read Nix substituters"
      );
      Vec::new()
    },
    Err(error) => {
      tracing::warn!("Failed to read Nix substituters: {error}");
      Vec::new()
    },
  }
}

/// Paths some substituter already serves. An agent fetches these instead of
/// building them, so they need no build of their own.
async fn substitutable(
  paths: &HashSet<String>,
  substituters: &[Substituter],
) -> Vec<String> {
  if paths.is_empty() || substituters.is_empty() {
    return Vec::new();
  }

  let client = match circus_http::api()
    .connect_timeout(Duration::from_secs(5))
    .timeout(Duration::from_secs(10))
    .build()
  {
    Ok(client) => client,
    Err(error) => {
      tracing::warn!("Failed to build the substituter client: {error}");
      return Vec::new();
    },
  };

  stream::iter(paths)
    .map(|path| {
      let client = &client;
      async move {
        let hash = Path::new(path)
          .file_name()
          .and_then(|name| name.to_str())
          .and_then(|name| name.get(..32))?;
        for substituter in substituters {
          // Errors and denials count as "not there": the path gets a build.
          let found = client
            .head(substituter.narinfo_url(hash, Timestamp::now()))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success());
          if found {
            return Some(path.clone());
          }
        }
        None
      }
    })
    .buffer_unordered(32)
    .filter_map(future::ready)
    .collect()
    .await
}

fn should_enqueue_derivation(
  info: &DerivationInfo,
  invalid_outputs: &HashSet<String>,
) -> bool {
  let Some(outputs) = &info.outputs else {
    return true;
  };
  outputs
    .values()
    .any(|output| invalid_outputs.contains(output))
}

fn dependency_job_name(drv_path: &str) -> String {
  let basename = Path::new(drv_path)
    .file_name()
    .and_then(|name| name.to_str())
    .unwrap_or(drv_path)
    .trim_end_matches(".drv");
  format!("{DEPENDENCY_JOB_PREFIX}{basename}")
}

/// Detect whether a derivation is a fixed-output derivation from the output
/// list of its `.drv`.
///
/// # Returns
///
/// Returns `(is_fod, fod_hash)`.
fn detect_fod(drv_path: &str) -> (bool, Option<String>) {
  let Ok(content) = std::fs::read_to_string(drv_path) else {
    return (false, None);
  };
  // ATerm: Derive([("out","<path>","<algo>","<hash>")],...), one output only.
  let Some((outputs, _)) = content
    .strip_prefix("Derive([(")
    .and_then(|rest| rest.split_once(")],"))
  else {
    return (false, None);
  };
  match outputs.split(',').collect::<Vec<_>>().as_slice() {
    ["\"out\"", _path, algo, hash] if *algo != "\"\"" && *hash != "\"\"" => {
      (true, Some(hash.trim_matches('"').to_string()))
    },
    _ => (false, None),
  }
}

/// Resolve the dependency edges to insert for `jobs`, deduplicated. Jobs can
/// alias one drv path (`packages.default`), so edges are anchored to each
/// job's own build id rather than the drv-keyed map, which only retains the
/// last build per drv.
fn resolve_dependency_pairs(
  jobs: &[NixJob],
  build_ids: &[Uuid],
  drv_to_build: &HashMap<String, Uuid>,
  name_to_build: &HashMap<String, Uuid>,
) -> Vec<(Uuid, Uuid)> {
  let mut seen = HashSet::new();
  let mut pairs = Vec::new();
  for (job, &build_id) in jobs.iter().zip(build_ids) {
    if let Some(input_drvs) = &job.input_drvs {
      for dep_drv in input_drvs.keys() {
        if let Some(&dep_build_id) = drv_to_build.get(dep_drv)
          && dep_build_id != build_id
          && seen.insert((build_id, dep_build_id))
        {
          pairs.push((build_id, dep_build_id));
        }
      }
    }

    if let Some(constituents) = &job.constituents {
      for constituent_name in constituents {
        if let Some(&dep_build_id) = name_to_build.get(constituent_name)
          && dep_build_id != build_id
          && seen.insert((build_id, dep_build_id))
        {
          pairs.push((build_id, dep_build_id));
        }
      }
    }
  }
  pairs
}

/// Records an evaluation's builds batch by batch while it is still running.
///
/// Each batch commits together with every dependency build and edge it needs,
/// so the queue runner never sees a build before the edges that hold it back.
/// Aggregates wait for [`BuildStream::complete`] because their constituents can
/// arrive in any later batch.
pub(crate) struct BuildStream<'a> {
  pub(crate) priority: i32,
  pool:                &'a PgPool,
  eval_id:             Uuid,
  memory_limit:        MemoryLimit,
  allowed_systems:     Option<&'a HashSet<String>>,
  drv_to_build:        HashMap<String, Uuid>,
  name_to_build:       HashMap<String, Uuid>,
  name_to_drv:         HashMap<String, String>,
  dependencies:        HashMap<String, Uuid>,
  present:             HashSet<String>,
  touched:             HashSet<Uuid>,
  substituters:        Vec<Substituter>,
  trust_local_outputs: bool,
  aggregates:          Vec<NixJob>,
  dropped:             usize,
}

impl<'a> BuildStream<'a> {
  /// An orphaned evaluation re-runs under the same id, so builds its earlier
  /// attempt recorded are picked up instead of inserted again.
  pub(crate) async fn open(
    pool: &'a PgPool,
    eval_id: Uuid,
    memory_limit: MemoryLimit,
    allowed_systems: Option<&'a HashSet<String>>,
    s3_substituters: Option<&S3SubstituterCredentials>,
    snapshot_substituters: Option<&[String]>,
  ) -> misstep::Result<Self> {
    let mut drv_to_build = HashMap::new();
    let mut name_to_build = HashMap::new();
    let mut name_to_drv = HashMap::new();
    let mut dependencies = HashMap::new();
    for build in repo::builds::list_for_evaluation(pool, eval_id).await? {
      if build.job_name.starts_with(DEPENDENCY_JOB_PREFIX) {
        dependencies.insert(build.drv_path.clone(), build.id);
      }
      drv_to_build.insert(build.drv_path.clone(), build.id);
      name_to_build.insert(build.job_name.clone(), build.id);
      name_to_drv.insert(build.job_name, build.drv_path);
    }

    Ok(Self {
      priority: 0,
      pool,
      eval_id,
      memory_limit,
      allowed_systems,
      drv_to_build,
      name_to_build,
      name_to_drv,
      dependencies,
      present: HashSet::new(),
      touched: HashSet::new(),
      substituters: if let Some(urls) = snapshot_substituters {
        urls
          .iter()
          .filter_map(|url| Substituter::parse(url, s3_substituters))
          .collect()
      } else {
        nix_substituters(memory_limit, s3_substituters).await
      },
      trust_local_outputs: snapshot_substituters.is_none(),
      aggregates: Vec::new(),
      dropped: 0,
    })
  }

  /// Record a batch of jobs.
  ///
  /// # Returns
  ///
  /// The builds created, or `None` once the evaluation is no longer running.
  pub(crate) async fn insert(
    &mut self,
    jobs: Vec<NixJob>,
  ) -> misstep::Result<Option<Vec<Build>>> {
    let jobs = self.allowed(jobs);
    let (aggregates, jobs): (Vec<_>, Vec<_>) =
      jobs.into_iter().partition(|job| job.constituents.is_some());
    self.aggregates.extend(aggregates);
    self.persist(jobs, false, None).await
  }

  /// Record the aggregates and mark the evaluation completed.
  ///
  /// # Returns
  ///
  /// `false` if the evaluation stopped running first.
  pub(crate) async fn complete(
    mut self,
    evaluation_error: Option<&str>,
  ) -> misstep::Result<bool> {
    let aggregates = std::mem::take(&mut self.aggregates);
    let completed = self
      .persist(aggregates, true, evaluation_error)
      .await?
      .is_some();
    if self.dropped > 0 {
      tracing::info!(
        dropped = self.dropped,
        "Dropped jobs for systems outside evaluator.systems"
      );
    }
    Ok(completed)
  }

  fn allowed(&mut self, jobs: Vec<NixJob>) -> Vec<NixJob> {
    let total = jobs.len();
    let jobs = jobs
      .into_iter()
      .filter(|job| system_allowed(job.system.as_deref(), self.allowed_systems))
      .collect::<Vec<_>>();
    self.dropped += total - jobs.len();
    jobs
  }

  async fn expand(
    &mut self,
    jobs: Vec<NixJob>,
  ) -> (Vec<NixJob>, HashMap<String, DerivationInfo>) {
    let top_level_drvs = jobs
      .iter()
      .map(|job| job.drv_path.clone())
      .collect::<Vec<_>>();
    let mut derivations =
      show_derivations(&top_level_drvs, self.memory_limit).await;
    let mut included = top_level_drvs.into_iter().collect::<HashSet<_>>();
    let mut frontier = jobs
      .iter()
      .filter_map(|job| job.input_drvs.as_ref())
      .flat_map(|input_drvs| input_drvs.keys().cloned())
      .collect::<Vec<_>>();
    let mut expanded = jobs;

    loop {
      frontier.retain(|drv_path| {
        !included.contains(drv_path)
          && !self.drv_to_build.contains_key(drv_path)
          && !self.present.contains(drv_path)
      });
      frontier.sort_unstable();
      frontier.dedup();
      if frontier.is_empty() {
        break;
      }

      let shown = show_derivations(&frontier, self.memory_limit).await;
      let invalid_outputs = invalid_output_paths(
        shown.values(),
        self.trust_local_outputs,
        &self.substituters,
        self.memory_limit,
      )
      .await;
      let mut next = Vec::new();
      for drv_path in std::mem::take(&mut frontier) {
        let Some(info) = shown.get(&drv_path) else {
          continue;
        };
        if !should_enqueue_derivation(info, &invalid_outputs) {
          self.present.insert(drv_path);
          continue;
        }

        included.insert(drv_path.clone());
        if let Some(input_drvs) = &info.input_drvs {
          next.extend(input_drvs.keys().cloned());
        }
        expanded.push(NixJob {
          name: dependency_job_name(&drv_path),
          drv_path,
          system: info.system.clone(),
          outputs: info.outputs.clone(),
          input_drvs: info.input_drvs.clone(),
          constituents: None,
          meta: crate::nix::NixMeta::default(),
        });
      }
      derivations.extend(shown);
      frontier = next;
    }

    (expanded, derivations)
  }

  async fn persist(
    &mut self,
    jobs: Vec<NixJob>,
    finish: bool,
    evaluation_error: Option<&str>,
  ) -> misstep::Result<Option<Vec<Build>>> {
    // A restart keeps running and succeeded builds, which only stand in for a
    // job whose derivation is unchanged.
    let mut stale = Vec::new();
    let jobs = jobs
      .into_iter()
      .filter(|job| {
        let Some(&id) = self.name_to_build.get(&job.name) else {
          return true;
        };
        if self.name_to_drv.get(&job.name) == Some(&job.drv_path) {
          self.touched.insert(id);
          return false;
        }
        stale.push(id);
        true
      })
      .collect::<Vec<_>>();
    let (expanded, derivations) = self.expand(jobs).await;
    let jobs = self.allowed(expanded);

    let mut builds = Vec::with_capacity(jobs.len());
    for job in &jobs {
      let outputs_json = job
        .outputs
        .as_ref()
        .map(|o| serde_json::to_value(o).unwrap_or_default());
      let constituents_json = job
        .constituents
        .as_ref()
        .map(|c| serde_json::to_value(c).unwrap_or_default());
      let is_aggregate = job.constituents.is_some();

      let (is_fod, fod_hash) = detect_fod(&job.drv_path);
      let required_features = match derivations.get(&job.drv_path) {
        Some(info) => info.required_features.clone(),
        None => read_required_features(&job.drv_path, self.memory_limit).await,
      };
      builds.push(CreateBuild {
        evaluation_id: self.eval_id,
        job_name: job.name.clone(),
        drv_path: job.drv_path.clone(),
        system: job.system.clone(),
        outputs: outputs_json,
        is_aggregate: Some(is_aggregate),
        constituents: constituents_json,
        is_fod: Some(is_fod),
        fod_hash,
        meta_description: job.meta.description.clone(),
        meta_license: job.meta.license.clone(),
        meta_homepage: job.meta.homepage.clone(),
        meta_maintainers: job.meta.maintainers.clone(),
        required_features,
      });
    }

    let mut client = self.pool.get().await?;
    let tx = client.transaction().await?;
    if !repo::evaluations::lock_running(&tx, self.eval_id).await? {
      return Ok(None);
    }

    for &id in &stale {
      repo::builds::delete_in_transaction(&tx, id).await?;
    }
    self
      .drv_to_build
      .retain(|_, build_id| !stale.contains(build_id));
    self
      .dependencies
      .retain(|_, build_id| !stale.contains(build_id));

    let mut created = Vec::with_capacity(builds.len());
    let mut build_ids = Vec::with_capacity(builds.len());
    let mut adopted_ids = HashSet::new();
    for build in builds {
      let adopted = if build.job_name.starts_with(DEPENDENCY_JOB_PREFIX)
        || build.is_aggregate == Some(true)
      {
        None
      } else {
        self.dependencies.remove(&build.drv_path)
      };
      let mut build = match adopted {
        Some(id) => {
          adopted_ids.insert(id);
          repo::builds::adopt_dependency_in_transaction(&tx, id, &build).await?
        },
        None => repo::builds::create_in_transaction(&tx, build).await?,
      };
      if self.priority != 0 {
        tx.execute("UPDATE builds SET priority = $1 WHERE id = $2", &[
          &self.priority,
          &build.id,
        ])
        .await?;
        build.priority = self.priority;
      }
      if build.job_name.starts_with(DEPENDENCY_JOB_PREFIX) {
        self.dependencies.insert(build.drv_path.clone(), build.id);
      }
      self.drv_to_build.insert(build.drv_path.clone(), build.id);
      self.name_to_build.insert(build.job_name.clone(), build.id);
      self
        .name_to_drv
        .insert(build.job_name.clone(), build.drv_path.clone());
      self.touched.insert(build.id);
      build_ids.push(build.id);
      created.push(build);
    }

    let pairs = resolve_dependency_pairs(
      &jobs,
      &build_ids,
      &self.drv_to_build,
      &self.name_to_build,
    );
    for (build_id, dep_build_id) in pairs
      .into_iter()
      .filter(|(build_id, _)| !adopted_ids.contains(build_id))
    {
      repo::build_dependencies::create_in_transaction(
        &tx,
        build_id,
        dep_build_id,
      )
      .await?;
    }

    if finish {
      // A restart keeps builds the new attempt may no longer want.
      let kept = self.touched.iter().copied().collect::<Vec<_>>();
      repo::builds::delete_unreached_in_transaction(&tx, self.eval_id, &kept)
        .await?;

      let finished = repo::evaluations::finish_running_in_transaction(
        &tx,
        self.eval_id,
        EvaluationStatus::Completed,
        evaluation_error,
      )
      .await?;
      if !finished {
        return Err(misstep::report!(
          "evaluation {} lost its running state while locked",
          self.eval_id
        ));
      }
    }
    tx.commit().await?;
    Ok(Some(created))
  }
}

/// Compute a deterministic hash over the commit and all jobset inputs.
/// Used for evaluation caching, so skip re-eval when inputs haven't changed.
pub(crate) fn compute_inputs_hash(
  commit_hash: &str,
  inputs: &[JobsetInput],
) -> String {
  use sha2::{Digest, Sha256};

  let mut hasher = Sha256::new();
  hasher.update(commit_hash.as_bytes());

  // Sort inputs by name for deterministic hashing
  let mut sorted_inputs: Vec<&JobsetInput> = inputs.iter().collect();
  sorted_inputs.sort_by_key(|i| &i.name);

  for input in sorted_inputs {
    hasher.update(input.name.as_bytes());
    hasher.update(input.input_type.as_str().as_bytes());
    hasher.update(input.value.as_bytes());
    if let Some(ref rev) = input.revision {
      hasher.update(rev.as_bytes());
    }
  }

  HEXLOWER.encode(&hasher.finalize())
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  use circus_common::{
    PgPool,
    models::{
      BinaryCacheUpstreams,
      CreateBuild,
      CreateProject,
      EvaluationStatus,
    },
    repo,
  };
  use uuid::Uuid;

  use super::{BuildStream, resolve_dependency_pairs};
  use crate::{
    memory::MemoryLimit,
    nix::{NixJob, NixMeta},
  };

  fn job(name: &str, drv_path: &str, input_drvs: &[&str]) -> NixJob {
    NixJob {
      name:         name.to_owned(),
      drv_path:     drv_path.to_owned(),
      system:       None,
      outputs:      None,
      input_drvs:   (!input_drvs.is_empty()).then(|| {
        input_drvs
          .iter()
          .map(|drv| ((*drv).to_owned(), serde_json::Value::Null))
          .collect()
      }),
      constituents: None,
      meta:         NixMeta::default(),
    }
  }

  #[tokio::test]
  #[ignore = "requires a writable Nix store"]
  async fn local_only_outputs_are_missing_for_snapshot_builders()
  -> misstep::Result<()> {
    let fixture = tempfile::NamedTempFile::new()?;
    std::fs::write(fixture.path(), "circus snapshot locality test")?;
    let output = tokio::process::Command::new("nix-store")
      .arg("--add")
      .arg(fixture.path())
      .output()
      .await?;
    assert!(output.status.success());
    let path = String::from_utf8(output.stdout)?.trim().to_owned();
    let info = super::DerivationInfo {
      system:            None,
      outputs:           Some(HashMap::from([(
        "out".to_owned(),
        path.clone(),
      )])),
      input_drvs:        None,
      required_features: Vec::new(),
    };
    let local = super::invalid_output_paths(
      [&info].into_iter(),
      true,
      &[],
      MemoryLimit::new(None),
    )
    .await;
    let remote = super::invalid_output_paths(
      [&info].into_iter(),
      false,
      &[],
      MemoryLimit::new(None),
    )
    .await;
    assert!(local.is_empty());
    assert!(remote.contains(&path));
    Ok(())
  }

  #[test]
  fn aliased_jobs_get_unique_edges_on_their_own_builds() {
    let jobs = [
      job("packages.x86_64-linux.default", "/drv/pkg.drv", &[
        "/drv/dep.drv",
      ]),
      job("packages.x86_64-linux.pkg", "/drv/pkg.drv", &[
        "/drv/dep.drv",
      ]),
      job("drv:dep", "/drv/dep.drv", &[]),
    ];
    let build_ids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    let drv_to_build = HashMap::from([
      ("/drv/pkg.drv".to_owned(), build_ids[1]),
      ("/drv/dep.drv".to_owned(), build_ids[2]),
    ]);
    let name_to_build = jobs
      .iter()
      .zip(build_ids)
      .map(|(job, id)| (job.name.clone(), id))
      .collect();

    let pairs = resolve_dependency_pairs(
      &jobs,
      &build_ids,
      &drv_to_build,
      &name_to_build,
    );

    assert_eq!(pairs, vec![
      (build_ids[0], build_ids[2]),
      (build_ids[1], build_ids[2]),
    ]);
  }

  fn drv(name: &str) -> String {
    format!("/nix/store/{}-{name}.drv", Uuid::new_v4().simple())
  }

  /// A running evaluation on a fresh database, or `None` without
  /// `TEST_DATABASE_URL`.
  async fn running_evaluation() -> Option<(PgPool, Uuid)> {
    let url = std::env::var("TEST_DATABASE_URL").ok()?;
    circus_migrations::run_migrations(&url)
      .await
      .expect("migration failed");
    let pool = circus_common::build_pool(&url, 5).expect("failed to connect");

    let project = repo::projects::create(&pool, CreateProject {
      name:            format!("stream-{}", Uuid::new_v4().simple()),
      description:     None,
      repository_url:  "https://github.com/test/stream".to_owned(),
      cache_enabled:   false,
      cache_url:       None,
      cache_upstreams: BinaryCacheUpstreams::default(),
    })
    .await
    .expect("create project");

    let row = pool
      .get()
      .await
      .expect("pool client")
      .query_one(
        "WITH jobset AS (
           INSERT INTO jobsets (project_id, name, nix_expression)
           VALUES ($1, 'stream', 'packages')
           RETURNING id
         )
         INSERT INTO evaluations (jobset_id, commit_hash, status)
         SELECT id, 'stream', 'running' FROM jobset
         RETURNING id",
        &[&project.id],
      )
      .await
      .expect("create evaluation");

    Some((pool, row.get(0)))
  }

  async fn open(pool: &PgPool, eval_id: Uuid) -> BuildStream<'_> {
    BuildStream::open(pool, eval_id, MemoryLimit::new(None), None, None, None)
      .await
      .expect("open stream")
  }

  async fn seed(
    pool: &PgPool,
    eval_id: Uuid,
    name: &str,
    drv_path: &str,
  ) -> Uuid {
    repo::builds::create(pool, CreateBuild {
      evaluation_id: eval_id,
      job_name: name.to_owned(),
      drv_path: drv_path.to_owned(),
      ..Default::default()
    })
    .await
    .expect("seed build")
    .id
  }

  #[tokio::test]
  async fn later_batches_depend_on_earlier_ones() {
    let Some((pool, eval_id)) = running_evaluation().await else {
      return;
    };
    let lib = drv("lib");
    let mut stream = open(&pool, eval_id).await;

    let first = stream.insert(vec![job("lib", &lib, &[])]).await;
    let lib_id = first.expect("insert").expect("running")[0].id;
    let second = stream
      .insert(vec![job("app", &drv("app"), &[&lib])])
      .await
      .expect("insert")
      .expect("running");

    let deps = repo::build_dependencies::list_for_build(&pool, second[0].id)
      .await
      .expect("list deps");
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0].dependency_build_id, lib_id);
  }

  #[tokio::test]
  async fn restart_keeps_only_builds_the_new_attempt_reaches() {
    let Some((pool, eval_id)) = running_evaluation().await else {
      return;
    };
    let lib = drv("lib");
    let kept = seed(&pool, eval_id, "lib", &lib).await;
    let kept_dep = seed(&pool, eval_id, "drv:dep", &drv("dep")).await;
    repo::build_dependencies::create(&pool, kept, kept_dep)
      .await
      .expect("seed edge");
    let stale = seed(&pool, eval_id, "app", &drv("app")).await;
    let dropped = seed(&pool, eval_id, "removed", &drv("removed")).await;
    let app = drv("app");
    let mut stream = open(&pool, eval_id).await;

    let created = stream
      .insert(vec![job("lib", &lib, &[]), job("app", &app, &[])])
      .await
      .expect("insert")
      .expect("running");
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].drv_path, app);
    assert!(
      stream
        .complete(Some("1 attribute failed to evaluate"))
        .await
        .expect("complete")
    );

    let ids = repo::builds::list_for_evaluation(&pool, eval_id)
      .await
      .expect("list builds")
      .into_iter()
      .map(|build| build.id)
      .collect::<Vec<_>>();
    assert!(ids.contains(&kept));
    assert!(ids.contains(&kept_dep));
    assert!(!ids.contains(&stale));
    assert!(!ids.contains(&dropped));
    let evaluation = repo::evaluations::get(&pool, eval_id)
      .await
      .expect("get evaluation");
    assert_eq!(
      evaluation.error_message.as_deref(),
      Some("1 attribute failed to evaluate")
    );
  }

  #[tokio::test]
  async fn aggregates_wait_for_completion() {
    let Some((pool, eval_id)) = running_evaluation().await else {
      return;
    };
    let mut stream = open(&pool, eval_id).await;
    let aggregate = NixJob {
      constituents: Some(vec!["a".to_owned()]),
      ..job("all", &drv("all"), &[])
    };

    let created = stream
      .insert(vec![aggregate, job("a", &drv("a"), &[])])
      .await
      .expect("insert")
      .expect("running");
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].job_name, "a");

    assert!(stream.complete(None).await.expect("complete"));

    let builds = repo::builds::list_for_evaluation(&pool, eval_id)
      .await
      .expect("list builds");
    let all = builds
      .iter()
      .find(|build| build.job_name == "all")
      .expect("aggregate created");
    let deps = repo::build_dependencies::list_for_build(&pool, all.id)
      .await
      .expect("list deps");
    assert_eq!(deps[0].dependency_build_id, created[0].id);

    let evaluation = repo::evaluations::get(&pool, eval_id)
      .await
      .expect("get evaluation");
    assert_eq!(evaluation.status, EvaluationStatus::Completed);
  }
}

#[cfg(test)]
mod substituter_tests {
  use circus_config::S3SubstituterCredentials;
  use jiff::Timestamp;

  use super::Substituter;

  const NIKS3: &str = "s3://nix-cache-brkn-lol?endpoint=s3.example.org&\
                       profile=niks3&region=default&scheme=https";

  fn credentials() -> S3SubstituterCredentials {
    S3SubstituterCredentials {
      access_key_id: Some("AKIDEXAMPLE".into()),
      secret_access_key: Some("secret".into()),
      ..Default::default()
    }
  }

  #[test]
  fn http_substituter_drops_store_settings() {
    let substituter =
      Substituter::parse("https://cache.nixos.org/?priority=40", None);
    assert!(matches!(
      substituter,
      Some(Substituter::Http(ref base)) if base == "https://cache.nixos.org"
    ));
  }

  #[test]
  fn s3_substituter_needs_credentials() {
    assert!(Substituter::parse(NIKS3, None).is_none());
    assert!(
      Substituter::parse(NIKS3, Some(&S3SubstituterCredentials::default()))
        .is_none()
    );
  }

  #[test]
  fn s3_substituter_uses_url_parameters() {
    let substituter = Substituter::parse(NIKS3, Some(&credentials()));
    assert!(matches!(
      &substituter,
      Some(Substituter::S3(presigner))
        if presigner.bucket == "nix-cache-brkn-lol"
          && presigner.region == "default"
          && presigner.endpoint_url.as_deref() == Some("https://s3.example.org")
          && presigner.use_path_style
          && presigner.prefix.is_none()
    ));
  }

  #[test]
  fn s3_narinfo_url_is_a_presigned_head() {
    let substituter =
      Substituter::parse(NIKS3, Some(&credentials())).expect("s3 substituter");
    let url = substituter.narinfo_url(
      "0123456789abcdfghijklmnpqrsvwxyz",
      Timestamp::from_second(1_800_000_000).expect("timestamp"),
    );
    assert!(url.starts_with(
      "https://s3.example.org/nix-cache-brkn-lol/0123456789abcdfghijklmnpqrsvwxyz.narinfo?"
    ));
    assert!(url.contains("X-Amz-Signature="));
    assert!(url.contains("AKIDEXAMPLE"));
  }

  #[test]
  fn other_store_types_are_skipped() {
    assert!(
      Substituter::parse("ssh-ng://builder", Some(&credentials())).is_none()
    );
    assert!(Substituter::parse("daemon", Some(&credentials())).is_none());
  }
}
