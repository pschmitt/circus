use std::{
  collections::{HashMap, VecDeque},
  sync::Arc,
  time::{Duration, Instant},
};

use circus_common::{PgPool, overflow_demand, repo};
use circus_config::{EphemeralPoolConfig, GithubActionsPoolConfig};
use circus_http::RequestExt as _;
use misstep::{ResultExt as _, bail};
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::{
  dispatch::{supports_required_features, trusted_build_github_repository},
  rpc::AgentPool,
};

pub struct Autoscaler {
  cfg:            EphemeralPoolConfig,
  token:          String,
  http:           reqwest::Client,
  pool:           PgPool,
  agent_pool:     Arc<AgentPool>,
  inflight:       VecDeque<InflightLaunch>,
  last_scale_up:  Option<Instant>,
  shutdown_token: CancellationToken,
}

#[derive(Debug)]
struct InflightLaunch {
  launched_at: Instant,
}

#[derive(Serialize)]
struct WorkflowDispatch<'a> {
  #[serde(rename = "ref")]
  ref_name: &'a str,
  inputs:   WorkflowInputs<'a>,
}

#[derive(Serialize)]
struct WorkflowInputs<'a> {
  runner_url:         &'a str,
  oidc_audience:      &'a str,
  agent_binary_url:   &'a str,
  systems:            String,
  supported_features: String,
  mandatory_features: String,
  max_jobs:           String,
  cores:              String,
  speed_factor:       String,
  agent_name:         String,
}

#[derive(Debug)]
struct Demand {
  eligible_pending:  usize,
  live_capacity:     u32,
  live_agents:       u32,
  overflow_count:    u32,
  inflight_capacity: u32,
}

struct EligibleBuild {
  features:          Vec<String>,
  overflow_eligible: bool,
}

impl Autoscaler {
  /// # Errors
  ///
  /// Returns an error if the GitHub token cannot be loaded or the HTTP client
  /// cannot be constructed.
  pub async fn new(
    cfg: EphemeralPoolConfig,
    pool: PgPool,
    agent_pool: Arc<AgentPool>,
    shutdown_token: CancellationToken,
  ) -> misstep::Result<Self> {
    let token = load_token(&cfg.github_actions).await?;
    let http = circus_http::api().build()?;
    Ok(Self {
      cfg,
      token,
      http,
      pool,
      agent_pool,
      inflight: VecDeque::new(),
      last_scale_up: None,
      shutdown_token,
    })
  }

  pub async fn run(mut self) {
    tracing::info!(
      pool = %self.cfg.name,
      repository = %self.cfg.github_actions.workflow_repository,
      workflow = %self.cfg.github_actions.workflow,
      ref_name = %self.cfg.github_actions.ref_name,
      systems = ?self.cfg.systems,
      "GitHub Actions autoscaler started"
    );
    let mut interval =
      tokio::time::interval(Duration::from_secs(self.cfg.poll_interval_secs));
    loop {
      tokio::select! {
        () = self.shutdown_token.cancelled() => {
          tracing::info!("GitHub Actions autoscaler stopped");
          return;
        }
        _ = interval.tick() => {
          if let Err(e) = self.tick().await {
            tracing::warn!("GitHub Actions autoscaler tick failed: {e}");
          }
        }
      }
    }
  }

  async fn tick(&mut self) -> misstep::Result<()> {
    self.prune_inflight();
    let demand = self.demand().await?;
    let mut dispatch_error = None;
    if !self.in_cooldown() {
      let pending = demand.eligible_pending as u32;
      // A registered launch already shows in live_capacity, so only count
      // inflight launches without an agent yet.
      let pending_inflight =
        (self.inflight.len() as u32).saturating_sub(demand.live_agents);
      let available = demand.live_capacity.saturating_add(pending_inflight);
      let deficit = pending.saturating_sub(available);
      let remaining_inflight = self
        .cfg
        .max_inflight
        .saturating_sub(self.inflight.len() as u32);
      let launches = deficit.min(remaining_inflight);

      for _ in 0..launches {
        // Reserve launch capacity before the request. A transport failure can
        // mean GitHub accepted the workflow dispatch but its response was
        // lost, so keep the reservation until its normal expiry.
        self.inflight.push_back(InflightLaunch {
          launched_at: Instant::now(),
        });
        self.last_scale_up = Some(Instant::now());
        // Publish the reserved native slot before the external side effect,
        // so the autoscaler cannot create a QEMU worker in the dispatch gap.
        let reserved_demand = self.demand().await?;
        self.publish_overflow_demand(&reserved_demand).await?;
        if let Err(error) = self.dispatch_workflow().await {
          // Cooldown on failure too, so a transient error does not respin the
          // loop.
          dispatch_error = Some(error);
          break;
        }
      }
    }
    let refreshed_demand = self.demand().await?;
    self.publish_overflow_demand(&refreshed_demand).await?;
    dispatch_error.map_or(Ok(()), Err)
  }

  fn prune_inflight(&mut self) {
    let ttl = Duration::from_secs(self.cfg.inflight_ttl_secs);
    while self
      .inflight
      .front()
      .is_some_and(|launch| launch.launched_at.elapsed() >= ttl)
    {
      self.inflight.pop_front();
    }
  }

  fn in_cooldown(&self) -> bool {
    self.last_scale_up.is_some_and(|last| {
      last.elapsed() < Duration::from_secs(self.cfg.scale_up_cooldown_secs)
    })
  }

  async fn demand(&self) -> misstep::Result<Demand> {
    let builds =
      repo::builds::list_pending_for_systems(&self.pool, &self.cfg.systems)
        .await
        .context("load pending builds for GHA autoscaler")?;
    let mut eligible_requirements = Vec::new();
    let mut trusted_cache: HashMap<uuid::Uuid, Option<Option<String>>> =
      HashMap::new();
    for build in builds {
      if !supports_required_features(
        build.scheduling_features(),
        &self.cfg.supported_features,
        &self.cfg.mandatory_features,
      ) {
        continue;
      }
      let trusted_repo =
        if let Some(cached) = trusted_cache.get(&build.evaluation_id) {
          cached.clone()
        } else {
          let repo = trusted_build_github_repository(&self.pool, &build).await;
          trusted_cache.insert(build.evaluation_id, repo.clone());
          repo
        };
      if trusted_repo
        .as_ref()
        .and_then(|repo| repo.as_deref())
        .is_some_and(|repo| {
          self
            .cfg
            .allowed_build_repositories
            .iter()
            .any(|r| r == repo)
        })
      {
        let overflow_eligible =
          self.cfg.qemu_overflow.as_ref().is_some_and(|overflow| {
            let threshold = jiff::Timestamp::now().as_second().saturating_sub(
              i64::try_from(overflow.grace_secs).unwrap_or(i64::MAX),
            );
            build.system.as_deref() == Some(overflow.system.as_str())
              && build.created_at.as_second() <= threshold
              && supports_required_features(
                build.scheduling_features(),
                &overflow.supported_features,
                &[],
              )
          });
        eligible_requirements.push(EligibleBuild {
          features: build.scheduling_features().to_vec(),
          overflow_eligible,
        });
      }
    }

    let gha = &self.cfg.github_actions;
    let live: Vec<_> = self
      .agent_pool
      .snapshot_all()
      .into_iter()
      .filter(|agent| {
        agent.ephemeral
          && agent.auth_kind == circus_common::models::AuthKind::Oidc
          && agent.oidc_repository.as_deref()
            == Some(gha.workflow_repository.as_str())
          && agent_name_matches_pool(&agent.name, &self.cfg.name)
          && self
            .cfg
            .systems
            .iter()
            .any(|system| agent.systems.iter().any(|s| s == system))
      })
      .collect();
    let live_agents = live.len() as u32;

    let mut live_slots = Vec::new();
    for agent in &live {
      let free = agent.max_jobs.saturating_sub(agent.current_jobs);
      for _ in 0..free {
        live_slots.push(agent.clone());
      }
    }

    // Total order on both sides so capacity does not depend on HashMap
    // iteration order. Constrained builds take the least-capable slot.
    eligible_requirements.sort_by(|a, b| {
      b.features
        .len()
        .cmp(&a.features.len())
        .then(a.features.cmp(&b.features))
    });
    live_slots.sort_by(|a, b| {
      a.supported_features
        .len()
        .cmp(&b.supported_features.len())
        .then_with(|| a.supported_features.cmp(&b.supported_features))
        .then_with(|| a.mandatory_features.cmp(&b.mandatory_features))
        .then_with(|| a.machine_id.cmp(&b.machine_id))
    });

    let mut live_capacity = 0u32;
    let mut matched = vec![false; eligible_requirements.len()];
    for (idx, build) in eligible_requirements.iter().enumerate() {
      if let Some(pos) = live_slots.iter().position(|agent| {
        supports_required_features(
          &build.features,
          &agent.supported_features,
          &agent.mandatory_features,
        )
      }) {
        live_slots.remove(pos);
        matched[idx] = true;
        live_capacity = live_capacity.saturating_add(1);
      }
    }

    let inflight_capacity = (self.inflight.len() as u32)
      .saturating_sub(live_agents)
      .saturating_mul(self.cfg.max_jobs);
    let overflow_count = calculate_overflow_count(
      &eligible_requirements,
      &matched,
      inflight_capacity,
    );

    Ok(Demand {
      eligible_pending: eligible_requirements.len(),
      live_capacity,
      live_agents,
      overflow_count,
      inflight_capacity,
    })
  }

  async fn publish_overflow_demand(
    &self,
    demand: &Demand,
  ) -> misstep::Result<()> {
    let Some(overflow) = self.cfg.qemu_overflow.as_ref() else {
      return Ok(());
    };
    let report = overflow_demand::OverflowDemandReport {
      published_at:             jiff::Timestamp::now().as_second(),
      overflow_count:           demand.overflow_count,
      native_live_capacity:     demand.live_capacity,
      native_inflight_capacity: demand.inflight_capacity,
    };
    overflow_demand::write_atomic(&overflow.status_file, report)
      .await
      .map_err(|error| {
        misstep::report!(
          "publish QEMU overflow demand status {}: {error}",
          overflow.status_file.display()
        )
      })?;
    Ok(())
  }

  async fn dispatch_workflow(&self) -> misstep::Result<()> {
    let gha = &self.cfg.github_actions;
    let (owner, repo) = gha
      .workflow_repository
      .split_once('/')
      .ok_or_else(|| misstep::report!("repository must be owner/repo"))?;
    let workflow = urlencoding::encode(&gha.workflow);
    let url = format!(
      "https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow}/dispatches"
    );
    let body = WorkflowDispatch {
      ref_name: &gha.ref_name,
      inputs:   WorkflowInputs {
        runner_url:         &gha.runner_url,
        oidc_audience:      &gha.oidc_audience,
        agent_binary_url:   &gha.agent_binary_url,
        systems:            csv(&self.cfg.systems),
        supported_features: csv(&self.cfg.supported_features),
        mandatory_features: csv(&self.cfg.mandatory_features),
        max_jobs:           self.cfg.max_jobs.to_string(),
        cores:              self.cfg.cores.to_string(),
        speed_factor:       self.cfg.speed_factor.to_string(),
        agent_name:         self.cfg.name.clone(),
      },
    };
    self
      .http
      .post(url)
      .header(reqwest::header::ACCEPT, "application/vnd.github+json")
      .header("X-GitHub-Api-Version", circus_http::GITHUB_API_VERSION)
      .bearer_auth(&self.token)
      .json(&body)
      .send_checked()
      .await
      .context("dispatch GitHub Actions builder workflow")?;
    tracing::info!(
      pool = %self.cfg.name,
      repository = %gha.workflow_repository,
      workflow = %gha.workflow,
      ref_name = %gha.ref_name,
      "dispatched GitHub Actions builder workflow"
    );
    Ok(())
  }
}

fn calculate_overflow_count(
  builds: &[EligibleBuild],
  matched: &[bool],
  inflight_capacity: u32,
) -> u32 {
  let unmatched = builds
    .iter()
    .zip(matched)
    .filter(|(build, matched)| build.overflow_eligible && !**matched)
    .count() as u32;
  unmatched.saturating_sub(inflight_capacity)
}

fn csv(values: &[String]) -> String {
  values.join(",")
}

fn agent_name_matches_pool(agent_name: &str, pool_name: &str) -> bool {
  agent_name == pool_name
    || agent_name
      .strip_prefix(pool_name)
      .is_some_and(|suffix| suffix.starts_with('-'))
}

async fn load_token(cfg: &GithubActionsPoolConfig) -> misstep::Result<String> {
  if let Some(token) = cfg.token.as_deref()
    && !token.trim().is_empty()
  {
    return Ok(token.trim().to_owned());
  }
  let Some(path) = cfg.token_file.as_ref() else {
    bail!("github_actions pool requires token or token_file");
  };
  let token = tokio::fs::read_to_string(path)
    .await
    .with_context(|| format!("read GitHub token {}", path.display()))?;
  let token = token.trim();
  if token.is_empty() {
    bail!("GitHub token file {} is empty", path.display());
  }
  Ok(token.to_owned())
}

#[cfg(test)]
mod tests {
  use super::*;

  fn test_http_client() -> reqwest::Client {
    let _ = rustls::crypto::ring::default_provider().install_default();
    circus_http::api().build().expect("build test HTTP client")
  }

  #[tokio::test]
  async fn prunes_expired_inflight_launches() {
    let mut autoscaler = Autoscaler {
      cfg:            EphemeralPoolConfig {
        inflight_ttl_secs: 1,
        ..EphemeralPoolConfig::default()
      },
      token:          "token".into(),
      http:           test_http_client(),
      pool:           circus_common::build_pool(
        "postgres://localhost/circus",
        1,
      )
      .expect("lazy pool should construct"),
      agent_pool:     AgentPool::new(),
      inflight:       VecDeque::from([InflightLaunch {
        launched_at: Instant::now()
          .checked_sub(Duration::from_secs(2))
          .expect("two seconds before now should be representable"),
      }]),
      last_scale_up:  None,
      shutdown_token: CancellationToken::new(),
    };
    autoscaler.prune_inflight();
    assert!(autoscaler.inflight.is_empty());
  }

  #[test]
  fn overflow_demand_excludes_native_live_and_inflight_capacity() {
    let builds = vec![
      EligibleBuild { features: Vec::new(), overflow_eligible: true },
      EligibleBuild { features: Vec::new(), overflow_eligible: true },
      EligibleBuild { features: Vec::new(), overflow_eligible: true },
      EligibleBuild { features: Vec::new(), overflow_eligible: false },
    ];
    assert_eq!(
      calculate_overflow_count(&builds, &[true, false, false, false], 1),
      1,
    );
    assert_eq!(
      calculate_overflow_count(&builds, &[true, false, false, false], 2),
      0,
    );
  }

  #[tokio::test]
  async fn cooldown_blocks_immediate_second_scale_up() {
    let autoscaler = Autoscaler {
      cfg:            EphemeralPoolConfig {
        scale_up_cooldown_secs: 30,
        ..EphemeralPoolConfig::default()
      },
      token:          "token".into(),
      http:           test_http_client(),
      pool:           circus_common::build_pool(
        "postgres://localhost/circus",
        1,
      )
      .expect("lazy pool should construct"),
      agent_pool:     AgentPool::new(),
      inflight:       VecDeque::new(),
      last_scale_up:  Some(Instant::now()),
      shutdown_token: CancellationToken::new(),
    };
    assert!(autoscaler.in_cooldown());
  }

  #[test]
  fn workflow_inputs_are_plain_csv_values() {
    assert_eq!(
      csv(&["x86_64-linux".into(), "kvm".into()]),
      "x86_64-linux,kvm"
    );
  }

  #[test]
  fn pool_agent_names_match_unique_ephemeral_suffixes() {
    assert!(agent_name_matches_pool("gha-linux", "gha-linux"));
    assert!(agent_name_matches_pool(
      "gha-linux-gh123.1-deadbeef",
      "gha-linux"
    ));
    assert!(!agent_name_matches_pool(
      "other-gh123.1-deadbeef",
      "gha-linux"
    ));
  }
}
