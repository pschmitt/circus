use std::collections::HashMap;

use axum::{
  Json,
  Router,
  body::Bytes,
  extract::{DefaultBodyLimit, Path, Query, State},
  http::Extensions,
  routing::{get, post},
};
use circus_common::{
  CreateEvaluation,
  Evaluation,
  JobsetTriggerMode,
  PaginatedResponse,
  PaginationParams,
  Validate,
  repo,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::{
  error::ApiError,
  permissions::{self, Permission},
  state::AppState,
};

#[derive(Debug, Deserialize)]
struct ListEvaluationsParams {
  jobset_id: Option<Uuid>,
  status:    Option<String>,
  limit:     Option<i64>,
  offset:    Option<i64>,
}

async fn list_evaluations(
  State(state): State<AppState>,
  Query(params): Query<ListEvaluationsParams>,
  extensions: Extensions,
) -> Result<Json<PaginatedResponse<Evaluation>>, ApiError> {
  let include_hidden = permissions::check(&extensions, Permission::Admin);
  let pagination = PaginationParams {
    limit:  params.limit,
    offset: params.offset,
  };
  let limit = pagination.limit();
  let offset = pagination.offset();
  let items = circus_common::repo::evaluations::list_filtered_with_visibility(
    &state.pool,
    params.jobset_id,
    params.status.as_deref(),
    limit,
    offset,
    include_hidden,
  )
  .await?;
  let total = circus_common::repo::evaluations::count_filtered_with_visibility(
    &state.pool,
    params.jobset_id,
    params.status.as_deref(),
    include_hidden,
  )
  .await?;
  Ok(Json(PaginatedResponse {
    items,
    total,
    limit,
    offset,
  }))
}

async fn get_evaluation(
  State(state): State<AppState>,
  Path(id): Path<Uuid>,
  extensions: Extensions,
) -> Result<Json<Evaluation>, ApiError> {
  let evaluation = circus_common::repo::evaluations::get_visible(
    &state.pool,
    id,
    permissions::check(&extensions, Permission::Admin),
  )
  .await?;
  Ok(Json(evaluation))
}

async fn trigger_evaluation(
  extensions: Extensions,
  State(state): State<AppState>,
  Json(input): Json<CreateEvaluation>,
) -> Result<Json<Evaluation>, ApiError> {
  permissions::require_api(&extensions, Permission::EvalJobset)?;
  input
    .validate()
    .map_err(|msg| ApiError(circus_common::CiError::Validation(msg)))?;

  let jobset = repo::jobsets::get(&state.pool, input.jobset_id).await?;
  if jobset.trigger_mode != JobsetTriggerMode::SourceChange {
    return Err(ApiError(circus_common::CiError::Validation(
      "manual evaluation triggers are disabled for interval-mode jobsets"
        .to_string(),
    )));
  }

  let evaluation = repo::evaluations::create_manual(&state.pool, input).await?;
  Ok(Json(evaluation))
}

#[derive(Debug, Deserialize)]
struct SubmitSnapshotParams {
  jobset_id:       Uuid,
  hosts:           Option<String>,
  #[serde(default)]
  flake_jobs:      bool,
  #[serde(default)]
  normal_priority: bool,
  #[serde(default)]
  termuxpp:        bool,
  pool:            Option<String>,
  agent:           Option<Uuid>,
  #[serde(default)]
  fallback:        bool,
  #[serde(default)]
  front:           bool,
}

const SNAPSHOT_LIMIT: usize = 16 * 1024 * 1024;

async fn submit_snapshot(
  extensions: Extensions,
  State(state): State<AppState>,
  Query(params): Query<SubmitSnapshotParams>,
  archive: Bytes,
) -> Result<Json<Evaluation>, ApiError> {
  use circus_common::CiError;

  permissions::require_api(&extensions, Permission::Admin)?;
  let jobset = repo::jobsets::get(&state.pool, params.jobset_id).await?;
  let project = repo::projects::get(&state.pool, jobset.project_id).await?;
  if !jobset.enabled
    || !jobset.flake_mode
    || jobset.branch.as_deref().is_none_or(str::is_empty)
    || jobset.trigger_mode != JobsetTriggerMode::SourceChange
    || !state
      .config
      .server
      .snapshot_repositories
      .contains(&project.repository_url)
  {
    return Err(ApiError(CiError::Forbidden(
      "Snapshot submissions are disabled for this jobset".into(),
    )));
  }
  let mut hosts: Vec<_> =
    params.hosts.as_deref().map_or_else(Vec::new, |hosts| {
      hosts.split(',').map(str::to_owned).collect()
    });
  hosts.sort_unstable();
  hosts.dedup();
  if params.flake_jobs && (params.hosts.is_some() || params.termuxpp) {
    return Err(ApiError(CiError::Validation(
      "Flake job snapshots cannot select NixOS or TermuxPP hosts".into(),
    )));
  }
  if !params.flake_jobs
    && (hosts.is_empty()
      || hosts.len() > 16
      || hosts.iter().any(|host| {
        host.is_empty()
          || host.len() > 63
          || !host.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
          })
      }))
  {
    return Err(ApiError(CiError::Validation(
      "Invalid snapshot hosts".into(),
    )));
  }
  if archive.is_empty() || archive.len() > SNAPSHOT_LIMIT {
    return Err(ApiError(CiError::Validation(
      "Invalid snapshot archive size".into(),
    )));
  }

  if params.pool.is_some() && params.agent.is_some() {
    return Err(ApiError(CiError::Validation(
      "Choose either a pool or an agent".into(),
    )));
  }
  if params.fallback && params.agent.is_none() {
    return Err(ApiError(CiError::Validation(
      "Fallback requires an exact agent".into(),
    )));
  }
  if params.front && params.normal_priority {
    return Err(ApiError(CiError::Validation(
      "Choose either front or normal priority".into(),
    )));
  }
  if let Some(pool) = &params.pool {
    let known = state.config.server.snapshot_pools.as_ref().map_or_else(
      || {
        state
          .config
          .queue_runner
          .ephemeral_pools
          .iter()
          .any(|candidate| {
            candidate.name == *pool && !candidate.is_pull_request_pool()
          })
      },
      |pools| pools.contains(pool),
    );
    if !known {
      return Err(ApiError(CiError::Validation(
        "Unknown snapshot pool".into(),
      )));
    }
  }
  if let Some(agent) = params.agent {
    repo::builder_sessions::get(&state.pool, agent).await?;
  }
  let scheduling = repo::evaluations::SnapshotScheduling {
    pool:     params.pool,
    agent:    params.agent,
    front:    params.front,
    fallback: params.fallback,
  };
  let priority = if params.normal_priority { 0 } else { 10 };
  // Preserve the adhoc host snapshot lifecycle. Generated flake jobs are
  // immutable and can be submitted again after a client loses the response.
  let snapshot_id = if params.flake_jobs {
    let mut hash = Sha256::new();
    hash.update(b"circus-flake-jobs-v1\0");
    hash.update([
      u8::from(scheduling.front),
      u8::from(scheduling.fallback),
      u8::from(params.normal_priority),
    ]);
    hash.update(scheduling.agent.unwrap_or_default().as_bytes());
    let pool = scheduling.pool.as_deref().unwrap_or("").as_bytes();
    hash.update(pool.len().to_be_bytes());
    hash.update(pool);
    hash.update(&archive);
    Uuid::from_slice(&hash.finalize()[..16])
      .expect("SHA-256 supplies 16 bytes")
      .simple()
      .to_string()
  } else {
    Uuid::new_v4().simple().to_string()
  };

  let result = repo::evaluations::create_snapshot(
    &state.pool,
    jobset.id,
    &snapshot_id,
    &archive,
    &hosts,
    params.termuxpp,
    priority,
    &scheduling,
  )
  .await;
  match result {
    Ok(evaluation) => Ok(Json(evaluation)),
    Err(error) if params.flake_jobs => {
      let existing = repo::evaluations::get_by_jobset_and_commit(
        &state.pool,
        jobset.id,
        &snapshot_id,
      )
      .await?;
      existing.map(Json).ok_or(ApiError(error))
    },
    Err(error) => Err(ApiError(error)),
  }
}

async fn restart_evaluation(
  extensions: Extensions,
  State(state): State<AppState>,
  Path(id): Path<Uuid>,
) -> Result<Json<Evaluation>, ApiError> {
  permissions::require_api(&extensions, Permission::RestartJobs)?;
  // Unknown ids are 404, not a conflict.
  repo::evaluations::get(&state.pool, id).await?;
  let evaluation = repo::evaluations::restart(&state.pool, id)
    .await?
    .ok_or_else(|| {
      ApiError(circus_common::CiError::Conflict(
        "Only failed, cancelled, or timed-out evaluations with active jobsets \
         can be restarted"
          .to_string(),
      ))
    })?;

  tracing::info!(
    eval_id = %id,
    commit = %evaluation.commit_hash,
    "Evaluation restarted"
  );

  Ok(Json(evaluation))
}

#[derive(Debug, Deserialize)]
struct CompareParams {
  to: Uuid,
}

#[derive(Debug, Serialize)]
struct EvalComparison {
  from_id:         Uuid,
  to_id:           Uuid,
  new_jobs:        Vec<JobDiff>,
  removed_jobs:    Vec<JobDiff>,
  changed_jobs:    Vec<JobChange>,
  unchanged_count: usize,
}

#[derive(Debug, Serialize)]
struct JobDiff {
  job_name: String,
  system:   Option<String>,
  drv_path: String,
  status:   String,
}

#[derive(Debug, Serialize)]
struct JobChange {
  job_name:   String,
  system:     Option<String>,
  old_drv:    String,
  new_drv:    String,
  old_status: String,
  new_status: String,
}

async fn compare_evaluations(
  State(state): State<AppState>,
  Path(id): Path<Uuid>,
  Query(params): Query<CompareParams>,
) -> Result<Json<EvalComparison>, ApiError> {
  // Verify both evaluations exist
  let _from_eval =
    circus_common::repo::evaluations::get(&state.pool, id).await?;
  let _to_eval =
    circus_common::repo::evaluations::get(&state.pool, params.to).await?;

  let from_builds =
    circus_common::repo::builds::list_for_evaluation(&state.pool, id).await?;
  let to_builds =
    circus_common::repo::builds::list_for_evaluation(&state.pool, params.to)
      .await?;

  let from_map: HashMap<&str, &circus_common::Build> = from_builds
    .iter()
    .map(|b| (b.job_name.as_str(), b))
    .collect();
  let to_map: HashMap<&str, &circus_common::Build> =
    to_builds.iter().map(|b| (b.job_name.as_str(), b)).collect();

  let mut new_jobs = Vec::new();
  let mut removed_jobs = Vec::new();
  let mut changed_jobs = Vec::new();
  let mut unchanged_count = 0;

  // Jobs in `to` but not in `from` are new
  for (name, build) in &to_map {
    if !from_map.contains_key(name) {
      new_jobs.push(JobDiff {
        job_name: name.to_string(),
        system:   build.system.clone(),
        drv_path: build.drv_path.clone(),
        status:   format!("{:?}", build.status),
      });
    }
  }

  // Jobs in `from` but not in `to` are removed
  for (name, build) in &from_map {
    if !to_map.contains_key(name) {
      removed_jobs.push(JobDiff {
        job_name: name.to_string(),
        system:   build.system.clone(),
        drv_path: build.drv_path.clone(),
        status:   format!("{:?}", build.status),
      });
    }
  }

  // Jobs in both: compare derivation paths
  for (name, from_build) in &from_map {
    if let Some(to_build) = to_map.get(name) {
      if from_build.drv_path == to_build.drv_path {
        unchanged_count += 1;
      } else {
        changed_jobs.push(JobChange {
          job_name:   name.to_string(),
          system:     to_build.system.clone(),
          old_drv:    from_build.drv_path.clone(),
          new_drv:    to_build.drv_path.clone(),
          old_status: format!("{:?}", from_build.status),
          new_status: format!("{:?}", to_build.status),
        });
      }
    }
  }

  Ok(Json(EvalComparison {
    from_id: id,
    to_id: params.to,
    new_jobs,
    removed_jobs,
    changed_jobs,
    unchanged_count,
  }))
}

pub fn router() -> Router<AppState> {
  Router::new()
    .route("/evaluations", get(list_evaluations))
    .route("/evaluations/{id}", get(get_evaluation))
    .route("/evaluations/{id}/compare", get(compare_evaluations))
    .route("/evaluations/{id}/restart", post(restart_evaluation))
    .route("/evaluations/trigger", post(trigger_evaluation))
    .route(
      "/evaluations/snapshot",
      post(submit_snapshot).layer(DefaultBodyLimit::max(SNAPSHOT_LIMIT)),
    )
}
