use std::collections::HashMap;

use circus_common::{
  models::{Build, BuildStatus, Evaluation},
  overflow_demand,
};
use jiff::Timestamp;
use serde::Serialize;
use uuid::Uuid;

use crate::{error::ApiError, state::AppState};

type Result<T> = std::result::Result<T, ApiError>;

#[derive(Debug, Clone, Serialize)]
pub struct OperatorOverview {
  pub total_builds:       i64,
  pub completed_builds:   i64,
  pub failed_builds:      i64,
  pub running_builds:     i64,
  pub pending_builds:     i64,
  pub recent_builds:      Vec<OperatorBuild>,
  pub failed_builds_list: Vec<OperatorBuild>,
  pub projects:           Vec<OperatorProject>,
  pub queue_by_system:    Vec<OperatorQueueSystem>,
  pub workers:            Vec<OperatorWorker>,
  pub worker_online:      i64,
  pub worker_total:       i64,
  pub refreshed_at:       String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OperatorBuild {
  pub id:            Uuid,
  pub job_name:      String,
  pub project_id:    Option<Uuid>,
  pub project_name:  String,
  pub jobset_id:     Option<Uuid>,
  pub jobset_name:   String,
  pub status:        String,
  pub status_text:   String,
  pub status_class:  String,
  pub system:        String,
  pub created_at:    String,
  pub started_at:    String,
  pub completed_at:  String,
  pub duration:      String,
  pub started_epoch: Option<i64>,
  pub priority:      i32,
  pub is_aggregate:  bool,
  pub signed:        bool,
  pub drv_path:      String,
  pub output_path:   String,
  pub error_message: String,
  pub has_log:       bool,
}

impl OperatorBuild {
  fn from_build(b: &Build, context: Option<&BuildContext>) -> Self {
    let (text, class) = b.status.badge();
    let (project_id, project_name, jobset_id, jobset_name) = context
      .map_or_else(
        || (None, String::new(), None, String::new()),
        |context| {
          (
            Some(context.project_id),
            context.project_name.clone(),
            Some(context.jobset_id),
            context.jobset_name.clone(),
          )
        },
      );

    Self {
      id: b.id,
      job_name: b.job_name.clone(),
      project_id,
      project_name,
      jobset_id,
      jobset_name,
      status: format!("{:?}", b.status),
      status_text: text.to_string(),
      status_class: class.to_string(),
      system: b.system.clone().unwrap_or_else(|| "-".to_string()),
      created_at: b.created_at.strftime("%Y-%m-%d %H:%M").to_string(),
      started_at: b
        .started_at
        .map(|t| t.strftime("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_default(),
      completed_at: b
        .completed_at
        .map(|t| t.strftime("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_default(),
      duration: format_duration(b.started_at.as_ref(), b.completed_at.as_ref()),
      started_epoch: if b.completed_at.is_none() {
        b.started_at.map(jiff::Timestamp::as_second)
      } else {
        None
      },
      priority: b.priority,
      is_aggregate: b.is_aggregate,
      signed: b.signed,
      drv_path: b.drv_path.clone(),
      output_path: b.build_output_path.clone().unwrap_or_default(),
      error_message: b.error_message.clone().unwrap_or_default(),
      has_log: b.log_path.as_deref().is_some_and(|p| !p.is_empty()),
    }
  }
}

#[derive(Debug, Clone, Serialize)]
pub struct OperatorProject {
  pub id:               Uuid,
  pub name:             String,
  pub jobset_count:     i64,
  pub last_eval_status: String,
  pub last_eval_class:  String,
  pub last_eval_time:   String,
  pub failing_jobs:     i64,
  pub queued_jobs:      i64,
  pub latest_running:   i64,
  pub latest_queued:    i64,
  pub latest_succeeded: i64,
  pub latest_failed:    i64,
  pub systems:          String,
  pub updated_at:       String,
}

impl OperatorProject {
  async fn list(
    state: &AppState,
    include_hidden: bool,
    include_latest_job_statuses: bool,
  ) -> Result<Vec<Self>> {
    let mut all_projects = Vec::new();
    let mut offset = 0;
    loop {
      let page = circus_common::repo::projects::list(&state.pool, 100, offset)
        .await
        .map_err(ApiError)?;
      if page.is_empty() {
        break;
      }
      offset += page.len() as i64;
      all_projects.extend(page);
    }
    let mut project_summaries = Vec::with_capacity(all_projects.len());
    for p in &all_projects {
      let jobsets =
        circus_common::repo::jobsets::list_all_for_project(&state.pool, p.id)
          .await
          .map_err(ApiError)?;
      let mut last_eval: Option<Evaluation> = None;
      let mut latest_running = 0;
      let mut latest_queued = 0;
      let mut latest_succeeded = 0;
      let mut latest_failed = 0;
      for js in &jobsets {
        let js_evals =
          circus_common::repo::evaluations::list_filtered_with_visibility(
            &state.pool,
            Some(js.id),
            None,
            1,
            0,
            include_hidden,
          )
          .await
          .map_err(ApiError)?;
        let latest_for_jobset = js_evals.into_iter().next();
        if let Some(evaluation) = latest_for_jobset {
          if last_eval
            .as_ref()
            .is_none_or(|le| evaluation.evaluation_time > le.evaluation_time)
          {
            last_eval = Some(evaluation.clone());
          }
          if include_latest_job_statuses {
            for (status, count) in
              circus_common::repo::builds::job_status_counts(
                &state.pool,
                evaluation.id,
              )
              .await
              .map_err(ApiError)?
            {
              match status {
                BuildStatus::Running => latest_running += count,
                BuildStatus::Pending => latest_queued += count,
                BuildStatus::Succeeded => latest_succeeded += count,
                _ if status.badge().1 == "failed" => latest_failed += count,
                _ => {},
              }
            }
          }
        }
      }
      let (last_eval_status, last_eval_class, last_eval_time) =
        last_eval.as_ref().map_or_else(
          || ("Unknown".into(), "unknown".into(), "-".into()),
          |e| {
            let (text, class) = e.status.badge();
            (
              text.to_string(),
              class.to_string(),
              e.evaluation_time.strftime("%Y-%m-%d %H:%M").to_string(),
            )
          },
        );
      let summary = circus_common::repo::builds::project_summary(
        &state.pool,
        p.id,
        &FAILED_STATUSES,
      )
      .await
      .map_err(ApiError)?;
      let failing_jobs = summary.failing;
      let queued_jobs = summary.queued;
      let systems = summary.systems;
      project_summaries.push(Self {
        id: p.id,
        name: p.name.clone(),
        jobset_count: jobsets.len() as i64,
        last_eval_status,
        last_eval_class,
        last_eval_time,
        failing_jobs,
        queued_jobs,
        latest_running,
        latest_queued,
        latest_succeeded,
        latest_failed,
        systems: if systems.is_empty() {
          "-".into()
        } else {
          systems.join(", ")
        },
        updated_at: p.updated_at.strftime("%Y-%m-%d %H:%M").to_string(),
      });
    }
    Ok(project_summaries)
  }
}

#[derive(Debug, Clone, Serialize)]
pub struct OperatorQueueSystem {
  pub system:              String,
  pub count:               i64,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub qemu_overflow_count: Option<u32>,
}

impl OperatorQueueSystem {
  fn from_pending(
    pending: &[Build],
    qemu_overflow_count: Option<u32>,
  ) -> Vec<Self> {
    let mut queue_counts: HashMap<String, i64> = HashMap::new();
    for build in pending {
      let system = build
        .system
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
      *queue_counts.entry(system).or_default() += 1;
    }
    let canonical_systems = [
      "x86_64-linux",
      "aarch64-linux",
      "aarch64-darwin",
      "x86_64-darwin",
    ];
    canonical_systems
      .iter()
      .filter_map(|system| {
        queue_counts.get(*system).map(|count| {
          Self {
            system:              (*system).to_string(),
            count:               *count,
            qemu_overflow_count: (*system == "aarch64-linux")
              .then_some(qemu_overflow_count)
              .flatten(),
          }
        })
      })
      .chain(
        queue_counts
          .iter()
          .filter(|(system, _)| !canonical_systems.contains(&system.as_str()))
          .map(|(system, count)| {
            Self {
              system:              system.clone(),
              count:               *count,
              qemu_overflow_count: None,
            }
          }),
      )
      .collect()
  }
}

#[derive(Debug, Clone, Serialize)]
pub struct OperatorWorker {
  pub name:         String,
  pub system:       String,
  pub status_text:  String,
  pub status_class: String,
  pub current_jobs: i32,
  pub max_jobs:     i32,
}

impl OperatorWorker {
  async fn list(state: &AppState) -> Result<(Vec<Self>, i64, i64)> {
    let worker_sessions =
      circus_common::repo::builder_sessions::list(&state.pool)
        .await
        .map_err(ApiError)?;
    let worker_total = worker_sessions.len() as i64;
    let worker_online =
      worker_sessions.iter().filter(|w| w.connected).count() as i64;
    let workers = worker_sessions
      .iter()
      .take(8)
      .map(|w| {
        let status_text = if !w.connected {
          "offline"
        } else if w.current_jobs > 0 {
          "busy"
        } else {
          "idle"
        };
        Self {
          name:         w.name.clone(),
          system:       if w.systems.is_empty() {
            "unknown".into()
          } else {
            w.systems.join(", ")
          },
          status_text:  status_text.into(),
          status_class: if w.connected {
            "running".into()
          } else {
            "skipped".into()
          },
          current_jobs: w.current_jobs,
          max_jobs:     w.max_jobs,
        }
      })
      .collect();
    Ok((workers, worker_online, worker_total))
  }
}

/// Build the operator dashboard overview.
///
/// # Errors
///
/// Returns an error when the underlying repository queries fail.
pub async fn overview(
  state: &AppState,
  include_hidden: bool,
) -> Result<OperatorOverview> {
  let build_stats = circus_common::repo::builds::get_stats(&state.pool)
    .await
    .map_err(ApiError)?;
  let recent_raw = circus_common::repo::builds::list_recent(&state.pool, 40)
    .await
    .map_err(ApiError)?;
  let failed_raw = circus_common::repo::builds::list_filtered(
    &state.pool,
    None,
    Some("failed"),
    None,
    None,
    12,
    0,
  )
  .await
  .map_err(ApiError)?;
  let pending_raw =
    circus_common::repo::builds::list_pending_in_scheduler_order(
      &state.pool,
      100,
      0,
    )
    .await
    .map_err(ApiError)?;

  let context_by_eval =
    context_for_builds(state, recent_raw.iter().chain(failed_raw.iter()))
      .await?;
  let recent_builds = recent_raw
    .iter()
    .map(|b| {
      OperatorBuild::from_build(b, context_by_eval.get(&b.evaluation_id))
    })
    .collect();
  let failed_builds_list = failed_raw
    .iter()
    .map(|b| {
      OperatorBuild::from_build(b, context_by_eval.get(&b.evaluation_id))
    })
    .collect();

  let projects = OperatorProject::list(state, include_hidden, false).await?;
  let queue_by_system = OperatorQueueSystem::from_pending(&pending_raw, None);
  let (workers, worker_online, worker_total) =
    OperatorWorker::list(state).await?;

  Ok(OperatorOverview {
    total_builds: build_stats.total_builds.unwrap_or(0),
    completed_builds: build_stats.completed_builds.unwrap_or(0),
    failed_builds: build_stats.failed_builds.unwrap_or(0),
    running_builds: build_stats.running_builds.unwrap_or(0),
    pending_builds: build_stats.pending_builds.unwrap_or(0),
    recent_builds,
    failed_builds_list,
    projects,
    queue_by_system,
    workers,
    worker_online,
    worker_total,
    refreshed_at: Timestamp::now().strftime("%H:%M UTC").to_string(),
  })
}

/// Return recent builds for the operator dashboard.
///
/// # Errors
///
/// Returns an error when build or build-context queries fail.
pub async fn recent_builds(state: &AppState) -> Result<Vec<OperatorBuild>> {
  let builds = circus_common::repo::builds::list_recent(&state.pool, 40)
    .await
    .map_err(ApiError)?;
  let context_by_eval = context_for_builds(state, builds.iter()).await?;
  Ok(
    builds
      .iter()
      .map(|b| {
        OperatorBuild::from_build(b, context_by_eval.get(&b.evaluation_id))
      })
      .collect(),
  )
}

/// Return recent failed builds for the operator dashboard.
///
/// # Errors
///
/// Returns an error when build or build-context queries fail.
pub async fn failures(state: &AppState) -> Result<Vec<OperatorBuild>> {
  let builds = circus_common::repo::builds::list_filtered(
    &state.pool,
    None,
    Some("failed"),
    None,
    None,
    50,
    0,
  )
  .await
  .map_err(ApiError)?;
  let context_by_eval = context_for_builds(state, builds.iter()).await?;
  Ok(
    builds
      .iter()
      .map(|b| {
        OperatorBuild::from_build(b, context_by_eval.get(&b.evaluation_id))
      })
      .collect(),
  )
}

/// Return project summaries for the operator dashboard.
///
/// # Errors
///
/// Returns an error when the project query fails.
pub async fn projects(
  state: &AppState,
  include_hidden: bool,
) -> Result<Vec<OperatorProject>> {
  OperatorProject::list(state, include_hidden, false).await
}

/// Return project summaries with latest build-status counts for the
/// read-only compact dashboard.
///
/// # Errors
///
/// Returns error when a project, jobset, evaluation, or build query fails.
pub async fn projects_with_latest_status(
  state: &AppState,
  include_hidden: bool,
) -> Result<Vec<OperatorProject>> {
  OperatorProject::list(state, include_hidden, true).await
}

/// Return pending build counts grouped by target system.
///
/// # Errors
///
/// Returns an error when the pending-build query fails.
pub async fn queue(state: &AppState) -> Result<Vec<OperatorQueueSystem>> {
  let pending = circus_common::repo::builds::list_pending_in_scheduler_order(
    &state.pool,
    100,
    0,
  )
  .await
  .map_err(ApiError)?;
  let qemu_overflow_count = current_qemu_overflow_count(state).await;
  let mut queue =
    OperatorQueueSystem::from_pending(&pending, qemu_overflow_count);
  if let Some(count) = qemu_overflow_count
    && count > 0
    && !queue.iter().any(|row| row.system == "aarch64-linux")
  {
    queue.push(OperatorQueueSystem {
      system:              "aarch64-linux".to_owned(),
      count:               0,
      qemu_overflow_count: Some(count),
    });
  }
  Ok(queue)
}

async fn current_qemu_overflow_count(state: &AppState) -> Option<u32> {
  let (status_file, status_ttl) =
    if let Some(overflow) = &state.config.server.qemu_overflow_status {
      (&overflow.status_file, overflow.status_ttl_secs)
    } else {
      let overflow = state
        .config
        .queue_runner
        .ephemeral_pools
        .iter()
        .find_map(|pool| pool.qemu_overflow.as_ref())?;
      (&overflow.status_file, overflow.status_ttl_secs)
    };
  match overflow_demand::read_fresh(status_file, status_ttl).await {
    Ok(Some(report)) => Some(report.overflow_count),
    Ok(None) => None,
    Err(error) => {
      tracing::warn!(
        path = %status_file.display(),
        "failed to read fresh QEMU overflow demand state: {error}"
      );
      None
    },
  }
}

/// Return worker summaries for the operator dashboard.
///
/// # Errors
///
/// Returns an error when builder or agent session queries fail.
pub async fn worker_summary(state: &AppState) -> Result<Vec<OperatorWorker>> {
  let (workers, ..) = OperatorWorker::list(state).await?;
  Ok(workers)
}

#[derive(Clone)]
struct BuildContext {
  project_id:   Uuid,
  project_name: String,
  jobset_id:    Uuid,
  jobset_name:  String,
}

async fn context_for_builds<'a>(
  state: &AppState,
  builds: impl Iterator<Item = &'a Build>,
) -> Result<HashMap<Uuid, BuildContext>> {
  let mut eval_ids = Vec::new();
  for item in builds {
    if !eval_ids.contains(&item.evaluation_id) {
      eval_ids.push(item.evaluation_id);
    }
  }
  if eval_ids.is_empty() {
    return Ok(HashMap::new());
  }

  let rows = circus_common::repo::evaluations::get_build_contexts(
    &state.pool,
    &eval_ids,
  )
  .await
  .map_err(ApiError)?;

  Ok(
    rows
      .into_iter()
      .map(|row| {
        (row.evaluation_id, BuildContext {
          project_id:   row.project_id,
          project_name: row.project_name,
          jobset_id:    row.jobset_id,
          jobset_name:  row.jobset_name,
        })
      })
      .collect(),
  )
}

fn format_duration(
  started: Option<&jiff::Timestamp>,
  completed: Option<&jiff::Timestamp>,
) -> String {
  match (started, completed) {
    (Some(s), Some(c)) => {
      let secs = c.duration_since(*s).as_secs();
      if secs < 0 {
        return String::new();
      }
      let mins = secs / 60;
      let rem = secs % 60;
      if mins > 0 {
        format!("{mins}m {rem}s")
      } else {
        format!("{rem}s")
      }
    },
    _ => String::new(),
  }
}

const FAILED_STATUSES: [BuildStatus; 8] = [
  BuildStatus::Failed,
  BuildStatus::DependencyFailed,
  BuildStatus::FailedWithOutput,
  BuildStatus::Timeout,
  BuildStatus::CachedFailure,
  BuildStatus::LogLimitExceeded,
  BuildStatus::NarSizeLimitExceeded,
  BuildStatus::NonDeterministic,
];
