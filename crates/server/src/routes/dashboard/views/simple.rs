//! The compact, read-only build dashboard used by build.brkn.lol.

use std::{collections::HashMap, fs};

use serde::Deserialize;
use topcoat::{
  Result,
  context::{Cx, app_context},
  router::{
    error::{bad_request, not_found},
    page,
    path_param,
    query_params,
  },
  runtime::shard,
  view::{View, component, emit, live, view},
};
use uuid::Uuid;

use super::super::{
  layout::{document, shard_viewer, viewer},
  live::{BuildsChanged, Wake},
  shared::{BuildView, DashboardPage, ProjectSummaryView},
};
use crate::{operator, state::AppState};

const EFFECT_STATUS_DIR: &str = "/var/lib/circus-effects/public-status";
const SIMPLE_BUILD_PAGE_SIZE: i64 = 50;

#[derive(Deserialize)]
struct EffectSnapshot {
  commit:     String,
  updated_at: i64,
  effects:    Vec<EffectStatus>,
}

#[derive(Deserialize)]
struct EffectStatus {
  name:    String,
  state:   String,
  message: Option<String>,
}

path_param!(simple_project_id: Uuid, error = not_found);

struct ProjectJobsetView {
  name:          String,
  branch:        String,
  latest:        Option<ProjectEvaluationView>,
  builds_commit: Option<String>,
  build_count:   i64,
  jobs:          Vec<ProjectJobView>,
}

struct ProjectEvaluationView {
  commit:       String,
  status_text:  &'static str,
  status_class: &'static str,
}

struct ProjectJobView {
  id:           Uuid,
  name:         String,
  system:       String,
  status_text:  &'static str,
  status_class: &'static str,
}

struct BuildActivity {
  builds:  Vec<BuildView>,
  running: i64,
  queued:  i64,
  failed:  i64,
}

#[query_params(error = bad_request)]
struct SimpleQuery {
  status: Option<String>,
  offset: Option<i64>,
}

struct SimpleProjectView {
  summary:         ProjectSummaryView,
  history:         Vec<EvaluationHistoryPoint>,
  median_duration: String,
  pass_rate:       String,
  status_text:     &'static str,
  status_class:    &'static str,
  status_icon:     &'static str,
}

struct EvaluationHistoryPoint {
  status_text:      &'static str,
  status_class:     &'static str,
  duration_seconds: Option<i64>,
  height_percent:   i64,
  finished:         bool,
}

impl EvaluationHistoryPoint {
  fn label(&self) -> String {
    match self.duration_seconds {
      Some(seconds) => {
        format!("{}, {}", self.status_text, duration_label(seconds))
      },
      None => self.status_text.to_owned(),
    }
  }
}

struct WorkerView {
  name:         String,
  systems:      String,
  status_text:  &'static str,
  status_class: &'static str,
  current_jobs: i32,
  max_jobs:     i32,
  cpu_count:    i32,
  speed_factor: f32,
  last_seen:    String,
  builds:       Vec<WorkerBuildView>,
}

struct WorkerBuildView {
  id:      Uuid,
  name:    String,
  project: String,
}

#[page("/dash")]
pub(in crate::routes::dashboard) async fn simple_page(
  cx: &Cx,
) -> Result<impl View> {
  let viewer = viewer(cx, DashboardPage::Home).await?;
  let query = query_params::<SimpleQuery>(cx)?;
  let build_status = match query.status.as_deref() {
    Some("running" | "pending" | "failed") => query.status.clone(),
    Some(_) => {
      return Err(bad_request("unsupported compact build status").into());
    },
    None => None,
  };
  let build_offset = query.offset.unwrap_or_default().clamp(0, 100_000);
  Ok(view! {
    document(title: "Builds", viewer: &viewer, topbar: false,
      <section class="simple-dashboard">
        <header class="simple-header">
          <a class="simple-wordmark" href="/dash" aria-label="Build dashboard home">
            <img src="/static/favicon.svg" alt="">
          </a>
          <nav aria-label="Account">
            <span class="simple-live"><span class="simple-live-dot" aria-hidden="true"></span>"Live"</span>
            <a href="/builds">"Circus console"</a>
            if viewer.auth_name.is_empty() {
              <a href="/login">"Sign in"</a>
            } else {
              <span>"Signed in as " (viewer.auth_name.as_str())</span>
            }
          </nav>
        </header>
        <main class="simple-content">
          if let Some(status) = build_status.as_deref() {
            simple_build_results(status: status.to_owned(), offset: build_offset)
          } else {
            <fieldset class="simple-tabs">
              <legend class="sr-only">"Dashboard sections"</legend>
              <input class="simple-tab-toggle" type="radio" name="dashboard-tab" id="tab-activity" checked="checked">
              <label for="tab-activity"><span aria-hidden="true">"◷"</span>"Activity"</label>
              <input class="simple-tab-toggle" type="radio" name="dashboard-tab" id="tab-repositories">
              <label for="tab-repositories"><span aria-hidden="true">"▣"</span>"Repositories"</label>
              <input class="simple-tab-toggle" type="radio" name="dashboard-tab" id="tab-workers">
              <label for="tab-workers"><span aria-hidden="true">"⚙"</span>"Workers"</label>
              <input class="simple-tab-toggle" type="radio" name="dashboard-tab" id="tab-effects">
              <label for="tab-effects"><span aria-hidden="true">"✦"</span>"Effects"</label>
              <div class="simple-tab-panels">
                <section class="simple-tab-panel" id="activity-panel" aria-label="Activity">
                  build_activity()
                </section>
                <section class="simple-tab-panel" id="repositories-panel" aria-label="Repositories">
                  repositories_activity()
                </section>
                <section class="simple-tab-panel" id="workers-panel" aria-label="Workers">
                  workers_activity()
                </section>
                <section class="simple-tab-panel" id="effects-panel" aria-label="Effects">
                  effects_activity()
                </section>
              </div>
            </fieldset>
          }
        </main>
      </section>
    )
  })
}

#[shard]
async fn simple_build_results(
  cx: &Cx,
  status: String,
  mut offset: i64,
) -> Result<impl View> {
  let viewer = shard_viewer(cx, DashboardPage::Home).await?;
  let state = app_context::<AppState>(cx);
  let mut changed = BuildsChanged::subscribe(cx);

  Ok(live! {
    loop {
      let total = circus_common::repo::builds::count_filtered_with_visibility(
        &state.pool,
        None,
        Some(&status),
        None,
        None,
        viewer.is_admin,
      )
      .await
      .unwrap_or_default();
      offset = if total == 0 {
        0
      } else {
        offset.min(total - 1) / SIMPLE_BUILD_PAGE_SIZE * SIMPLE_BUILD_PAGE_SIZE
      };
      let builds = circus_common::repo::builds::list_filtered_with_visibility(
        &state.pool,
        None,
        Some(&status),
        None,
        None,
        SIMPLE_BUILD_PAGE_SIZE,
        offset,
        viewer.is_admin,
      )
      .await
      .unwrap_or_default()
      .iter()
      .map(BuildView::from)
      .collect::<Vec<_>>();
      let token = emit! {
        simple_build_results_view(
          status: &status,
          offset: offset,
          total: total,
          builds: &builds,
        )
      }?;
      tokio::select! {
        wake = changed.next(cx, DashboardPage::Home, false) => {
          match wake? {
            None => break Ok(token),
            Some(Wake::Changed(_current)) => {},
            Some(Wake::Tick) => {},
          }
        },
        () = tokio::time::sleep(std::time::Duration::from_secs(15)) => {
          if !topcoat::runtime::connected(cx) {
            break Ok(token);
          }
        },
      }
    }
  })
}

#[component]
async fn simple_build_results_view(
  status: &str,
  offset: i64,
  total: i64,
  builds: &[BuildView],
) -> Result<impl View> {
  let (heading, status_label) = match status {
    "running" => ("Running builds", "running"),
    "pending" => ("Queued builds", "queued"),
    "failed" => ("Failed builds", "failed"),
    _ => return Err(bad_request("unsupported compact build status").into()),
  };
  let page_end = offset + builds.len() as i64;

  Ok(view! {
    <section class="simple-section simple-build-results" aria-labelledby="simple-build-results-heading">
      <div class="simple-section-heading">
        <h1 id="simple-build-results-heading">(heading)</h1>
        <a href="/dash">"Back to dashboard"</a>
      </div>
      if total > 0 {
        <p class="simple-results-summary">
          "Showing " (offset + 1) "–" (page_end) " of " (total) " " (status_label) " builds"
        </p>
      }
      if total == 0 {
        <p class="simple-empty">"No matching builds right now."</p>
      } else {
        <ul class="simple-build-list">
          for build in builds {
            simple_build_row(details: build)
          }
        </ul>
      }
      if offset > 0 || page_end < total {
        <nav class="simple-results-pagination" aria-label="Build result pages">
          if offset > 0 {
            <a href=(format!("/dash?status={status}&offset={}", (offset - SIMPLE_BUILD_PAGE_SIZE).max(0)))>"Previous"</a>
          }
          if page_end < total {
            <a href=(format!("/dash?status={status}&offset={page_end}"))>"Next"</a>
          }
        </nav>
      }
    </section>
  })
}

#[page("/dash/project/{simple_project_id}")]
pub(in crate::routes::dashboard) async fn simple_project_page(
  cx: &Cx,
) -> Result<impl View> {
  let viewer = viewer(cx, DashboardPage::Home).await?;
  let state = app_context::<AppState>(cx);
  let project_id = *path_param::<SimpleProjectId>(cx)?;
  let project = circus_common::repo::projects::get(&state.pool, project_id)
    .await
    .map_err(|_| not_found())?;
  let jobsets =
    circus_common::repo::jobsets::list_all_for_project(&state.pool, project_id)
      .await?;
  let client = state
    .pool
    .get()
    .await
    .map_err(circus_common::CiError::from)?;
  let mut jobset_views = Vec::with_capacity(jobsets.len());
  for jobset in jobsets {
    // Keep the branch's current state visible while a new evaluation has
    // no builds. Use the most recent branch evaluation with real jobs for
    // the log list, including superseded evaluations as history.
    let summary = client
      .query_one(
        "SELECT current.commit_hash, current.status, recent.id, \
         recent.commit_hash
       FROM (VALUES ($1::uuid)) AS requested(jobset_id)
       LEFT JOIN LATERAL (
         SELECT e.commit_hash, e.status FROM evaluations e
         WHERE e.jobset_id = requested.jobset_id AND e.pr_number IS NULL
           AND ($2::boolean OR NOT e.hidden)
         ORDER BY e.evaluation_time DESC, e.id DESC LIMIT 1
       ) AS current ON true
       LEFT JOIN LATERAL (
         SELECT e.id, e.commit_hash FROM evaluations e
         WHERE e.jobset_id = requested.jobset_id AND e.pr_number IS NULL
           AND ($2::boolean OR NOT e.hidden)
           AND EXISTS (
             SELECT 1 FROM builds b WHERE b.evaluation_id = e.id
               AND b.job_name NOT LIKE 'drv:%'
           )
         ORDER BY e.evaluation_time DESC, e.id DESC LIMIT 1
       ) AS recent ON true",
        &[&jobset.id, &viewer.is_admin],
      )
      .await
      .map_err(circus_common::CiError::from)?;

    let latest = summary.get::<_, Option<String>>(0).map(|commit| {
      let status: Option<String> = summary.get(1);
      let (status_text, status_class) = status
        .as_deref()
        .and_then(|status| {
          status
            .parse::<circus_common::models::EvaluationStatus>()
            .ok()
        })
        .map_or(("Unknown", "unknown"), |status| status.badge());
      ProjectEvaluationView {
        commit,
        status_text,
        status_class,
      }
    });
    let mut jobs = Vec::new();
    let mut build_count = 0;
    if let Some(evaluation) = summary.get::<_, Option<Uuid>>(2) {
      let rows = client
        .query(
          "SELECT id, job_name, system, status, count(*) OVER () FROM builds
         WHERE evaluation_id = $1 AND job_name NOT LIKE 'drv:%'
         ORDER BY CASE status WHEN 'running' THEN 0 WHEN 'pending' THEN 2
           WHEN 'succeeded' THEN 3 WHEN 'cancelled' THEN 4 ELSE 1 END,
           created_at DESC, id DESC LIMIT 32",
          &[&evaluation],
        )
        .await
        .map_err(circus_common::CiError::from)?;
      for row in rows {
        let status: String = row.get(3);
        let (status_text, status_class) = status
          .parse::<circus_common::models::BuildStatus>()
          .map_or(("Unknown", "unknown"), |status| status.badge());
        build_count = row.get(4);
        jobs.push(ProjectJobView {
          id: row.get(0),
          name: row.get(1),
          system: row
            .get::<_, Option<String>>(2)
            .unwrap_or_else(|| "unknown".to_owned()),
          status_text,
          status_class,
        });
      }
    }
    jobset_views.push(ProjectJobsetView {
      name: jobset.name,
      branch: jobset.branch.unwrap_or_else(|| "default branch".to_owned()),
      latest,
      builds_commit: summary.get(3),
      build_count,
      jobs,
    });
  }

  Ok(view! {
    document(title: &project.name, viewer: &viewer, topbar: false,
      <section class="simple-dashboard">
        <header class="simple-header">
          <a class="simple-wordmark" href="/dash" aria-label="Build dashboard home">
            <img src="/static/favicon.svg" alt="">
          </a>
          <a href="/dash">"Dashboard"</a>
        </header>
        <main class="simple-content">
          <h1 class="simple-project-title">(project.name.as_str())</h1>
          if let Some(description) = &project.description {
            <p class="simple-project-description">(description.as_str())</p>
          }
          <section class="simple-section" aria-labelledby="project-jobsets-heading">
            <div class="simple-section-heading">
              <h2 id="project-jobsets-heading">"Jobsets"</h2>
              <span class="simple-section-note">(jobset_views.len())</span>
            </div>
            if jobset_views.is_empty() {
              <p class="simple-empty">"No jobsets are configured."</p>
            } else {
              <ul class="simple-project-list">
                for jobset in &jobset_views {
                  <li>
                    <div class="simple-project-main">
                      <strong>(jobset.name.as_str())</strong>
                      if jobset.branch != jobset.name {
                        <span>(jobset.branch.as_str())</span>
                      }
                      if let Some(latest) = &jobset.latest {
                        <div class="simple-jobset-evaluation">
                          <span class=(format!("simple-status-label {}", latest.status_class))>(latest.status_text)</span>
                          <code>(short_commit(&latest.commit))</code>
                        </div>
                      }
                    </div>
                    if let Some(commit) = &jobset.builds_commit {
                      <div class="simple-jobset-build-heading">
                        if jobset.latest.as_ref().is_some_and(|latest| latest.commit != *commit) {
                          <strong>"Builds from the previous evaluation"</strong>
                        } else {
                          <strong>"Builds"</strong>
                        }
                        <code>(short_commit(commit))</code>
                        <span>(jobset.jobs.len()) " of " (jobset.build_count)</span>
                      </div>
                      <ul class="simple-project-jobs">
                        for job in &jobset.jobs {
                          <li>
                            <a class="simple-row-link simple-build-row" href=(format!("/build/{}/log", job.id))>
                              <span class="simple-build-main">
                                <strong title=(job.name.as_str())>(display_job_name(&job.name))</strong>
                                <span>(job.system.as_str())</span>
                              </span>
                              <span class=(format!("simple-status-label {}", job.status_class))>(job.status_text)</span>
                            </a>
                          </li>
                        }
                      </ul>
                    } else {
                      <p class="simple-jobset-empty">"No builds have been queued for this branch yet."</p>
                    }
                  </li>
                }
              </ul>
            }
          </section>
        </main>
      </section>
    )
  })
}

#[shard]
pub(in crate::routes::dashboard) async fn build_activity(
  cx: &Cx,
) -> Result<impl View> {
  let state = app_context::<AppState>(cx);
  let mut changed = BuildsChanged::subscribe(cx);
  let viewer = shard_viewer(cx, DashboardPage::Home).await?;
  let mut activity = load_builds(state, viewer.is_admin).await;

  Ok(live! {
    loop {
      let token = emit! { build_activity_view(activity: &activity) }?;
      if !topcoat::runtime::connected(cx) {
        break Ok(token);
      }
      tokio::select! {
        wake = changed.next(cx, DashboardPage::Home, false) => {
          match wake? {
            None => break Ok(token),
            Some(Wake::Changed(_current)) => {
              activity = load_builds(state, _current.is_admin).await;
            },
            Some(Wake::Tick) => {},
          }
        },
        () = tokio::time::sleep(std::time::Duration::from_secs(15)) => {
          if !topcoat::runtime::connected(cx) {
            break Ok(token);
          }
        },
      }
    }
  })
}

async fn load_builds(state: &AppState, include_hidden: bool) -> BuildActivity {
  let stats = circus_common::repo::builds::get_stats_with_visibility(
    &state.pool,
    include_hidden,
  )
  .await
  .unwrap_or_default();
  let builds = circus_common::repo::builds::list_filtered_with_visibility(
    &state.pool,
    None,
    None,
    None,
    None,
    40,
    0,
    include_hidden,
  )
  .await
  .unwrap_or_default()
  .into_iter()
  .take(8)
  .map(|build| BuildView::from(&build))
  .collect();
  BuildActivity {
    builds,
    running: stats.running_builds.unwrap_or_default(),
    queued: stats.pending_builds.unwrap_or_default(),
    failed: stats.failed_builds.unwrap_or_default(),
  }
}

#[shard]
pub(in crate::routes::dashboard) async fn repositories_activity(
  cx: &Cx,
) -> Result<impl View> {
  let viewer = shard_viewer(cx, DashboardPage::Home).await?;
  let state = app_context::<AppState>(cx);
  let mut changed = BuildsChanged::subscribe(cx);
  let mut projects = load_projects(state, viewer.is_admin).await;
  Ok(live! {
    loop {
      let token = emit! { repositories_view(projects: &projects) }?;
      if !topcoat::runtime::connected(cx) {
        break Ok(token);
      }
      tokio::select! {
        wake = changed.next(cx, DashboardPage::Home, false) => {
          match wake? {
            None => break Ok(token),
            Some(Wake::Changed(current)) => {
              projects = load_projects(state, current.is_admin).await;
            },
            Some(Wake::Tick) => {},
          }
        },
        () = tokio::time::sleep(std::time::Duration::from_secs(15)) => {
          if !topcoat::runtime::connected(cx) {
            break Ok(token);
          }
        },
      }
    }
  })
}

async fn load_projects(
  state: &AppState,
  include_hidden: bool,
) -> Vec<SimpleProjectView> {
  let mut projects: Vec<_> =
    operator::projects_with_latest_status(state, include_hidden)
      .await
      .unwrap_or_default()
      .iter()
      .map(ProjectSummaryView::from)
      .collect();
  projects.sort_by_cached_key(|project| project.name.to_lowercase());

  let project_ids: Vec<_> = projects.iter().map(|project| project.id).collect();
  let histories = circus_common::repo::evaluations::project_history(
    &state.pool,
    &project_ids,
    include_hidden,
  )
  .await
  .unwrap_or_default();
  let mut histories_by_project: HashMap<Uuid, Vec<EvaluationHistoryPoint>> =
    HashMap::new();
  for evaluation in histories {
    let (status_text, status_class) = match evaluation.status.as_str() {
      "failed" => ("Failed", "failed"),
      "running" => ("Running", "running"),
      "pending" => ("Queued", "pending"),
      "succeeded" => ("Succeeded", "succeeded"),
      "no_builds" => ("Evaluated, no builds", "no-builds"),
      _ => ("Cancelled", "cancelled"),
    };
    histories_by_project
      .entry(evaluation.project_id)
      .or_default()
      .push(EvaluationHistoryPoint {
        status_text,
        status_class,
        duration_seconds: evaluation
          .duration_seconds
          .filter(|seconds| *seconds >= 0),
        height_percent: 0,
        finished: evaluation.finished,
      });
  }

  projects
    .into_iter()
    .map(|summary| {
      let mut history =
        histories_by_project.remove(&summary.id).unwrap_or_default();
      let mut durations: Vec<_> = history
        .iter()
        .filter_map(|point| point.duration_seconds)
        .collect();
      durations.sort_unstable();
      let median_duration = if durations.is_empty() {
        "—".to_owned()
      } else {
        let middle = durations.len() / 2;
        let left = durations[(durations.len() - 1) / 2];
        duration_label(left + (durations[middle] - left) / 2)
      };
      let passed = history
        .iter()
        .filter(|point| point.status_class == "succeeded")
        .count();
      let failed = history
        .iter()
        .filter(|point| point.status_class == "failed" && point.finished)
        .count();
      let decided = passed + failed;
      let pass_rate = if decided == 0 {
        "—".to_owned()
      } else {
        format!("{}%", (100 * passed + decided / 2) / decided)
      };
      let max_duration = durations.last().copied().unwrap_or(1).max(1);
      for point in &mut history {
        point.height_percent = point
          .duration_seconds
          .map_or(0, |seconds| seconds.saturating_mul(100) / max_duration);
      }
      let (status_text, status_class) =
        history.last().map_or(("No evaluation", ""), |point| {
          (point.status_text, point.status_class)
        });
      let status_icon = match status_class {
        "succeeded" => "✓",
        "failed" => "×",
        "running" => "↻",
        "pending" => "…",
        _ => "–",
      };
      SimpleProjectView {
        status_text,
        status_class,
        status_icon,
        summary,
        history,
        median_duration,
        pass_rate,
      }
    })
    .collect()
}

fn duration_label(seconds: i64) -> String {
  if seconds >= 3600 {
    format!("{}h {}m", seconds / 3600, seconds / 60 % 60)
  } else if seconds >= 60 {
    format!("{}m {}s", seconds / 60, seconds % 60)
  } else {
    format!("{seconds}s")
  }
}

#[component]
async fn repositories_view(
  projects: &[SimpleProjectView],
) -> Result<impl View> {
  Ok(view! {
    <section class="simple-section" aria-labelledby="repositories-heading">
      <div class="simple-section-heading">
        <h2 id="repositories-heading">"Repositories"</h2>
        <span class="simple-section-note">(projects.len()) " configured"</span>
      </div>
      if projects.is_empty() {
        <p class="simple-empty">"No repository information is available yet."</p>
      } else {
        <ul class="simple-project-list">
          for project in projects {
            <li><a class="simple-row-link simple-repo-row" href=(format!("/dash/project/{}", project.summary.id))>
              <span class=(format!("simple-project-icon {}", project.status_class)) role="img" aria-label=(project.status_text) title=(project.status_text)>(project.status_icon)</span>
              <div class="simple-project-main">
                <strong>(project.summary.name.as_str())</strong>
                <span>(project.summary.systems.as_str())</span>
                if !project.summary.last_eval_time.is_empty() {
                  <time>(project.summary.last_eval_time.as_str())</time>
                }
              </div>
              <div class="simple-history-column">
                if project.history.is_empty() {
                  <span class="simple-history-empty">"No history"</span>
                } else {
                  <ol class="simple-project-history" aria-label="Recent evaluations, oldest first">
                    for point in &project.history {
                      <li class=(format!("simple-history-point {}", point.status_class))
                          style=(format!("--history-height: {}%", point.height_percent))
                          aria-label=(point.label()) title=(point.label())></li>
                    }
                  </ol>
                }
              </div>
              <dl class="simple-project-stats">
                <div title="Median elapsed time of recent evaluations with recorded durations">
                  <dt>"Median duration"</dt><dd>(project.median_duration.as_str())</dd>
                </div>
                <div title="Share of recently completed evaluations with a success or failure outcome that succeeded">
                  <dt>"Pass rate"</dt><dd>(project.pass_rate.as_str())</dd>
                </div>
              </dl>
            </a></li>
          }
        </ul>
      }
    </section>
  })
}

#[shard]
pub(in crate::routes::dashboard) async fn workers_activity(
  cx: &Cx,
) -> Result<impl View> {
  let viewer = shard_viewer(cx, DashboardPage::Home).await?;
  let state = app_context::<AppState>(cx);
  let mut workers = load_workers(state, viewer.is_admin).await?;
  Ok(live! {
    loop {
      let token = emit! { workers_view(workers: &workers) }?;
      if !topcoat::runtime::connected(cx) {
        break Ok(token);
      }
      tokio::time::sleep(std::time::Duration::from_secs(15)).await;
      if !topcoat::runtime::connected(cx) {
        break Ok(token);
      }
      let viewer = shard_viewer(cx, DashboardPage::Home).await?;
      workers = load_workers(state, viewer.is_admin).await?;
    }
  })
}

#[component]
async fn workers_view(workers: &[WorkerView]) -> Result<impl View> {
  Ok(view! {
    <section class="simple-section" aria-labelledby="workers-heading">
      <div class="simple-section-heading">
        <h2 id="workers-heading">"Workers"</h2>
        <span class="simple-section-note">
          (workers.iter().filter(|worker| worker.status_class != "skipped").count()) " online"
        </span>
      </div>
      if workers.is_empty() {
        <p class="simple-empty">"No workers have registered."</p>
      } else {
        <ul class="simple-worker-list">
          for worker in workers.iter().filter(|worker| worker.status_class != "skipped") {
            worker_row(worker: worker)
          }
        </ul>
        if workers.iter().any(|worker| worker.status_class == "skipped") {
          <details class="simple-offline-workers">
            <summary>
              "Offline workers ("
              (workers.iter().filter(|worker| worker.status_class == "skipped").count())
              ")"
            </summary>
            <ul class="simple-worker-list">
              for worker in workers.iter().filter(|worker| worker.status_class == "skipped") {
                worker_row(worker: worker)
              }
            </ul>
          </details>
        }
      }
    </section>
  })
}

#[component]
async fn worker_row(worker: &WorkerView) -> Result<impl View> {
  Ok(view! {
    <li><details class="simple-worker-detail">
      <summary class="simple-row-link simple-worker-row">
        <span class="simple-worker-main">
          <strong>(worker.name.as_str())</strong>
          <span>(worker.systems.as_str())</span>
        </span>
        <span class="simple-worker-jobs">(worker.current_jobs) "/" (worker.max_jobs) " jobs"</span>
        <span class=(format!("simple-status-label {}", worker.status_class))>(worker.status_text)</span>
      </summary>
      <dl class="simple-detail-facts">
        <div><dt>"CPU cores"</dt><dd>(worker.cpu_count)</dd></div>
        <div><dt>"Speed factor"</dt><dd>(worker.speed_factor)</dd></div>
        <div><dt>"Last seen"</dt><dd>(worker.last_seen.as_str())</dd></div>
      </dl>
      if !worker.builds.is_empty() {
        <ul class="simple-project-jobs" aria-label="Running builds">
          for build in &worker.builds {
            <li>
              <a class="simple-row-link simple-build-row" href=(format!("/build/{}/log", build.id))>
                <span class="simple-build-main">
                  <strong title=(build.name.as_str())>(display_job_name(&build.name))</strong>
                  <span>(build.project.as_str())</span>
                </span>
                <span class="simple-status-label running">"Running"</span>
              </a>
            </li>
          }
        </ul>
      }
    </details></li>
  })
}

async fn load_workers(
  state: &AppState,
  include_hidden: bool,
) -> Result<Vec<WorkerView>> {
  let workers =
    circus_common::repo::builder_sessions::list(&state.pool).await?;
  let client = state
    .pool
    .get()
    .await
    .map_err(circus_common::CiError::from)?;
  let rows = client
    .query(
      "SELECT b.agent_machine_id, b.id, b.job_name, p.name
     FROM builds b
     JOIN evaluations e ON e.id = b.evaluation_id
     JOIN jobsets j ON j.id = e.jobset_id
     JOIN projects p ON p.id = j.project_id
     WHERE b.status = 'running' AND b.agent_machine_id IS NOT NULL
       AND ($1::boolean OR NOT e.hidden)
     ORDER BY b.started_at DESC NULLS LAST, b.id",
      &[&include_hidden],
    )
    .await
    .map_err(circus_common::CiError::from)?;
  let mut builds: HashMap<Uuid, Vec<WorkerBuildView>> = HashMap::new();
  for row in rows {
    builds.entry(row.get(0)).or_default().push(WorkerBuildView {
      id:      row.get(1),
      name:    row.get(2),
      project: row.get(3),
    });
  }
  Ok(
    workers
      .into_iter()
      .map(|worker| {
        WorkerView {
          builds:       if worker.connected {
            builds.remove(&worker.machine_id).unwrap_or_default()
          } else {
            Vec::new()
          },
          name:         worker.name,
          systems:      if worker.systems.is_empty() {
            "unknown".to_owned()
          } else {
            worker.systems.join(", ")
          },
          status_text:  if !worker.connected {
            "Offline"
          } else if worker.current_jobs > 0 {
            "Busy"
          } else {
            "Idle"
          },
          status_class: if !worker.connected {
            "skipped"
          } else if worker.current_jobs > 0 {
            "running"
          } else {
            "succeeded"
          },
          current_jobs: worker.current_jobs,
          max_jobs:     worker.max_jobs,
          cpu_count:    worker.cpu_count,
          speed_factor: worker.speed_factor,
          last_seen:    worker.last_seen.map_or_else(
            || "Unknown".to_owned(),
            |time| relative_time(time.as_second()),
          ),
        }
      })
      .collect(),
  )
}

#[component]
async fn build_activity_view(activity: &BuildActivity) -> Result<impl View> {
  Ok(view! {
    <section class="simple-section" aria-labelledby="builds-heading">
      <div class="simple-section-heading">
        <h2 id="builds-heading">"Build activity"</h2>
      </div>
      <div class="simple-counts" aria-label="Recent build counts">
        <a class="simple-count-card running" href="/dash?status=running" aria-label=(format!("{} running builds", activity.running))>
          <strong>(activity.running)</strong><span>"Running"</span>
        </a>
        <a class="simple-count-card queued" href="/dash?status=pending" aria-label=(format!("{} queued builds", activity.queued))>
          <strong>(activity.queued)</strong><span>"Queued"</span>
        </a>
        <a class="simple-count-card failed" href="/dash?status=failed" aria-label=(format!("{} failed builds", activity.failed))>
          <strong>(activity.failed)</strong><span>"Failed"</span>
        </a>
      </div>
      if activity.builds.is_empty() {
        <p class="simple-empty">"No builds yet. New builds will appear here."</p>
      } else {
        <ul class="simple-build-list">
          for build in &activity.builds {
            simple_build_row(details: build)
          }
        </ul>
      }
    </section>
  })
}

#[component]
async fn simple_build_row(details: &BuildView) -> Result<impl View> {
  Ok(view! {
    <li><a class="simple-row-link simple-build-row" href=(format!("/build/{}/log", details.id))>
      <div class="simple-build-main">
        <strong title=(details.job_name.as_str())>(display_job_name(&details.job_name))</strong>
        <span>
          if !details.project_name.is_empty() {
            (details.project_name.as_str()) " / "
          }
          (details.system.as_str())
          if details.is_snapshot {
            <span class="snapshot-badge">"Ad hoc"</span>
          }
        </span>
      </div>
      <span class=(format!("simple-status-label {}", details.status_class))>(details.status_text.as_str())</span>
    </a></li>
  })
}

fn display_job_name(job: &str) -> &str {
  job
    .strip_prefix("drv:")
    .and_then(|job| job.split_once('-'))
    .filter(|(hash, name)| hash.len() == 32 && !name.is_empty())
    .map_or(job, |(_, name)| name)
}

#[shard]
pub(in crate::routes::dashboard) async fn effects_activity(
  cx: &Cx,
) -> Result<impl View> {
  shard_viewer(cx, DashboardPage::Home).await?;
  Ok(live! {
    loop {
      let effects = load_effects();
      let token = emit! { effects_activity_view(effects: &effects) }?;
      if !topcoat::runtime::connected(cx) {
        break Ok(token);
      }
      tokio::time::sleep(std::time::Duration::from_secs(15)).await;
    }
  })
}

fn load_effects() -> Vec<EffectSnapshot> {
  let Ok(entries) = fs::read_dir(EFFECT_STATUS_DIR) else {
    return Vec::new();
  };

  let mut snapshots: Vec<_> = entries
    .filter_map(|entry| entry.ok())
    .filter_map(|entry| {
      if !entry.file_type().ok()?.is_file()
        || entry.metadata().ok()?.len() > 65_536
      {
        return None;
      }
      let content = fs::read_to_string(entry.path()).ok()?;
      serde_json::from_str::<EffectSnapshot>(&content).ok()
    })
    .filter_map(|mut snapshot| {
      if !valid_commit(&snapshot.commit) {
        return None;
      }
      snapshot.effects.retain(|effect| effect.name.len() <= 120);
      for effect in &mut snapshot.effects {
        effect.message = effect.message.take().map(|message| {
          message
            .chars()
            .filter(|character| !character.is_control())
            .take(512)
            .collect()
        });
      }
      snapshot.effects.truncate(32);
      Some(snapshot)
    })
    .collect();
  snapshots.sort_by_key(|snapshot| std::cmp::Reverse(snapshot.updated_at));
  snapshots.truncate(3);
  snapshots
}

fn valid_commit(commit: &str) -> bool {
  matches!(commit.len(), 40 | 64)
    && commit
      .bytes()
      .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[component]
async fn effects_activity_view(
  effects: &[EffectSnapshot],
) -> Result<impl View> {
  Ok(view! {
    <section class="simple-section" aria-labelledby="effects-heading">
      <div class="simple-section-heading">
        <h2 id="effects-heading">"Publishing effects"</h2>
      </div>
      if effects.is_empty() {
        <p class="simple-empty">"No publishing activity yet."</p>
      } else {
        <ul class="simple-effect-list">
          for snapshot in effects {
            <li class="simple-effect-group">
              <div class="simple-effect-commit">
                <span class="simple-commit-label">"Commit"</span>
                <code>(short_commit(&snapshot.commit))</code>
                <time>(relative_time(snapshot.updated_at))</time>
              </div>
              for effect in &snapshot.effects {
                <details class="simple-effect-detail">
                  <summary class="simple-effect-row">
                    <span class=(format!("simple-effect-state {}", effect_state_class(&effect.state))) aria-hidden="true"></span>
                    <span>(effect.name.as_str())</span>
                    <span class=(format!("simple-effect-state-label {}", effect_state_class(&effect.state)))>
                      (effect_label(&effect.state))
                    </span>
                  </summary>
                  <div class="simple-effect-detail-body">
                    <span>"Commit " <code>(short_commit(&snapshot.commit))</code></span>
                    <time>(relative_time(snapshot.updated_at))</time>
                    <p>(effect.message.as_deref().unwrap_or_else(|| effect_description(&effect.state)))</p>
                  </div>
                </details>
              }
            </li>
          }
        </ul>
      }
    </section>
  })
}

fn short_commit(commit: &str) -> &str {
  commit.get(..12).unwrap_or(commit)
}

fn relative_time(timestamp: i64) -> String {
  let seconds = (jiff::Timestamp::now().as_second() - timestamp).max(0);
  if seconds < 60 {
    format!("{seconds}s ago")
  } else if seconds < 3600 {
    format!("{}m ago", seconds / 60)
  } else {
    format!("{}h ago", seconds / 3600)
  }
}

fn effect_label(state: &str) -> &str {
  match state {
    "pending" => "Waiting",
    "running" => "Running",
    "dry_run" => "Dry run",
    "success" => "Published",
    "failed" => "Failed",
    "skipped" => "Skipped",
    _ => "Unknown",
  }
}

fn effect_description(state: &str) -> &'static str {
  match state {
    "pending" => "Waiting for its publish conditions.",
    "running" => "Publishing is in progress.",
    "dry_run" => "Dry run finished without publishing.",
    "success" => "Published successfully.",
    "failed" => "Publishing failed.",
    "skipped" => "Publishing was skipped.",
    _ => "No additional details are available.",
  }
}

fn effect_state_class(state: &str) -> &str {
  match state {
    "running" | "success" | "failed" => state,
    "pending" | "dry_run" | "skipped" => "pending",
    _ => "unknown",
  }
}
