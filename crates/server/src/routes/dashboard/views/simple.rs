//! The compact, read-only build dashboard used by build.brkn.lol.

use std::fs;

use serde::Deserialize;
use topcoat::{
  Result,
  context::{Cx, app_context},
  router::{error::not_found, page, path_param},
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
  name:   String,
  branch: String,
  jobs:   Vec<ProjectJobView>,
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

struct WorkerView {
  name:         String,
  systems:      String,
  status_text:  &'static str,
  status_class: &'static str,
  current_jobs: i32,
  max_jobs:     i32,
}

#[page("/simple")]
pub(in crate::routes::dashboard) async fn simple_page(
  cx: &Cx,
) -> Result<impl View> {
  let viewer = viewer(cx, DashboardPage::Home).await?;
  Ok(view! {
    document(title: "Builds", viewer: &viewer, topbar: false,
      <section class="simple-dashboard">
        <header class="simple-header">
          <a class="simple-wordmark" href="/simple" aria-label="Build dashboard home">
            <img src="/static/favicon.svg" alt="">
          </a>
          <nav aria-label="Account">
            if viewer.auth_name.is_empty() {
              <a href="/login">"Sign in"</a>
            } else {
              <span>"Signed in as " (viewer.auth_name.as_str())</span>
            }
          </nav>
        </header>
        <main class="simple-content">
          <fieldset class="simple-tabs">
            <legend class="sr-only">"Dashboard sections"</legend>
            <input class="simple-tab-toggle" type="radio" name="dashboard-tab" id="tab-activity" checked="checked">
            <label for="tab-activity">"Activity"</label>
            <input class="simple-tab-toggle" type="radio" name="dashboard-tab" id="tab-repositories">
            <label for="tab-repositories">"Repositories"</label>
            <input class="simple-tab-toggle" type="radio" name="dashboard-tab" id="tab-workers">
            <label for="tab-workers">"Workers"</label>
            <input class="simple-tab-toggle" type="radio" name="dashboard-tab" id="tab-effects">
            <label for="tab-effects">"Effects"</label>
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
          <footer class="simple-footer">
            <span class="simple-live-dot" aria-hidden="true"></span>
            "Updates automatically"
          </footer>
        </main>
      </section>
    )
  })
}

#[page("/simple/project/{simple_project_id}")]
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
  let mut jobset_views = Vec::with_capacity(jobsets.len());
  for jobset in jobsets {
    let evaluations =
      circus_common::repo::evaluations::list_filtered_with_visibility(
        &state.pool,
        Some(jobset.id),
        None,
        1,
        0,
        viewer.is_admin,
      )
      .await?;
    let mut jobs = Vec::new();
    if let Some(evaluation) = evaluations.first() {
      for status in ["running", "pending"] {
        let builds = circus_common::repo::builds::list_filtered(
          &state.pool,
          Some(evaluation.id),
          Some(status),
          None,
          None,
          100,
          0,
        )
        .await?;
        jobs.extend(builds.into_iter().map(|build| {
          let (status_text, status_class) = build.status.badge();
          ProjectJobView {
            id: build.id,
            name: build.job_name,
            system: build.system.unwrap_or_else(|| "unknown".to_owned()),
            status_text,
            status_class: if status_class == "pending" {
              "queued"
            } else {
              status_class
            },
          }
        }));
      }
    }
    jobs.sort_by(|left, right| left.name.cmp(&right.name));
    jobset_views.push(ProjectJobsetView {
      name: jobset.name,
      branch: jobset.branch.unwrap_or_else(|| "default branch".to_owned()),
      jobs,
    });
  }

  Ok(view! {
    document(title: &project.name, viewer: &viewer, topbar: false,
      <section class="simple-dashboard">
        <header class="simple-header">
          <a class="simple-wordmark" href="/simple" aria-label="Build dashboard home">
            <img src="/static/favicon.svg" alt="">
          </a>
          <a href="/simple">"Dashboard"</a>
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
                      <span>(jobset.branch.as_str())</span>
                    </div>
                    if jobset.jobs.is_empty() {
                      <span class="simple-status-label">"No queued or running jobs"</span>
                    } else {
                      <ul class="simple-project-jobs">
                        for job in &jobset.jobs {
                          <li>
                            <span class=(format!("simple-status-label {}", job.status_class))>(job.status_text)</span>
                            <a href=(format!("/build/{}/log", job.id))>(job.name.as_str())</a>
                            <span>(job.system.as_str())</span>
                          </li>
                        }
                      </ul>
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
  shard_viewer(cx, DashboardPage::Home).await?;
  let state = app_context::<AppState>(cx);
  let mut changed = BuildsChanged::subscribe(cx);
  let mut activity = load_builds(state).await;

  Ok(live! {
    loop {
      let token = emit! { build_activity_view(activity: &activity) }?;
      tokio::select! {
        wake = changed.next(cx, DashboardPage::Home, false) => {
          match wake? {
            None => break Ok(token),
            Some(Wake::Changed(_current)) => {
              activity = load_builds(state).await;
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

async fn load_builds(state: &AppState) -> BuildActivity {
  let stats = circus_common::repo::builds::get_stats(&state.pool)
    .await
    .unwrap_or_default();
  let builds = operator::recent_builds(state)
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
) -> Vec<ProjectSummaryView> {
  operator::projects_with_latest_status(state, include_hidden)
    .await
    .unwrap_or_default()
    .iter()
    .map(ProjectSummaryView::from)
    .collect()
}

#[component]
async fn repositories_view(
  projects: &[ProjectSummaryView],
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
            <li>
              <div class="simple-project-main">
                <a href=(format!("/simple/project/{}", project.id))><strong>(project.name.as_str())</strong></a>
                <span>(project.jobset_count) " jobsets · " (project.systems.as_str())</span>
              </div>
              <div class="simple-project-health">
                if project.latest_running > 0 {
                  <span class="simple-status-label running">"Building " (project.latest_running)</span>
                }
                if project.latest_queued > 0 {
                  <span class="simple-status-label queued">"Queued " (project.latest_queued)</span>
                }
                if project.latest_succeeded > 0 {
                  <span class="simple-status-label succeeded">"Succeeded " (project.latest_succeeded)</span>
                }
                if project.latest_failed > 0 {
                  <span class="simple-status-label failed">"Failed " (project.latest_failed)</span>
                }
                <time>(project.last_eval_time.as_str())</time>
              </div>
            </li>
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
  shard_viewer(cx, DashboardPage::Home).await?;
  let state = app_context::<AppState>(cx);
  let mut workers = load_workers(state).await;
  Ok(live! {
    loop {
      let token = emit! { workers_view(workers: &workers) }?;
      tokio::time::sleep(std::time::Duration::from_secs(15)).await;
      if !topcoat::runtime::connected(cx) {
        break Ok(token);
      }
      workers = load_workers(state).await;
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
    <li>
      <span class=(format!("simple-status {}", worker.status_class)) aria-hidden="true"></span>
      <div class="simple-worker-main">
        <strong>(worker.name.as_str())</strong>
        <span>(worker.systems.as_str())</span>
      </div>
      <span class=(format!("simple-status-label {}", worker.status_class))>(worker.status_text)</span>
      <span class="simple-worker-jobs">(worker.current_jobs) "/" (worker.max_jobs) " jobs"</span>
    </li>
  })
}

async fn load_workers(state: &AppState) -> Vec<WorkerView> {
  circus_common::repo::builder_sessions::list(&state.pool)
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|worker| {
      WorkerView {
        name:         worker.name,
        systems:      if worker.systems.is_empty() {
          "unknown".to_owned()
        } else {
          worker.systems.join(", ")
        },
        status_text:  if !worker.connected {
          "offline"
        } else if worker.current_jobs > 0 {
          "busy"
        } else {
          "idle"
        },
        status_class: if !worker.connected {
          "skipped"
        } else {
          "running"
        },
        current_jobs: worker.current_jobs,
        max_jobs:     worker.max_jobs,
      }
    })
    .collect()
}

#[component]
async fn build_activity_view(activity: &BuildActivity) -> Result<impl View> {
  Ok(view! {
    <section class="simple-section" aria-labelledby="builds-heading">
      <div class="simple-section-heading">
        <h2 id="builds-heading">"Build activity"</h2>
      </div>
      <div class="simple-counts" aria-label="Recent build counts">
        <div><strong>(activity.running)</strong><span>"Running"</span></div>
        <div><strong>(activity.queued)</strong><span>"Queued"</span></div>
        <div><strong>(activity.failed)</strong><span>"Failed"</span></div>
      </div>
      if activity.builds.is_empty() {
        <p class="simple-empty">"No builds yet. New builds will appear here."</p>
      } else {
        <ul class="simple-build-list">
          for build in &activity.builds {
            <li>
              <span class=(format!("simple-status {}", build.status_class)) aria-hidden="true"></span>
              <div class="simple-build-main">
                if build.has_log || matches!(build.status_text.as_str(), "Running" | "Pending") {
                  <a href=(format!("/build/{}/log", build.id))><strong>(build.job_name.as_str())</strong></a>
                } else {
                  <strong>(build.job_name.as_str())</strong>
                }
                <span>(build.project_name.as_str()) " · " (build.system.as_str())</span>
              </div>
              <span class=(format!("simple-status-label {}", build.status_class))>(build.status_text.as_str())</span>
            </li>
          }
        </ul>
      }
    </section>
  })
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
        <span class="simple-section-note">"Automatic or manual runs"</span>
      </div>
      if effects.is_empty() {
        <p class="simple-empty">"No publishing activity yet."</p>
      } else {
        <ul class="simple-effect-list">
          for snapshot in effects {
            <li>
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
