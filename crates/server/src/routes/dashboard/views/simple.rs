//! The compact, read-only build dashboard used by build.brkn.lol.

use std::fs;

use serde::Deserialize;
use topcoat::{
  Result,
  context::{Cx, app_context},
  router::page,
  runtime::shard,
  view::{View, component, emit, live, view},
};

use super::super::{
  layout::{document, shard_viewer, viewer},
  live::{BuildsChanged, Wake},
  shared::{BuildView, DashboardPage},
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
  name:  String,
  state: String,
}

struct BuildActivity {
  builds:  Vec<BuildView>,
  running: i64,
  queued:  i64,
  failed:  i64,
}

#[page("/simple")]
async fn simple_page(cx: &Cx) -> Result<impl View> {
  let viewer = viewer(cx, DashboardPage::Home).await?;
  Ok(view! {
    document(title: "Builds", viewer: &viewer, topbar: false,
      <section class="simple-dashboard">
        <header class="simple-header">
          <a class="simple-wordmark" href="/simple">"build.brkn.lol"</a>
          <nav aria-label="Account">
            if viewer.auth_name.is_empty() {
              <a href="/login">"Sign in"</a>
            } else {
              <span>"Signed in as " (viewer.auth_name.as_str())</span>
            }
          </nav>
        </header>
        <main class="simple-content">
          <div class="simple-title">
            <h1>"NixOS builds"</h1>
            <p>"Live activity for the nixos-config repository."</p>
          </div>
          build_activity()
          effects_activity()
          <footer class="simple-footer">
            <span class="simple-live-dot" aria-hidden="true"></span>
            "Updates automatically"
          </footer>
        </main>
      </section>
    )
  })
}

#[shard]
async fn build_activity(cx: &Cx) -> Result<impl View> {
  let viewer = shard_viewer(cx, DashboardPage::Home).await?;
  let state = app_context::<AppState>(cx);
  let mut changed = BuildsChanged::subscribe(cx);
  let mut activity = load_builds(state, viewer.is_admin).await;

  Ok(live! {
    loop {
      let running = activity.running > 0;
      let token = emit! { build_activity_view(activity: &activity) }?;
      match changed.next(cx, DashboardPage::Home, running).await? {
        None => break Ok(token),
        Some(Wake::Changed(current)) => {
          activity = load_builds(state, current.is_admin).await;
        },
        Some(Wake::Tick) => {},
      }
    }
  })
}

async fn load_builds(state: &AppState, include_hidden: bool) -> BuildActivity {
  let overview = operator::overview(state, include_hidden).await.ok();
  let builds = overview
    .as_ref()
    .map(|overview| {
      overview
        .recent_builds
        .iter()
        .take(8)
        .map(BuildView::from)
        .collect()
    })
    .unwrap_or_default();
  BuildActivity {
    builds,
    running: overview
      .as_ref()
      .map_or(0, |overview| overview.running_builds),
    queued: overview
      .as_ref()
      .map_or(0, |overview| overview.pending_builds),
    failed: overview
      .as_ref()
      .map_or(0, |overview| overview.failed_builds),
  }
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
                <strong>(build.job_name.as_str())</strong>
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
async fn effects_activity(cx: &Cx) -> Result<impl View> {
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
        <span class="simple-section-note">"After a green main build"</span>
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
                <div class="simple-effect-row">
                  <span class=(format!("simple-effect-state {}", effect_state_class(&effect.state))) aria-hidden="true"></span>
                  <span>(effect.name.as_str())</span>
                  <span class=(format!("simple-effect-state-label {}", effect_state_class(&effect.state)))>
                    (effect_label(&effect.state))
                  </span>
                </div>
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

fn effect_state_class(state: &str) -> &str {
  match state {
    "running" | "success" | "failed" => state,
    "pending" | "dry_run" | "skipped" => "pending",
    _ => "unknown",
  }
}
