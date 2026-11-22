//! The delivery-facing snapshot of a build event.
//!
//! [`BuildEvent`] is built once from a [`Build`] and [`Project`] and carries
//! everything a notification channel needs to render and deliver a message. It
//! is also the unit serialized into the retry-queue task payload, so it owns
//! the status-to-forge mappings that delivery relies on.

use circus_common::models::{Build, BuildStatus, EvaluationStatus, Project};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A point-in-time view of a build, decoupled from the live database rows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildEvent {
  pub build_id:     Uuid,
  pub status:       BuildStatus,
  pub job_name:     String,
  pub drv_path:     String,
  pub build_output: Option<String>,
  pub project_name: String,
  /// The project's repository URL (used to derive forge owner/repo/project).
  pub project_url:  String,
  pub commit_hash:  String,
}

impl BuildEvent {
  /// Capture a build event from the live build and project rows.
  #[must_use]
  pub fn from_build(
    build: &Build,
    project: &Project,
    commit_hash: &str,
  ) -> Self {
    Self {
      build_id:     build.id,
      status:       build.status,
      job_name:     build.job_name.clone(),
      drv_path:     build.drv_path.clone(),
      build_output: build.build_output_path.clone(),
      project_name: project.name.clone(),
      project_url:  project.repository_url.clone(),
      commit_hash:  commit_hash.to_string(),
    }
  }

  /// Whether this event represents a failed build (anything that is not a
  /// success). Used by channels honoring `on_failure_only`.
  #[must_use]
  pub const fn is_failure(&self) -> bool {
    !self.status.is_success()
  }

  /// Whether this event is an intermediate dependency build synthesized from
  /// the derivation graph rather than a top-level jobset job. Such builds are
  /// excluded from notification dispatch to avoid polluting commit statuses.
  #[must_use]
  pub fn is_dependency(&self) -> bool {
    self
      .job_name
      .starts_with(circus_common::models::DEPENDENCY_JOB_PREFIX)
  }

  /// Coarse status string for generic webhook payloads.
  #[must_use]
  pub const fn generic_status(&self) -> &'static str {
    match self.status {
      BuildStatus::Succeeded => "success",
      BuildStatus::Failed
      | BuildStatus::CachedFailure
      | BuildStatus::DependencyFailed
      | BuildStatus::FailedWithOutput
      | BuildStatus::Timeout
      | BuildStatus::LogLimitExceeded
      | BuildStatus::NarSizeLimitExceeded
      | BuildStatus::NonDeterministic
      | BuildStatus::OomKilled => "failure",
      BuildStatus::Cancelled => "cancelled",
      BuildStatus::Aborted => "aborted",
      BuildStatus::UnsupportedSystem => "skipped",
      BuildStatus::Pending | BuildStatus::Running => "pending",
    }
  }

  /// GitHub/Gitea commit-status `(state, description)` mapping.
  #[must_use]
  pub const fn github_state(&self) -> (&'static str, &'static str) {
    match self.status {
      BuildStatus::Succeeded => ("success", "Build succeeded"),
      BuildStatus::Failed
      | BuildStatus::DependencyFailed
      | BuildStatus::FailedWithOutput
      | BuildStatus::NonDeterministic => ("failure", "Build failed"),
      BuildStatus::CachedFailure => ("failure", "Build failed previously"),
      BuildStatus::Running => ("pending", "Build in progress"),
      BuildStatus::Pending => ("pending", "Build queued"),
      BuildStatus::Cancelled => ("error", "Build cancelled"),
      BuildStatus::Aborted => ("error", "Build aborted"),
      BuildStatus::Timeout => ("error", "Build timed out"),
      BuildStatus::UnsupportedSystem => ("error", "Unsupported system"),
      BuildStatus::LogLimitExceeded => ("error", "Log limit exceeded"),
      BuildStatus::NarSizeLimitExceeded => ("error", "NAR size limit exceeded"),
      BuildStatus::OomKilled => ("failure", "Build failed (OOM)"),
    }
  }

  /// GitLab commit-status `(state, description)` mapping (distinct state
  /// vocabulary from GitHub/Gitea).
  #[must_use]
  pub const fn gitlab_state(&self) -> (&'static str, &'static str) {
    match self.status {
      BuildStatus::Succeeded => ("success", "Build succeeded"),
      BuildStatus::Failed
      | BuildStatus::DependencyFailed
      | BuildStatus::FailedWithOutput
      | BuildStatus::NonDeterministic => ("failed", "Build failed"),
      BuildStatus::CachedFailure => ("failed", "Build failed previously"),
      BuildStatus::Running => ("running", "Build in progress"),
      BuildStatus::Pending => ("pending", "Build queued"),
      BuildStatus::Cancelled => ("canceled", "Build cancelled"),
      BuildStatus::Aborted => ("canceled", "Build aborted"),
      BuildStatus::Timeout => ("failed", "Build timed out"),
      BuildStatus::UnsupportedSystem => ("skipped", "Unsupported system"),
      BuildStatus::LogLimitExceeded => ("failed", "Log limit exceeded"),
      BuildStatus::NarSizeLimitExceeded => {
        ("failed", "NAR size limit exceeded")
      },
      BuildStatus::OomKilled => ("failed", "Build failed (OOM)"),
    }
  }

  /// Human-facing status label for email subjects/bodies.
  #[must_use]
  pub const fn email_status(&self) -> &'static str {
    match self.status {
      BuildStatus::Succeeded => "SUCCESS",
      BuildStatus::Failed
      | BuildStatus::CachedFailure
      | BuildStatus::DependencyFailed
      | BuildStatus::FailedWithOutput
      | BuildStatus::Timeout
      | BuildStatus::LogLimitExceeded
      | BuildStatus::NarSizeLimitExceeded
      | BuildStatus::NonDeterministic
      | BuildStatus::OomKilled => "FAILURE",
      BuildStatus::Cancelled => "CANCELLED",
      BuildStatus::Aborted => "ABORTED",
      BuildStatus::UnsupportedSystem => "UNSUPPORTED",
      BuildStatus::Pending | BuildStatus::Running => "PENDING",
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn event_with_job(job_name: &str) -> BuildEvent {
    BuildEvent {
      build_id:     Uuid::nil(),
      status:       BuildStatus::Succeeded,
      job_name:     job_name.to_string(),
      drv_path:     "/nix/store/x.drv".to_string(),
      build_output: None,
      project_name: "proj".to_string(),
      project_url:  "https://github.com/owner/repo".to_string(),
      commit_hash:  "abc".to_string(),
    }
  }

  #[test]
  fn top_level_job_is_not_a_dependency() {
    assert!(!event_with_job("hello").is_dependency());
  }

  #[test]
  fn drv_prefixed_job_is_a_dependency() {
    assert!(event_with_job("drv:abc-foo-1.0").is_dependency());
  }
}

/// How an evaluation's top-level jobs ended, counted by outcome.
#[derive(
  Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
pub struct JobSummary {
  pub total:     i64,
  pub succeeded: i64,
  pub failed:    i64,
  pub cancelled: i64,
  pub skipped:   i64,
  pub pending:   i64,
  pub running:   i64,
}

impl JobSummary {
  /// Summarize per-status job counts. Returns `None` while any job is still
  /// pending or running, i.e. while the evaluation is not finished yet.
  #[must_use]
  pub fn from_counts(counts: &[(BuildStatus, i64)]) -> Option<Self> {
    let mut summary = Self::default();
    for &(status, count) in counts {
      if !status.is_finished() {
        return None;
      }
      summary.add(status, count);
    }
    Some(summary)
  }

  /// Summarize all top-level jobs, including jobs still pending or running.
  #[must_use]
  pub fn from_counts_including_pending(counts: &[(BuildStatus, i64)]) -> Self {
    let mut summary = Self::default();
    for &(status, count) in counts {
      summary.add(status, count);
    }
    summary
  }

  const fn add(&mut self, status: BuildStatus, count: i64) {
    self.total += count;
    match status {
      BuildStatus::Succeeded => self.succeeded += count,
      BuildStatus::Cancelled | BuildStatus::Aborted => {
        self.cancelled += count;
      },
      BuildStatus::UnsupportedSystem => self.skipped += count,
      BuildStatus::Pending => self.pending += count,
      BuildStatus::Running => self.running += count,
      _ => self.failed += count,
    }
  }

  /// Coarse outcome of the whole evaluation: `success` only when every job
  /// succeeded, `cancelled` when jobs were cancelled but none failed.
  #[must_use]
  pub const fn status(&self) -> &'static str {
    if self.failed > 0 {
      "failure"
    } else if self.pending > 0 || self.running > 0 {
      "pending"
    } else if self.cancelled > 0 {
      "cancelled"
    } else if self.skipped > 0 {
      "partial"
    } else {
      "success"
    }
  }
}

/// An evaluation snapshot, including jobs that may still be pending or
/// running. Generic webhooks receive only final snapshots; PR comments also
/// report evaluation lifecycle changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationEvent {
  pub evaluation_id:     Uuid,
  pub project_name:      String,
  pub project_url:       String,
  pub jobset_name:       String,
  pub commit_hash:       String,
  pub evaluation_status: EvaluationStatus,
  #[serde(default)]
  pub evaluation_error:  Option<String>,
  /// The branch the evaluation is for: the pull request's head branch, else
  /// the jobset's branch.
  pub branch:            Option<String>,
  pub pr_number:         Option<i32>,
  pub source_scope:      Option<String>,
  pub superseded_by:     Option<Uuid>,
  pub jobs:              JobSummary,
}

impl EvaluationEvent {
  /// A fail-closed GitHub release status for the complete default-branch
  /// jobset.
  #[must_use]
  pub fn github_jobset_status(&self) -> Option<&'static str> {
    if self.pr_number.is_some() || self.superseded_by.is_some() {
      return None;
    }
    match self.evaluation_status {
      EvaluationStatus::Running if self.jobs.total == 0 => Some("pending"),
      EvaluationStatus::Pending | EvaluationStatus::Running => None,
      EvaluationStatus::Completed if self.evaluation_error.is_some() => {
        Some("failure")
      },
      EvaluationStatus::Completed
        if self.jobs.pending > 0 || self.jobs.running > 0 =>
      {
        None
      },
      EvaluationStatus::Completed
        if self.jobs.total > 0 && self.jobs.status() == "success" =>
      {
        Some("success")
      },
      EvaluationStatus::Completed
      | EvaluationStatus::Failed
      | EvaluationStatus::Cancelled
      | EvaluationStatus::TimedOut => Some("failure"),
    }
  }

  /// The JSON body of the generic webhook for this event.
  #[must_use]
  pub fn webhook_payload(&self) -> serde_json::Value {
    serde_json::json!({
      "event":         "evaluation_finished",
      "evaluation_id": self.evaluation_id,
      "status":        self.jobs.status(),
      "jobs":          self.jobs,
      "project_name":  self.project_name,
      "project_url":   self.project_url,
      "jobset_name":   self.jobset_name,
      "commit_hash":   self.commit_hash,
      "branch":        self.branch,
      "pr_number":     self.pr_number,
      "source_scope":  self.source_scope,
      "superseded_by": self.superseded_by,
    })
  }
}

#[cfg(test)]
mod evaluation_tests {
  use super::*;

  #[test]
  fn unfinished_jobs_mean_no_summary() {
    let counts = [(BuildStatus::Succeeded, 3), (BuildStatus::Running, 1)];
    assert_eq!(JobSummary::from_counts(&counts), None);
    let counts = [(BuildStatus::Pending, 1)];
    assert_eq!(JobSummary::from_counts(&counts), None);
  }

  #[test]
  fn summary_counts_and_status() {
    let ok = JobSummary::from_counts(&[(BuildStatus::Succeeded, 5)])
      .unwrap_or_default();
    assert_eq!(ok.total, 5);
    assert_eq!(ok.status(), "success");

    let mixed = JobSummary::from_counts(&[
      (BuildStatus::Succeeded, 3),
      (BuildStatus::Failed, 1),
      (BuildStatus::Cancelled, 2),
      (BuildStatus::UnsupportedSystem, 1),
    ])
    .unwrap_or_default();
    assert_eq!(mixed, JobSummary {
      total:     7,
      succeeded: 3,
      failed:    1,
      cancelled: 2,
      skipped:   1,
      pending:   0,
      running:   0,
    });
    assert_eq!(mixed.status(), "failure");

    let cancelled = JobSummary::from_counts(&[
      (BuildStatus::Succeeded, 1),
      (BuildStatus::Aborted, 1),
    ])
    .unwrap_or_default();
    assert_eq!(cancelled.status(), "cancelled");

    let empty = JobSummary::from_counts(&[]).unwrap_or_default();
    assert_eq!(empty.status(), "success");
  }

  #[test]
  fn pending_arm_jobs_keep_evaluation_status_in_progress() {
    let summary = JobSummary::from_counts_including_pending(&[
      (BuildStatus::Succeeded, 3),
      (BuildStatus::Running, 1),
      (BuildStatus::Pending, 2),
    ]);
    assert_eq!(summary.total, 6);
    assert_eq!(summary.status(), "pending");
    assert_eq!(summary.succeeded, 3);
    assert_eq!(summary.running, 1);
    assert_eq!(summary.pending, 2);
  }

  #[test]
  fn release_status_never_succeeds_before_evaluation_and_jobs_complete() {
    let mut event = EvaluationEvent {
      evaluation_id:     Uuid::nil(),
      project_name:      "p".into(),
      project_url:       "https://github.com/o/r".into(),
      jobset_name:       "main-arm".into(),
      commit_hash:       "abc".into(),
      evaluation_status: EvaluationStatus::Running,
      evaluation_error:  None,
      branch:            Some("main".into()),
      pr_number:         None,
      source_scope:      None,
      superseded_by:     None,
      jobs:              JobSummary::default(),
    };
    assert_eq!(event.github_jobset_status(), Some("pending"));
    event.evaluation_status = EvaluationStatus::Completed;
    assert_eq!(event.github_jobset_status(), Some("failure"));
    event.jobs = JobSummary::from_counts_including_pending(&[
      (BuildStatus::Succeeded, 1),
      (BuildStatus::Running, 1),
    ]);
    assert_eq!(event.github_jobset_status(), None);
    event.jobs =
      JobSummary::from_counts_including_pending(&[(BuildStatus::Succeeded, 2)]);
    assert_eq!(event.github_jobset_status(), Some("success"));
    event.evaluation_error = Some("1 attribute failed".into());
    assert_eq!(event.github_jobset_status(), Some("failure"));
    event.evaluation_error = None;
    event.jobs =
      JobSummary::from_counts_including_pending(&[(BuildStatus::Failed, 1)]);
    assert_eq!(event.github_jobset_status(), Some("failure"));
    event.evaluation_status = EvaluationStatus::Failed;
    assert_eq!(event.github_jobset_status(), Some("failure"));
    event.pr_number = Some(12);
    assert_eq!(event.github_jobset_status(), None);
    event.pr_number = None;
    event.superseded_by = Some(Uuid::nil());
    assert_eq!(event.github_jobset_status(), None);
  }

  #[test]
  fn payload_is_tagged() {
    let event = EvaluationEvent {
      evaluation_id:     Uuid::nil(),
      project_name:      "p".into(),
      project_url:       "https://github.com/o/r".into(),
      jobset_name:       "main".into(),
      commit_hash:       "abc".into(),
      evaluation_status: EvaluationStatus::Completed,
      evaluation_error:  None,
      branch:            Some("main".into()),
      pr_number:         None,
      source_scope:      None,
      superseded_by:     None,
      jobs:              JobSummary {
        total: 1,
        succeeded: 1,
        ..JobSummary::default()
      },
    };
    let payload = event.webhook_payload();
    assert_eq!(payload["event"], "evaluation_finished");
    assert_eq!(payload["status"], "success");
    assert_eq!(payload["jobs"]["total"], 1);
    assert!(payload["pr_number"].is_null());
  }
}
