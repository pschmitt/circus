use std::time::Duration;

use circus_common::models::{BuildStatus, EvaluationStatus};
use circus_config::EmailConfig;
use circus_http::{RequestExt as _, ResponseExt as _, guard::resolve_public};
use circus_logs::ErrorChain;
use lettre::{
  AsyncSmtpTransport,
  AsyncTransport,
  Message,
  Tokio1Executor,
  message::{Mailbox, header::ContentType},
  transport::smtp::{
    authentication::Credentials,
    client::{Tls, TlsParameters},
  },
};
use reqwest::{StatusCode, Url};
use tracing::{info, warn};

use super::{
  GiteaStatusChannel,
  GithubStatusChannel,
  GitlabStatusChannel,
  Notifier,
  Origin,
  RESERVED_HEADERS,
  SIGNATURE_HEADER,
  SlackChannel,
  WebhookChannel,
  sign_body,
};
use crate::{
  BuildEvent,
  EvaluationEvent,
  parse_gitea_repo,
  parse_github_repo,
  parse_gitlab_project,
};

const PR_COMMENT_MARKER: &str = "<!-- circus-pr-status -->";
const PR_COMMENT_AUTHOR: &str = "circus-pschmitt[bot]";

#[derive(serde::Deserialize)]
struct PullRequestHead {
  head: PullRequestRef,
}

#[derive(serde::Deserialize)]
struct PullRequestRef {
  sha: String,
}

#[derive(serde::Deserialize)]
struct IssueComment {
  id:   u64,
  body: Option<String>,
  user: Option<CommentAuthor>,
}

#[derive(serde::Deserialize)]
struct CommentAuthor {
  login: String,
}

/// Upsert Circus's single status comment for a pull request. A transaction
/// advisory lock serializes deliveries across Circus processes sharing DB.
pub async fn upsert_github_pr_comment(
  pool: &circus_common::PgPool,
  token: &str,
  circus_base_url: &str,
  event: &EvaluationEvent,
) -> Result<(), String> {
  let number = event
    .pr_number
    .filter(|number| *number > 0)
    .ok_or_else(|| "Evaluation has no valid pull request number".to_owned())?;
  let (owner, repo) =
    parse_github_repo(&event.project_url).ok_or_else(|| {
      format!("Cannot parse GitHub repo from {}", event.project_url)
    })?;
  let lock_key = format!("{owner}/{repo}#{number}");
  let mut connection = pool.get().await.map_err(|error| {
    format!("Cannot acquire database connection for PR comment: {error}")
  })?;
  let transaction = connection
    .transaction()
    .await
    .map_err(|error| format!("Cannot start PR comment transaction: {error}"))?;
  transaction
    .batch_execute("SET LOCAL lock_timeout = '45s'")
    .await
    .map_err(|error| format!("Cannot set PR comment lock timeout: {error}"))?;
  transaction
    .query_one("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", &[
      &lock_key,
    ])
    .await
    .map_err(|error| format!("Cannot acquire PR comment lock: {error}"))?;

  let evaluation = transaction
    .query_one(
      "SELECT e.commit_hash, e.superseded_by, e.status, e.error_message, \
       j.nix_expression FROM evaluations e JOIN jobsets j ON j.id = \
       e.jobset_id WHERE e.id = $1",
      &[&event.evaluation_id],
    )
    .await
    .map_err(|error| {
      format!("Cannot refresh evaluation for PR comment: {error}")
    })?;
  let current_commit: String = evaluation.get("commit_hash");
  let superseded_by: Option<uuid::Uuid> = evaluation.get("superseded_by");
  let status: String = evaluation.get("status");
  let error: Option<String> = evaluation.get("error_message");
  let expression: String = evaluation.get("nix_expression");
  if current_commit != event.commit_hash
    || superseded_by.is_some()
    || status != event.evaluation_status.as_db_str()
    || event.evaluation_status == EvaluationStatus::Pending
  {
    let _ = transaction.rollback().await;
    return Ok(());
  }
  let rows = transaction
    .query(
      "SELECT status, COUNT(*) AS count FROM builds WHERE evaluation_id = $1 \
       AND job_name NOT LIKE 'drv:%' GROUP BY status",
      &[&event.evaluation_id],
    )
    .await
    .map_err(|error| format!("Cannot refresh PR build counts: {error}"))?;
  let counts = rows
    .into_iter()
    .map(|row| {
      let status: String = row.get("status");
      let status = status.parse::<BuildStatus>().map_err(|error| {
        format!("Cannot parse build status {status:?}: {error}")
      })?;
      let count: i64 = row.get("count");
      Ok((status, count))
    })
    .collect::<Result<Vec<_>, String>>()?;
  let mut latest_event = event.clone();
  latest_event.jobs = crate::JobSummary::from_counts_including_pending(&counts);

  let update = tokio::time::timeout(
    Duration::from_secs(40),
    upsert_github_pr_comment_locked(
      token,
      circus_base_url,
      &owner,
      &repo,
      number,
      &latest_event,
      error
        .as_deref()
        .and_then(|error| evaluation_error_summary(error, &expression)),
    ),
  )
  .await
  .map_err(|_| "Timed out while updating GitHub PR comment".to_owned())?;
  if let Err(error) = update {
    let _ = transaction.rollback().await;
    return Err(error);
  }
  transaction
    .commit()
    .await
    .map_err(|error| format!("Cannot release PR comment lock: {error}"))?;
  Ok(())
}

async fn upsert_github_pr_comment_locked(
  token: &str,
  circus_base_url: &str,
  owner: &str,
  repo: &str,
  number: i32,
  event: &EvaluationEvent,
  error_summary: Option<String>,
) -> Result<(), String> {
  let base_url = Url::parse(circus_base_url)
    .map_err(|error| format!("Invalid Circus base URL: {error}"))?;
  if base_url.scheme() != "https"
    || base_url.host_str().is_none()
    || base_url.query().is_some()
    || base_url.fragment().is_some()
  {
    return Err(
      "Circus base URL must be an HTTPS URL without query or fragment"
        .to_owned(),
    );
  }
  let detail_url = format!(
    "{}/evaluation/{}",
    circus_base_url.trim_end_matches('/'),
    event.evaluation_id
  );
  let http = crate::http_client(Origin::Instance);
  let api = format!("https://api.github.com/repos/{owner}/{repo}");
  let api_request = |url: String| {
    http
      .get(url)
      .bearer_auth(token)
      .header("Accept", "application/vnd.github+json")
      .header("X-GitHub-Api-Version", circus_http::GITHUB_API_VERSION)
  };
  let pull: PullRequestHead = api_request(format!("{api}/pulls/{number}"))
    .send_checked()
    .await
    .map_err(|error| {
      format!("Cannot read GitHub PR head: {}", ErrorChain(&error))
    })?
    .json_capped(circus_http::JSON_LIMIT)
    .await
    .map_err(|error| {
      format!("Cannot decode GitHub PR head: {}", ErrorChain(&error))
    })?;
  if !is_current_pr_head(&event.commit_hash, &pull.head.sha) {
    info!(
      pull_request = number,
      event_commit = %event.commit_hash,
      current_head = %pull.head.sha,
      "Skipping stale GitHub PR comment update"
    );
    return Ok(());
  }

  let comments: Vec<IssueComment> = api_request(format!(
    "{api}/issues/{number}/comments?per_page=100&sort=updated&direction=desc"
  ))
  .send_checked()
  .await
  .map_err(|error| {
    format!("Cannot list GitHub PR comments: {}", ErrorChain(&error))
  })?
  .json_capped(circus_http::JSON_LIMIT)
  .await
  .map_err(|error| {
    format!("Cannot decode GitHub PR comments: {}", ErrorChain(&error))
  })?;

  let owned_ids = owned_pr_comment_ids(&comments);
  let current_head: PullRequestHead =
    api_request(format!("{api}/pulls/{number}"))
      .send_checked()
      .await
      .map_err(|error| {
        format!("Cannot recheck GitHub PR head: {}", ErrorChain(&error))
      })?
      .json_capped(circus_http::JSON_LIMIT)
      .await
      .map_err(|error| {
        format!("Cannot decode GitHub PR head: {}", ErrorChain(&error))
      })?;
  if !is_current_pr_head(&event.commit_hash, &current_head.head.sha) {
    info!(
      pull_request = number,
      event_commit = %event.commit_hash,
      current_head = %current_head.head.sha,
      "Skipping stale GitHub PR comment update"
    );
    return Ok(());
  }
  let body = render_pr_comment(event, &detail_url, error_summary.as_deref());
  let target = owned_ids.first().copied();
  let request = target.map_or_else(
    || http.post(format!("{api}/issues/{number}/comments")),
    |id| {
      http.patch(format!(
        "https://api.github.com/repos/{owner}/{repo}/issues/comments/{id}"
      ))
    },
  );
  request
    .bearer_auth(token)
    .header("Accept", "application/vnd.github+json")
    .header("X-GitHub-Api-Version", circus_http::GITHUB_API_VERSION)
    .json(&serde_json::json!({ "body": body }))
    .send_checked()
    .await
    .map_err(|error| {
      format!("Cannot upsert GitHub PR comment: {}", ErrorChain(&error))
    })?;

  // Remove older duplicates created before upserts were serialized. The
  // author and marker checks ensure human comments are never touched.
  for duplicate in owned_ids.into_iter().skip(1) {
    http
      .delete(format!(
        "https://api.github.com/repos/{owner}/{repo}/issues/comments/{duplicate}"
      ))
      .bearer_auth(token)
      .header("Accept", "application/vnd.github+json")
      .header("X-GitHub-Api-Version", circus_http::GITHUB_API_VERSION)
      .send_checked()
      .await
      .map_err(|error| format!("Cannot remove duplicate Circus PR comment: {}", ErrorChain(&error)))?;
  }
  info!(pull_request = number, "Updated Circus GitHub PR comment");
  Ok(())
}

fn is_owned_pr_comment(author: Option<&str>, body: Option<&str>) -> bool {
  author == Some(PR_COMMENT_AUTHOR)
    && body.is_some_and(|body| body.contains(PR_COMMENT_MARKER))
}

fn is_current_pr_head(event_commit: &str, current_head: &str) -> bool {
  event_commit == current_head
}

fn owned_pr_comment_ids(comments: &[IssueComment]) -> Vec<u64> {
  comments
    .iter()
    .filter(|comment| {
      is_owned_pr_comment(
        comment.user.as_ref().map(|user| user.login.as_str()),
        comment.body.as_deref(),
      )
    })
    .map(|comment| comment.id)
    .collect()
}

fn evaluation_error_summary(error: &str, expression: &str) -> Option<String> {
  // Evaluator stderr may contain repository-controlled text or private data.
  // Publish only known diagnostics and the configured output name.
  if error.contains("missing attribute")
    && error.contains(&format!("fragment attr \"{expression}\""))
    && !expression.is_empty()
    && expression.len() <= 80
    && expression
      .bytes()
      .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
  {
    Some(format!(
      "The PR head is missing the configured flake output `{expression}`."
    ))
  } else if error.contains("syntax error") {
    Some("Nix reported a syntax error while evaluating the PR head.".into())
  } else if error.contains("failed while reading handshake for <startup>") {
    Some("The Nix evaluator exited during startup.".into())
  } else {
    None
  }
}

fn render_pr_comment(
  event: &EvaluationEvent,
  detail_url: &str,
  error_summary: Option<&str>,
) -> String {
  let (icon, label) = match event.evaluation_status {
    EvaluationStatus::Pending => ("⏳", "Evaluation queued"),
    EvaluationStatus::Running => ("⏳", "Evaluation is running"),
    EvaluationStatus::Failed => ("❌", "Evaluation failed"),
    EvaluationStatus::TimedOut => ("⏱️", "Evaluation timed out"),
    EvaluationStatus::Cancelled => ("⚪", "Evaluation was cancelled"),
    EvaluationStatus::Completed => {
      match event.jobs.status() {
        "success" => ("✅", "All builds passed"),
        "failure" => ("❌", "Some builds failed"),
        "pending" => ("⏳", "Builds are still running"),
        "cancelled" => ("⚪", "Builds were cancelled"),
        _ => ("⚠️", "Builds finished with skipped jobs"),
      }
    },
  };
  let progress = if event.jobs.total == 0 {
    String::new()
  } else {
    let completed = event.jobs.total - event.jobs.pending - event.jobs.running;
    let headline = if event.evaluation_status == EvaluationStatus::Running {
      format!("{} jobs discovered", event.jobs.total)
    } else {
      format!("{completed}/{} jobs complete", event.jobs.total)
    };
    format!(
      "**{headline}** · {} succeeded · {} failed · {} running · {} queued · \
       {} cancelled · {} skipped\n\n",
      event.jobs.succeeded,
      event.jobs.failed,
      event.jobs.running,
      event.jobs.pending,
      event.jobs.cancelled,
      event.jobs.skipped,
    )
  };
  let diagnostic = if event.evaluation_status == EvaluationStatus::Failed {
    error_summary.map_or_else(String::new, |summary| format!("{summary}\n\n"))
  } else {
    String::new()
  };
  let commit = &event.commit_hash[..event.commit_hash.len().min(12)];
  format!(
    "{PR_COMMENT_MARKER}\n### {icon} Circus: \
     {label}\n\n{diagnostic}{progress}Commit `{commit}` · [View evaluation \
     details]({detail_url})",
  )
}

impl WebhookChannel {
  /// POST a JSON body, signed with the current key when one is configured.
  async fn post_signed(
    &self,
    payload: &serde_json::Value,
    origin: Origin,
  ) -> Result<(), String> {
    let http = crate::http_client(origin);
    let body = serde_json::to_vec(payload)
      .map_err(|e| format!("Failed to serialize webhook payload: {e}"))?;

    let mut req = http
      .post(&self.url)
      .header("Content-Type", "application/json");

    if let Some(secret) = self.signing_key()? {
      req = req.header(SIGNATURE_HEADER, sign_body(&secret, &body)?);
    }
    for (name, value) in &self.headers {
      if RESERVED_HEADERS
        .iter()
        .any(|r| r.eq_ignore_ascii_case(name))
      {
        warn!(header = %name, "Ignoring reserved webhook header override");
        continue;
      }
      req = req.header(name, value);
    }

    req
      .body(body)
      .send_checked()
      .await
      .map_err(|e| format!("Webhook failed: {}", ErrorChain(&e)))?;
    Ok(())
  }

  /// Deliver an evaluation-finished event (see [`EvaluationEvent`]).
  pub(crate) async fn deliver_evaluation(
    &self,
    event: &EvaluationEvent,
    origin: Origin,
  ) -> Result<(), String> {
    self.post_signed(&event.webhook_payload(), origin).await?;
    info!(
      evaluation_id = %event.evaluation_id,
      "Evaluation webhook notification sent"
    );
    Ok(())
  }
}

impl Notifier for WebhookChannel {
  fn applies_to(&self, event: &BuildEvent) -> bool {
    self.build_events && (!self.on_failure_only || event.is_failure())
  }

  async fn deliver(
    &self,
    event: &BuildEvent,
    origin: Origin,
  ) -> Result<(), String> {
    let payload = serde_json::json!({
      "event":        "build_finished",
      "build_id":     event.build_id,
      "build_status": event.generic_status(),
      "build_job":    event.job_name,
      "build_drv":    event.drv_path,
      "build_output": event.build_output.as_deref().unwrap_or(""),
      "project_name": event.project_name,
      "project_url":  event.project_url,
      "commit_hash":  event.commit_hash,
    });
    self.post_signed(&payload, origin).await?;
    info!(build_id = %event.build_id, "Webhook notification sent");
    Ok(())
  }
}

impl Notifier for GithubStatusChannel {
  fn applies_to(&self, _event: &BuildEvent) -> bool {
    true
  }

  async fn deliver(
    &self,
    event: &BuildEvent,
    origin: Origin,
  ) -> Result<(), String> {
    let http = crate::http_client(origin);
    let (owner, repo) =
      parse_github_repo(&event.project_url).ok_or_else(|| {
        format!("Cannot parse GitHub repo from {}", event.project_url)
      })?;
    let (state, description) = event.github_state();
    let url = format!(
      "https://api.github.com/repos/{owner}/{repo}/statuses/{}",
      event.commit_hash
    );
    let body = serde_json::json!({
      "state": state,
      "description": description,
      "context": format!("circus/{}", event.job_name),
    });

    let resp = http
      .post(&url)
      .bearer_auth(&self.token)
      .header("Accept", "application/vnd.github+json")
      .header("X-GitHub-Api-Version", circus_http::GITHUB_API_VERSION)
      .json(&body)
      .send_checked()
      .await
      .map_err(|e| format!("GitHub status failed: {}", ErrorChain(&e)))?;
    let rate_limit =
      super::super::extract_rate_limit_from_headers(resp.headers());
    info!(build_id = %event.build_id, "Set GitHub commit status: {state}");
    super::super::apply_github_rate_limit(rate_limit).await;
    Ok(())
  }
}

impl Notifier for GiteaStatusChannel {
  fn applies_to(&self, _event: &BuildEvent) -> bool {
    true
  }

  async fn deliver(
    &self,
    event: &BuildEvent,
    origin: Origin,
  ) -> Result<(), String> {
    let http = crate::http_client(origin);
    let (owner, repo) = parse_gitea_repo(&event.project_url, &self.base_url)
      .ok_or_else(|| {
        format!("Cannot parse Gitea repo from {}", event.project_url)
      })?;
    let (state, description) = event.github_state();
    let url = format!(
      "{}/api/v1/repos/{owner}/{repo}/statuses/{}",
      self.base_url.trim_end_matches('/'),
      event.commit_hash
    );
    let body = serde_json::json!({
      "state": state,
      "description": description,
      "context": format!("circus/{}", event.job_name),
    });

    http
      .post(&url)
      .header("Authorization", format!("token {}", self.token))
      .json(&body)
      .send_checked()
      .await
      .map_err(|e| format!("Gitea status failed: {}", ErrorChain(&e)))?;
    info!(build_id = %event.build_id, "Set Gitea commit status: {state}");
    Ok(())
  }
}

impl Notifier for GitlabStatusChannel {
  fn applies_to(&self, _event: &BuildEvent) -> bool {
    true
  }

  async fn deliver(
    &self,
    event: &BuildEvent,
    origin: Origin,
  ) -> Result<(), String> {
    let http = crate::http_client(origin);
    let project_path = parse_gitlab_project(&event.project_url, &self.base_url)
      .ok_or_else(|| {
        format!("Cannot parse GitLab project from {}", event.project_url)
      })?;
    let (state, description) = event.gitlab_state();
    let encoded_project = urlencoding::encode(&project_path);
    let url = format!(
      "{}/api/v4/projects/{}/statuses/{}",
      self.base_url.trim_end_matches('/'),
      encoded_project,
      event.commit_hash
    );
    let body = serde_json::json!({
      "state": state,
      "description": description,
      "name": format!("circus/{}", event.job_name),
    });

    http
      .post(&url)
      .header("PRIVATE-TOKEN", &self.token)
      .json(&body)
      .send_checked()
      .await
      .map_err(|e| format!("GitLab status failed: {}", ErrorChain(&e)))?;
    info!(build_id = %event.build_id, "Set GitLab commit status: {state}");
    Ok(())
  }
}

impl Notifier for SlackChannel {
  fn applies_to(&self, event: &BuildEvent) -> bool {
    !self.on_failure_only || event.is_failure()
  }

  async fn deliver(
    &self,
    event: &BuildEvent,
    origin: Origin,
  ) -> Result<(), String> {
    let http = crate::http_client(origin);
    let status = event.generic_status();
    let body = serde_json::json!({
      "text": format!("CI: {} - {status}", event.job_name),
      "blocks": [{
        "type": "section",
        "text": {
          "type": "mrkdwn",
          "text": format!(
            "*{}* - *{status}*\nProject: {} | Commit: `{}`",
            event.job_name, event.project_name, event.commit_hash
          ),
        },
      }],
    });

    let resp = http
      .post(&self.webhook_url)
      .json(&body)
      .send_redacted()
      .await
      .map_err(slack_failed)?;

    if resp.status() == StatusCode::TOO_MANY_REQUESTS {
      let retry = resp
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("60");
      return Err(format!("Slack rate limited; retry-after={retry}"));
    }

    resp.checked().await.map_err(slack_failed)?;
    info!(build_id = %event.build_id, "Slack notification sent");
    Ok(())
  }
}

/// The webhook URL is itself the Slack credential, so the error leaves it out.
fn slack_failed(error: circus_http::Error) -> String {
  match error {
    circus_http::Error::Status { status, body, .. } => {
      format!("Slack returned {status}: {body}")
    },
    other => {
      std::error::Error::source(&other).map_or_else(
        || "Slack webhook failed".to_owned(),
        |cause| format!("Slack webhook failed: {}", ErrorChain(cause)),
      )
    },
  }
}

impl Notifier for EmailConfig {
  fn applies_to(&self, event: &BuildEvent) -> bool {
    !self.on_failure_only || event.is_failure()
  }

  async fn deliver(
    &self,
    event: &BuildEvent,
    origin: Origin,
  ) -> Result<(), String> {
    let status = event.email_status();
    let subject = format!(
      "[circus] {status} - {} ({})",
      event.job_name, event.project_name
    );
    let body = format!(
      "Build notification from circus\n\nProject: {}\nJob: {}\nStatus: \
       {}\nDerivation: {}\nOutput: {}\nBuild ID: {}\n",
      event.project_name,
      event.job_name,
      status,
      event.drv_path,
      event.build_output.as_deref().unwrap_or("N/A"),
      event.build_id,
    );

    let from: Mailbox = self.from_address.parse().map_err(|e| {
      format!("Invalid from address '{}': {e}", self.from_address)
    })?;

    let transport = |server: String| {
      let mailer =
        AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(server)
          .port(self.smtp_port);
      let mailer = if self.tls {
        let tls = TlsParameters::new(self.smtp_host.clone())
          .map_err(|e| format!("Failed to create SMTP transport: {e}"))?;
        mailer.tls(Tls::Wrapper(tls))
      } else {
        mailer
      };
      let mailer = if let (Some(user), Some(pass)) =
        (&self.smtp_user, &self.smtp_password)
      {
        mailer.credentials(Credentials::new(user.clone(), pass.clone()))
      } else {
        mailer
      };

      Ok::<_, String>(mailer.build())
    };

    // lettre resolves the host itself, so project channels connect to an
    // address checked here and keep the name for TLS.
    let mailer = match origin {
      Origin::Instance => transport(self.smtp_host.clone())?,
      Origin::Project => {
        let addrs = resolve_public(&self.smtp_host, self.smtp_port)
          .await
          .map_err(|e| format!("Failed to resolve SMTP host: {e}"))?;
        let mut last_error = String::new();
        let mut reachable = None;

        for addr in addrs {
          let mailer = transport(addr.ip().to_string())?;

          match mailer.test_connection().await {
            Ok(true) => {
              reachable = Some(mailer);
              break;
            },
            Ok(false) => last_error = format!("{addr} did not answer NOOP"),
            Err(e) => last_error = format!("{addr}: {e}"),
          }
        }

        reachable.ok_or_else(|| {
          format!("Failed to connect to SMTP host: {last_error}")
        })?
      },
    };

    for to_addr in &self.to_addresses {
      let to: Mailbox = match to_addr.parse() {
        Ok(addr) => addr,
        Err(e) => {
          warn!("Invalid to address '{to_addr}': {e}");
          continue;
        },
      };
      let email = Message::builder()
        .from(from.clone())
        .to(to)
        .subject(&subject)
        .header(ContentType::TEXT_PLAIN)
        .body(body.clone())
        .map_err(|e| format!("Failed to build email: {e}"))?;
      AsyncTransport::send(&mailer, email)
        .await
        .map_err(|e| format!("Failed to send email to {to_addr}: {e}"))?;
      info!(build_id = %event.build_id, to = to_addr, "Email notification sent");
    }
    Ok(())
  }
}

#[cfg(test)]
mod pr_comment_tests {
  use uuid::Uuid;

  use super::*;
  use crate::JobSummary;

  fn event(jobs: JobSummary) -> EvaluationEvent {
    EvaluationEvent {
      evaluation_id: Uuid::nil(),
      project_name: "example".into(),
      project_url: "https://github.com/owner/repo".into(),
      jobset_name: "pr".into(),
      commit_hash: "0123456789abcdef".into(),
      evaluation_status: EvaluationStatus::Completed,
      branch: Some("feature".into()),
      pr_number: Some(12),
      source_scope: None,
      superseded_by: None,
      jobs,
    }
  }

  #[test]
  fn stale_head_is_not_current() {
    assert!(is_current_pr_head("new-head", "new-head"));
    assert!(!is_current_pr_head("old-head", "new-head"));
  }

  #[test]
  fn only_app_authored_marked_comments_are_owned() {
    let comments = [
      IssueComment {
        id:   1,
        body: Some(PR_COMMENT_MARKER.to_owned()),
        user: Some(CommentAuthor {
          login: "human".into(),
        }),
      },
      IssueComment {
        id:   2,
        body: Some("Circus says hello".into()),
        user: Some(CommentAuthor {
          login: PR_COMMENT_AUTHOR.into(),
        }),
      },
      IssueComment {
        id:   3,
        body: Some(format!("{PR_COMMENT_MARKER}\nold status")),
        user: Some(CommentAuthor {
          login: PR_COMMENT_AUTHOR.into(),
        }),
      },
    ];
    assert_eq!(owned_pr_comment_ids(&comments), [3]);
  }

  #[test]
  fn existing_owned_comment_is_reused_instead_of_created_again() {
    let comments = [IssueComment {
      id:   27,
      body: Some(format!("{PR_COMMENT_MARKER}\nold status")),
      user: Some(CommentAuthor {
        login: PR_COMMENT_AUTHOR.into(),
      }),
    }];
    let existing_id = owned_pr_comment_ids(&comments).first().copied();
    assert_eq!(existing_id, Some(27));
  }

  #[test]
  fn rendered_comment_shows_pending_jobs_and_evaluation_link() {
    let event = event(JobSummary::from_counts_including_pending(&[
      (circus_common::models::BuildStatus::Succeeded, 2),
      (circus_common::models::BuildStatus::Running, 1),
      (circus_common::models::BuildStatus::Pending, 1),
    ]));
    let body =
      render_pr_comment(&event, "https://circus.example/evaluation/abc", None);
    assert!(body.contains("Builds are still running"));
    assert!(body.contains("2/4 jobs complete"));
    assert!(body.contains("1 running"));
    assert!(body.contains("1 queued"));
    assert!(body.contains("https://circus.example/evaluation/abc"));
  }

  #[test]
  fn rendered_comment_uses_evaluation_failure_and_timeout_labels() {
    let mut event = event(JobSummary::default());
    event.evaluation_status = EvaluationStatus::Running;
    assert!(
      render_pr_comment(&event, "https://circus.example/evaluation/abc", None)
        .contains("Evaluation is running")
    );

    event.evaluation_status = EvaluationStatus::Failed;
    let body =
      render_pr_comment(&event, "https://circus.example/evaluation/abc", None);
    assert!(body.contains("Evaluation failed"));
    assert!(!body.contains("0/0 jobs"));

    event.evaluation_status = EvaluationStatus::TimedOut;
    assert!(
      render_pr_comment(&event, "https://circus.example/evaluation/abc", None)
        .contains("Evaluation timed out")
    );
  }

  #[test]
  fn running_evaluation_reports_discovered_jobs() {
    let mut event = event(JobSummary::from_counts_including_pending(&[(
      circus_common::models::BuildStatus::Succeeded,
      5,
    )]));
    event.evaluation_status = EvaluationStatus::Running;
    let body =
      render_pr_comment(&event, "https://circus.example/evaluation/abc", None);
    assert!(body.contains("5 jobs discovered"));
    assert!(!body.contains("5/5 jobs complete"));
  }

  #[test]
  fn failed_evaluation_shows_only_the_safe_nix_diagnostic() {
    let mut event = event(JobSummary::default());
    event.evaluation_status = EvaluationStatus::Failed;
    let diagnostic = evaluation_error_summary(
      "worker stderr: private details\nfragment attr \"circusJobs\": missing \
       attribute",
      "circusJobs",
    )
    .unwrap();
    let body = render_pr_comment(
      &event,
      "https://circus.example/evaluation/abc",
      Some(&diagnostic),
    );
    assert!(body.contains("missing the configured flake output `circusJobs`"));
    assert!(!body.contains("private details"));
    assert!(!body.contains("0/0 jobs"));
  }
}
