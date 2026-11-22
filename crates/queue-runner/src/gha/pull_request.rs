use std::time::Duration;

use circus_common::{PgPool, repo};
use circus_config::EphemeralPoolConfig;
use circus_http::RequestExt as _;
use futures::{StreamExt as _, stream};
use misstep::ResultExt as _;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{github_repository_slug, load_token};

/// Metadata returned by GitHub's pull request API. The account identification
/// comes from GitHub rather than author fields in a Git commit.
#[derive(Deserialize)]
pub(super) struct PullRequest {
  number:    i32,
  state:     String,
  closed_at: Option<jiff::Timestamp>,
  draft:     bool,
  user:      User,
  head:      Ref,
  base:      Ref,
}

#[derive(Deserialize)]
struct User {
  id: u64,
}

#[derive(Deserialize)]
struct Ref {
  sha:  String,
  #[serde(rename = "ref")]
  name: String,
  repo: Option<Repository>,
}

#[derive(Deserialize)]
struct Repository {
  id:        u64,
  full_name: String,
}

pub(super) struct Candidate<'a> {
  pub repository: &'a str,
  pub number:     i32,
  pub commit:     &'a str,
  pub head:       &'a str,
  pub base:       &'a str,
}

/// Confirm that this evaluation still describes an eligible open GitHub PR.
/// JSON errors and failed requests leave the evaluation ineligible.
pub(super) async fn verify(
  http: &reqwest::Client,
  token: &str,
  candidate: &Candidate<'_>,
  allowed_author_ids: &[u64],
) -> misstep::Result<bool> {
  if candidate.number <= 0 || allowed_author_ids.is_empty() {
    return Ok(false);
  }
  Ok(
    fetch(http, token, candidate.repository, candidate.number)
      .await?
      .allows(candidate, allowed_author_ids),
  )
}

async fn fetch(
  http: &reqwest::Client,
  token: &str,
  repository: &str,
  number: i32,
) -> misstep::Result<PullRequest> {
  let (owner, name) = repository
    .split_once('/')
    .ok_or_else(|| misstep::report!("repository must be owner/repo"))?;
  let mut url = url::Url::parse("https://api.github.com/repos")?;
  url
    .path_segments_mut()
    .expect("GitHub API URL is hierarchical")
    .extend([owner, name, "pulls", &number.to_string()]);
  let response = http
    .get(url)
    .header(reqwest::header::ACCEPT, "application/vnd.github+json")
    .header("X-GitHub-Api-Version", circus_http::GITHUB_API_VERSION)
    .bearer_auth(token)
    .send_checked()
    .await
    .context("get pull request from GitHub")?
    .json()
    .await
    .context("parse pull request from GitHub")?;
  Ok(response)
}

pub(super) async fn reconcile(
  config: &EphemeralPoolConfig,
  http: &reqwest::Client,
  pool: &PgPool,
  shutdown: &CancellationToken,
) {
  let mut interval = tokio::time::interval(Duration::from_secs(60));
  interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
  let mut cursor = (Uuid::nil(), 0);
  loop {
    tokio::select! {
      () = shutdown.cancelled() => return,
      _ = interval.tick() => {},
    }
    tokio::select! {
      () = shutdown.cancelled() => return,
      result = reconcile_page(config, http, pool, &mut cursor) => {
        if let Err(error) = result {
          tracing::warn!(%error, "could not reconcile closed GitHub pull requests");
        }
      },
    }
  }
}

async fn reconcile_page(
  config: &EphemeralPoolConfig,
  http: &reqwest::Client,
  pool: &PgPool,
  cursor: &mut (Uuid, i32),
) -> misstep::Result<()> {
  let token = load_token(&config.github_actions).await?;
  let candidates =
    repo::evaluations::unfinished_pull_requests(pool, *cursor).await?;
  *cursor = candidates
    .last()
    .map_or((Uuid::nil(), 0), |pr| (pr.project_id, pr.pr_number));
  stream::iter(candidates).for_each_concurrent(4, |pr| {
    let token = &token;
    async move {
      let Some(repository) = github_repository_slug(&pr.repository_url) else {
        return;
      };
      if !config.allowed_build_repositories.contains(&repository) {
        return;
      }
      let result = async {
        let observed_before = jiff::Timestamp::now();
        let details = fetch(http, token, &repository, pr.pr_number).await?;
        if details.is_closed(&repository, pr.pr_number) {
          repo::evaluations::cancel_closed_pr(pool, pr.project_id, pr.pr_number, observed_before).await?;
        }
        Ok::<_, misstep::Report>(())
      }.await;
      if let Err(error) = result {
        tracing::warn!(%error, %repository, pr_number = pr.pr_number, "could not reconcile PR");
      }
    }
  }).await;
  Ok(())
}

impl PullRequest {
  fn is_closed(&self, repository: &str, number: i32) -> bool {
    self.state == "closed"
      && self.number == number
      && self.closed_at.is_some()
      && self
        .base
        .repo
        .as_ref()
        .is_some_and(|repo| repo.full_name.eq_ignore_ascii_case(repository))
  }

  pub(super) fn allows(
    &self,
    candidate: &Candidate<'_>,
    allowed_author_ids: &[u64],
  ) -> bool {
    let (Some(head_repo), Some(base_repo)) = (&self.head.repo, &self.base.repo)
    else {
      return false;
    };
    self.number == candidate.number
      && self.state == "open"
      && !self.draft
      && allowed_author_ids.contains(&self.user.id)
      && head_repo.id == base_repo.id
      && base_repo
        .full_name
        .eq_ignore_ascii_case(candidate.repository)
      && self.head.sha == candidate.commit
      && self.head.name == candidate.head
      && self.base.name == candidate.base
  }
}

#[cfg(test)]
mod tests {
  use serde_json::json;

  use super::*;

  #[test]
  fn reconciliation_requires_a_confirmed_closure_for_the_same_pr() {
    let mut payload = json!({
      "number": 17, "state": "closed", "closed_at": "2026-11-22T10:22:44Z",
      "draft": false, "user": {"id": 11},
      "head": {"sha": "commit", "ref": "head", "repo": null},
      "base": {"sha": "base", "ref": "main",
        "repo": {"id": 23, "full_name": "Example/Project"}}
    });
    let closed: PullRequest = serde_json::from_value(payload.clone()).unwrap();
    assert!(closed.is_closed("example/project", 17));
    assert!(!closed.is_closed("example/project", 18));
    assert!(!closed.is_closed("example/other", 17));
    payload["state"] = json!("open");
    let open: PullRequest = serde_json::from_value(payload).unwrap();
    assert!(!open.is_closed("example/project", 17));
  }

  #[test]
  fn permits_only_an_allowlisted_same_repository_pr_at_its_evaluated_head() {
    let candidate = Candidate {
      repository: "example/project",
      number:     17,
      commit:     "0123456789abcdef0123456789abcdef01234567",
      head:       "bot/update",
      base:       "main",
    };
    let response = json!({
      "number": 17,
      "state": "open",
      "draft": false,
      "user": {"id": 11},
      "head": {"sha": candidate.commit, "ref": candidate.head,
        "repo": {"id": 23, "full_name": "example/project"}},
      "base": {"sha": "unused", "ref": candidate.base,
        "repo": {"id": 23, "full_name": "Example/Project"}}
    });
    let cases = [
      ("", json!(null), true),
      ("/number", json!(18), false),
      ("/state", json!("closed"), false),
      ("/draft", json!(true), false),
      ("/user/id", json!(12), false),
      ("/head/repo", json!(null), false),
      ("/head/repo/id", json!(24), false),
      ("/base/repo/full_name", json!("example/other"), false),
      ("/head/sha", json!("changed"), false),
      ("/head/ref", json!("bot/other"), false),
      ("/base/ref", json!("release"), false),
    ];
    for (path, value, expected) in cases {
      let mut payload = response.clone();
      if !path.is_empty() {
        *payload.pointer_mut(path).unwrap() = value;
      }
      let pr: PullRequest = serde_json::from_value(payload).unwrap();
      assert_eq!(pr.allows(&candidate, &[11]), expected, "field: {path}");
    }
  }
}
