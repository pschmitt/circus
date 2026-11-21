use circus_http::RequestExt as _;
use misstep::ResultExt as _;
use serde::Deserialize;

/// Metadata returned by GitHub's pull request API. The account identification
/// comes from GitHub rather than author fields in a Git commit.
#[derive(Deserialize)]
pub(super) struct PullRequest {
  number: i32,
  state:  String,
  draft:  bool,
  user:   User,
  head:   Ref,
  base:   Ref,
}

#[derive(Deserialize)]
struct User {
  id: u64,
}

#[derive(Deserialize)]
struct Ref {
  sha:      String,
  #[serde(rename = "ref")]
  name:     String,
  repo:     Option<Repository>,
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
  let Some((owner, name)) = candidate.repository.split_once('/') else {
    return Ok(false);
  };
  if candidate.number <= 0 || allowed_author_ids.is_empty() {
    return Ok(false);
  }
  let mut url = url::Url::parse("https://api.github.com/repos")?;
  url
    .path_segments_mut()
    .expect("GitHub API URL is hierarchical")
    .extend([owner, name, "pulls", &candidate.number.to_string()]);
  let response: PullRequest = http
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
  Ok(response.allows(candidate, allowed_author_ids))
}

impl PullRequest {
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
      && base_repo.full_name.eq_ignore_ascii_case(candidate.repository)
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
