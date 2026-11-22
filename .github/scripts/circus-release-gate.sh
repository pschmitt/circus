#!/usr/bin/env bash
set -euo pipefail

if [[ "$EVENT_NAME" == workflow_dispatch ]]
then
  echo 'ready=true' >> "$GITHUB_OUTPUT"
  exit 0
fi
statuses="$(gh api "repos/$GITHUB_REPOSITORY/commits/$GITHUB_SHA/status")"
runs="$(gh api "repos/$GITHUB_REPOSITORY/actions/workflows/ci.yml/runs?head_sha=$GITHUB_SHA&event=push&per_page=1")"
ready=false
if jq -e '
  [.statuses[] | select(.state == "success") | .context] as $contexts
  | ($contexts | index("circus/jobset/main")) != null
    and ($contexts | index("circus/jobset/main-arm")) != null
' >/dev/null <<< "$statuses" && jq -e '
  .workflow_runs[0] | .status == "completed" and .conclusion == "success"
' >/dev/null <<< "$runs"
then
  ready=true
fi
echo "ready=$ready" >> "$GITHUB_OUTPUT"
