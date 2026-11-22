#!/usr/bin/env bash
set -euo pipefail

tags=()
case "$EVENT_NAME" in
  push|workflow_dispatch)
    tag="${TAG_INPUT:-$GITHUB_REF_NAME}"
    if [[ "$tag" != v*-pschmitt.* ]] || ! git rev-parse --verify --quiet "refs/tags/$tag^{commit}" >/dev/null
    then
      echo 'Invalid or missing release tag' >&2
      exit 1
    fi
    revision="$(git rev-parse "refs/tags/$tag^{commit}")"
    tags+=("$tag")
    ;;
  status)
    revision="$GITHUB_SHA"
    mapfile -t tags < <(git tag --points-at "$revision" --list 'v*-pschmitt.*')
    ;;
  *) exit 1 ;;
esac

ready=false
if ((${#tags[@]})) && gh api "repos/$GITHUB_REPOSITORY/commits/$revision/status" |
  jq -e 'any(.statuses[]; .context == "circus/artifacts" and .state == "success")' >/dev/null
then
  ready=true
fi
if [[ "$ready" == false ]]
then
  tags=()
fi
printf 'ready=%s\n' "$ready" >> "$GITHUB_OUTPUT"
printf '%s\n' "${tags[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))' | sed 's/^/tags=/' >> "$GITHUB_OUTPUT"
