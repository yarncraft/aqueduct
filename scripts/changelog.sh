#!/usr/bin/env bash
# Assemble the changelog fragments in changelog/unreleased/ into one section.
#
#   scripts/changelog.sh                  print the Unreleased section (preview)
#   scripts/changelog.sh --release 0.2.0  insert it into CHANGELOG.md as "## 0.2.0 (<date>)"
#                                         and delete the fragments
set -euo pipefail
cd "$(dirname "$0")/.."

dir=changelog/unreleased
sections=("breaking:Breaking changes and upgrade notes" "added:Added" "changed:Changed" "fixed:Fixed")

render() {
  local heading=$1
  echo "$heading"
  for s in "${sections[@]}"; do
    local key=${s%%:*} title=${s#*:}
    # Newest first: highest issue number first.
    local files
    files=$(find "$dir/$key" -maxdepth 1 -name '*.md' 2>/dev/null | sort -t/ -k4 -rn) || true
    [ -n "$files" ] || continue
    printf '\n### %s\n\n' "$title"
    for f in $files; do cat "$f"; done
  done
}

case "${1:-}" in
  "") render "## Unreleased" ;;
  --release)
    version=${2:?usage: scripts/changelog.sh --release <version>}
    if ! find "$dir" -name '*.md' -path "$dir/*/*" | grep -q .; then
      echo "no fragments in $dir" >&2; exit 1
    fi
    section=$(render "## $version ($(date +%Y-%m-%d))")
    marker='<!-- releases -->'
    grep -qF "$marker" CHANGELOG.md || { echo "CHANGELOG.md has no $marker line" >&2; exit 1; }
    tmp=$(mktemp)
    awk -v m="$marker" -v sec="$section" '{ print } $0 == m { print ""; print sec }' CHANGELOG.md > "$tmp"
    mv "$tmp" CHANGELOG.md
    find "$dir" -mindepth 2 -name '*.md' -delete
    echo "Released $version into CHANGELOG.md; fragments removed." >&2
    ;;
  *) echo "usage: scripts/changelog.sh [--release <version>]" >&2; exit 1 ;;
esac
