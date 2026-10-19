#!/usr/bin/env bash
# Assemble the changelog fragments in changelog/unreleased/ into one section.
#
#   scripts/changelog.sh                  print the Unreleased section (preview)
#   scripts/changelog.sh --release 0.2.0  insert it into CHANGELOG.md as "## 0.2.0 (<date>)"
#                                         and delete the fragments it released
set -euo pipefail
cd "$(dirname "$0")/.."

dir=changelog/unreleased
sections=("breaking:Breaking changes and upgrade notes" "added:Added" "changed:Changed" "fixed:Fixed")

# Every fragment must sit directly in a known section, or it would never be released.
check() {
  local known=() s stray
  for s in "${sections[@]}"; do known+=("$dir/${s%%:*}"); done
  stray=$(find "$dir" -type f -name '*.md' | while IFS= read -r f; do
    d=${f%/*}
    case " ${known[*]} " in *" $d "*) ;; *) echo "$f" ;; esac
  done)
  if [ -n "$stray" ]; then
    echo "fragments outside ${known[*]}:" >&2
    echo "$stray" >&2
    exit 1
  fi
}

# Files in release order: by section, then highest issue number first.
fragments() {
  local s
  for s in "${sections[@]}"; do
    find "$dir/${s%%:*}" -maxdepth 1 -type f -name '*.md' 2>/dev/null | sort -t/ -k4 -rn
  done
}

render() {
  echo "$1"
  local s key files f
  for s in "${sections[@]}"; do
    key=${s%%:*}
    files=$(find "$dir/$key" -maxdepth 1 -type f -name '*.md' 2>/dev/null | sort -t/ -k4 -rn)
    [ -n "$files" ] || continue
    printf '\n### %s\n\n' "${s#*:}"
    while IFS= read -r f; do
      # Strip CR, and end every fragment with exactly one newline so bullets never run together.
      tr -d '\r' < "$f" | sed -e '$a\'
    done <<< "$files"
  done
}

check
case "${1:-}" in
  "") render "## Unreleased" ;;
  --release)
    version=${2:?usage: scripts/changelog.sh --release <version>}
    released=$(fragments)
    [ -n "$released" ] || { echo "no fragments in $dir" >&2; exit 1; }
    marker='<!-- releases -->'
    grep -qF "$marker" CHANGELOG.md || { echo "CHANGELOG.md has no $marker line" >&2; exit 1; }
    section=$(mktemp); out=$(mktemp)
    render "## $version ($(date +%Y-%m-%d))" > "$section"
    # Read the section from a file: awk -v would rewrite backslashes in the text.
    awk -v m="$marker" -v f="$section" '
      { print; l = $0; sub(/$/, "", l) }
      l == m { print ""; while ((getline line < f) > 0) print line }
    ' CHANGELOG.md > "$out"
    mv "$out" CHANGELOG.md
    rm -f "$section"
    while IFS= read -r f; do rm -- "$f"; done <<< "$released"
    echo "Released $version into CHANGELOG.md; $(wc -l <<< "$released") fragments removed." >&2
    ;;
  *) echo "usage: scripts/changelog.sh [--release <version>]" >&2; exit 1 ;;
esac
