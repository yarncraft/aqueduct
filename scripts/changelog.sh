#!/usr/bin/env bash
# Assemble the changelog fragments in changelog/unreleased/ into one section.
#
#   scripts/changelog.sh                  print the Unreleased section (preview)
#   scripts/changelog.sh --release 0.2.0  insert it into CHANGELOG.md as "## 0.2.0 (<date>)"
#                                         and delete the fragments it released
set -euo pipefail
cd "$(dirname "$0")/.."

dir=changelog/unreleased
sections=("breaking:Breaking changes and upgrade notes" "added:Added" "security:Security" "changed:Changed" "fixed:Fixed")

# Every file must be a .md fragment directly in a known section (or a .gitkeep), or it would never be released.
check() {
  local known=() s stray
  for s in "${sections[@]}"; do known+=("$dir/${s%%:*}"); done
  stray=$( (find "$dir" -type f 2>/dev/null || true) | while IFS= read -r f; do
    case "${f##*/}" in .gitkeep) continue ;; esac
    case "$f" in *.md) case " ${known[*]} " in *" ${f%/*} "*) continue ;; esac ;; esac
    echo "$f"
  done)
  if [ -n "$stray" ]; then
    echo "not a fragment in ${known[*]}:" >&2
    echo "$stray" >&2
    exit 1
  fi
}

# Fragments in release order: by section, then highest issue number first.
fragments() {
  local s d
  for s in "${sections[@]}"; do
    d="$dir/${s%%:*}"
    [ -d "$d" ] || continue
    find "$d" -maxdepth 1 -type f -name '*.md' | sort -t/ -k4 -rn
  done
}

# render <heading> <fragment list>
render() {
  echo "$1"
  local s d f current=
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    d=${f%/*}
    if [ "$d" != "$current" ]; then
      current=$d
      for s in "${sections[@]}"; do
        [ "$dir/${s%%:*}" = "$d" ] && printf '\n### %s\n\n' "${s#*:}"
      done
    fi
    # Strip CRs and trailing blank lines; end with exactly one newline.
    awk '{ sub(/\r$/, ""); lines[NR] = $0; if ($0 ~ /[^ \t]/) last = NR }
         END { for (i = 1; i <= last; i++) print lines[i] }' "$f"
  done <<< "$2"
}

check
list=$(fragments)
case "${1:-}" in
  "") render "## Unreleased" "$list" ;;
  --release)
    version=${2:?usage: scripts/changelog.sh --release <version>}
    [ -n "$list" ] || { echo "no fragments in $dir" >&2; exit 1; }
    marker='<!-- releases -->'
    grep -qF "$marker" CHANGELOG.md || { echo "CHANGELOG.md has no $marker line" >&2; exit 1; }
    section=$(mktemp); out=$(mktemp)
    render "## $version ($(date +%Y-%m-%d))" "$list" > "$section"
    # Read the section from a file: awk -v would rewrite backslashes in the text.
    awk -v m="$marker" -v f="$section" '
      { print; l = $0; sub(/\r$/, "", l) }
      l == m { print ""; while ((getline line < f) > 0) print line }
    ' CHANGELOG.md > "$out"
    mv "$out" CHANGELOG.md
    rm -f "$section"
    # Delete exactly the files that were rendered.
    while IFS= read -r f; do rm -- "$f"; done <<< "$list"
    echo "Released $version into CHANGELOG.md; $(wc -l <<< "$list") fragments removed." >&2
    ;;
  *) echo "usage: scripts/changelog.sh [--release <version>]" >&2; exit 1 ;;
esac
