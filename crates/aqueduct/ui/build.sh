#!/bin/sh
# Regenerates src/admin.css from the utility classes used in src/admin.html.
# Needs the Tailwind CSS v4 standalone CLI: https://github.com/tailwindlabs/tailwindcss/releases
# Usage: TAILWIND=/path/to/tailwindcss crates/aqueduct/ui/build.sh
set -eu
cd "$(dirname "$0")"
"${TAILWIND:-tailwindcss}" -i input.css -o ../src/admin.css --minify
