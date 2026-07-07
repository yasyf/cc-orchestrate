#!/usr/bin/env bash
# Regenerates docs/assets/demo.png from a real `cco --help` run.
# Requires: go, freeze (github.com/charmbracelet/freeze), bat, and pngquant on PATH.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

(cd "$root" && CGO_ENABLED=0 go build -o "$tmp/cco" .)

"$tmp/cco" --help | bat --plain --color=always --language help > "$tmp/demo.ansi"

freeze "$tmp/demo.ansi" --language ansi \
  --theme github-dark --background "#0d1117" --window --padding 24 --font.size 28 \
  --output "$root/docs/assets/demo.png"

pngquant --force --skip-if-larger --output "$root/docs/assets/demo.png" \
  "$root/docs/assets/demo.png"
