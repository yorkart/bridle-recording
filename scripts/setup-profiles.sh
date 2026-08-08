#!/usr/bin/env bash
#
# setup-profiles.sh - install the default profile registry entries.
#
# Copies agent-home/profiles/*.json into
# $BRIDLE_HOME_ROOT/<name>/bridle-profile.json (default root
# ~/.bridle-recording) so each profile directory keeps its config together
# with its recordings. Existing profile configs are overwritten.
#
# The templates point at the user's own agent homes (~/.codex, ~/.claude), so
# no agent home copying or auth copying is needed.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="${BRIDLE_HOME_ROOT:-$HOME/.bridle-recording}"
TEMPLATE_DIR="$SCRIPT_DIR/../agent-home/profiles"

installed=0
for template in "$TEMPLATE_DIR"/*.json; do
  [[ -e "$template" ]] || continue
  name="$(basename "$template" .json)"
  dest_dir="$ROOT/$name"
  dest="$dest_dir/bridle-profile.json"
  install -d -m 700 "$dest_dir"
  install -m 600 "$template" "$dest"
  echo "installed $dest"
  installed=$((installed + 1))
done

echo "done: $installed profiles installed under $ROOT"
