#!/usr/bin/env bash
#
# run-agent.sh - entry point for the generic agent launcher.
#
# Delegates to run-agent.py, which reads the profile registry entry from
# $BRIDLE_HOME_ROOT/profiles/<profile>.json and executes the declared agent
# with its declared environment and arguments.
#
# Usage:
#   scripts/run-agent.sh <profile> [extra args...]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 is required to launch profiles" >&2
  exit 2
fi

exec python3 "$SCRIPT_DIR/run-agent.py" "$@"
