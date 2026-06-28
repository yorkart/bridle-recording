#!/usr/bin/env bash

set -euo pipefail

export BRIDLE_AGENT_HOME="${BRIDLE_AGENT_HOME:-$HOME/.bridle-recording/codex-http}"
export CODEX_HOME="${CODEX_HOME:-$BRIDLE_AGENT_HOME}"
export NO_PROXY="${NO_PROXY:-127.0.0.1,localhost}"
export no_proxy="${no_proxy:-$NO_PROXY}"

exec codex "$@"
