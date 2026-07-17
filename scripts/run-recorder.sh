#!/usr/bin/env bash

set -euo pipefail

export HTTP_PROXY="${HTTP_PROXY:-http://127.0.0.1:7890}"
export HTTPS_PROXY="${HTTPS_PROXY:-http://127.0.0.1:7890}"
export ALL_PROXY="${ALL_PROXY:-socks5://127.0.0.1:7890}"
export BRIDLE_HOME_ROOT="${BRIDLE_HOME_ROOT:-$HOME/.bridle-recording}"

exec cargo run -- \
  --listen "${RECORDER_LISTEN:-127.0.0.1:8787}"
