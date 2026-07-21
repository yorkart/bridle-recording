#!/usr/bin/env bash

set -euo pipefail

export NO_PROXY="${NO_PROXY:-127.0.0.1,localhost}"
export no_proxy="${no_proxy:-$NO_PROXY}"

exec claude \
  --settings '{"env":{"ANTHROPIC_BASE_URL":"http://127.0.0.1:8787/claude"}}' \
  "$@"
