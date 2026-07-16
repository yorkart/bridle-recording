#!/usr/bin/env bash

set -euo pipefail

export BRIDLE_AGENT_HOME="${BRIDLE_AGENT_HOME:-$HOME/.bridle-recording/codex-http}"
export CODEX_HOME="${CODEX_HOME:-$BRIDLE_AGENT_HOME}"
export NO_PROXY="${NO_PROXY:-127.0.0.1,localhost}"
export no_proxy="${no_proxy:-$NO_PROXY}"

exec codex \
  --config 'model_provider="recorder-openai-http"' \
  --config 'model_providers.recorder-openai-http.name="OpenAI"' \
  --config 'model_providers.recorder-openai-http.base_url="http://127.0.0.1:8787/codex-http"' \
  --config 'model_providers.recorder-openai-http.wire_api="responses"' \
  --config 'model_providers.recorder-openai-http.requires_openai_auth=true' \
  "$@"
