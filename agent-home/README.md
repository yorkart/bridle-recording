# agent-home

This directory contains local home templates for different agents.

Copy the template you want into your local `~/.bridle-recording/` directory,
then add the corresponding auth file before launching the agent with that home.

Current templates:

- `codex-http`: Codex home for HTTP Responses traffic through bridle-recording
- `codex-websocket`: Codex home for WebSocket-enabled traffic through bridle-recording

Each template points at a profile-prefixed base URL on the recorder:

- `codex-http` -> `http://127.0.0.1:8787/codex-http`
- `codex-websocket` -> `http://127.0.0.1:8787/codex-websocket`

Each profile directory also includes a `bridle-profile.toml` file used by the
recorder server to discover which profiles are available locally, including the
upstream URL for that profile.

Example:

```sh
mkdir -p ~/.bridle-recording
cp -R agent-home/codex-http ~/.bridle-recording/
cp ~/.codex/auth.json ~/.bridle-recording/codex-http/auth.json

RECORDER_PROXY_MODE=passthrough ./scripts/run-recorder.sh

BRIDLE_AGENT_HOME=~/.bridle-recording/codex-http \
CODEX_HOME=~/.bridle-recording/codex-http \
codex
```

Recorder modes:

- `passthrough`: pure proxy + recording, no intentional traffic mutation
- `compat`: compatibility mode for protocol workarounds such as stripping the
  Codex `responses-lite` marker before forwarding upstream

You can add more agent homes here later, for example `claude/`.
