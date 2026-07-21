# agent-home

This directory contains local home templates for different agents.

Copy the Codex template you want into your local `~/.bridle-recording/`
directory and add its corresponding auth file. Claude is auto-discovered from
the existing user settings; its directory here is an optional reference
template and does not need to be copied.

Current templates:

- `codex-http`: Codex home for HTTP Responses traffic through bridle-recording
- `codex-websocket`: Codex home for WebSocket-enabled traffic through bridle-recording
- `claude`: Claude Code profile using the existing `~/.claude/settings.json`

Each template points at a profile-prefixed base URL on the recorder:

- `codex-http` -> `http://127.0.0.1:8787/codex-http`
- `codex-websocket` -> `http://127.0.0.1:8787/codex-websocket`
- `claude` -> `http://127.0.0.1:8787/claude`

Each profile directory also includes a `bridle-profile.toml` file used by the
recorder server to discover which profiles are available locally, including the
upstream URL or upstream source for that profile.

Example:

```sh
mkdir -p ~/.bridle-recording
cp -R agent-home/codex-http ~/.bridle-recording/
cp ~/.codex/auth.json ~/.bridle-recording/codex-http/auth.json

./scripts/run-recorder.sh

BRIDLE_AGENT_HOME=~/.bridle-recording/codex-http \
CODEX_HOME=~/.bridle-recording/codex-http \
codex
```

Recorder contract:

- live traffic is forwarded as a transparent proxy
- recording is sidecar-only and must not mutate traffic
- headers are recorded verbatim, including sensitive headers
- request and response bodies are recorded in raw form

Additional agent profiles can follow the same directory layout.

Claude example:

```sh
./scripts/run-recorder.sh
./scripts/run-claude.sh
```

The Claude launcher passes the equivalent of `recorder-settings.json` as an
in-memory additional setting. The original `~/.claude/settings.json` remains
active and supplies the user's authentication; only the process-local
`ANTHROPIC_BASE_URL` is overridden.
