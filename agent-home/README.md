# agent-home

This directory contains the default profile registry templates. Each template
is one JSON file describing a profile: the agent command, the user's own agent
home, the recorder entry point, and the environment/argument overrides needed
to redirect only the model address.

The user's agent home is never copied. For Codex, `agent_home` points at your
existing `~/.codex`; for Claude Code, the existing `~/.claude/settings.json` is
reused as-is. Authentication, model settings, skills, and plugins all stay in
your own home.

Current templates:

- `codex-http.json`: Codex HTTP/Responses traffic -> `/codex-http`
- `codex-websocket.json`: Codex WebSocket traffic -> `/codex-websocket`
- `claude.json`: Claude Code -> `/claude`

## Install

Copy the templates into your local registry (existing profile configs are
overwritten):

```sh
./scripts/setup-profiles.sh
```

This installs each template as
`~/.bridle-recording/<name>/bridle-profile.json`, so config and recordings stay
in the same profile directory. Edit the installed files if your agent home or
recorder address differs.

## Launch

One launcher serves every profile:

```sh
./scripts/run-recorder.sh
./scripts/run-agent.sh codex-http
./scripts/run-agent.sh claude
```

The launcher is fully generic: it reads
`~/.bridle-recording/<name>/bridle-profile.json`, substitutes
`{{recorder_base_url}}` / `{{agent_home}}` in the declared environment and
arguments, exports the environment, and executes the command with those
arguments. It does not know anything about a specific agent; adding a new
agent is just adding one JSON file. Parsing is done by a small Python helper,
so `python3` is required; set `BRIDLE_LAUNCH_DEBUG=1` to print the resolved
launch values.

## Adding a profile

Create `~/.bridle-recording/<name>/bridle-profile.json` with the recorder metadata
(`upstream` or `upstream_from`, `supports_websocket`) and launcher metadata
(`command`, optional `agent_home` and `recorder_base_url`, `launch.env`, and
`launch.args`). JSON has no comments, so use a `description` field for human
notes. `launch.args` is a plain array of strings; use `\"` escapes inside a
string when a value itself contains double quotes (for example Codex
`--config` values). Restart the recorder to pick up new profiles.
