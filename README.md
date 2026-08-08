# bridle-recording

Records Codex, Claude Code, and model-provider traffic. Recorded OpenAI-compatible
traffic can also be replayed through an OpenAI-compatible mock endpoint.

## Start Recorder

If your local shell does not automatically inherit the same proxy settings as
Codex/Desktop apps, the simplest way to start the recorder is:

```sh
./scripts/run-recorder.sh
```

This script uses:

- `BRIDLE_HOME_ROOT=~/.bridle-recording`
- `HTTP_PROXY=http://127.0.0.1:7890`
- `HTTPS_PROXY=http://127.0.0.1:7890`
- `ALL_PROXY=socks5://127.0.0.1:7890`

You can still override those env vars before running the script if your local
proxy uses a different port.

Equivalent manual command:

```sh
HTTP_PROXY=http://127.0.0.1:7890 \
HTTPS_PROXY=http://127.0.0.1:7890 \
ALL_PROXY=socks5://127.0.0.1:7890 \
BRIDLE_HOME_ROOT=~/.bridle-recording \
cargo run -- \
  --listen 127.0.0.1:8787
```

This starts the recorder on `http://127.0.0.1:8787`.

The running service exposes machine-readable usage information, including its
active profiles, recording paths, testset discovery schema, and mock replay
paths:

```sh
curl -s http://127.0.0.1:8787/help | jq .
```

`/help` is the HTTP equivalent of CLI `--help`: it returns relative paths so
clients can resolve them against whichever recorder origin they are using. It
does not expose configured upstream URLs or credentials.

The recorder contract is:

- fully transparent proxying on the live path
- sidecar recording that must not change forwarded traffic
- 100% verbatim header recording, including sensitive headers
- raw request/response body recording without compatibility rewrites

## Profile Registry

`~/.bridle-recording/` is now a configuration registry plus recording output.
It does not contain copies of agent homes. Each profile is one directory that
holds a small JSON config (`bridle-profile.json`) together with that profile's
recording output:

```text
~/.bridle-recording/
  codex-http/
    bridle-profile.json
    recordings/
    derived/mock/
  codex-websocket/
    bridle-profile.json
    recordings/
  claude/
    bridle-profile.json
    recordings/
  access.log
```

Each profile config carries two kinds of information:

- recorder metadata: the real upstream (`upstream` or `upstream_from =
  "claude-settings"`) and whether the profile supports websockets
- launcher metadata: the agent `command`, the user's own `agent_home`, the
  recorder entry point (`recorder_base_url`), and the overrides the launcher
  applies (`launch.env`, `launch.args`)

JSON has no comments, so templates carry a `description` field for humans; it
is ignored by both the recorder and the launcher.

The user's agent home is never copied. For Codex, `agent_home` points at your
existing `~/.codex`; for Claude Code, the existing `~/.claude/settings.json` is
reused as-is. Authentication, model settings, skills, and plugins all stay in
your own home.

Recordings are written under the active profile, for example
`~/.bridle-recording/codex-http/recordings`, which is separate from the user's
agent home.

Mock-only indexes and optional response rewrite specifications are stored
separately under `~/.bridle-recording/<profile>/derived/mock/`. Replay never
writes `request_match.json`, `response_rewrite.json`, or other derived files
into a recording session.

## One-Time Setup

Install the default registry entries (existing profile configs are
overwritten):

```sh
./scripts/setup-profiles.sh
```

This creates
`~/.bridle-recording/{codex-http,codex-websocket,claude}/bridle-profile.json`
pointing at your own agent homes. Edit them if your home paths or recorder
address differ.

## Start Codex For Recording

One launcher serves every profile:

```sh
./scripts/run-recorder.sh
./scripts/run-agent.sh codex-http
```

`run-agent.sh` is fully generic: it reads
`~/.bridle-recording/codex-http/bridle-profile.json`, substitutes
`{{recorder_base_url}}` / `{{agent_home}}` in the declared environment and
arguments, exports the environment, and executes the agent with those
arguments. For this profile the registry file declares the recorder provider
through Codex `--config` flags on every launch. Parsing and assembly are done
by a small Python helper (`python3` is required); the shell entry point stays
`run-agent.sh`:

```sh
--config 'model_provider="recorder-openai-http"'
--config 'model_providers.recorder-openai-http.name="OpenAI"'
--config 'model_providers.recorder-openai-http.base_url="http://127.0.0.1:8787/codex-http"'
--config 'model_providers.recorder-openai-http.wire_api="responses"'
--config 'model_providers.recorder-openai-http.requires_openai_auth=true'
```

Only the model address is redirected. Your own `~/.codex/config.toml`,
authentication, skills, and plugins are untouched, and traffic still routes
through the recorder even if Codex updates your home files.

Equivalent manual command:

```sh
NO_PROXY=127.0.0.1,localhost \
no_proxy=127.0.0.1,localhost \
CODEX_HOME=~/.codex \
codex \
  --config 'model_provider="recorder-openai-http"' \
  --config 'model_providers.recorder-openai-http.name="OpenAI"' \
  --config 'model_providers.recorder-openai-http.base_url="http://127.0.0.1:8787/codex-http"' \
  --config 'model_providers.recorder-openai-http.wire_api="responses"' \
  --config 'model_providers.recorder-openai-http.requires_openai_auth=true'
```

`codex-http` is expected to use the recorder as a transparent proxy. If some
upstream/provider combination cannot work without mutating live traffic, that
scenario is outside the live recorder contract and should be handled by a
separate offline or compatibility workflow.

If you want the WebSocket-enabled variant instead, use the other registry
entry:

```sh
./scripts/run-agent.sh codex-websocket
```

`codex-websocket` supports upstream proxy traversal through
`HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY` as well, so recorder-side WebSocket
connections can use the same local proxy environment as HTTP forwarding.

Additional agent profiles follow the same model: create a
`~/.bridle-recording/<name>/bridle-profile.json` and launch it with
`run-agent.sh <name>`.

## Start Claude Code For Recording

Claude Code reuses the existing user configuration at `~/.claude/settings.json`
directly. No Claude profile copy or credential copy is required:

```sh
./scripts/run-recorder.sh
./scripts/run-agent.sh claude
```

The integration has two independent configuration paths:

- At startup the recorder discovers `~/.claude/settings.json` and reads the real
  upstream from `env.ANTHROPIC_BASE_URL`. If the setting is absent, it uses
  `https://api.anthropic.com`.
- `run-agent.sh claude` reads the `claude` registry entry and overrides only
  `ANTHROPIC_BASE_URL` for that process, pointing Claude Code at
  `http://127.0.0.1:8787/claude`.

Claude Code continues to load the original user settings, including
`ANTHROPIC_AUTH_TOKEN` or `ANTHROPIC_API_KEY`. The resulting `authorization` or
`x-api-key` request header is forwarded and recorded verbatim; bridle-recording
does not extract, use, inject, or rewrite the credential. Claude Code's own
`x-claude-code-session-id` header is used to group recordings into sessions.

If the user settings file is elsewhere, point only the recorder-side lookup at
it before starting the recorder:

```sh
BRIDLE_CLAUDE_SETTINGS_PATH=/path/to/settings.json ./scripts/run-recorder.sh
```

## Multi-Profile Routing

The recorder exposes one path prefix per profile registry entry. Today the
default profiles are:

- `/codex-http`
- `/codex-websocket`
- `/claude`

Each profile exposes:

- a recording proxy under `/{profile}/...`
- a replay/mock provider under `/{profile}/mock/...`

If the requested profile does not exist in the running server, the recorder
returns `404`.

## Use Replay From An OpenAI Client

The replay/mock base URL is:

```text
http://127.0.0.1:8787/codex-http/mock
```

Configure any OpenAI-compatible client or agent provider to use that as its
`base_url`.

For the Responses API, the client still sends:

```text
POST /responses
```

and it reaches:

```text
POST http://127.0.0.1:8787/codex-http/mock/responses
```

The client does not need recorder-specific logic. It should behave like a normal
OpenAI client; bridle-recording handles matching the request to existing
recordings and replaying the recorded response.

Replay first matches exported assets under
`testsets/<profile>/*/raw/`. This keeps the mock source aligned with the assets
returned by `/api/testsets`. Local profile recordings remain a fallback for
ad-hoc replay when no saved testset matches. Once a live client session is
matched, all later requests stay bound to that exact exported or local session
and must follow its recorded order.

## Replay Match Whitelist

Replay uses exact matching on a canonical whitelist of request fields. JSON
object key order is normalized before hashing.

The matched request fields are:

- HTTP method
- HTTP path
- query string for `GET` requests
- request body fields: `model`, `stream`, `store`, `include`,
  `parallel_tool_calls`, `tool_choice`, `reasoning`, `text`, `instructions`
- `input` items, limited to each item's `role`, `type`, and `content`

The following request data is intentionally not matched:

- request headers, including auth headers
- dynamic metadata such as `prompt_cache_key`, `client_metadata`, and internal
  chat message metadata
- top-level `tools`
- `input.content` text blocks starting with `<skills_instructions>`,
  `<apps_instructions>`, or `<plugins_instructions>`
