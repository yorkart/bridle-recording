# bridle-recording

Records Codex/OpenAI-compatible model traffic and replays it through an
OpenAI-compatible mock endpoint.

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

The recorder contract is:

- fully transparent proxying on the live path
- sidecar recording that must not change forwarded traffic
- 100% verbatim header recording, including sensitive headers
- raw request/response body recording without compatibility rewrites

Each configured profile forwards requests to the `upstream` declared in that
profile's `bridle-profile.toml`.

Recordings are written under the active profile home, for example
`~/.bridle-recording/codex-http/recordings`.

Mock-only indexes and optional response rewrite specifications are stored
separately under `~/.bridle-recording/<profile>/derived/mock/`. Replay never
writes `request_match.json`, `response_rewrite.json`, or other derived files
into a recording session.

## Start Codex For Recording

The repository keeps agent home templates under `agent-home/`. These
directories are intended to be copied into your local
`~/.bridle-recording/` directory instead of being used in-place from the
repository.

`agent-home/codex-http/config.toml` is configured to route Codex through the
recorder:

```toml
model_provider = "recorder-openai-http"

[model_providers.recorder-openai-http]
name = "OpenAI"
base_url = "http://127.0.0.1:8787/codex-http"
wire_api = "responses"
requires_openai_auth = true
```

Create a local config directory, copy one of the templates, and copy your
existing Codex auth state into it:

```sh
mkdir -p ~/.bridle-recording
cp -R agent-home/codex-http ~/.bridle-recording/
cp ~/.codex/auth.json ~/.bridle-recording/codex-http/auth.json
```

Then start Codex with that agent home:

```sh
./scripts/run-codex-http.sh
```

The helper script injects the `recorder-openai-http` provider and local recorder
URL on every launch. This keeps traffic routed through the recorder even if
Codex updates the profile's `config.toml`. It also sets
`NO_PROXY=127.0.0.1,localhost` so local traffic to `http://127.0.0.1:8787` does
not get sent back through your system proxy.

Equivalent manual command:

```sh
NO_PROXY=127.0.0.1,localhost \
no_proxy=127.0.0.1,localhost \
BRIDLE_AGENT_HOME=~/.bridle-recording/codex-http \
CODEX_HOME=~/.bridle-recording/codex-http \
codex
```

`BRIDLE_AGENT_HOME` is the neutral way to identify the active agent home.
`CODEX_HOME` is still set here because Codex uses it to locate `config.toml`.

`codex-http` is expected to use the recorder as a transparent proxy. If some
upstream/provider combination cannot work without mutating live traffic, that
scenario is outside the live recorder contract and should be handled by a
separate offline or compatibility workflow.

If you want the WebSocket-enabled variant instead, copy
`agent-home/codex-websocket/` the same way:

```sh
mkdir -p ~/.bridle-recording
cp -R agent-home/codex-websocket ~/.bridle-recording/
cp ~/.codex/auth.json ~/.bridle-recording/codex-websocket/auth.json

./scripts/run-codex-websocket.sh
```

Equivalent manual command:

```sh
NO_PROXY=127.0.0.1,localhost \
no_proxy=127.0.0.1,localhost \
BRIDLE_AGENT_HOME=~/.bridle-recording/codex-websocket \
CODEX_HOME=~/.bridle-recording/codex-websocket \
codex
```

`codex-websocket` now supports upstream proxy traversal through
`HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY` as well, so recorder-side WebSocket
connections can use the same local proxy environment as HTTP forwarding.

This layout leaves room for other agent homes later, for example:

```text
~/.bridle-recording/
  codex-websocket/
  codex-http/
  claude/
```

Copying `auth.json` keeps your private credentials out of the repository while
still letting the recorder use the same Codex login state.

## Multi-Profile Routing

The recorder exposes one path prefix per agent profile. Today the built-in
profiles are:

- `/codex-http`
- `/codex-websocket`

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
