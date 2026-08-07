# List available recipes.
default:
    @just --list

# Start the recorder.
recorder *args:
    #!/usr/bin/env bash
    exec ./scripts/run-recorder.sh "$@"

# Start Claude through the recorder.
claude *args:
    #!/usr/bin/env bash
    exec ./scripts/run-claude.sh "$@"

# Start Codex through the recorder's HTTP endpoint.
codex-http *args:
    #!/usr/bin/env bash
    exec ./scripts/run-codex-http.sh "$@"

# Start Codex through the recorder's WebSocket proxy.
codex-websocket *args:
    #!/usr/bin/env bash
    exec ./scripts/run-codex-websocket.sh "$@"
