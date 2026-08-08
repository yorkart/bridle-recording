# List available recipes.
default:
    @just --list

# Start the recorder.
recorder *args:
    #!/usr/bin/env bash
    exec ./scripts/run-recorder.sh "$@"

# Install the default profile registry entries.
setup:
    #!/usr/bin/env bash
    exec ./scripts/setup-profiles.sh "$@"

# Launch an agent through the recorder using a profile registry entry.
agent profile *args:
    #!/usr/bin/env bash
    exec ./scripts/run-agent.sh "{{profile}}" "$@"
