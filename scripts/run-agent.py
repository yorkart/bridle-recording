#!/usr/bin/env python3
#
# run-agent.py - generic agent launcher (JSON profiles).
#
# Reads one profile config from:
#   $BRIDLE_HOME_ROOT/<profile>/bridle-profile.json
#                                  (default: ~/.bridle-recording)
# and executes the agent declared there. The launcher has no knowledge of any
# specific agent: the profile fully declares the process name, environment
# overrides, and command-line arguments. Only placeholders are substituted:
#   {{recorder_base_url}}  -> recorder entry point
#   {{agent_home}}         -> the user's own agent home
#
# Profile keys:
#   command             Agent executable to launch (required)
#   agent_home          The user's own agent home ("~" is expanded)
#   recorder_base_url   Recorder entry point
#   launch.env          Environment overrides (object of string -> string)
#   launch.args         Command-line arguments (array of strings)
#   description         Human-readable notes; ignored by the launcher
#
# Usage:
#   scripts/run-agent.sh <profile> [extra args...]

import json
import os
import sys


def fail(message, code=2):
    print(f"error: {message}", file=sys.stderr)
    sys.exit(code)


def main():
    if len(sys.argv) < 2:
        print("usage: run-agent.sh <profile> [agent args...]", file=sys.stderr)
        print(
            "profiles are read from $BRIDLE_HOME_ROOT/<profile>/bridle-profile.json",
            file=sys.stderr,
        )
        sys.exit(2)

    profile = sys.argv[1]
    extra_args = sys.argv[2:]
    root = os.environ.get("BRIDLE_HOME_ROOT") or os.path.join(
        os.path.expanduser("~"), ".bridle-recording"
    )
    config = os.path.join(root, profile, "bridle-profile.json")

    if not os.path.isfile(config):
        print(f"error: profile '{profile}' not found at {config}", file=sys.stderr)
        names = sorted(
            name
            for name in os.listdir(root)
            if os.path.isfile(
                os.path.join(root, name, "bridle-profile.json")
            )
        )
        print(
            "available profiles: " + (" ".join(names) if names else "<none>"),
            file=sys.stderr,
        )
        sys.exit(2)

    try:
        with open(config, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError) as err:
        fail(f"cannot read profile {config}: {err}")

    command = data.get("command", "")
    if not isinstance(command, str) or not command:
        fail(f"profile '{profile}' is missing required 'command'")

    agent_home = data.get("agent_home", "") or ""
    recorder_base_url = data.get("recorder_base_url", "") or ""
    if agent_home == "~" or agent_home.startswith("~/"):
        agent_home = os.path.expanduser(agent_home)

    def subst(value):
        value = value.replace("{{recorder_base_url}}", recorder_base_url).replace(
            "{{agent_home}}", agent_home
        )
        if "{{" in value:
            fail(f"unresolved placeholder in {config}: {value}")
        return value

    launch = data.get("launch", {}) or {}
    env_overrides = launch.get("env", {}) or {}
    args = launch.get("args", []) or []
    if not isinstance(env_overrides, dict):
        fail(f"launch.env must be an object in {config}")
    if not isinstance(args, list):
        fail(f"launch.args must be an array in {config}")

    env = dict(os.environ)
    env.setdefault("NO_PROXY", "127.0.0.1,localhost")
    env.setdefault("no_proxy", env["NO_PROXY"])
    for key, value in env_overrides.items():
        if not isinstance(value, str):
            fail(f"launch.env.{key} must be a string in {config}")
        env[key] = subst(value)

    launch_args = []
    for index, item in enumerate(args):
        if not isinstance(item, str):
            fail(f"launch.args[{index}] must be a string in {config}")
        launch_args.append(subst(item))

    if os.environ.get("BRIDLE_LAUNCH_DEBUG") == "1":
        print(
            f"[run-agent] profile={profile} command={command} "
            f"agent_home={agent_home or '<unset>'} "
            f"recorder_base_url={recorder_base_url or '<unset>'}",
            file=sys.stderr,
        )
        print(f"[run-agent] env overrides: {env_overrides}", file=sys.stderr)
        print(f"[run-agent] args: {launch_args}", file=sys.stderr)

    os.execvpe(command, [command] + launch_args + extra_args, env)


if __name__ == "__main__":
    main()
