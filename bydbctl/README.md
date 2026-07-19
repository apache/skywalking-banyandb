# BanyanDB Command Line Tool

`bydbctl` is a command line tool to interact with BanyanD.

## BYDBQL Agent TUI

`bydbctl agent` uses the Codex CLI app-server for multi-turn schema and query conversations in a three-page terminal workspace. bydbctl owns the controlled
tool bridge, deterministically renders BYDBQL, and requires approval before execution unless the user explicitly trusts the session. See the
[BYDBQL Agent TUI guide](../docs/interacting/bydbctl/agent.md).

## Build

```
# If you haven't generated the API code yet
make -C ../api generate

make build
```
