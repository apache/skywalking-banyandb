# BanyanDB Agent Guide

BanyanDB is a distributed time-series database written in Go. Follow nearby code
and tests; keep changes focused on the requested behavior.

## Completion and scope

- Finish the requested change and relevant verification without asking about
  routine next steps. Stop when the requested outcome is verified or a concrete
  blocker requires user input; do not expand into unrelated fixes or redesigns.
- Preserve unrelated working-tree changes. Do not commit, push, publish, or change
  external resources unless requested or already authorized.
- Use independent subagents when their work saves time; small tasks do not need
  delegation or a separate review workflow.

## Go conventions

- `.golangci.yml` is authoritative for lint rules, protobuf import aliases,
  exclusions, and the 170-character line limit. `.golangci-format.yml` controls
  formatting with gci and gofumpt; its exclusions are not general lint exclusions.
- Import groups: standard library, third-party packages, then this repository.
- Avoid shadowing existing variables. Short names and `if err := ...` are valid
  when they do not shadow another variable; use descriptive names where useful.
- Check errors and add meaningful context with `%w` when wrapping. Preserve error
  identity and the relevant API's error-handling conventions.
- Document exported functions and types with concise, punctuated comments. Add
  implementation comments only for non-obvious logic.

## Correctness and validation

- Follow existing test conventions and run the smallest relevant suites first.
  Expand validation for affected integration boundaries, generated code, or shared
  infrastructure; avoid unrelated full builds during read-only or wording tasks.
- Preserve persisted-data and wire compatibility. For format, schema, or protocol
  changes, check existing data and mixed-version behavior as applicable.
- For concurrent or stateful changes, consider cancellation, resource ownership,
  bounded memory, and recovery. Use the repository's established helpers.
- Read design documents only for the subsystem or contract being changed. Follow
  any more-specific `AGENTS.md` in the affected directory.
