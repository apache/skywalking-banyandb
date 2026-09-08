# Changes by Version

Release Notes.

## 0.12.0

### Features

- Add logging related flags to the FODC proxy and agent. Every BanyanDB binary now shares the same logging flags and the matching `BYDB_LOGGING_*` environment variables.
- [Breaking Change] Remove the row-based query execution path from the Stream, Measure and Trace query engines. Measure TopN pre-aggregation is unchanged and still executes row-based. The `--stream-vectorized-enabled`, `--trace-vectorized-enabled` and `--measure-vectorized-enabled` flags stay registered but no longer select an engine; `=false` now fails fast at startup on standalone, data and liaison nodes. A query shape the vectorized engine cannot plan now returns an error instead of falling through to row execution. See [Upgrading to 0.12](docs/operation/upgrade.md#upgrading-to-012).

### Bug Fixes

- Fix FODC proxy `/metrics` returning partial data or timing out when concurrent scrapes overlap.

### Document

- Add the [native inverted-index replacement design package](docs/design/archive/0.12.0/native-inverted-index/README.md), including the implementation specification, ICE walkthrough, research plan, and visual report.
- Add mandatory size and TDD-feasibility audits to the BanyanDB GitHub issue skill.

### Chores

- Bump canopy and mcp npm dependencies to clear Dependabot CVEs (fast-uri, fastify, qs).

## 0.11.1

### Bug Fixes

- Enforce the Canopy readonly role on the `/monitoring/*` proxy the same way as `/api/*`.

## 0.11.0
