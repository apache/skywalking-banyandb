# Changes by Version

Release Notes.

## 0.12.0

### Features

- [Breaking Change] Remove the row-based query path from Stream, Measure and Trace. The `--stream-vectorized-enabled`, `--trace-vectorized-enabled` and `--measure-vectorized-enabled` flags stay registered but no longer select an engine; `=false` now fails fast at startup. See [Upgrading to 0.12](docs/operation/upgrade.md#upgrading-to-012).

### Document

- Add the [native inverted-index replacement design package](docs/design/archive/0.12.0/native-inverted-index/README.md), including the implementation specification, ICE walkthrough, research plan, and visual report.
- Add mandatory size and TDD-feasibility audits to the BanyanDB GitHub issue skill.

### Chores

- Bump canopy and mcp npm dependencies to clear Dependabot CVEs (fast-uri, fastify, qs).

## 0.11.0
