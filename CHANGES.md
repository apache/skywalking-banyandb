# Changes by Version

Release Notes.

## 0.12.0

### Features

- [Breaking Change] Remove the row-based query path from Stream, Measure and Trace. Every query executes through the vectorized columnar pipeline, and a distributed data node always answers with a native columnar frame. `--stream-vectorized-enabled`, `--trace-vectorized-enabled` and `--measure-vectorized-enabled` remain registered so existing command lines keep parsing, but they no longer select an engine: `=false` now fails fast at startup naming apache/skywalking#13998, and all three are removed in 0.13.0. Liaison nodes must still be upgraded before data nodes. See [Upgrading to 0.12](docs/operation/upgrade.md#upgrading-to-012).

### Document

- Add the [native inverted-index replacement design package](docs/design/archive/0.12.0/native-inverted-index/README.md), including the implementation specification, ICE walkthrough, research plan, and visual report.
- Add mandatory size and TDD-feasibility audits to the BanyanDB GitHub issue skill.

## 0.11.0
