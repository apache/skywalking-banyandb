# Changes by Version

Release Notes.

## 0.12.0

### Features

- Add logging related flags to the FODC proxy and agent. Every BanyanDB binary now shares the same logging flags and the matching `BYDB_LOGGING_*` environment variables.

### Bug Fixes

- Fix FODC proxy `/metrics` returning partial data or timing out when concurrent scrapes overlap.
- Bound Property, Stream, and Trace query allocations with shared memory admission and capacity hints so oversized limit/offset windows cannot force huge result buffers.
- Preserve list-all / max-limit queries (`limit=MaxUint32`) with incremental scan admission and result-count accounting.

### Document

- Add the [native inverted-index replacement design package](docs/design/archive/0.12.0/native-inverted-index/README.md), including the implementation specification, ICE walkthrough, research plan, and visual report.
- Add mandatory size and TDD-feasibility audits to the BanyanDB GitHub issue skill.

### Chores

- Bump canopy and mcp npm dependencies to clear Dependabot CVEs (fast-uri, fastify, qs).

## 0.11.1

### Bug Fixes

- Enforce the Canopy readonly role on the `/monitoring/*` proxy the same way as `/api/*`.

## 0.11.0
