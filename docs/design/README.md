# Design Documents

Design documents are grouped by the milestone in which they were introduced. The current
milestone sits directly under this directory and holds live working documents; new design work
must be added there when it is introduced. When a milestone ships, its directory moves into
[`archive/`](archive/README.md).

## Current milestone — 0.12.0

- [Tag aggregation and time bucketing in the measure query engine](0.12.0/tag-aggregation/README.md)
- [Data export and import](0.12.0/data-export-import/README.md)
- [Docker-canonical build and license generation](0.12.0/docker-canonical-build/README.md)

The native inverted-index replacement design shipped and its legacy engine
dependency has since been fully removed; see
[`archive/0.12.0/native-inverted-index/README.md`](archive/0.12.0/native-inverted-index/README.md)
(archived ahead of the rest of 0.12.0, which has not shipped yet).

## Shipped milestones

See [`archive/`](archive/README.md) for 0.11.0 and earlier.
