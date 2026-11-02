# Bounded membership and count scans

Large bound IN/NOT IN lists formerly expanded into per-element expression nodes.
Complete count queries also retained timestamp-order scan frontiers even when
row order could not affect their result. This change stores bound string
membership compactly, skips input proven empty, and selects series-order scans
only after proving count and final grouping-order eligibility. Query-owned decode
slots retain their reservations while reusable; pinned batches keep ownership
until their last release. No admission limit, protobuf or persisted format changes.

The [scan-path contract](../measure-scan-path-v2/README.md#24-ql-count-scans-and-reusable-decoded-storage)
describes eligibility and buffer ownership. Unit tests cover NULL and mixed
membership, conservative empty proofs, hidden aggregates, group-key ordering,
repeated decode use, retained batches and cleanup. Generated QL-only cases use
independent answers and both standalone and distributed RPC suites.

## Validation on current main

The delivery branch is based on `2d9f456d5` (including the newer query-budget
policy). Protobufs and mocks were regenerated from that source, not copied from
the historical measurement checkout. A test-only lease implements the added
Refund interface as a no-op to preserve its monotonic-credit reuse assertion.

- Affected engine package tests pass with querymetrics: `pkg/bydbql`,
  `pkg/query/ql`, `pkg/query/vectorized/measure`, its `plan` package,
  `banyand/measure`, `banyand/liaison/grpc` and `banyand/query`.
- Generated `QL-P1` passes four specs and all 48 verification RPCs in each of
  standalone and distributed mode, with zero capacity rejections. These are
  correctness qualifications, not fresh performance measurements.
- The full `test/cases/measure/qlgen` test suite passes.
- Generated fixture drift check, scoped golangci-lint/revive, changed-Go-file
  formatting, import boundaries and repository license headers pass.

The RPC cohort uses `P1_VERIFY=1`, `-ginkgo.label-filter=QL-P1` and
`-ginkgo.fail-on-empty`; distributed also sets `P1_EXPECT_WORKERS=2`.

## Historical measured evidence

Raw runners, manifests, logs and profiles remain on the separate
[evidence branch](https://github.com/hanahmily/skywalking-banyandb/tree/ae95292e9/verification).
They are excluded from this PR's implementation diff. Each report pins its own
source and budgets. These measurements predate upstream query-budget changes in
#1450 and are not performance or capacity claims for the rebased PR.

- [Membership and empty-input comparison](https://github.com/hanahmily/skywalking-banyandb/blob/ae95292e9/verification/qlcapacitybench/improvement-results.md):
  five same-query process pairs; IN 10k mean process p50 fell 56.240 to 41.946 ms,
  and allocations fell 31.395 to 8.729 MB/RPC. Small-IN controls had measured
  latency increases; the report preserves them and their uncertainty.
- [Count-scan and buffer reuse](https://github.com/hanahmily/skywalking-banyandb/blob/ae95292e9/verification/qlcapacitybench/series-results.md):
  48 capacity requests and 568 compatibility specs passed per topology;
  10,000 timed RPCs passed. Previously rejected standalone NOT IN/CASE count
  workloads became admitted under unchanged budgets.
- [Shard/stage and distinct capacity](https://github.com/hanahmily/skywalking-banyandb/blob/ae95292e9/verification/qlcapacitybench/p2-results.md):
  distributed and actual hot/warm/cold unions pass; eight broad standalone
  SUM/combined requests retain native-budget rejections.
- [Patterns](https://github.com/hanahmily/skywalking-banyandb/blob/ae95292e9/verification/dpo85bench/p3followup/results.md),
  [composition](https://github.com/hanahmily/skywalking-banyandb/blob/ae95292e9/verification/qg2bench/p4/results.md)
  and [decimals](https://github.com/hanahmily/skywalking-banyandb/blob/ae95292e9/verification/qg3bench/p5/results.md):
  bounded correctness/cost/ownership checks pass; ordinary baseline comparisons
  remain inconclusive. No additional optimization was justified.
- [Complete export](https://github.com/hanahmily/skywalking-banyandb/blob/ae95292e9/verification/qlcapacitybench/p6/results.md):
  1k output passes on both topologies; standalone 10k rejects twice during sort
  candidate retention under the historical 96 MiB monotonic lease. Recovery
  passes and 100k is deliberately unattempted. That lease policy has since changed
  upstream; this is historical evidence, not the current capacity limit.

No complete-export capacity guarantee, flat page latency, retained-result session,
or total/truncation metadata is introduced. In particular, these investigations
are not universal deployment acceptance.
