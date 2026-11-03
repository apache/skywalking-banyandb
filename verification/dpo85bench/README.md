# Indexed LIKE/ILIKE query benchmark

This focused harness runs real standalone BydbQL RPCs against 1,000 or 10,000
whole-value indexed names, with two timestamps per name. Exactly eight names
match both the LIKE and ILIKE predicates. A no-match pair exercises dictionary
work that produces no rows. Equivalent IN lists are derived independently from
the fixture outside timing; their name-discovery cost is deliberately excluded.
All arms execute the same scalar COUNT DISTINCT shape and check every result.

The unchanged exact-name query is compared on merged baseline
`b54a9fc76e74baa1c0ecbf75513a082c32649a72` and the candidate, using the identical
harness in separate fresh processes. It supplies a compatibility/performance
control, not a promise that dictionary pattern search is as cheap as exact IN.

The checked-in [results](results.md), [54 individual samples](benchmark-samples.json)
and [campaign provenance](benchmark-provenance.json) describe the original campaign
before the later review fixes. They are retained without retiming or claiming
measurements of the corrected source. Local `logs/` and `validation-logs/` directories
contain verbose output, binaries and source snapshots and are intentionally ignored
by Git. Verification reports summarize their evidence inline rather than linking to
files that disappear in a clean checkout.

The runner uses two pinned CPUs, 4 GiB memory with no additional swap, explicit
3 GiB native query allowance, three predetermined fresh-process rounds, rotated
feature-arm order and alternating baseline/candidate control order. It warms each
query once before timing 100 invocations. Binding, live index validation and
pattern construction remain inside the timed RPC. Setup and visibility checks
use QL; normal write/schema RPCs install the independent fixture.

Build with the repository's Go toolchain and generated API files available:

```sh
# Use the Go toolchain specified in go.mod and make generated API files available.
./verification/dpo85bench/reproduce.zsh /path/to/isolated-merged-baseline
```

The baseline directory is an isolated writable checkout/archive at the named
commit. The runner copies only this benchmark harness there. Logs contain
latency, process CPU per operation, allocations, tracked query peak bytes and
native input rows. Fixture setup, binary hashes, source revision, resource limits
and image identity are retained. Native input-row counts establish whether
nonmatching raw rows were scanned; native focused tests separately verify
bounded dictionary work and term expansion. The RPC harness does not expose
per-request dictionary counters and does not claim to measure them.

No timing percentage gate is imposed. Keep all three rounds, report dispersion,
and retain inconclusive controls as inconclusive. Do not rerun until a desired
percentage appears. Standalone timing includes the local RPC transport; the
separate generated distributed integration cohort verifies worker transport and
results, without claiming distributed latency from this harness.

The later shared-wildcard refactor has its own [narrow native comparison](wildcard-reuse/results.md)
and [final integration validation](wildcard-reuse/validation.md). Its timings use the unchanged
native wildcard request and must not be substituted for this historical QL RPC campaign.
