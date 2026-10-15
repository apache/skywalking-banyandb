# T01 verification and baseline

Revision: `012115c56652691d4163f69d0460dfa9b064191f` plus the local T01 test changes,
verified on 2026-10-15 using Go 1.26.9. Production execution, protobuf source,
generated API contracts, native transport and persisted formats are unchanged.
The engine capabilities remain planned and unimplemented. The revised
[ticket plan](implementation-tickets.md) folds this completed verification work
into T02's shared batch-execution capability. This record preserves the original
T01 labels, scenario, measurements and scope; it does not mark T02 complete.

## Verification

T01's RED condition was the absent QL-only result-verification and `qlb_t01`
scenario contracts on the base: the existing QL helper checked only the response
variant. This is infrastructure coverage evidence, not a production defect claim.
The first focused build also exposed missing ignored generated protobuf files and
mocks. They were generated with existing buf plugins and a Go 1.26-built mockgen;
no generated source is part of the requested change.

The following focused checks passed:

```sh
go test ./test/cases/measure/data -run 'Test(CompareQL|VerifyQL)' -count=1
go test -race ./test/cases/measure/... -count=1
go test ./test/integration/distributed/querybench -race -count=1
# Repository-pinned v2.14.0, built with Go 1.26.9:
golangci-lint run --new-from-rev=HEAD \
  ./test/cases/measure/... ./test/integration/distributed/querybench/...
bash -n test/integration/distributed/querybench/orchestrate.sh
git diff --check
```

The verifier rejects wrong values, types, ordering, timestamps, names, nulls and
duplicate multiplicities, and rejects a wrong response variant or gRPC status.
Only SID, version and trace are ignored; timestamps remain significant. Floating
values have absolute tolerance `1e-12`; integers and protobuf value types are exact.
Review regressions also cover near-zero floats in both comparison modes and
overlapping tolerance matches under every two-row permutation. These failed
before the fixes. Float comparison now preserves the protobuf default zero,
and unordered comparison finds a complete one-to-one matching rather than
committing to the first approximate match. The focused tests and measure race
suite passed again after these changes.

Both real endpoint suites passed all four T01 cases: `legacy-select-golden`,
`legacy-parameter-rebind`, `invalid-ql-status` and `native-equivalence`.

```sh
TMPDIR=/mnt/d/qlb-t01-tmp go test ./test/integration/standalone/query \
  -v -count=1 -timeout=30m -run '^TestPropertyIntegrationQuery$' \
  -ginkgo.label-filter='measure-ql && QLB-T01' -ginkgo.fail-on-empty
TMPDIR=/mnt/d/qlb-t01-tmp go test ./test/integration/distributed/query \
  -v -count=1 -timeout=30m -run '^TestPropertyQuery$' \
  -ginkgo.label-filter='measure-ql && QLB-T01' -ginkgo.fail-on-empty
```

Standalone selected 4 of 943 specs: 4 passed, 0 failed (14.863 seconds).
Distributed selected 4 of 626 specs: 4 passed, 0 failed (28.557 seconds).
The other specs were excluded by the requested label filter. Raw logs are local
at `.omx/bench-reports/qlb-t01-e2e/{standalone,distributed}.log`.

The initial standalone attempt failed before executing specs because the root
filesystem was 97–98% full and storage rejected writes with `STATUS_DISK_FULL`.
Using the workspace-backed temporary directory above resolved that environment
failure without changing production configuration. The Docker orchestrator also
keeps temporary files and its compiled binary on mounted report storage.

Fixture F is minute-relative within retention and aligned to the epoch 2-second
grid. It uses the existing two-shard, two-replica group; the distributed suite has
two data nodes. The six-row golden additionally verifies an obsolete row version
is replaced and replicas do not multiply output. Before the liaison assertions,
the distributed suite queries each data node directly through the existing
cluster RPC. Each node must independently return all six golden rows with
version 2, proving actual placement and replacement of the obsolete version.

After the review fixes, standalone passed 4/4 cases again (14.806 seconds),
and distributed passed 4/4 (29.838 seconds). The distributed log records six
version-2 rows from each of two distinct node addresses, followed by 28 vectorized
dispatches and 10 frames encoded/decoded. Updated raw logs:

- `.omx/bench-reports/qlb-t01-e2e/standalone-review-fixes.log`;
- `.omx/bench-reports/qlb-t01-e2e/distributed-node-observation.log`.

The fixes affect verification only; the benchmark workload and production path
are unchanged, so the five retained benchmark repetitions below remain the baseline.
The querybench race suite also passed again. Full lint of the measure cases,
both query integration packages and querybench reported zero issues after
reordering the new report fields to satisfy the repository's alignment check.

## Docker baseline

All five prescribed Docker repetitions passed both cardinalities and report merge.
The final reports contain 40 verified variant records and 40,000 timed real RPCs,
with 80 nonempty CPU/heap profile artifacts.
The prescribed command is executed sequentially for `r1` through `r5`:

```sh
DQB_ENGINE=measure DQB_MODES=vec \
DQB_SCENARIOS=qlb_t01 DQB_CARDINALITIES=1024,100000 \
DQB_QUERY_WORKERS=1 DQB_QUERY_ITERATIONS=1000 DQB_WARMUP_ITERATIONS=20 \
DQB_PROFILE=1 DQB_REPORT_DIR=/work/.omx/bench-reports/qlb-t01-r1 \
test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
```

Each repetition starts isolated scenario/cardinality processes. Within each process,
`native`, `native_rebound`, `legacy_ql` and `legacy_ql_rebound` execute sequentially
on one cluster with the same query projection and two tag bindings. Later variants
can observe prior cache/heap residency. Each emits exactly 1,000 timed requests,
latency percentiles, throughput, process-wide CPU/allocation observations and
CPU/heap profiles. Setup, visibility waits, expected-response hashing and sampled
full-result verification are outside measurement. The first, every hundredth and
last response are retained for exact validation after profiling stops; retained
sample memory remains part of the process-wide heap observation.

The output is 64 rows per binding at input cardinality 1,024 and 6,300 rows at
100,000. Expected checksums include timestamps, value types, names and values,
ignoring only SID/version. Unavailable leaf-call, operator-retention, lease,
post-close reservation and planning/cache counters are explicitly marked
unavailable rather than recorded as zero. No G2 enforcement claim is made by T01.

Local raw evidence is retained at:

- `.omx/bench-reports/qlb-t01-r{1..5}/distributed-querybench.{json,md}`;
- each repetition's `shards/` and `profiles/` subdirectories;
- `.omx/bench-reports/qlb-t01-logs/r{1..5}.log`.

Docker-created artifacts are normalized to the host user's ownership after runs.
This captures the unchanged-production baseline for later tickets; it does not
claim an optimization speedup or a comparative G1 pass.


Median-of-five and full run ranges (not confidence intervals):

| Input rows | Variant | p50 ms median [min,max] | p95 ms median [min,max] | Bytes/query median [min,max] |
| ---: | --- | ---: | ---: | ---: |
| 1024 | legacy_ql | 1.575 [1.520, 1.697] | 2.233 [2.153, 2.431] | 762743.408 [760768.472, 764810.896] |
| 1024 | legacy_ql_rebound | 1.580 [1.500, 1.730] | 2.213 [2.149, 2.475] | 761328.864 [759395.008, 763434.432] |
| 1024 | native | 1.587 [1.563, 1.658] | 2.285 [2.180, 2.351] | 759833.024 [758406.664, 761151.456] |
| 1024 | native_rebound | 1.536 [1.508, 1.683] | 2.187 [2.118, 2.459] | 759160.400 [757599.424, 761767.688] |
| 100000 | legacy_ql | 40.805 [40.613, 45.477] | 54.125 [53.956, 67.560] | 26879289.688 [26825820.008, 26916267.088] |
| 100000 | legacy_ql_rebound | 40.956 [40.673, 41.658] | 54.801 [53.953, 55.836] | 26836215.472 [26809974.040, 26884845.520] |
| 100000 | native | 40.762 [40.373, 44.971] | 54.672 [53.543, 59.322] | 26840344.920 [26815747.768, 26950654.744] |
| 100000 | native_rebound | 40.991 [40.570, 42.814] | 54.482 [53.758, 57.456] | 26865790.080 [26862034.600, 26906064.904] |


The ranges expose run-to-run variation, especially the first larger-cardinality
run. These observations freeze a baseline; they do not establish stable relative
performance between native and QL or against a prior revision. Future G1 comparisons
must use matching machine/container, dataset, schema, concurrency and Go build.

Host-readable local merged JSON reports:
[r1](../../../../.omx/bench-reports/qlb-t01-r1/distributed-querybench.json),
[r2](../../../../.omx/bench-reports/qlb-t01-r2/distributed-querybench.json),
[r3](../../../../.omx/bench-reports/qlb-t01-r3/distributed-querybench.json),
[r4](../../../../.omx/bench-reports/qlb-t01-r4/distributed-querybench.json),
[r5](../../../../.omx/bench-reports/qlb-t01-r5/distributed-querybench.json).
The [local summary](../../../../.omx/bench-reports/qlb-t01-logs/summary.md) and
per-run Markdown reports provide the same table and detailed percentiles, QPS,
allocation records and profile paths. Raw artifacts are local ignored files;
this document records the reproducible commands and measured summary in the repo.

Environment: Linux amd64, Go 1.26.9, Docker image
`banyandb-distributed-querybench:go1.26`, cgroup v2, 4-CPU quota and 8-GiB memory
limit on a host reporting 32 logical CPUs. No competing test/build workload was
run during timing. Existing periodic schema-registry activity remains included
in the process-wide observations.
