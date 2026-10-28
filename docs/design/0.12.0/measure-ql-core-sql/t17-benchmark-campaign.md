# T17 revised benchmark campaign

Run on 2026-10-18 after commit `1179c97a` (revised acceptance harness).
The only additional executable source was the opt-in
`qlb_t17_large_rpc_test.go` feasibility probe. Production execution was unchanged.

## Successful RPC timing

Five fresh sequential Docker repetitions used 4 CPUs, 8 GiB RAM, no additional
swap, and a 256 MiB query tracker. Source and binary fingerprints were checked
before and after every repetition. No build or other benchmark ran concurrently.
Each repetition contains the unchanged four timed variants at 1,024 and 32,768
rows. All **3,120 timed calls** passed exact response checks. All 40 report rows
pass performance protocol, overload evidence validation and recovery evidence
validation. Overload/recovery evidence is the previously verified 1,024-row
artifact, not a new overload run or a 32,768-row overload claim.

Values below are medians of five per-run measurements. CPU and allocations cover
the benchmark process (client, liaison and two data nodes), not just sorting.

| Rows | Variant | p50 ms | p95 ms | p99 ms | CPU ms/query | Allocated KiB/query |
|---:|---|---:|---:|---:|---:|---:|
| 1,024 | composed_workers_1 | 3.219 | 4.649 | 7.335 | 7.500 | 1131.75 |
| 1,024 | composed_workers_8 | 8.407 | 16.728 | 20.126 | 4.688 | 1195.69 |
| 1,024 | fullsort_slice | 5.886 | 8.960 | 9.964 | 13.300 | 2484.53 |
| 1,024 | topk | 4.127 | 6.027 | 7.018 | 10.300 | 1699.52 |
| 32,768 | composed_workers_1 | 39.390 | 47.543 | 52.138 | 70.938 | 27108.52 |
| 32,768 | composed_workers_8 | 111.438 | 165.082 | 209.454 | 56.875 | 25440.24 |
| 32,768 | fullsort_slice | 150.629 | 172.198 | 176.378 | 236.500 | 71730.04 |
| 32,768 | topk | 45.879 | 51.750 | 55.232 | 101.000 | 39976.67 |

Top-k/full-sort p50 ratios across paired repetitions were 0.680–0.734 at
1,024 rows and 0.298–0.314 at 32,768 rows. The median p50 values show about
30% and 70% lower latency respectively. This is a focused same-binary sort
comparison, not the full PR #1412 compatibility performance matrix or a new T02
gate. Historical timing used different response-retention rules and is not an
unchanged baseline for this run.

## Large-input feasibility

A separate untimed probe keeps the same container limits and 256 MiB tracker.
It verifies the complete fixture in bounded host/time chunks, then attempts raw
full sort, raw top-k and three composed-query bindings, each with one caller.
Each completed attempt checks cleanup and a same-cluster, same-sort-mode query
over a 1,024-row time range. Rejections never contribute timing samples.

- **100,000 rows:** readiness passed; all five owning requests returned
  `ResourceExhausted` from native `DrainPipelineToFrame` input materialization.
  Raw queries attempted 266,926,459 + 5,027,760 bytes against a 268,435,456-byte
  tracker. Composed requests reached the same tracker boundary. All five cleanup
  checks and all five exact recovery queries passed. This establishes bounded
  rejection/recovery, not successful large-query throughput.
- **1,000,000 rows:** fixture readiness failed before the owning requests.
  One bounded read had 1,024 expected and actual rows but different content
  hashes (`1e19a7d846aa78a` versus `b5ab7b4527068420`). The test failed after
  190.29 seconds. The readiness hash ignores row order and storage SID/version;
  the cause remains unresolved. The current error does not identify the chunk
  or differing tuple. No owning-query memory or performance conclusion is valid
  for this size, and it is not classified as an accepted resource rejection.

The next diagnostic for 1m is to capture the failing host/time chunk and first
differing tuple. Memory limits were not increased; spilling remains deferred.

## Reproduction and evidence

Build the querybench test binary with `-tags=querymetrics` in the fixed container,
then run `TestQLBT17LargeRPC` with `DQB_T17_LARGE_ROWS=100000` or `1000000` and
`RUN_DISTRIBUTED_QUERY_BENCH=1`, `DQB_IN_CONTAINER=1`, `DQB_ENGINE=measure`, and
`DQB_REPORT_DIR`. The test checks actual cgroup limits. Use a fresh process for
each size. The focused outcome-contract test and scoped querymetrics lint passed.

The five-repetition campaign used `run-t17-frozen.sh rpc`, with
`DQB_T17_BINARY=.omx/t17-campaign.test`,
`DQB_T17_REPORT_ROOT=.omx/t17-campaign-rpc`, and
`DQB_T17_OVERLOAD_REPORT=.omx/t17-revised-overload/t17-overload-1024.json`.

Local evidence:

- `.omx/t17-campaign-rpc/r{1..5}/`: complete JSON/Markdown reports, shards,
  raw latencies, CPU and heap profiles, and container logs.
- `.omx/t17-campaign-rpc/summary.json`: medians listed above.
- `.omx/t17-campaign-rpc/{source,binary}.sha256`: frozen provenance.
- `.omx/t17-campaign-large/t17-large-rpc-{100000,1000000}.json` and
  `.omx/t17-campaign-large-{100000,1000000}.log`: both successful rejection
  evidence and failed readiness evidence retained.

Source fingerprint:
`8aafe6301a8e09bf4b7d13f72f7ea335561ba94749762f848bed4de4758c7d87`.
Binary SHA-256:
`f24e6e790d4e5717680950334c1a0cae804b7819ac185c9aa87a160094b0f3de`.

## Subsequent readiness fix

The [million-row investigation](t17-1m-readiness-fix.md) resolved the content
mismatch as a storage merge buffer-ownership bug. With that fix, all one million
fixture rows verify correctly. The large queries then reach the unchanged memory
limit, with exact preflight/post-rejection bounded-query recovery and zero cleanup
counters. The original failed artifact above remains unchanged. This follow-up
was untimed and does not replace the five-repetition sort measurements.
