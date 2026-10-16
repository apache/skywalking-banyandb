# T05 compiled measure QL plan reuse

Implementation base: `0bc1ab2b` (T03), on PR #1412. Verification uses Go 1.26.9.
This ticket adds liaison-local compiled-template reuse for parameterized legacy
and typed measure queries. No protobuf, native operator or frame format changes.

## Execution and ownership

The existing prepared-statement cache still parses exact query text. A separate
compiled cache retains immutable `MeasureTemplate` objects. A hit skips schema
analysis and recipe construction, then binds fresh parameters, captures one fresh
execution clock and constructs fresh requests and execution state. Authorization
evaluates each request's current snapshot and principal after lowering.

Each cached dependency includes its measure identity, revision and local watch
generation. Schema object, locator revision and generation are captured together;
the query gate's resolver must agree. Replacement at the same or a lower revision,
deletion and recreation invalidate old entries. Every requested group contributes
a dependency; unrelated measures do not invalidate a plan. Native index/topology
choices remain deferred to current native planning. Registry fallback executes
uncached when the local view cannot establish coherence.

Dependencies are checked on lookup, after compilation, before publication and
around binding. A moving schema gets one compilation retry, followed by uncached
execution under the normal gates. The request also bounds binding retries. An
exact-text reparse remains compatible when the separate prepared cache evicts its
statement. No parameter values, principals, clocks or permission decisions appear
in compiled keys or retained templates.

Default limits are 4,000 retained templates and 64 MiB of estimated plan bytes.
Both limits include evicted plans held by active requests. Eviction removes lookup
eligibility immediately; request handles retain templates through execution.
When admission cannot fit a candidate, it executes uncached with a held compile
reservation. Oversized candidates do not evict unrelated entries merely to fail
admission. Footprint estimates include syntax, recipes, maps, key text and
dependencies, deduplicate shared pointers/backing arrays and saturate on overflow.
They are estimates, not allocator-exact heap measurements.

Four compilation slots bound active work; at most 64 distinct shared compilations
are tracked, with separately bounded bookkeeping bytes. The first caller does
not own shared cancellation or carry request context values into compilation.
Waiters cancel independently; the last waiter cancels shared work. Abandoned work
remains accounted until cleanup but cannot accept new waiters. Failed candidates
are not cached. Shutdown cancels compilation and clears lookup residency.

Compile work reserves an estimate from the existing protector query budget before
private schema cloning and analysis. The estimate scales prepared syntax and
schema size for clones, maps, recipes and transient capacity. Published plans move
to the cache's bounded residency; uncached execution retains its reservation until
its final handle releases. Existing native query and batch budgets remain active.

Flags are `--bydbql-plan-cache-size` (zero disables reuse),
`--bydbql-plan-cache-max-bytes`, `--bydbql-plan-max-compiles` and
`--bydbql-plan-max-in-flight`. Plan outcomes appear separately from parse-cache
outcomes in QL debug/access logs. Bounded-label metrics report lookup outcomes,
compilation count, validation/compilation/instantiation durations, entries,
retained/compile/uncached bytes, waiters and shared work. Test hooks and process
observations are available only in `querymetrics` builds.

## Correctness verification

The QL generator adds 18 reuse processes with 57 independent response contracts
over an isolated 129-row fixture. The original 293 generated query cases remain
unchanged. Six rebind cohorts vary values with identical query text and compare
cached versus bypassed execution. They cover legacy/typed projections, clocks,
nullable outputs, lists and limits. Empty bound arrays and null time parameters
retain their existing rejection behavior, followed by successful recovery.

Lifecycle processes cover schema replacement/deletion/recreation, all multi-group
dependencies, unrelated updates, same/lower revision watches, changes during
compilation, literal bypass, actual registry fallback, byte/count admission,
eviction while executing, first/all waiter cancellation and authorization changes
between hits. The cold burst holds 32 callers at one compile barrier, asserts one
compile and 31 coalesced waiters, then limits downstream execution to four queries.
That isolates shared compilation from the native shared-memory admission limit.
Deletion visibility is established through the generated QL response, avoiding a
broader all-node schema-absence barrier unrelated to cache correctness.

RED logs include `.omx/t05-plan-cache-red.log`, generator/source/template RED
logs, and `.omx/t05-benchmark-red.log`. Later integration failures exposed an
unintended registry-error status change, which was corrected to preserve the
existing `Internal` transform contract. Explicit cancellation and compile-budget
errors retain their typed statuses.

Both final topology runs pass all 167 selected cases: 18 T05, 142 T03 and seven
T01/T02 cases, with no selected case skipped. Reports are
`.omx/t05-standalone-final.json` and `.omx/t05-distributed-v3.json`.

```sh
go test -tags=querymetrics ./test/integration/standalone/query \
  -ginkgo.label-filter='QLB-T01 || QLB-T02 || QLB-T03 || QLB-T05' \
  -ginkgo.json-report="$PWD/.omx/t05-standalone-final.json"
go test -tags=querymetrics ./test/integration/distributed/query \
  -ginkgo.label-filter='QLB-T01 || QLB-T02 || QLB-T03 || QLB-T05' \
  -ginkgo.json-report="$PWD/.omx/t05-distributed-v3.json"
```

Full normal and instrumented race suites pass for `./banyand/liaison/grpc`.
Normal and race suites also pass for `./pkg/bydbql`, the QL generator and the
query benchmark package. Source/template tests include immutable snapshot lifetime,
reparse compatibility, concurrent binding, schema races and footprint accounting;
cache tests include in-use retention, independent/last-waiter cancellation,
failed-compilation recovery and shutdown cleanup. Logs are
`.omx/t05-grpc-{normal,race}.log`, `.omx/t05-{normal,race}.log`,
`.omx/t05-plan-cache-race.log` and `.omx/t05-schema-final-{normal,race}.log`.

`make lint`, `make check-ql-test-cases`, the changed-package formatter diff and
`git diff --check` pass. The formatter emits an empty diff. The license-check
command exits successfully but reports zero checked files, so it is not evidence
of header coverage for this work. The 151
pre-existing generated cases with nine known failures are outside this selected
run; their cases and expectations remain exactly retained. T05 does not claim to
resolve their indexed string-array or empty entity-IN issues.

The separate raw-goroutine checker exposed an outdated pre-existing line-number
baseline. Newly added launches use `run.Go` or existing supervised/library helpers;
no old sites or baseline entries are changed by this ticket.

## Narrow benchmark protocol

The user's scope decision supersedes the original working-set/churn matrix.
Exactly two query shapes are measured with compiled caching enabled and disabled:
simple legacy projection and a typed projection with 16 value aliases plus eight
parameterized time comparisons. Both alternate two binding sets while retaining
the query text. Each returns 32 independently specified fixture rows.

P(T05) uses 1,024 rows, eight workers, 20 checked warmups and 1,000 timed RPCs per
variant. Cache settings use separate fresh Go processes and distributed clusters
with two data nodes. A parent process creates no cluster and combines the two
child reports into the same four variants. Five repetitions run in 4-CPU, 8-GiB Docker containers. Full fixture hashes
are checked outside timing; timed requests check row counts. Profiles, process
CPU/allocations and cache/resource counters accompany the raw latency samples.
Process totals include client, liaison and both data nodes; they are not planning
CPU measurements. No previous-ticket baseline, large dataset, working-set, churn
or additional worker-count scenario is included.

```sh
for repetition in 1 2 3 4 5; do
  DQB_ENGINE=measure DQB_SCENARIOS=qlb_t05 DQB_CARDINALITIES=1024 \
    DQB_QUERY_WORKERS=8 DQB_QUERY_ITERATIONS=1000 DQB_WARMUP_ITERATIONS=20 \
    DQB_PROFILE=1 DQB_REPORT_DIR="/work/.omx/bench-reports/t05-isolated-r${repetition}" \
    test/integration/distributed/querybench/run-docker.sh --cpus 4 --memory 8g
done
```

The focused planning microbenchmark includes fresh binding and native-request
construction, excluding parse and execution. It measures the same four variants,
with normal production accounting and instrumentation disabled.

```sh
go test ./banyand/liaison/grpc -run '^$' \
  -bench '^BenchmarkQLBPlanReuse$/^T05$' -benchmem -benchtime=2s -count=5
```

G3 requires zero timed compilations and 1,000 hits per warm cached variant,
compilation on every uncached request, and typed-wide median planning ns/op and
B/op at most 0.80 times uncached. Functional tests establish cold coalescing and
lifecycle bounds. Simple planning and RPC outcomes are reported separately;
planning savings do not by themselves establish an end-to-end latency benefit.

## Planning results and initial RPC observations

All five RPC repetitions and all 20 planning measurements completed under the
specified resource limits. Reports are `.omx/bench-reports/t05-r{1..5}`, with
individual logs `.omx/t05-rpc-r{1..5}.log`. Planning output is
`.omx/t05-planning-benchmark.log` and its extracted records are
`.omx/t05-planning-benchmark-clean.txt`. Combined observations are retained in
`.omx/t05-benchmark-summary.json`.

Planning medians across five measurements:

| Shape | Cache off, us/op | Cache on, us/op | B/op, off → on | Allocs/op, off → on |
|---|---:|---:|---:|---:|
| Simple legacy | 32.042 | 3.838 | 14,212 → 1,504 | 92 → 33 |
| Wide typed | 191.319 | 13.613 | 108,521 → 7,441 | 203 → 97 |

The typed-wide time ratio is **0.0712** and byte ratio **0.0686**, both below the
0.80 G3 target. Every warm cached RPC variant records exactly **1,000 hits and
zero compilations** during timing; each disabled variant records **1,000
compilations and zero hits**. Warmup compilation counts are one and 20 respectively.
At timing completion, waiters, in-flight work, compile reservations, uncached-plan
bytes and retired-plan bytes are zero. Native owner/pool-reference/reservation
deltas after execution close are zero. Warm cache residency is about 6 KiB after
the simple shape and 21 KiB after the wide shape, including readiness templates.

RPC medians across five repetitions:

| Shape/cache | p50, ms | p95, ms | p99, ms | Process CPU, s/1,000 RPCs | Allocated B/RPC |
|---|---:|---:|---:|---:|---:|
| Simple/off | 4.518 | 9.445 | 12.719 | 2.46 | 554,119 |
| Simple/on | 5.705 | 13.786 | 17.572 | 3.20 | 539,750 |
| Wide/off | 7.321 | 14.366 | 17.857 | 3.85 | 926,999 |
| Wide/on | 7.974 | 18.463 | 23.089 | 4.53 | 822,106 |

**This initial RPC experiment is confounded and cannot establish a cache effect.**
Cache-on median p50 is 26.3% higher for simple queries and 8.9% higher for wide
queries; p95 is 46.0% and 28.5% higher. Process CPU is 30.1% and 17.7% higher.
Allocation bytes decrease by 2.6% and 11.3%. Planning benefits are repeatable in
the isolated measurement, but do not establish a production RPC latency benefit.

Both settings perform identical measured native work: 64,000 native source/frame
rows, 2,000 native leaf calls and 32,000 output rows per 1,000 queries; typed
variants perform 1,000 projection calls. Physical storage-scanned rows remain an
unavailable metric, so these counters do not prove identical storage cost.

These initial repetitions ran cache-off before cache-on inside one test process.
The source audit found that `schemaRegistryRoster` appends every registry and
does not remove closed registries; `SchemaRegistry.Close` stops watchers and
connections but retains cache/handler references. The second cluster therefore
inherits closed-cluster object graphs. Mean post-GC live heap in the collected
profiles rises from 13.91 to 18.54 MiB for simple queries and 15.82 to 18.54 MiB
for wide queries, far exceeding the few KiB of compiled plans. This establishes
retained process state as a confounder, without proving it caused every observed
latency difference. The metadata lifecycle is outside T05 and remains unchanged.

The existing CPU profiles show cumulative planning CPU falling from 0.55 to
0.15 seconds for 5,000 simple RPCs and 1.41 to 0.36 seconds for 5,000 wide RPCs.
Dependency validation accounts for only 0.02/0.03 seconds; total mutex-stack
samples do not increase. Extra sampled cost occurs mainly in native query/runtime
and GC stacks. Background-owner work and joined shutdown workers do not provide
evidence of active old-cluster workers. These groups overlap and must not be
added as independent costs. Read-only analysis is retained in
`.omx/t05-profile-summary.json`.

The T05-only runner now isolates cache settings in fresh child processes and
combines their reports. The same five-repetition, four-variant RPC comparison is
repeated because of this diagnosed harness defect. No dataset, shape, worker
count or other benchmark scenario is added, and production cache code is unchanged.
The original observations above remain preserved; no run is selected according
to favorable latency. The normal-tag planning measurements do not create
registries and remain valid without repetition.

## Process-isolated RPC results

All five corrected repetitions pass the same four-variant checks. Each setting
executes in a fresh child process with a five-minute deadline. The parent creates
no cluster, verifies reported PIDs against the actual child PIDs and requires
different PIDs between cache settings. Environment overrides replace existing
values, and each child retains its logs, profiles and two-shape report. The
subprocess regression test verifies real process isolation and environment
replacement; normal, tagged race and lint checks pass for the harness change.

Corrected reports are `.omx/bench-reports/t05-isolated-r{1..5}` and their aggregate
is `.omx/t05-isolated-benchmark-summary.json`. The production cache code and
planning benchmark were unchanged between the initial and corrected RPC runs.

| Shape/cache | p50, ms | p95, ms | p99, ms | Process CPU, s/1,000 RPCs | Allocated B/RPC |
|---|---:|---:|---:|---:|---:|
| Simple/off | 4.285 | 9.087 | 11.839 | 2.40 | 555,165 |
| Simple/on | 4.171 | 8.819 | 12.361 | 2.32 | 543,185 |
| Wide/off | 6.558 | 12.811 | 15.251 | 3.50 | 926,890 |
| Wide/on | 6.211 | 12.054 | 14.979 | 3.27 | 826,812 |

Cache-on median p50 falls **2.7%/5.3%**, p95 falls **3.0%/5.9%**, process CPU
falls **3.3%/6.6%** and allocated bytes fall **2.2%/10.8%** for simple/wide
queries. Simple p99 rises **4.4%** while wide p99 falls **1.8%**. These are medians
of five fixed-workload observations, not a production latency guarantee or a
claim of statistical significance. The large initial RPC regression pattern
disappears after isolating process state; its observations remain preserved above.

All ten cached variants again record exactly 1,000 hits and zero timed compiles;
all ten disabled variants compile 1,000 times. Independent output/hash checks,
native-work parity and post-execution resource checks pass. Each corrected run
uses different child PIDs for off/on. Architectural reuse and the typed-wide
planning time/byte targets pass G3. Existing T02/T03 performance conclusions are
unchanged; no earlier-ticket benchmark matrix is rerun.
