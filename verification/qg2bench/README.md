# QG2 RPC benchmark

The harness starts a standalone server and writes 64 rows across eight entities.
The integer `value` and `prior + 1` inputs have identical values. It verifies the
scalar and per-entity results against independently calculated totals before
benchmarking. The entity-constrained float cases use the same values.

The isolated run was performed on 2026-10-27 with Linux on an AMD EPYC 7B13,
Go 1.26.10, Docker pinned to CPUs 0 and 1, a 4 GiB memory and swap limit, and
`GOMAXPROCS=2`. From the repository root, the exact command was:

```sh
docker run --rm --name qg2-benchmark --cpuset-cpus=0,1 --memory=4g --memory-swap=4g -e GOMAXPROCS=2 -e GOCACHE=/gocache -e GOPATH=/go -v "$PWD:/workspace" -v /home/gao/go/pkg/mod:/go/pkg/mod -v /home/gao/.cache/go-build:/gocache -w /workspace golang:1.26.10 /bin/sh -c 'go test ./verification/qg2bench -run "^$" -bench BenchmarkQG2RPC -benchtime=2s -count=1 -timeout=10m' > /tmp/qg2-benchmark-cache.log 2>&1
```

The complete output is in [cache-comparison.log](cache-comparison.log). The
[literal-bypass.log](literal-bypass.log) file preserves the earlier literal-only
run. Literal queries bypass the prepared-statement cache; parameterized cases
are repeated after a verification request primes their cache entry.
Latency is wall-clock time per RPC. CPU is process user plus system CPU per
operation, including the in-process client and server. Allocations likewise
include client and server work and result verification in the timed loop.

| Mode and case | Latency (ns/op) | CPU (ns/op) | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| literal/entity_float_sum | 690,598 | 1,023,794 | 228,535 | 1,785 |
| literal/entity_float_coalesce | 892,935 | 1,241,225 | 268,185 | 2,221 |
| literal/scalar_sum | 646,980 | 870,405 | 159,097 | 1,789 |
| literal/scalar_coalesce | 816,554 | 1,076,838 | 200,236 | 2,202 |
| literal/scalar_computed | 811,016 | 1,064,108 | 203,523 | 2,240 |
| literal/grouped_sum | 776,846 | 1,048,166 | 214,541 | 2,256 |
| literal/grouped_coalesce | 1,000,105 | 1,309,519 | 274,934 | 2,947 |
| literal/grouped_computed | 1,028,538 | 1,354,595 | 280,741 | 2,995 |
| parameterized/entity_float_sum | 498,874 | 801,531 | 166,193 | 1,045 |
| parameterized/entity_float_coalesce | 513,665 | 830,367 | 175,995 | 1,104 |
| parameterized/scalar_sum | 501,954 | 686,054 | 108,496 | 1,169 |
| parameterized/scalar_coalesce | 523,332 | 709,081 | 116,451 | 1,217 |
| parameterized/scalar_computed | 527,526 | 720,658 | 120,490 | 1,241 |
| parameterized/grouped_sum | 564,069 | 796,012 | 146,248 | 1,477 |
| parameterized/grouped_coalesce | 569,539 | 814,616 | 156,978 | 1,538 |
| parameterized/grouped_computed | 573,068 | 829,918 | 163,334 | 1,571 |

These are one 2-second measurement per case, with no statistical comparison or
performance gate. Queries run after fixture readiness scans and repeated timed iterations; this is not a true cold-cache measurement. The actual execution
path was not instrumented: the entity-constrained float composition is eligible
for native pushdown, but the run does not establish that pushdown occurred.
Integer SUM over this hour-long range is ineligible for the typed native integer
SUM pushdown; a plain single SUM may instead use the legacy native path.

The generated six-row QG2 fixture was also checked against PostgreSQL 17.6.
`postgres-oracle.sql` contains the independent aggregate queries and
`postgres-oracle.log` records their output. PostgreSQL's `SUM(bigint)` returns
`numeric`; the comparison covers values and NULL/empty behavior, with BydbQL's
existing checked int64 result type retained.

```sh
docker run -d --rm --name qg2-postgres-oracle -e POSTGRES_PASSWORD=qg2 -e POSTGRES_DB=qg2 postgres:17.6-bookworm
docker exec -i qg2-postgres-oracle psql -U postgres -d qg2 -At -v ON_ERROR_STOP=1 < verification/qg2bench/postgres-oracle.sql
docker stop qg2-postgres-oracle
```

To verify compiled-plan caching separately, an instrumented narrow run was
performed with `querymetrics`. After each parameterized composition warmup, the
harness checks that a further request increases the compiled-plan cache hit
counter without increasing its compile counter. The check passed for the float,
scalar, and grouped COALESCE cases and the scalar and grouped computed-input
cases. Its complete output is in [cache-observe.log](cache-observe.log). The
instrumented measurements are separate from the uninstrumented comparison above.

```sh
docker run --rm --name qg2-cache-observe --cpuset-cpus=0,1 --memory=4g --memory-swap=4g -e GOMAXPROCS=2 -e QG2_CACHE_OBSERVE=1 -e GOCACHE=/gocache -e GOPATH=/go -v "$PWD:/workspace" -v /home/gao/go/pkg/mod:/go/pkg/mod -v /home/gao/.cache/go-build:/gocache -w /workspace golang:1.26.10 /bin/sh -c 'go test -tags=querymetrics ./verification/qg2bench -run "^$" -bench "BenchmarkQG2RPC/parameterized/(scalar_coalesce|scalar_computed|grouped_coalesce|grouped_computed)$" -benchtime=1s -count=1 -timeout=10m -v' > /tmp/qg2-cache-observe.log 2>&1
```

## Paired follow-up (2026-10-27)

Five fresh-process baseline/candidate pairs ran per fixture size, alternating their
order each round. Each process warmed and checked each parameterized query before
measurement; counters verified a compiled-plan hit without another compile for
every case. Native merge counters also confirmed both eligible float SUM forms
used native reduction. Native merge was not recorded for the integer and
computed-input cases; computed inputs use local aggregation.
All binaries used `querymetrics`, including the baseline, so these figures include
instrumentation overhead. Fixtures contain 64 or 8,192 rows in eight groups with
unique entity/timestamp pairs and the same aggregate values for `value` and
`prior + 1`. Each sample used 1,800 RPCs for 64 rows or 500 for 8,192 rows.
Each result was checked against the independently calculated expected value in
the timed loop; setup and warmup were excluded, while result decoding, validation,
and sorting are included. CPU and allocations cover client and server in the
same process. Containers were isolated from each other and limited to CPUs 0–1,
4 GiB, and GOMAXPROCS=2. The larger fixture completed within that budget.

The table shows the median of five process samples for each metric; latency and
CPU are milliseconds per RPC. Raw, unfiltered output is in [paired-logs](paired-logs/).

| Rows | Version | Query | Latency ms | CPU ms | Bytes/op | Allocs/op |
| ---: | --- | --- | ---: | ---: | ---: | ---: |
| 64 | baseline | float_sum | 0.508 | 0.777 | 164,377 | 1,030 |
| 64 | baseline | scalar_sum | 0.493 | 0.683 | 109,421 | 1,165 |
| 64 | baseline | group_sum | 0.546 | 0.766 | 147,536 | 1,471 |
| 64 | current | float_sum | 0.511 | 0.797 | 164,561 | 1,031 |
| 64 | current | scalar_sum | 0.527 | 0.727 | 110,372 | 1,185 |
| 64 | current | group_sum | 0.572 | 0.807 | 148,562 | 1,488 |
| 64 | current | float_coalesce | 0.517 | 0.823 | 174,898 | 1,094 |
| 64 | current | scalar_coalesce | 0.522 | 0.713 | 118,699 | 1,228 |
| 64 | current | scalar_computed | 0.537 | 0.730 | 122,878 | 1,249 |
| 64 | current | group_coalesce | 0.576 | 0.812 | 159,451 | 1,542 |
| 64 | current | group_computed | 0.579 | 0.824 | 165,882 | 1,575 |
| 8192 | baseline | float_sum | 0.665 | 0.981 | 167,758 | 1,109 |
| 8192 | baseline | scalar_sum | 1.821 | 2.256 | 536,380 | 1,208 |
| 8192 | baseline | group_sum | 2.915 | 3.454 | 584,785 | 1,519 |
| 8192 | current | float_sum | 0.659 | 0.946 | 162,242 | 1,101 |
| 8192 | current | scalar_sum | 1.817 | 2.242 | 536,667 | 1,192 |
| 8192 | current | group_sum | 2.857 | 3.354 | 577,721 | 1,503 |
| 8192 | current | float_coalesce | 0.662 | 0.962 | 172,180 | 1,163 |
| 8192 | current | scalar_coalesce | 1.854 | 2.293 | 541,623 | 1,240 |
| 8192 | current | scalar_computed | 2.022 | 2.480 | 611,182 | 1,271 |
| 8192 | current | group_coalesce | 2.736 | 3.227 | 590,664 | 1,564 |
| 8192 | current | group_computed | 2.920 | 3.443 | 662,456 | 1,605 |

The following ratios are the **median of five within-round ratios**, followed by
the observed minimum and maximum; they are not ratios of the table medians.
Baseline comparisons pair alternating fresh processes within each round. These
ranges describe the five observations, not confidence intervals.

| Rows | Comparison | Median ratio | Observed range |
| ---: | --- | ---: | ---: |
| 64 | float_coalesce / float_sum | 1.016 | 0.922–1.084 |
| 64 | scalar_coalesce / scalar_sum | 1.003 | 0.959–1.073 |
| 64 | scalar_computed / scalar_sum | 1.006 | 0.968–1.095 |
| 64 | group_coalesce / group_sum | 1.006 | 0.984–1.118 |
| 64 | group_computed / group_sum | 1.024 | 0.949–1.069 |
| 64 | float_sum: current / baseline | 1.017 | 0.976–1.081 |
| 64 | scalar_sum: current / baseline | 1.074 | 0.970–1.105 |
| 64 | group_sum: current / baseline | 1.037 | 0.948–1.065 |
| 8192 | float_coalesce / float_sum | 1.005 | 0.952–1.124 |
| 8192 | scalar_coalesce / scalar_sum | 0.990 | 0.954–1.075 |
| 8192 | scalar_computed / scalar_sum | 1.099 | 1.057–1.126 |
| 8192 | group_coalesce / group_sum | 0.969 | 0.930–0.989 |
| 8192 | group_computed / group_sum | 1.044 | 0.986–1.071 |
| 8192 | float_sum: current / baseline | 0.980 | 0.755–1.019 |
| 8192 | scalar_sum: current / baseline | 1.006 | 0.714–1.087 |
| 8192 | group_sum: current / baseline | 1.008 | 0.794–1.045 |

On 8,192 rows, computed scalar SUM had a median paired latency ratio of 1.099
(range 1.057–1.126); the added per-row expression also increased median allocated
bytes from 536,667 to 611,182 per RPC. Direct float COALESCE retained native
merge and had a median ratio of 1.005 (0.952–1.124). All ordinary SUM
candidate/baseline ranges include 1.0. The 64-row scalar SUM median ratio is
1.074, with a range of 0.970–1.105, so this comparison cannot establish either
the presence or absence of a small baseline regression. The results support
proceeding to QG3 with the computed-input cost documented; they do not establish
a performance guarantee or resolve smaller possible regressions. No fixed
threshold or statistical significance is inferred from five pairs.

Reproduce from the candidate root with `zsh verification/qg2bench/reproduce-paired.zsh`.
Set `QG2_BASELINE_WORKTREE` if the baseline checkout differs from
`/tmp/qg1-baseline-check`; it must be at commit
`111193cab562b2d120a3e003b609f22923c9ff55` and contain the ignored generated
protobuf and mock files needed for builds. The script copies the benchmark files
into that checkout and writes raw logs under `/tmp`. The harness was reformatted
and its setup factored into helpers after measurement; query, sample and fixture
behavior are unchanged.
