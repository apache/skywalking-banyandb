# QG3 exact decimal SUM verification

This narrow benchmark compares ordinary integer `SUM(value)` with
`SUM(DECIMAL_FROM_SCALED(value, 10))` through the real BydbQL Query RPC.
Both use identical integer inputs, time ranges, eight entities, and one scalar
or eight grouped result rows. This measures the complete decimal query path,
including its input projection and final decimal-string encoding; it does not
isolate the cost of wider addition. Integer outputs remain integers.

## Reproduction

From the QG3 worktree root, run `./verification/qg3bench/reproduce.zsh`.
The script uses Docker with two pinned CPUs (0–1), a 4 GiB memory/swap limit,
Go 1.26.10, `GOMAXPROCS=2`, and `querymetrics` instrumentation in every run.
It expects the module/build cache mounts specified in that script. The measured
host was Linux on an AMD EPYC 7B13, on 2026-10-27.

There are three fresh processes per size, with query order rotated/reversed
between rounds. Each process warms and verifies all four parameterized queries,
then measures 1,000 requests per case for 64 rows or 300 requests for 8,192 rows.
Every result is checked against independently calculated totals. Fixture setup,
visibility polling, cache checks, and execution counters are outside timing.
Timed work includes the in-process client/server, response decoding and result
checking. CPU and allocations are process-wide, so include background server
work. Fixture-registration retries in the logs occur before measurement.

All 24 samples passed. Cache counters confirmed hits without compilation for all
four query forms. No native aggregate merge was recorded: these queries use
local aggregation. Tracked reservations returned to zero after every validation
request. The engine was held unchanged during the retained measurements.
Development measurements before the final memory-accounting and lint cleanup
were replaced, not combined with these results. A digest of runtime Go sources
confirmed that the final measured implementation remained unchanged throughout
the six runs. Raw output is retained in [logs](logs/).

## Measurements

These are medians of the three process samples, not confidence intervals.

| Input rows | Query | Latency ms/RPC | CPU ms/RPC | Bytes/RPC | Allocations/RPC |
| ---: | --- | ---: | ---: | ---: | ---: |
| 64 | Scalar integer SUM | 0.514 | 0.698 | 106,186 | 1,124 |
| 64 | Scalar decimal SUM | 0.711 | 0.888 | 114,478 | 1,181 |
| 64 | Grouped integer SUM | 0.575 | 0.811 | 144,527 | 1,436 |
| 64 | Grouped decimal SUM | 0.653 | 0.910 | 159,367 | 1,543 |
| 8,192 | Scalar integer SUM | 1.889 | 2.323 | 529,391 | 1,189 |
| 8,192 | Scalar decimal SUM | 1.926 | 2.378 | 537,434 | 1,244 |
| 8,192 | Grouped integer SUM | 2.632 | 3.093 | 576,045 | 1,501 |
| 8,192 | Grouped decimal SUM | 2.776 | 3.262 | 591,674 | 1,606 |

The median **within-round** decimal/integer latency ratios and observed ranges
are below. Ratios of the table's medians can differ from these paired ratios.

| Input rows | Scalar ratio (range) | Grouped ratio (range) |
| ---: | --- | --- |
| 64 | 1.385 (1.018–1.767) | 0.984 (0.773–1.154) |
| 8,192 | 1.044 (1.019–1.139) | 1.044 (1.043–1.099) |

Both larger cases show added latency in all three rounds, with median paired
ratios near 1.044. The small scalar case also runs slower in all rounds, but its
1.018–1.767 range is wide: these measurements do not establish a small or stable
cost for that case. The small grouped latency comparison remains uncertain.
Decimal queries consistently allocate more and require wider aggregate state.
This is a cost comparison for exact wider sums and decimal output, not a claimed
speed improvement. The host was shared with unrelated test processes; CPU
pinning limits this benchmark but does not reserve those CPUs exclusively.
These observations are not a statistical performance gate, a comparison against
the pre-QG3 engine, a production capacity estimate, or a distributed benchmark.
No additional rounds were selected to obtain a favorable verdict.

## Tracked memory

The integer accumulator plus count consumes 16 bytes per group; decimal uses
32 bytes (24-byte signed coefficient plus 8-byte count). Measured scalar state
was 16 versus 32 bytes; eight-group state was 128 versus 256 bytes, for both
fixture sizes. This excludes grouping keys, batches and expression buffers.

The existing peak counter is a process-wide high-water mark that cannot be
reset per query. For example, round zero reported 1,709,444 → 1,729,940 bytes
after scalar integer/decimal validation at 64 rows, and 1,877,336 → 1,908,056
after grouped validation. At 8,192 rows the corresponding cumulative readings
were 18,102,852 → 18,123,348 and 20,335,928 → 20,366,840 bytes. These are
**not isolated per-query peaks or peak deltas**: previous queries contribute
to each high-water mark. The raw logs retain all readings and zero-live checks.

## Independent numeric oracle

[postgres-oracle.sql](postgres-oracle.sql) was executed against PostgreSQL
17.6; [postgres-oracle.log](postgres-oracle.log) records the actual output.
It checks the same 13-row decimal fixture used by generated QL tests: ten
maximum `numeric(18,10)` values in one group, negative values, NULLs,
HAVING, numeric ordering, empty/all-NULL sums, cancellation, and safe mixed
integer/decimal sums. The first group's scaled sum exceeds int64 and yields
`999999999.9999999990`. Ordinary integer SUM must continue to reject that
coefficient overflow.

```sh
docker run -d --rm --name qg3-postgres-oracle --memory=512m --cpus=1 \
  -e POSTGRES_PASSWORD=qg3 -e POSTGRES_DB=qg3 postgres:17.6-bookworm
# Wait for pg_isready, then:
docker exec -i qg3-postgres-oracle psql -U postgres -d qg3 -At -v ON_ERROR_STOP=1 \
  < verification/qg3bench/postgres-oracle.sql
docker stop qg3-postgres-oracle
```
