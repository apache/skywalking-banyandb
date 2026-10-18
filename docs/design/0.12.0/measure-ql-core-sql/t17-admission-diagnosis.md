# T17 concurrent admission diagnosis

Investigated on 2026-10-18 after committing the legacy integration repairs as
`b4be414f`. All Catalog B feature tickets are implemented; this follows up the
remaining 32-caller acceptance blocker. No production code, memory limits,
protector policy, protobuf or benchmark acceptance rules changed.

## Finding

The 32-caller failure is a legitimate shared-pool rejection under the current
native receive reservation policy. It is independent of top-k state and can occur
before any input rows are read. Thirty-two clients are not a promise that all
32 queries can simultaneously retain their maximum allowed transport buffers.

The fixed Docker container has four CPUs, 8 GiB memory and no additional swap.
The benchmark starts two data nodes and one liaison in the same process. Each
node creates its own protector and query pool; these are not one combined pool
for the test process. The following calculation concerns the liaison pool.

| Budget or reservation | Value |
|---|---:|
| Default protector limit: 75% of 8 GiB | 6 GiB |
| Query pool: protector limit / 4 | 1,536 MiB |
| Per-query admission ceiling: pool / 8 | 192 MiB |
| Separate vectorized tracker ceiling | 256 MiB |
| Initial query admission charge | 1 MiB |
| Fixed workspace for two streams | 640 KiB |
| Per-stream maximum message allowance | 8,317,610 bytes |
| Two-stream receive reservation | 50,561,020 bytes |
| Admission plus receive reservation | 49.218746 MiB |

`QueryBudget.AdmitMeasureContext` charges the initial 1 MiB.
`NativeReceiveScope.ReserveFanout` uses the smaller remaining tracker/admission
allowance: 191 MiB here. Each stream needs a 64-KiB static receive window and
256-KiB parser workspace. One quarter of the remaining allowance covers three
payload copies, with the message cap divided between the two streams:

```text
available = 192 MiB - 1 MiB
fixed = 2 * (64 KiB + 256 KiB)
message_cap = floor((available - fixed) / 4 / 3 / 2)
receive_reservation = fixed + 3 * message_cap * 2
32 * (1 MiB + receive_reservation) = 1,574.999878 MiB > 1,536 MiB
```

Additional allocations before fanout cannot improve this bound: they increase
parent commitment while only reducing the receive allowance by approximately a
quarter of that increase. Local tracker releases also do not refund the parent's
cumulative lease charge; parent reservations remain owned until transport/query
completion. This is intentional ownership accounting, not evidence of a leak.

The reservation covers the permitted maximum message, not the actual 1,024-row
fixture size. Dropping the reservation while retaining the same receive cap would
undercount possible transport memory. A different reservation/transport strategy
would require a separate optimization with large-response and concurrency proof.
This investigation does not introduce one or relax protection to pass a gate.

## Reproduction and controls

The existing `TestQLBT17Admission` was rerun at `b4be414f` with `querymetrics`,
`DQB_T17_ADMISSION_ROWS=1024`, `RUN_DISTRIBUTED_QUERY_BENCH=1`,
`DQB_IN_CONTAINER=1` and `DQB_ENGINE=measure` inside the fixed Docker container.
The test fails in 8.99 seconds at `composed_workers_32` with:

```text
ResourceExhausted: ... vec distributed plan: broadcast: query memory pool exhausted
```

Cleanup succeeds. Evidence:
`.omx/t17-next-admission-baseline.log` and
`.omx/t17-next-admission-baseline/t17-admission-1024.json`.

The existing `TestQLBT17ConcurrencyControl`, enabled by
`DQB_T17_CONCURRENCY_CONTROL=1` with the same limits, runs the forced-full-sort
reference. It records 32 peak client calls, 95/96 completed queries, the same
broadcast rejection and successful cleanup. The diagnostic test itself exits
successfully because it records this expected control result; the report's
correctness remains `blocked`, and `throughput_evidence` is false. Evidence:
`.omx/t17-next-fullsort-control.log` and
`.omx/t17-next-fullsort-control/fullsort32-control/t17-admission-1024.json`.

An ignored minimal executable probe exercises the real `QueryBudget`,
`MemoryTracker`, `NativeReceiveScope` and transport ownership with a static
6-GiB protector. It holds transport scopes open to remove scheduling ambiguity:

- 31 receive fanouts are admitted; fanout 32 returns `query memory pool exhausted`.
- The failed reservation rolls back its local tracker charge.
- Local scope/lease closes retain transport-owned pool reservations correctly.
- Closing all transports returns the shared pool to zero.
- A subsequent query succeeds; its close returns the pool to zero again.

The probe and exact byte results are retained as
`.omx/t17-admission-contract-probe/main.go` and `result.json`. This isolates the
cause without query parsing, data ingestion or sorting. The normal 32-client
benchmark can occasionally succeed through staggered completion; such a run
would not disprove this simultaneous-residency bound.

## Acceptance consequence

The existing strict benchmark remains `partial_known_32_blocked`; this diagnosis
does not convert a rejected query into a successful timing sample or retroactively
pass T17. It identifies why mandatory success at 32 clients is not a stable
capacity-independent gate under the fixed reservation policy.

The user approved the [revised acceptance criteria](implementation-tickets.md#concurrent-admission-acceptance-revised-2026-10-18)
on 2026-10-18: compare full sort/top-k on fixed, fully admitted timing cohorts;
check 32-caller overload separately for explicit `ResourceExhausted`, complete
cleanup and subsequent recovery. The subsequent [harness implementation](t17-acceptance-verification.md) adds
separate overload/recovery verification and same-cluster RPC recovery. Historical
reports retain their original verdicts; the minimal probe above remains evidence
of admission recovery rather than an RPC recovery test. A requirement for 32 simultaneously resident queries would
instead require a separately justified transport/reservation optimization.
The existing 1/8-caller measurements remain evidence for their recorded revisions.
Large-input RPC/full compatibility performance evidence and historical T02
acceptance remain separate outstanding work. Spilling and retained-result
pagination sessions remain deferred.
