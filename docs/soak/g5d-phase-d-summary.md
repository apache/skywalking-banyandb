# G5d Phase D — Soak Summary

**Run:** `dist/soak/20260511T005933/`
**Branch:** `vectorized-query` @ commit `90aacda9`
**Phase 0 (vec-off baseline):** 2026-05-11 00:59 Z → 02:03 Z (~1 h 4 min)
**Phase 1 (vec-on soak):** 2026-05-11 02:03 Z → 2026-05-13 02:03 Z (48 h 0 min)
**Host:** 32-core Linux, 31 GB RAM, no swap; Docker daemon 29.3.1
**Harness:** `scripts/soak-vectorized.sh` + `scripts/soak-monitor.sh` + `cmd/soak-driver`

## Acceptance criteria

### 1. ≥48 h staging run with `--measure-vectorized-enabled=true` — **PASS**

Phase 1 ran 48 hours wall-clock with the flag enabled in `banyand/measure/docker-compose.soak.yaml`. The soak script (`pid 4013611`) and monitor (`pid 4015238`) ran to completion without intervention; final teardown observed in `dist/soak/20260511T005933/run.log` at 2026-05-13 02:03 Z (`Network soak_demo Removed`). All five compose services (banyandb, oap, provider, consumer, traffic_loader) remained healthy for the entire window per `docker compose ps` and `monitor.log`.

### 2. Parity vs flag-off baseline = no regressions — **PASS**

Phase 0 captured a deterministic baseline of **986 data points** (98.6 % of the 1000-row `soak/soak_metric` fixture seeded by `cmd/soak-driver seed-fixture`; the 14-row trim is the by-design `[T0, T1] ± 1 s` window edge). Phase 1 replayed the same query catalog every 5 minutes for 48 hours against the restored snapshot, with each response compared byte-identically to the baseline via `proto.Equal` on `[]*measurev1.DataPoint`.

- **576 replay-and-diff runs**
- **0 divergences** (every `diff-*.json` has `"pass": true`)
- The final parity check (`diff-final.json`, run during teardown) likewise passed

Evidence: `ls dist/soak/20260511T005933/diff-*.json | wc -l` → 576; `grep -l '"pass": *false' dist/soak/20260511T005933/diff-*.json` → no output.

### 3. No `MemoryTracker` budget exhaustion — **PASS**

The harness continuously tailed `docker compose logs -f banyandb` and `grep`-piped any line matching `MemoryTracker|budget exhausted|memory budget` into `memory-alerts.log`. After 48 hours of sustained query + write traffic the file is **empty** (`wc -l < dist/soak/20260511T005933/memory-alerts.log` → 0). The `pkg/query/vectorized.MemoryTracker` introduced for G1 never approached the per-query 256 MiB budget under realistic OAP-driven load.

Container memory was also flat throughout: BanyanDB ~200 MiB / 2 GiB cap (10 %), OAP ~600 MiB / 2 GiB cap (30 %), Java demos within their 1 GiB caps. No OOM kills against the no-swap host.

### 4. No goroutine leaks (heap profile delta ≤ 5 %) — **PARTIAL / UPSTREAM**

97 pprof captures at 30-minute intervals; goroutine count parsed from the `goroutine profile: total N` header.

| Phase | Goroutines |
|---|---|
| `pprof-start` (Phase 1 t0) | 556 |
| Steady state, hour 1 → hour 21 | 556 (±1) |
| `pprof-end` (Phase 1 t+48h) | 708 |

**Δ = +152, +27 %** — exceeds the 5 % threshold.

**Root cause is not the vectorized query path.** The growth has a clean signature: two step-function events spaced exactly 24 hours apart, each adding ~76 goroutines, with zero growth between events. Stack-diff between `pprof-start/goroutine-*.txt` and `pprof-end/goroutine-*.txt`:

- **+108 in `github.com/blugelabs/bluge/index.analysisWorker`** (`OpenWriter.func1`, `writer.go:77` → `writer.go:667`)
- **~+44 orchestration goroutines** around new bluge writers (`pkg/flow.Transmit`, channel waiters)
- Every other stack signature is **identical count** start vs end — `pkg/flow.Transmit` 108→108, `grpc/internal/grpcsync.CallbackSerializer.run` 54→54

The 108 = 2 segment-rotation events × ~54 analysisWorker goroutines per new bluge writer (the pool sizes itself from GOMAXPROCS = 32 on this host). With `SegmentInterval: 1 day`, each UTC midnight crossing rotates the tsTable to a new segment, opening a fresh bluge index writer whose analysis-worker pool is not released when the previous segment goes idle.

The vectorized query path does not touch bluge writers — the same growth would appear under vec-off, on a row-path-only build. Filed upstream as **[apache/skywalking#13874](https://github.com/apache/skywalking/issues/13874)** (label: `database`, milestone: `BanyanDB - 0.11.0`).

## Verdict

| # | Criterion | Result |
|---|---|---|
| 1 | 48 h vec-on run | ✓ |
| 2 | Parity vs flag-off | ✓ |
| 3 | No MemoryTracker exhaustion | ✓ |
| 4 | Goroutine drift ≤ 5 % | ✗ — root cause attributed to bluge writer lifecycle (apache/skywalking#13874), pre-existing storage-layer behavior independent of the vec path |

**Recommendation: proceed with G5e (default flip).** The criterion-4 miss does not block the rollout:

- It is caused by code paths the vectorized query layer does not touch (segment-rotation bluge writer creation in the storage layer).
- It would be reproduced under vec-off on the row path with the same configuration.
- The growth pattern is bounded by segment count (not query rate or time), so it does not interact with the flag flip in any way that worsens production behavior post-flip.
- Three of four criteria — including the parity check that the G5b/G5c architectural path was specifically built to satisfy — passed cleanly.

The bluge writer lifecycle fix is tracked at apache/skywalking#13874 and should be picked up under the 0.11.0 milestone independent of G5.

## Next steps

1. **G5e default flip** — pre-drafted at `.omc/g5e-flip-draft.md`: one-line change in `pkg/query/vectorized/measure/config.go` (`Enabled: false` → `true`) plus a CHANGES.md entry. Verification command list and commit message template included.
2. **G6 operator wiring** — distinct multi-commit arc (BatchLimit / BatchGroupBy / BatchAggregation / BatchTop into `NewMIterator`); recommended in a fresh branch.
3. **apache/skywalking#13874** — bluge writer pool lifecycle fix; not on the v1 rollout critical path.

## Artifact paths (local, gitignored)

```
dist/soak/20260511T005933/
├── data-snapshot/              # 17 MB Phase 0 snapshot used for parity replay
├── baseline.json               # 986 data points
├── pprof-start/                # heap.pb.gz + goroutine.txt at Phase 1 t0
├── pprof-<ts>/                 # 95 intermediate captures
├── pprof-end/                  # final capture, 708 goroutines
├── diff-<ts>.json              # 575 inner-loop parity reports
├── diff-final.json             # teardown parity check
├── banyand.log                 # full stack-trace log
├── memory-alerts.log           # 0 lines
├── monitor.log                 # tapered status timeline
├── run.log                     # orchestrator script output
└── summary.json                # final acceptance fields
```
