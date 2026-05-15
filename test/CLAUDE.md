# Test Infrastructure Guidance

Lessons distilled from setting up the G5d soak harness (commit `abedc3f0` — see `docs/soak/g5d-runbook.md` and `scripts/soak-vectorized.sh` for a worked example). Apply these to any new harness in this repo: integration suites under `test/`, chaos scenarios, perf rigs, soak runs.

## Diagnostic visibility — design the harness so failure leaves evidence

- **Tee the orchestrator log to disk from the very first command.** Put `exec > >(tee -a "${DIST}/run.log") 2>&1` immediately after the run dir is created. Without this, `set -e` exits leave only the tail of stdout and you debug blind.
- **Inspect every gRPC response, don't drain it.** When using streaming RPCs (e.g. `MeasureService.Write`), check `resp.GetStatus()` against `modelv1.Status_STATUS_SUCCEED.String()`. Non-success statuses are not errors at the transport layer — they're per-record rejections that disappear if you ignore the response.
- **Default to fail-fast in setup, fail-tolerant in the inner loop.** Setup phases (compose up, schema create, fixture seed, baseline record) should `set -e` so a 48 h run aborts in 5 minutes when misconfigured. Inner periodic loops (pprof grab, parity diff, log scrape) should `|| log WARN` so a single bad sample doesn't kill a long-running observation window.
- **`set -e` + bash arithmetic + multi-line `$()` capture is a silent-failure pattern.** `x=$(grep -c PAT file || echo 0)` returns `"0\n0"` when grep finds nothing, which `(( x == 0 ))` parses as a syntax error and silently treats as false. For anything beyond pure integer comparison, hand off to `python3 -c` or set `pipefail` and avoid the `|| echo` fallback.

## Container hygiene on a single-host (no swap, no orchestrator) test box

- **Set explicit `deploy.resources.limits.memory` and `cpus` on every service.** A 31 GB no-swap host OOM-kills immediately at the limit; defaults will eventually exceed it. Total compose footprint must leave at least 8 GB host headroom for the OS and IDE.
- **Java + SkyWalking agent + Spring Boot demo needs ≥ 1 GB even with `-Xmx384m`.** The non-heap JVM overhead (metaspace, code cache, native, classloading) routinely exceeds heap. 512 MB caps OOM-kill at startup before `-Xmx` is meaningful.
- **Containers default to root with mode 0700.** If the host shell needs to read or copy bind-mounted contents (snapshot/restore between phases), set `user: "${SOAK_UID:-1000}:${SOAK_GID:-1000}"` and export both from the orchestrator. Without this, `cp -a` from the host silently copies empty subdirs.
- **`expose:` is for inter-container only; clients on the host need `ports:`.** A `connection refused` on `localhost:<port>` from a host-side test driver almost always means the port is exposed but not published.
- **`docker compose down -v` is the correct teardown.** Plain `down` leaves volumes; subsequent `up` reuses them and you debug stale state.

## BanyanDB-specific timing and config semantics

These are the things you will assume wrong on first contact. Pin them in any harness that talks to BanyanDB:

- **Flush is asynchronous and ~5 s by default.** Both the schema-property server (`--schema-server-flush-timeout`) and measure data (`--measure-flush-timeout`) buffer writes for up to 5 s before persisting. Snapshot-immediately-after-write produces silently-empty snapshots. Either pass smaller flush timeouts (`--measure-flush-timeout=500ms` etc.) or have the test driver poll-until-queryable instead of sleeping for a fixed delay.
- **Query `Limit` defaults to 100.** A query against 1000 written rows returns 100 silently. Always set `Limit` explicitly when the test cares about totals; a parity check that "passes" by comparing 100 vs 100 truncations is meaningless.
- **Write timestamps must be millisecond-aligned.** The validator rejects sub-millisecond precision with `STATUS_INVALID_TIMESTAMP`. `time.Now()` has nanosecond precision and will be rejected — `time.Now().Truncate(time.Millisecond)` first.
- **Default root paths point at `/tmp`, not at any subdirectory.** `--measure-root-path` (and `--stream-root-path`, `--property-root-path`, `--trace-root-path`, `--schema-server-root-path`) all default to `/tmp`. The quick-start docker-compose mounts `/tmp/banyanDB` and ignores it; the binary still writes to `/tmp/measure/...`. For any harness that needs persistence, override every root path to your bind-mounted dir.
- **`FieldSpec` requires both `EncodingMethod` and `CompressionMethod`.** Creating a measure programmatically without setting these returns `compression method is unspecified`. Use `EncodingMethod_ENCODING_METHOD_GORILLA` + `CompressionMethod_COMPRESSION_METHOD_ZSTD` as a safe default for numeric fields.

## Fixture strategy

- **Self-seed deterministic data in tests; do not piggyback on OAP-managed measures.** OAP renames measures with downsampling suffixes (e.g. `service_traffic` becomes `service_traffic_minute` / `_hour` / `_day`) and groups them by bucket (`sw_metricsMinute` etc.). Targeting these is fragile across OAP versions and depends on traffic timing. A self-seeded `Group + Measure + N rows` is ~80 LOC of harness code, deterministic, and decouples the test from OAP's schema-install behavior.
- **Time-bound parity-check queries to a known data window.** If the harness keeps writing while the parity loop runs (e.g. background OAP traffic during a soak), bound the diff queries to `[T0, T_snapshot]` so subsequent writes don't pollute the comparison. The data inside the window stays identical; everything outside is invisible to the diff.

## Iteration discipline

- **Add instrumentation before re-running, not after.** A 25 min smoke gives you one observation per run. The cheapest path is to add the next probe (status logging, fixture verification, intermediate dump) on each iteration, then re-run. Shotgun fixes that change three things at once make root-causing the next failure impossible.
- **Budget multiple iterations on first contact.** A multi-component harness (compose + Go driver + bash orchestrator + binary protocol) routinely needs ~10 iterations before passing end-to-end. Document each fix inline (`# Reason: <why>`) so the next person doesn't repeat the discovery.
- **Smoke time should be << real run time.** A SMOKE preset of ~25 min for a 48 h soak (~30× compression) is the right ratio. If smoke is too fast, it skips the timing-sensitive bugs (flush, schema persistence). If it's too slow, you can't iterate.
