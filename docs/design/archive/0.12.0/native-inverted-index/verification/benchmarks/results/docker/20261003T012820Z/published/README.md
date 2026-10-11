<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The ASF licenses this
file to you under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain a
copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
-->

# Docker native-series benchmark results (bounded run)

> **Publication note:** These are historical, bounded observations. Scored query rows and native-vs-legacy performance claims are withdrawn; the reports below do not claim a completed native query cutover or a native win.

**Source:** `94160e78199e4f95a71a957663d686b4fe02bf9d` (worktree `nativefix`; clean), Go `go1.25.13`, Linux amd64. **Container limits:** `--cpus=4 --memory=8g --memory-swap=8g --pids-limit=256 --cpuset-cpus=0-3 --network=none`, `GOMAXPROCS=4`. Host preflight showed 32 CPUs, 33,638,424,576 bytes RAM and no swap; Docker 29.5.0.

Raw files and inspect metadata are in the external benchmark artifact storage. The image is `banyandb-series-benchmark:94160e78-go12513`, based on the repository build image containing exact Go 1.25.13. Harness and image digests are in the external benchmark artifact archive.

## Correctness smoke

Both modes completed a smoke run (`seed=100`, `large=1000`, one duplicate pass) with visible counts 100 and 1000, duplicate full-scan count 1000, and identical scan checksum `b6d0c27e51ec516d`. Exact hit queries returned 1 and exact miss returned 0. Native and legacy used the same canonical fixture digest.

## Workload A: duplicate pilot (3 paired repetitions)

Each process seeded 10,000 canonical series and submitted 100 passes × 10,000 duplicate attempts in batches of 100, with one writer worker (`workers=1`; `GOMAXPROCS=4` is not four writer goroutines). Seed callbacks were 100/100 and duplicate callbacks were 0/0 (the production `InsertIfAbsent` path returns without a persisted callback for all-existing duplicate batches). Visible count stayed 10,000 in every run. Warm duplicate mean latency per pass and derived attempts/s (10,000 attempts divided by pass wall time):

| pair | native mean pass | legacy mean pass | native attempts/s | legacy attempts/s | native/legacy latency |
|---:|---:|---:|---:|---:|---:|
| 1 | 6.455 ms | 6.426 ms | 1,549,261 | 1,556,279 | 1.005 |
| 2 | 6.659 ms | 6.399 ms | 1,501,671 | 1,562,809 | 1.041 |
| 3 | 6.796 ms | 6.618 ms | 1,471,522 | 1,510,993 | 1.027 |

Across 3 pairs: native 6.636 ms/pass vs legacy 6.481 ms/pass (1.024× native/legacy); this is a 3-pair pilot with high variance, not statistical evidence of a 2.4% effect. Duplicate output callback count was zero and logical count was unchanged. Per-pass allocations were recorded (native roughly 2.7–3.3 MB, legacy roughly 2.2–2.3 MB), but process CPU/RSS and writer-level accepted/new-segment/manifest counters were not instrumented. Seed/final `Stats` bytes and file counts were captured, but changed under asynchronous merge (native final files 32/22/14 and legacy 7/9/9), so this is **not** a proof of physical no-op. No idle-control subtraction was available in this temporary harness; classify physical idempotency as unverified. Treat this as a pilot measurement, not a 10-repetition confidence claim.

## Corrected physical-observability duplicate check

A corrected v5 harness reran one 10K/100-pass run per mode with directory paths measured at the actual run root (not `/`) and seed/final snapshots. Seed/final snapshots were native: `Stats` bytes 756,984→652,475, directory bytes 758,922→652,475, files 51→12; legacy: 1,207,016→1,044,127, directory bytes 1,207,402→1,044,127, files 13→5. Duplicate per-pass `StatsCount` deltas stayed zero, but `StatsBytes`, file counts, and directory bytes changed during asynchronous merge; therefore physical no-op/idempotency is **unverified**, not proven. Duplicate callbacks remained zero and visible count remained 10,000.

## Workload B: 1.2M query/scan

The first native full run used the required 1,200,000 canonical fixture under the fixed 8 GiB cap and was OOM-killed by cgroup at 16m29s (exit 137). The harness was then corrected to stream canonical input in 100-document batches while retaining only three query targets and a SHA-256 digest; the corrected native stream run was still OOM-killed at 15m17s (exit 137), reaching approximately 7.2 GiB under the fixed 8 GiB cap. Both OOM runs occurred during dataset construction before any exact-hit, miss, or full-scan sample was emitted; no native query was executed. This establishes a native dataset-construction capacity failure, not a query latency result.

The legacy full run completed with the original harness in 119.65 seconds (exit 0), consumed all 1,200,000 IDs, and produced scan checksum `ec8f51b1ef121606`. Three fresh-process reuse reads against its committed fixture completed without reseeding; exact hits returned 1, misses returned 0, and all scans returned 1,200,000 with the same checksum. The corrected stream harness reports `Expected: 0` for misses.

A common-input probe was prepared from the exact committed legacy fixture: source and destination manifests each had 24 files and 105,758,941 bytes with identical manifest SHA-256; the independently completed legacy scan had count 1,200,000. The probe mounted the destination at the exact `query-native-0/sidx` path used by `StoreOpts.Path`, with no fresh empty directory. Native open/read was bounded to 10 minutes; it emitted no result before the container was stopped, so it is classified timeout/incomplete rather than a correctness failure. Thus there is no valid native-versus-legacy query ratio.

Legacy-only reopened-reader reads (three fresh processes, no reseed) completed
full scans in 0.968–1.095 s (1.096–1.240 M series/s). The exact-hit latency
rows were removed because the old request implicitly enabled scoring and are
not a no-score membership baseline. All three scans consumed exactly
1,200,000 IDs with checksum `ec8f51b1ef121606`.

The corrected generator no longer retains the 1.2M input document slice; it streams production-encoded documents and retains only query targets plus digest. Native still exceeded the cap, so this is not solely an input-fixture-retention artifact. Do not raise memory or silently substitute a smaller dataset. The 1.2M native result is an explicit resource/correctness failure, while legacy read numbers are descriptive only.

### Separate code-path investigation

The routing/ownership evidence and static performance audit are recorded separately in [`code-audit.md`](code-audit.md), with scratch notes at `<external-scratch>/series-benchmark/code-audit.md`. The historical measurements above are unchanged.

## Reproduction

Build (network-enabled setup only, source mounted read-only):

```sh
docker run --rm --name series-bench-setup --cpus=4 --memory=8g --memory-swap=8g --pids-limit=256 \
  --network=bridge -v "$NATIVEFIX:/src:ro" -v "$RUN:/run" -w /bench \
  banyandb-series-benchmark:94160e78-go12513 sh -c \
  'go mod download && go build -trimpath -o /run/artifacts/series-benchmark-12513c /bench/main.go'
```

Benchmark (network disabled):

```sh
docker run --rm --name series-bench-dup-native \
  --cpus=4 --memory=8g --memory-swap=8g --pids-limit=256 --cpuset-cpus=0-3 \
  --network=none -e GOMAXPROCS=4 -v "$NATIVEFIX:/src:ro" -v "$RUN/artifacts:/out:ro" \
  -v "$OUT:/data" banyandb-series-benchmark:94160e78-go12513 \
  /out/series-benchmark-12513c -mode native -workload duplicates -seed 10000 -passes 100
```

No credentials, Docker socket, privileged mode, drop-caches, or broad cleanup were used. Generated benchmark directories remain outside the repository in the external benchmark artifact archive.
