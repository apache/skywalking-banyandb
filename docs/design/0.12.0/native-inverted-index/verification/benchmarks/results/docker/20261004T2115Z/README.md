<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The ASF licenses
this file to You under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Q1–Q3 native operation benchmark rerun

- **Source:** commit `e123ec22` (`fix(index): complete native Q3 presence and range semantics`).
- **Image:** `banyandb-series-benchmark:94160e78-go12513-v6` (Go 1.25.13).
- **Container limits:** `--cpus=4 --memory=8g --memory-swap=8g --pids-limit=256 --cpuset-cpus=0-3 --network=none`; `GOMAXPROCS=4`.
- **Command:** `go test ./pkg/index/native ./pkg/index/nativeadapter -run '^$' -bench 'BenchmarkNative(StoredFields|SeriesIterator|MatchTerms|MatchField|Range)' -benchmem -benchtime=100ms -count=5`; plus `go test ./pkg/index/native -run '^$' -bench '^BenchmarkNativePartSeriesMap$' -benchmem -benchtime=100ms -count=5`.
- **Correctness:** each benchmark harness checks literal output counts/IDs/timestamps before timing. These are small operation fixtures, not macro throughput or scoring benchmarks. The live series-index path remains outside this operation benchmark.

The committed archive did not include generated protobuf Go files; the benchmark container used generated `api/proto/**/*.pb.go` files copied from the current worktree into the isolated read-only source. Their SHA256 manifest is `generated-proto-sha256.manifest`; they are not claimed to be commit-only provenance. The image immutable ID is in `container-image-id.txt`. Dependency download was performed in a separate network-enabled setup container; benchmark execution used `--network=none`.

## Five-sample medians

| Operation | Native median (ns/op, B/op, allocs) | Oracle median (ns/op, B/op, allocs) |
|---|---:|---:|
| StoredFields | 3,487; 1,408; 25 | 3,877; 3,441; 50 |
| SeriesIterator | 7,123; 2,520; 94 | 18,234; 4,193; 106 |
| MatchTerms | 30,814; 1,643; 68 | 54,378; 34,554; 265 |
| MatchField | 51,656; 6,289; 192 | 216,644; 366,727; 446 |
| Range | 51,601; 7,822; 201 | 206,261; 388,359; 473 |
| PartSeriesMap | 17,542; 5,152; 162 | 45,596; 9,667; 307 |

Raw Go benchmark output is in [`raw-bench.log`](raw-bench.log), with PartSeriesMap in [`raw-partseries.log`](raw-partseries.log). Results are
operation-level observations under the stated container controls; they do not
generalize to product throughput, full-series scans at production scale, or
macro service performance.
