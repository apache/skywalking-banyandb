# Shared native wildcard benchmark

[Recorded results](results.md) include all 18 samples and the measured source and container provenance.

This compares the existing `TermSetRequest.Wildcard` operation before and after
LIKE/ILIKE were moved onto a shared native wildcard engine. It measures compiler,
dictionary traversal, posting selection and selected-hit projection together.
It does not measure QL RPC latency, the complete DPO workload, or plan caching.

The unchanged harness is
[`wildcard_benchmark_test.go`](../../../pkg/index/native/wildcard_benchmark_test.go).
Both versions build one native in-memory index with 10,000 distinct neutral
names. Sixteen names start with `api-`; all other names start with `other-`.

| Arm | Pattern | Expected rows |
| --- | --- | --- |
| Prefix | `api-*` | 16 |
| Leading wildcard | `*api*` | 16 |
| Leading wildcard without a match | `*absent*` | 0 |

Index construction and one readiness query are excluded from timing. Every timed
query validates row identities and uses a fresh allocation scope with a 256-MiB
allowance. Closing the scope must release all reservations. Both versions use
this same harness and accounting callback. The candidate additionally accounts
for wildcard compilation and traversal allocations.

The preset campaign has three rounds of 100 queries per arm and revision. Each
sample runs in a fresh Docker container/process with two pinned CPUs, a two-CPU
quota, 4 GiB memory, no additional swap and `GOMAXPROCS=2`. Revision order
alternates between rounds. There is no repeat-until-pass rule or statistical
performance gate.

`B/op` and `allocs/op` are Go benchmark allocation measurements. `reserved-B/op`
is the conservative reservation retained by one query before closing its scope;
it is not allocated bytes or process RSS. A larger reservation after adding
tracking does not by itself mean more physical memory was allocated.

To reproduce, use the Go toolchain declared by the repository and an isolated
source archive at commit `b54a9fc76e74baa1c0ecbf75513a082c32649a72`, with generated
Go files available. The runner verifies the archive's tracked library Go sources
against that commit before copying the identical benchmark harness into it:

```sh
python3 verification/dpo85bench/wildcard-reuse/reproduce.py /path/to/baseline
```

`WILDCARD_BENCH_IMAGE` and `WILDCARD_BENCH_CPUS` select the locally available
benchmark image and CPU pair. The default image is
`banyandb-distributed-querybench:go1.26.10`; the default pair is `0,1`.

The runner preserves every sample, records source and binary hashes, checks for
source drift and refuses to overwrite an existing campaign. Full process logs
and binaries stay under ignored `logs/`; portable samples, summaries and
provenance are written beside this document.
