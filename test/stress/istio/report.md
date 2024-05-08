# Testing Report

## Scenario

### SkyWalking Entities

Service: 256
Instances: 2048, 8 per service

### Demo cluster

Traffic RPS: 4352
VUS: 8192

### Machine Info

Machine and OS information:

- CPU: CPU: AMD EPYC 7B12, VendorID: AuthenticAMD, Family: 23, Model: 49, Cores: 8
- Memory: Total: 47176 MB
- OS: OS: linux, Platform: ubuntu, Platform Family: debian, Platform Version: 21.04

## Result

written 16186000 items in 38m38.657047557s
throughput: 6980.765015 items/s
throughput(kb/s) 1786.494948
latency: 2ns

The CPU usage and Disk IO is better than before due to the optimization of parts merge strategy.
But the disk space take a bit more than before, because the parts are not merged aggressively.

We also remove raw value from Series Index, which reduce the disk space of such index.

### Problem

Memory: Series Index(bluge) index building took most of the memory.

CPU: As memory's part, Series Index(bluge) index building took most of the CPU.

Next step: Optimize the Series Index(bluge) index building.

## CPU

CPU 95th-percentile: 1.24 cores (8 cores in total)

```bash
Showing top 10 nodes out of 326
      flat  flat%   sum%        cum   cum%
   229.15s  7.80%  7.80%    229.15s  7.80%  runtime/internal/syscall.Syscall6
   152.36s  5.18% 12.98%    436.89s 14.86%  runtime.mallocgc
   107.72s  3.66% 16.64%    321.02s 10.92%  runtime.scanobject
    93.73s  3.19% 19.83%    179.11s  6.09%  github.com/blevesearch/vellum.registryCache.entry
    87.88s  2.99% 22.82%    104.15s  3.54%  runtime.findObject
    68.76s  2.34% 25.16%     68.96s  2.35%  github.com/blevesearch/vellum.(*builderNode).equiv (inline)
    56.29s  1.91% 27.08%     56.29s  1.91%  runtime.nextFreeFast (inline)
    49.87s  1.70% 28.77%     49.87s  1.70%  runtime.memmove
    46.09s  1.57% 30.34%     69.86s  2.38%  runtime.(*mspan).writeHeapBitsSmall
    45.67s  1.55% 31.89%     45.67s  1.55%  runtime.memclrNoHeapPointers
```

## Heap Profile

`alloc_bytes` 95th-percentile: 1.32 GB.
`heap_inuse_bytes` 95th-percentile: 1.48 GB.
`sys_bytes` 95th-percentile: 1.90 GB.
`stack_inuse_bytes` 95th-percentile: 21.46 MB.

```bash
Showing top 10 nodes out of 220
      flat  flat%   sum%        cum   cum%
   33.04GB  9.72%  9.72%    33.04GB  9.72%  github.com/blevesearch/vellum.(*unfinishedNodes).get
   25.57GB  7.52% 17.24%    25.57GB  7.52%  reflect.New
   16.59GB  4.88% 22.12%    16.59GB  4.88%  github.com/blevesearch/vellum.(*builderNodePool).Get
   15.92GB  4.68% 26.80%    15.92GB  4.68%  github.com/apache/skywalking-banyandb/banyand/measure.(*blockPointer).append
   13.99GB  4.11% 30.91%    13.99GB  4.11%  github.com/blugelabs/bluge/analysis.TokenFrequency
   12.70GB  3.74% 34.65%    12.70GB  3.74%  github.com/blugelabs/ice.(*interim).processDocument.func1.1
   10.59GB  3.11% 37.76%    10.59GB  3.11%  github.com/blevesearch/vellum.(*builderNodeUnfinished).lastCompiled
    9.40GB  2.76% 40.53%     9.40GB  2.76%  bytes.growSlice
    8.53GB  2.51% 43.03%     8.53GB  2.51%  github.com/RoaringBitmap/roaring.(*Bitmap).Iterator
    8.15GB  2.40% 45.43%     8.15GB  2.40%  github.com/blugelabs/ice.(*interim).prepareDictsForDocument.func1.1
```

## Disk Usage

```bash
measure: 331 MB
measure/measure-default: 100 MB
measure/measure-default/idx-17cd0bcb17960000: 36 MB
measure/measure-default/shard-0: 64 MB
measure/measure-default/shard-0/seg-20240507: 64 MB
measure/measure-default/shard-0/seg-20240507/000000000000183a: 384 kB
measure/measure-default/shard-0/seg-20240507/0000000000001fa0: 72 kB
measure/measure-default/shard-0/seg-20240507/0000000000001fe4: 175 kB
measure/measure-default/shard-0/seg-20240507/0000000000002009: 49 MB
measure/measure-default/shard-0/seg-20240507/000000000000203f: 3.4 MB
measure/measure-default/shard-0/seg-20240507/0000000000002042: 140 kB
measure/measure-default/shard-0/seg-20240507/000000000000206f: 3.0 MB
measure/measure-default/shard-0/seg-20240507/0000000000002072: 140 kB
measure/measure-default/shard-0/seg-20240507/00000000000020b2: 4.2 MB
measure/measure-default/shard-0/seg-20240507/00000000000020b5: 164 kB
measure/measure-default/shard-0/seg-20240507/00000000000020db: 3.0 MB
measure/measure-minute: 231 MB
measure/measure-minute/idx-17cd0bcb17960000: 26 MB
measure/measure-minute/shard-0: 102 MB
measure/measure-minute/shard-0/seg-20240507: 102 MB
measure/measure-minute/shard-0/seg-20240507/0000000000000c01: 39 MB
measure/measure-minute/shard-0/seg-20240507/0000000000000e80: 26 MB
measure/measure-minute/shard-0/seg-20240507/0000000000000f2c: 6.9 MB
measure/measure-minute/shard-0/seg-20240507/0000000000000f2e: 113 kB
measure/measure-minute/shard-0/seg-20240507/0000000000000fc9: 12 MB
measure/measure-minute/shard-0/seg-20240507/0000000000000fcc: 119 kB
measure/measure-minute/shard-0/seg-20240507/0000000000001001: 61 kB
measure/measure-minute/shard-0/seg-20240507/0000000000001032: 2.8 MB
measure/measure-minute/shard-0/seg-20240507/000000000000106c: 6.6 MB
measure/measure-minute/shard-0/seg-20240507/0000000000001070: 246 kB
measure/measure-minute/shard-0/seg-20240507/0000000000001080: 1.1 MB
measure/measure-minute/shard-0/seg-20240507/00000000000010a2: 3.9 MB
measure/measure-minute/shard-0/seg-20240507/00000000000010bc: 1.4 MB
measure/measure-minute/shard-0/seg-20240507/00000000000010bd: 73 kB
measure/measure-minute/shard-0/seg-20240507/00000000000010d4: 1.2 MB
measure/measure-minute/shard-0/seg-20240507/00000000000010e2: 994 kB
measure/measure-minute/shard-1: 103 MB
measure/measure-minute/shard-1/seg-20240507: 103 MB
measure/measure-minute/shard-1/seg-20240507/0000000000000af5: 38 MB
measure/measure-minute/shard-1/seg-20240507/0000000000000e1b: 28 MB
measure/measure-minute/shard-1/seg-20240507/0000000000000ebc: 7.8 MB
measure/measure-minute/shard-1/seg-20240507/0000000000000ebf: 173 kB
measure/measure-minute/shard-1/seg-20240507/0000000000000f50: 11 MB
measure/measure-minute/shard-1/seg-20240507/0000000000000fbd: 3.1 MB
measure/measure-minute/shard-1/seg-20240507/0000000000000ff6: 6.8 MB
measure/measure-minute/shard-1/seg-20240507/0000000000000ff7: 57 kB
measure/measure-minute/shard-1/seg-20240507/0000000000001023: 62 kB
measure/measure-minute/shard-1/seg-20240507/000000000000107f: 7.8 MB
```

## Disk IO

| Metric              | 95th-percentile per second  |
|---------------------|-----------------------------|
| read_count          | 89.106667                   |
| merged_read_count   | 7.2866670                   |
| write_count         | 48.198333                   |
| merged_write_count  | 53.538333                   |
| read_bytes          | 1096663.04                  |
| write_bytes         | 8741847.04                  |
| io_time(ms)         | 42.82                       |
| weighted_io(ms)     | 408.57                      |
