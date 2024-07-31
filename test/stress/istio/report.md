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

The Memory and Disk usage is improved due to moving indexed value from data files to index files. 

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

`alloc_bytes` 95th-percentile: 947.97 MB.
`heap_inuse_bytes` 95th-percentile: 1.10 GB.
`sys_bytes` 95th-percentile: 1.75 GB.
`stack_inuse_bytes` 95th-percentile: 20.58 MB.

```bash
Showing top 10 nodes out of 225
      flat  flat%   sum%        cum   cum%
   32.36GB  9.63%  9.63%    32.36GB  9.63%  github.com/blevesearch/vellum.(*unfinishedNodes).get
   26.01GB  7.74% 17.36%    26.01GB  7.74%  reflect.New
   16.32GB  4.86% 22.22%    16.32GB  4.86%  github.com/blevesearch/vellum.(*builderNodePool).Get
   13.81GB  4.11% 26.33%    13.81GB  4.11%  github.com/apache/skywalking-banyandb/banyand/measure.(*blockPointer).append
   13.74GB  4.09% 30.42%    13.74GB  4.09%  github.com/blugelabs/bluge/analysis.TokenFrequency
   12.63GB  3.76% 34.17%    12.63GB  3.76%  github.com/blugelabs/ice.(*interim).processDocument.func1.1
   10.44GB  3.11% 37.28%    10.44GB  3.11%  github.com/blevesearch/vellum.(*builderNodeUnfinished).lastCompiled
    9.42GB  2.80% 40.08%     9.42GB  2.80%  bytes.growSlice
    8.57GB  2.55% 42.63%    22.18GB  6.60%  github.com/apache/skywalking-banyandb/banyand/measure.(*writeCallback).handle
    8.40GB  2.50% 45.13%     8.40GB  2.50%  github.com/RoaringBitmap/roaring.(*Bitmap).Iterator
```

## Disk Usage

```bash
measure: 368 MB
measure/measure-default: 182 MB
measure/measure-default/seg-20240731: 182 MB
measure/measure-default/seg-20240731/shard-0: 147 MB
measure/measure-default/seg-20240731/shard-0/0000000000001b5c: 75 MB
measure/measure-default/seg-20240731/shard-0/0000000000001c0b: 76 kB
measure/measure-default/seg-20240731/shard-0/0000000000001c6c: 41 kB
measure/measure-default/seg-20240731/shard-0/0000000000001d37: 24 MB
measure/measure-default/seg-20240731/shard-0/0000000000001dae: 80 kB
measure/measure-default/seg-20240731/shard-0/0000000000001edc: 22 MB
measure/measure-default/seg-20240731/shard-0/0000000000001fad: 91 kB
measure/measure-default/seg-20240731/shard-0/0000000000001fb1: 218 kB
measure/measure-default/seg-20240731/shard-0/0000000000002018: 588 kB
measure/measure-default/seg-20240731/shard-0/0000000000002078: 23 MB
measure/measure-default/seg-20240731/shard-0/0000000000002081: 879 kB
measure/measure-default/seg-20240731/sidx: 36 MB
measure/measure-minute: 185 MB
measure/measure-minute/seg-20240731: 185 MB
measure/measure-minute/seg-20240731/shard-0: 80 MB
measure/measure-minute/seg-20240731/shard-0/0000000000000e4d: 52 MB
measure/measure-minute/seg-20240731/shard-0/0000000000000f21: 9.3 MB
measure/measure-minute/seg-20240731/shard-0/0000000000000ff8: 9.4 MB
measure/measure-minute/seg-20240731/shard-0/0000000000001068: 296 kB
measure/measure-minute/seg-20240731/shard-0/00000000000010cd: 9.1 MB
measure/measure-minute/seg-20240731/shard-1: 82 MB
measure/measure-minute/seg-20240731/shard-1/0000000000000de3: 51 MB
measure/measure-minute/seg-20240731/shard-1/0000000000000ead: 9.6 MB
measure/measure-minute/seg-20240731/shard-1/0000000000000f82: 9.1 MB
measure/measure-minute/seg-20240731/shard-1/0000000000000fb0: 2.1 MB
measure/measure-minute/seg-20240731/shard-1/0000000000000fe0: 2.3 MB
measure/measure-minute/seg-20240731/shard-1/0000000000000fe6: 1.4 MB
measure/measure-minute/seg-20240731/shard-1/0000000000000fe9: 162 kB
measure/measure-minute/seg-20240731/shard-1/0000000000000ff1: 383 kB
measure/measure-minute/seg-20240731/shard-1/0000000000001017: 1.8 MB
measure/measure-minute/seg-20240731/shard-1/000000000000101b: 229 kB
measure/measure-minute/seg-20240731/shard-1/0000000000001053: 2.8 MB
measure/measure-minute/seg-20240731/shard-1/0000000000001057: 231 kB
measure/measure-minute/seg-20240731/shard-1/0000000000001058: 74 kB
measure/measure-minute/seg-20240731/shard-1/0000000000001063: 477 kB
measure/measure-minute/seg-20240731/sidx: 24 MB
```

## Disk IO

| Metric              | 95th-percentile per second  |
|---------------------|-----------------------------|
| read_count          | 0.010000                    |
| merged_read_count   | 0.000000                    |
| write_count         | 20.978333                   |
| merged_write_count  | 25.373333                   |
| read_bytes          | 40.960000                   |
| write_bytes         | 2603731.626667              |
| io_time(ms)         | 13.360000                   |
| weighted_io(ms)     | 60.828333                   |
