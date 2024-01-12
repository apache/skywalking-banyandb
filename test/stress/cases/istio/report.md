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

written 16186000 items in 38m43.221090505s
throughput: 6967.051077 items/s
throughput(kb/s) 1782.985321
latency: 2ns

### Problem

Memory: blockPointer.append used 22.55% of total memory, that is significantly larger than expected.

Disk IO: weighted_io(ms) is 504.606667, which is larger than 100ms. This means a lot of IO operations are blocked.

## CPU

CPU 95th-percentile: 2.74 cores (8 cores in total)

```bash
Showing top 10 nodes out of 291
      flat  flat%   sum%        cum   cum%
   657.01s 10.54% 10.54%    657.01s 10.54%  runtime/internal/syscall.Syscall6
   242.03s  3.88% 14.43%    457.06s  7.33%  github.com/blevesearch/vellum.registryCache.entry
   211.69s  3.40% 17.82%    218.18s  3.50%  github.com/klauspost/compress/zstd.(*fseEncoder).buildCTable
   173.92s  2.79% 20.61%    697.34s 11.19%  runtime.mallocgc
   172.26s  2.76% 23.38%    172.75s  2.77%  github.com/blevesearch/vellum.(*builderNode).equiv (inline)
   164.02s  2.63% 26.01%    188.35s  3.02%  runtime.findObject
   131.81s  2.12% 28.13%    169.79s  2.72%  github.com/klauspost/compress/zstd.(*fastEncoder).EncodeNoHist
   127.66s  2.05% 30.17%    127.66s  2.05%  runtime.memmove
   123.01s  1.97% 32.15%    141.11s  2.26%  github.com/klauspost/compress/huff0.(*Scratch).huffSort
    99.42s  1.60% 33.74%    134.90s  2.16%  github.com/blevesearch/vellum.(*builderNodeUnfinished).lastCompiled
```

From the top 10 list, we can see that the CPU is mainly used by `Syscall6`.

## Heap Profile

`alloc_bytes` 95th-percentile: 968.94 MB.
`heap_inuse_bytes` 95th-percentile: 1054.00 MB.
`sys_bytes` 95th-percentile: 1445.43 MB.
`stack_inuse_bytes` 95th-percentile: 18.03 MB.

```bash
Showing top 10 nodes out of 212
      flat  flat%   sum%        cum   cum%
  132.68GB 22.55% 22.55%   132.68GB 22.55%  github.com/apache/skywalking-banyandb/banyand/measure.(*blockPointer).append
   46.53GB  7.91% 30.47%    46.53GB  7.91%  github.com/blevesearch/vellum.(*unfinishedNodes).get
   25.95GB  4.41% 34.88%    25.95GB  4.41%  reflect.New
   25.34GB  4.31% 39.19%    25.34GB  4.31%  github.com/blevesearch/vellum.(*builderNodePool).Get
   19.88GB  3.38% 42.57%    29.48GB  5.01%  github.com/apache/skywalking-banyandb/banyand/measure.(*blockMetadata).unmarshal
   18.69GB  3.18% 45.74%    18.69GB  3.18%  github.com/RoaringBitmap/roaring.(*Bitmap).Iterator
   16.04GB  2.73% 48.47%    22.22GB  3.78%  github.com/apache/skywalking-banyandb/banyand/measure.(*blockMetadata).copyFrom
   15.90GB  2.70% 51.17%    15.90GB  2.70%  github.com/blevesearch/vellum.(*builderNodeUnfinished).lastCompiled
   13.91GB  2.36% 53.54%    13.91GB  2.36%  github.com/blugelabs/bluge/analysis.TokenFrequency
   13.91GB  2.36% 55.90%    13.91GB  2.36%  github.com/apache/skywalking-banyandb/banyand/measure.(*columnFamilyMetadata).resizeColumnMetadata (inline)
```

`blockPointer.append` is the most memory consuming function.

## Disk Usage

```bash
measure: 300 MB
measure/measure-default: 137 MB
measure/measure-default/idx: 53 MB
measure/measure-default/shard-0: 84 MB
measure/measure-default/shard-0/seg-20240111: 84 MB
measure/measure-default/shard-0/seg-20240111/000000000000208b: 84 MB
measure/measure-minute: 162 MB
measure/measure-minute/idx: 32 MB
measure/measure-minute/shard-0: 65 MB
measure/measure-minute/shard-0/seg-20240111: 65 MB
measure/measure-minute/shard-0/seg-20240111/00000000000010f4: 65 MB
measure/measure-minute/shard-1: 65 MB
measure/measure-minute/shard-1/seg-20240111: 65 MB
measure/measure-minute/shard-1/seg-20240111/000000000000109a: 65 MB
```

## Disk IO

| Metric              | 95th-percentile per second  |
|---------------------|-----------------------------|
| read_count          | 0.856667                    |
| merged_read_count   | 0.603333                    |
| write_count         | 136.976667                  |
| merged_write_count  | 269.993333                  |
| read_bytes          | 13817.173333                |
| write_bytes         | 17439505.066667             |
| io_time(ms)         | 125.560000                  |
| weighted_io(ms)     | 504.606667                  |
