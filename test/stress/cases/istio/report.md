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

Memory: blockPointer.append used 22.55% of total memory. That's due to the fact that parts without the time overlap are merged. The unnecessary unpacking block operations take up a lot of memory.

CPU: As memory's part, the unnecessary unpacking block operations take up a lot of CPU.

## CPU

CPU 95th-percentile: 2.91 cores (8 cores in total)

```bash
Showing top 10 nodes out of 327
      flat  flat%   sum%        cum   cum%
   216.45s  4.14%  4.14%    226.17s  4.32%  github.com/klauspost/compress/zstd.(*fseEncoder).buildCTable
   171.32s  3.27%  7.41%    171.32s  3.27%  runtime/internal/syscall.Syscall6
   155.50s  2.97% 10.38%    606.59s 11.59%  runtime.mallocgc
   135.71s  2.59% 12.98%    169.48s  3.24%  github.com/klauspost/compress/zstd.(*fastEncoder).EncodeNoHist
   134.63s  2.57% 15.55%    248.72s  4.75%  github.com/blevesearch/vellum.registryCache.entry
   133.90s  2.56% 18.11%    133.90s  2.56%  runtime.memmove
   130.33s  2.49% 20.60%    149.37s  2.85%  github.com/klauspost/compress/huff0.(*Scratch).huffSort
   118.60s  2.27% 22.87%    136.91s  2.62%  runtime.findObject
    90.35s  1.73% 24.59%     92.01s  1.76%  github.com/blevesearch/vellum.(*builderNode).equiv (inline)
    83.19s  1.59% 26.18%     83.19s  1.59%  runtime.nextFreeFast (inline)
```

From the top 10 list, we can see that the CPU is mainly used by `zstd.Encoder`.

## Heap Profile

`alloc_bytes` 95th-percentile: 1.33 GB.
`heap_inuse_bytes` 95th-percentile: 1.46 GB.
`sys_bytes` 95th-percentile: 1.95 GB.
`stack_inuse_bytes` 95th-percentile: 17.89 MB.

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
measure: 281 MB
measure/measure-default: 122 MB
measure/measure-default/idx: 38 MB
measure/measure-default/shard-0: 84 MB
measure/measure-default/shard-0/seg-20240117: 84 MB
measure/measure-default/shard-0/seg-20240117/0000000000002157: 84 MB
measure/measure-minute: 159 MB
measure/measure-minute/idx: 29 MB
measure/measure-minute/shard-0: 65 MB
measure/measure-minute/shard-0/seg-20240117: 65 MB
measure/measure-minute/shard-0/seg-20240117/0000000000001240: 65 MB
measure/measure-minute/shard-1: 65 MB
measure/measure-minute/shard-1/seg-20240117: 65 MB
measure/measure-minute/shard-1/seg-20240117/00000000000011ce: 65 MB
```

## Disk IO

| Metric              | 95th-percentile per second  |
|---------------------|-----------------------------|
| read_count          | 0.003333                    |
| merged_read_count   | 0                           |
| write_count         | 114.936667                  |
| merged_write_count  | 70.315                      |
| read_bytes          | 13.653333                   |
| write_bytes         | 22672254.29                 |
| io_time(ms)         | 57.906667                   |
| weighted_io(ms)     | 922.218333                  |
