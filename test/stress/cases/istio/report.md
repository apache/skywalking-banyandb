# Testing Report

## Scenario

### SkyWalking Entities

Service: 256
Instances: 2048, 8 per service


### Demo cluster

Traffic RPS: 4352
VUS: 8192

## Result

written 16186000 items in 38m43.221090505s
throughput: 6967.051077 items/s
throughput(kb/s) 1782.985321
latency: 2ns

## CPU Profile

CPU Usage: 324%

```bash
Showing top 10 nodes out of 300
      flat  flat%   sum%        cum   cum%
   348.58s  4.62%  4.62%    383.07s  5.08%  runtime.findObject
   272.92s  3.62%  8.24%    272.92s  3.62%  runtime.memmove
   240.53s  3.19% 11.43%    240.53s  3.19%  runtime/internal/syscall.Syscall6
   239.05s  3.17% 14.60%    239.05s  3.17%  runtime.memclrNoHeapPointers
   210.82s  2.80% 17.40%    340.64s  4.52%  github.com/klauspost/compress/zstd.(*doubleFastEncoder).Encode
   189.80s  2.52% 19.92%   1111.75s 14.74%  runtime.mallocgc
   182.17s  2.42% 22.33%    687.47s  9.12%  runtime.scanobject
   134.93s  1.79% 24.12%    202.49s  2.69%  github.com/dgraph-io/badger/v3/table.(*MergeIterator).Value
   116.62s  1.55% 25.67%    116.62s  1.55%  runtime.nextFreeFast (inline)
   110.73s  1.47% 27.14%    110.73s  1.47%  github.com/klauspost/compress/zstd.sequenceDecs_decodeSync_bmi2
```

From the top 10 list, we can see that the CPU is mainly used by `compaction`.

## Heap Profile

Heap Size: 1.2GB

```bash
Showing top 10 nodes out of 104
      flat  flat%   sum%        cum   cum%
  690.27MB 53.22% 53.22%   690.27MB 53.22%  github.com/dgraph-io/ristretto/z.Calloc (inline)
  172.07MB 13.27% 66.48%   172.07MB 13.27%  runtime.malg
     128MB  9.87% 76.35%      128MB  9.87%  github.com/klauspost/compress/zstd.(*fastBase).ensureHist (inline)
   78.98MB  6.09% 82.44%    78.98MB  6.09%  github.com/dgraph-io/badger/v3/skl.newArena
   57.51MB  4.43% 86.87%   141.71MB 10.92%  github.com/dgraph-io/badger/v3/table.(*Builder).addHelper.func1
   36.02MB  2.78% 89.65%   177.73MB 13.70%  github.com/dgraph-io/badger/v3/table.(*Builder).addHelper
   28.97MB  2.23% 91.88%    28.97MB  2.23%  runtime/pprof.(*profMap).lookup
   26.50MB  2.04% 93.93%   757.59MB 58.41%  github.com/dgraph-io/badger/v3/table.(*Builder).addInternal
    8.21MB  0.63% 94.56%     8.21MB  0.63%  github.com/klauspost/compress/zstd.encoderOptions.encoder
       4MB  0.31% 94.87%    48.50MB  3.74%  github.com/dgraph-io/badger/v3/table.(*Table).block
```

From the top 10 list, we can see that the memory is mainly used by write `buffer(skl)` and `compaction(table)`.
Especially, the compaction includes several table related operations, such as `table.(*Builder).addHelper`,
consumes most of the memory.


## Disk Usage

```bash
measure: 446 MB
measure/measure-default: 272 MB
measure/measure-default/shard-0: 272 MB
measure/measure-default/shard-0/buffer-0: 1.4 MB
measure/measure-default/shard-0/buffer-1: 2.8 MB
measure/measure-default/shard-0/seg-20231015: 247 MB
measure/measure-default/shard-0/seg-20231015/block-2023101516: 247 MB
measure/measure-default/shard-0/seg-20231015/block-2023101516/encoded: 74 MB
measure/measure-default/shard-0/seg-20231015/block-2023101516/lsm: 83 MB
measure/measure-default/shard-0/seg-20231015/block-2023101516/tst: 90 MB
measure/measure-default/shard-0/seg-20231015/index: 0 B
measure/measure-default/shard-0/series: 21 MB
measure/measure-default/shard-0/series/inverted: 2.9 MB
measure/measure-default/shard-0/series/lsm: 1.0 MB
measure/measure-default/shard-0/series/md: 17 MB
measure/measure-minute: 173 MB
measure/measure-minute/shard-0: 89 MB
measure/measure-minute/shard-0/buffer-0: 2.0 MB
measure/measure-minute/shard-0/buffer-1: 1.6 MB
measure/measure-minute/shard-0/seg-20231015: 76 MB
measure/measure-minute/shard-0/seg-20231015/block-2023101516: 76 MB
measure/measure-minute/shard-0/seg-20231015/block-2023101516/encoded: 23 MB
measure/measure-minute/shard-0/seg-20231015/block-2023101516/lsm: 26 MB
measure/measure-minute/shard-0/seg-20231015/block-2023101516/tst: 28 MB
measure/measure-minute/shard-0/seg-20231015/index: 0 B
measure/measure-minute/shard-0/series: 8.7 MB
measure/measure-minute/shard-0/series/inverted: 2.1 MB
measure/measure-minute/shard-0/series/lsm: 1.0 MB
measure/measure-minute/shard-0/series/md: 5.6 MB
measure/measure-minute/shard-1: 84 MB
measure/measure-minute/shard-1/buffer-0: 1.5 MB
measure/measure-minute/shard-1/buffer-1: 698 kB
measure/measure-minute/shard-1/seg-20231015: 73 MB
measure/measure-minute/shard-1/seg-20231015/block-2023101516: 73 MB
measure/measure-minute/shard-1/seg-20231015/block-2023101516/encoded: 19 MB
measure/measure-minute/shard-1/seg-20231015/block-2023101516/lsm: 26 MB
measure/measure-minute/shard-1/seg-20231015/block-2023101516/tst: 28 MB
measure/measure-minute/shard-1/seg-20231015/index: 0 B
measure/measure-minute/shard-1/series: 9.3 MB
measure/measure-minute/shard-1/series/inverted: 2.7 MB
measure/measure-minute/shard-1/series/lsm: 1.0 MB
measure/measure-minute/shard-1/series/md: 5.6 MB
```
