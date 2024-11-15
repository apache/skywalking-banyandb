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

- CPU: CPU: AMD EPYC 7B13, VendorID: AuthenticAMD, Family: 25, Model: 1, Cores: 16
- Memory: Total: 15986 MB
- OS: OS: linux, Platform: ubuntu, Platform Family: debian, Platform Version: 24.04

## Result

written 16186000 items in 38m25.757182449s
throughput: 7019.819833 items/s
throughput(kb/s) 1796.489734
latency: 167.193µs

CPU and Memory usage are improved due to reducing the updates of the index.

### Problem

Series Index Searching took most of the time. Add a cache would be helpful.

## CPU

CPU 95th-percentile: 0.75 cores (16 cores in total)

```bash
Showing top 10 nodes out of 301
      flat  flat%   sum%        cum   cum%
   335.22s 18.10% 18.10%    335.22s 18.10%  internal/runtime/syscall.Syscall6
    75.99s  4.10% 22.20%    225.86s 12.19%  runtime.mallocgc
    43.86s  2.37% 24.57%     48.40s  2.61%  github.com/blevesearch/vellum.(*fstStateV1).atSingle
    39.80s  2.15% 26.72%     39.80s  2.15%  runtime.memmove
    39.45s  2.13% 28.85%     39.45s  2.13%  runtime.usleep
    36.23s  1.96% 30.80%    218.49s 11.80%  github.com/blevesearch/vellum.(*FST).get
    32.43s  1.75% 32.56%     32.43s  1.75%  runtime.nextFreeFast (inline)
    30.64s  1.65% 34.21%     30.64s  1.65%  runtime.futex
    28.75s  1.55% 35.76%    148.49s  8.02%  github.com/blevesearch/vellum.(*decoderV1).stateAt
    27.72s  1.50% 37.26%     27.72s  1.50%  runtime.memclrNoHeapPointers
```

## Heap Profile

`alloc_bytes` 95th-percentile: 431.5 MB.
`heap_inuse_bytes` 95th-percentile: 452.97 GB.
`sys_bytes` 95th-percentile: 892.03 GB.
`stack_inuse_bytes` 95th-percentile: 2.35 MB.

```bash
Showing top 10 nodes out of 238
      flat  flat%   sum%        cum   cum%
   27.77GB 12.09% 12.09%    27.77GB 12.09%  reflect.New
   22.72GB  9.89% 21.98%    22.72GB  9.89%  github.com/blugelabs/bluge.Identifier.Term
    8.50GB  3.70% 25.68%     8.50GB  3.70%  github.com/blevesearch/vellum.(*decoderV1).stateAt
    7.54GB  3.28% 28.96%    54.19GB 23.59%  github.com/apache/skywalking-banyandb/test/stress/istio.ReadAndWriteFromFile.func4
    7.53GB  3.28% 32.23%    22.32GB  9.72%  github.com/apache/skywalking-banyandb/banyand/measure.(*writeCallback).handle
    6.57GB  2.86% 35.09%     6.57GB  2.86%  strings.(*Builder).grow
    5.39GB  2.34% 37.44%     5.69GB  2.48%  google.golang.org/protobuf/proto.MarshalOptions.marshal
    5.30GB  2.30% 39.74%     5.30GB  2.30%  google.golang.org/protobuf/internal/encoding/json.(*Decoder).parseString
    4.95GB  2.15% 41.90%     4.95GB  2.16%  github.com/klauspost/compress/s2.encodeBlockBetter
    4.71GB  2.05% 43.95%     4.71GB  2.05%  github.com/apache/skywalking-banyandb/banyand/measure.fastFieldAppend
```

## Disk Usage

```bash
measure: 562 MB
measure/measure-default: 303 MB
measure/measure-default/seg-20241115: 303 MB
measure/measure-default/seg-20241115/shard-0: 271 MB
measure/measure-default/seg-20241115/shard-0/0000000000000b08: 83 MB
measure/measure-default/seg-20241115/shard-0/00000000000014bb: 80 MB
measure/measure-default/seg-20241115/shard-0/0000000000001e6b: 143 kB
measure/measure-default/seg-20241115/shard-0/0000000000001e6d: 329 kB
measure/measure-default/seg-20241115/shard-0/0000000000001e6e: 78 kB
measure/measure-default/seg-20241115/shard-0/0000000000001f35: 83 MB
measure/measure-default/seg-20241115/shard-0/0000000000001f75: 4.5 MB
measure/measure-default/seg-20241115/shard-0/0000000000001ff7: 9.2 MB
measure/measure-default/seg-20241115/shard-0/0000000000002009: 150 kB
measure/measure-default/seg-20241115/shard-0/0000000000002045: 11 MB
measure/measure-default/seg-20241115/sidx: 32 MB
measure/measure-minute: 259 MB
measure/measure-minute/seg-20241115: 259 MB
measure/measure-minute/seg-20241115/shard-0: 120 MB
measure/measure-minute/seg-20241115/shard-0/00000000000004d4: 33 MB
measure/measure-minute/seg-20241115/shard-0/00000000000009c5: 34 MB
measure/measure-minute/seg-20241115/shard-0/0000000000000bc9: 59 kB
measure/measure-minute/seg-20241115/shard-0/0000000000000f2c: 35 MB
measure/measure-minute/seg-20241115/shard-0/0000000000000f8f: 2.9 MB
measure/measure-minute/seg-20241115/shard-0/0000000000000f96: 60 kB
measure/measure-minute/seg-20241115/shard-0/0000000000000fb8: 5.5 MB
measure/measure-minute/seg-20241115/shard-0/0000000000000ff6: 2.8 MB
measure/measure-minute/seg-20241115/shard-0/0000000000000ff7: 60 kB
measure/measure-minute/seg-20241115/shard-0/000000000000101e: 1.8 MB
measure/measure-minute/seg-20241115/shard-0/0000000000001067: 3.3 MB
measure/measure-minute/seg-20241115/shard-0/000000000000106a: 114 kB
measure/measure-minute/seg-20241115/shard-0/000000000000107f: 1.2 MB
measure/measure-minute/seg-20241115/shard-1: 116 MB
measure/measure-minute/seg-20241115/shard-1/0000000000000515: 35 MB
measure/measure-minute/seg-20241115/shard-1/0000000000000a5c: 36 MB
measure/measure-minute/seg-20241115/shard-1/0000000000000da4: 369 kB
measure/measure-minute/seg-20241115/shard-1/0000000000000e67: 116 kB
measure/measure-minute/seg-20241115/shard-1/0000000000000f34: 34 MB
measure/measure-minute/seg-20241115/shard-1/0000000000000fa1: 264 kB
measure/measure-minute/seg-20241115/shard-1/0000000000000ffc: 116 kB
measure/measure-minute/seg-20241115/shard-1/0000000000000ffd: 59 kB
measure/measure-minute/seg-20241115/shard-1/0000000000001030: 11 MB
measure/measure-minute/seg-20241115/sidx: 23 MB
```

## Disk IO

| Metric              | 95th-percentile per second |
|---------------------|----------------------------|
| read_count          | 0.06333                    |
| merged_read_count   | 0.000000                   |
| write_count         | 14.873333                  |
| merged_write_count  | 16.700000                  |
| read_bytes          | 996.693333                 |
| write_bytes         | 1689586.346667             |
| io_time(ms)         | 7.486667                   |
| weighted_io(ms)     | 121.133333                 |
