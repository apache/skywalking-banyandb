# WAL

## Benchmark

Testing environment:

```text
MacBook Pro (13-inch, M1, 2020)
Processor: Apple M1
Memory: 16 GB
CPU: 8 cores
```

Command used

```shell
go test -bench=. -benchmem -run=^$ -benchtime=10s -count 1 -memprofile=mem_profile.out -cpuprofile=cpu_profile.out
```

Test report

```text
goos: darwin
goarch: arm64
pkg: github.com/apache/skywalking-banyandb/pkg/wal

Benchmark_SeriesID_1-8                                                    299770             41357 ns/op            2654 B/op          3 allocs/op
Benchmark_SeriesID_20-8                                                   245113             42125 ns/op            1916 B/op          5 allocs/op
Benchmark_SeriesID_100-8                                                  296856             42177 ns/op            1291 B/op          7 allocs/op
Benchmark_SeriesID_500-8                                                  275554             42360 ns/op            1543 B/op          7 allocs/op
Benchmark_SeriesID_1000-8                                                 289639             39556 ns/op            1543 B/op          7 allocs/op
Benchmark_SeriesID_1000_Buffer_64K-8                                      282884             38827 ns/op            1543 B/op          7 allocs/op
Benchmark_SeriesID_1000_Buffer_128K-8                                     606891             20238 ns/op            1534 B/op          7 allocs/op
Benchmark_SeriesID_1000_Buffer_512K-8                                    1958060              5764 ns/op            1553 B/op          7 allocs/op
Benchmark_SeriesID_1000_Buffer_1MB-8                                     4478250              2738 ns/op            1522 B/op          6 allocs/op
Benchmark_SeriesID_1000_Buffer_2MB-8                                     7515986              1537 ns/op            1818 B/op          5 allocs/op
Benchmark_SeriesID_1000_Buffer_64K_NoSyncFlush-8                         7249369              1676 ns/op            1542 B/op          7 allocs/op
Benchmark_SeriesID_1000_Buffer_128K_NoSyncFlush-8                        7443198              1632 ns/op            1534 B/op          7 allocs/op
Benchmark_SeriesID_1000_Buffer_512K_NoSyncFlush-8                        7239253              1631 ns/op            1553 B/op          7 allocs/op
Benchmark_SeriesID_1000_Buffer_1MB_NoSyncFlush-8                         8047040              1497 ns/op            1521 B/op          6 allocs/op
Benchmark_SeriesID_1000_Buffer_2MB_NoSyncFlush-8                         7938543              1508 ns/op            1818 B/op          5 allocs/op
Benchmark_SeriesID_1000_Buffer_64K_NoSyncFlush_And_Rotate_16MB-8         7526020              1610 ns/op            1542 B/op          7 allocs/op
Benchmark_SeriesID_1000_Buffer_64K_NoSyncFlush_And_Rotate_32MB-8         7432317              1591 ns/op            1542 B/op          7 allocs/op
Benchmark_SeriesID_1000_Buffer_64K_NoSyncFlush_And_Rotate_64MB-8         7764439              1568 ns/op            1542 B/op          7 allocs/op
```

*Concluded from the benchmark test:*
- Turning off sync flush file performance is better.
- Increasing the buffer size is good for improving throughput.

### CPU profile

open with `go tool pprof -http="127.0.0.1:8080" cpu_profile.out` and you will get the following report

```text
Flat	Flat%	Sum%	Cum	Cum%	Name	Inlined?
81.19s	29.74%	29.74%	81.19s	29.74%	runtime.pthread_cond_signal	
65.81s	24.11%	53.84%	65.81s	24.11%	runtime.pthread_cond_wait	
49.78s	18.23%	72.08%	49.78s	18.23%	runtime.usleep	
31.62s	11.58%	83.66%	31.62s	11.58%	syscall.syscall	
7.06s	2.59%	86.25%	7.06s	2.59%	runtime.pthread_kill	
6.74s	2.47%	88.71%	6.74s	2.47%	runtime.madvise	
2.43s	0.89%	89.60%	2.76s	1.01%	github.com/golang/snappy.encodeBlock	
2.11s	0.77%	90.38%	6.77s	2.48%	runtime.scanobject	
1.40s	0.51%	90.89%	2.36s	0.86%	runtime.greyobject	
1.32s	0.48%	91.37%	1.84s	0.67%	runtime.mapaccess1	
1.03s	0.38%	91.75%	1.47s	0.54%	runtime.findObject	
0.98s	0.36%	92.11%	38.51s	14.11%	runtime.stealWork	
0.64s	0.23%	92.34%	2.43s	0.89%	runtime.mallocgc	
0.61s	0.22%	92.57%	2.34s	0.86%	runtime.mapassign	
0.33s	0.12%	92.69%	14.24s	5.22%	runtime.gcDrain	
0.20s	0.07%	92.76%	13.41s	4.91%	runtime.lock2	
0.20s	0.07%	92.84%	4.42s	1.62%	github.com/apache/skywalking-banyandb/pkg/wal.(*buffer).write	(inline)
0.12s	0.04%	92.88%	112.36s	41.16%	runtime.findRunnable	
0.10s	0.04%	92.92%	36.86s	13.50%	runtime.runqgrab	
0.09s	0.03%	92.95%	1.57s	0.58%	runtime.growslice	
0.06s	0.02%	92.97%	36.91s	13.52%	github.com/apache/skywalking-banyandb/pkg/wal.(*log).flushBuffer	
0.06s	0.02%	92.99%	1.76s	0.64%	github.com/apache/skywalking-banyandb/pkg/wal.(*buffer).notifyRequests	
0.05s	0.02%	93.01%	106.31s	38.94%	runtime.systemstack	
0.05s	0.02%	93.03%	6.56s	2.40%	runtime.(*mheap).allocSpan	
0.05s	0.02%	93.05%	5.78s	2.12%	runtime.(*gcWork).balance	
0.04s	0.01%	93.06%	8.32s	3.05%	runtime.gcBgMarkWorker	
0.03s	0.01%	93.07%	36.89s	13.51%	runtime.runqsteal	
0.02s	0.01%	93.08%	67.03s	24.55%	runtime.semasleep	
0.02s	0.01%	93.09%	117.19s	42.93%	runtime.schedule	
0.02s	0.01%	93.10%	6.49s	2.38%	runtime.preemptone	
0.02s	0.01%	93.10%	2.80s	1.03%	github.com/golang/snappy.Encode	
0.02s	0.01%	93.11%	7.13s	2.61%	github.com/apache/skywalking-banyandb/pkg/wal.(*log).start.func1	
0.01s	0.00%	93.11%	1.41s	0.52%	time.NewTimer	
0.01s	0.00%	93.12%	83.77s	30.68%	runtime.wakep	
0.01s	0.00%	93.12%	7.07s	2.59%	runtime.signalM	(inline)
0.01s	0.00%	93.12%	65.70s	24.07%	runtime.notesleep	
0.01s	0.00%	93.13%	3.79s	1.39%	runtime.newstack	
0.01s	0.00%	93.13%	78.99s	28.93%	runtime.goready.func1	
0.01s	0.00%	93.14%	2.60s	0.95%	runtime.forEachP	
0.01s	0.00%	93.14%	5.94s	2.18%	runtime.(*mheap).alloc.func1	
0.01s	0.00%	93.14%	26.23s	9.61%	os.(*File).Write	
0.01s	0.00%	93.15%	26.22s	9.60%	internal/poll.(*FD).Write	
0.01s	0.00%	93.15%	31.28s	11.46%	github.com/apache/skywalking-banyandb/pkg/wal.(*log).writeWorkSegment	
0.01s	0.00%	93.15%	38.70s	14.18%	github.com/apache/skywalking-banyandb/pkg/wal.(*log).start.func2	
0.01s	0.00%	93.16%	2.81s	1.03%	github.com/apache/skywalking-banyandb/pkg/wal.(*bufferWriter).WriteData	
0	0.00%	93.16%	26.21s	9.60%	syscall.write	
0	0.00%	93.16%	5.04s	1.85%	syscall.fcntl	
0	0.00%	93.16%	26.21s	9.60%	syscall.Write	(inline)
0	0.00%	93.16%	6.30s	2.31%	runtime.sysUsedOS	(inline)
0	0.00%	93.16%	6.31s	2.31%	runtime.sysUsed	(inline)
0	0.00%	93.16%	66.66s	24.42%	runtime.stopm	
0	0.00%	93.16%	80.05s	29.32%	runtime.startm	
0	0.00%	93.16%	1.42s	0.52%	runtime.startTheWorldWithSema	
0	0.00%	93.16%	81.29s	29.78%	runtime.semawakeup	
0	0.00%	93.16%	4.51s	1.65%	runtime.resetspinning	
0	0.00%	93.16%	78.98s	28.93%	runtime.ready	
0	0.00%	93.16%	7.07s	2.59%	runtime.preemptM	
0	0.00%	93.16%	115.92s	42.46%	runtime.park_m	
0	0.00%	93.16%	13.03s	4.77%	runtime.osyield	(inline)
0	0.00%	93.16%	80.58s	29.52%	runtime.notewakeup	
0	0.00%	93.16%	3.58s	1.31%	runtime.morestack	
0	0.00%	93.16%	116.10s	42.53%	runtime.mcall	
0	0.00%	93.16%	1.45s	0.53%	runtime.markroot	
0	0.00%	93.16%	65.70s	24.07%	runtime.mPark	(inline)
0	0.00%	93.16%	13.41s	4.91%	runtime.lockWithRank	(inline)
0	0.00%	93.16%	13.41s	4.91%	runtime.lock	(inline)
0	0.00%	93.16%	3.35s	1.23%	runtime.goschedImpl	
0	0.00%	93.16%	3.32s	1.22%	runtime.gopreempt_m	
0	0.00%	93.16%	1.49s	0.55%	runtime.gcMarkDone.func1	
0	0.00%	93.16%	14.24s	5.22%	runtime.gcBgMarkWorker.func2	
0	0.00%	93.16%	5.55s	2.03%	runtime.(*gcControllerState).enlistWorker	
0	0.00%	93.16%	26.22s	9.60%	os.(*File).write	(inline)
0	0.00%	93.16%	5.04s	1.85%	os.(*File).Sync	
0	0.00%	93.16%	26.21s	9.60%	internal/poll.ignoringEINTRIO	(inline)
0	0.00%	93.16%	5.04s	1.85%	internal/poll.ignoringEINTR	(inline)
0	0.00%	93.16%	5.04s	1.85%	internal/poll.(*FD).Fsync.func1	(inline)
0	0.00%	93.16%	5.04s	1.85%	internal/poll.(*FD).Fsync
```

### Memory profile

open with `go tool pprof -http="127.0.0.1:8080" mem_profile.out` and you will get the following report

```text
Flat	        Flat%	Sum%	Cum		Cum%	Name
117496.32MB	87.39%	87.39%	117496.32MB	87.39%	github.com/apache/skywalking-banyandb/pkg/wal.(*buffer).write
16789.80MB	12.49%	99.88%	16789.80MB	12.49%	time.NewTimer	
2.50MB		0.00%	99.88%	134335.12MB	99.91%	github.com/apache/skywalking-banyandb/pkg/wal.(*log).start.func1
```