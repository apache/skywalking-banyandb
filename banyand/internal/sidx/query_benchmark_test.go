// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package sidx

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

func BenchmarkSIDXStreamingQuery(b *testing.B) {
	const (
		numSeries        = 16
		pointsPerSeries  = 256
		streamElementCap = 128
	)

	testCases := []struct {
		name         string
		reportAllocs bool
	}{
		{name: "MemoryUsage", reportAllocs: true},
		{name: "TotalThroughput", reportAllocs: false},
	}

	for _, tc := range testCases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			runStreamingAndBlockingBenchmarks(b, numSeries, pointsPerSeries, streamElementCap, tc.reportAllocs)
		})
	}
}

func BenchmarkSIDXStreamingQuery_FirstResultLatency(b *testing.B) {
	const (
		numSeries        = 16
		pointsPerSeries  = 256
		streamElementCap = 128
	)

	sidx, req, expected := setupBenchmarkSIDX(b, numSeries, pointsPerSeries, streamElementCap)
	ctx := context.Background()
	var totalFirst time.Duration
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		resultsCh, errCh := sidx.StreamingQuery(ctx, req)

		firstObserved := false
		var firstLatency time.Duration
		var count int

		for res := range resultsCh {
			if res == nil {
				continue
			}
			if !firstObserved && res.Len() > 0 {
				firstLatency = time.Since(start)
				firstObserved = true
			}
			count += res.Len()
		}

		if err, ok := <-errCh; ok && err != nil {
			b.Fatalf("streaming query returned error: %v", err)
		}

		if count != expected {
			b.Fatalf("unexpected streaming result count: got %d, want %d", count, expected)
		}
		if !firstObserved {
			b.Fatal("no streaming batches were observed")
		}

		totalFirst += firstLatency
	}

	if b.N > 0 {
		b.ReportMetric(float64(totalFirst)/float64(b.N), "ns_first_result")
	}
}

func BenchmarkSIDXStreamingQuery_Backpressure(b *testing.B) {
	const consumerDelay = 200 * time.Microsecond

	testCases := []struct {
		name             string
		numSeries        int
		pointsPerSeries  int
		streamElementCap int
	}{
		{
			name:             "Baseline",
			numSeries:        8,
			pointsPerSeries:  128,
			streamElementCap: 32,
		},
		{
			name:             "HighCardinality",
			numSeries:        32,
			pointsPerSeries:  512,
			streamElementCap: 32,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()

			sidx, req, expected := setupBenchmarkSIDX(b, tc.numSeries, tc.pointsPerSeries, tc.streamElementCap)
			ctx := context.Background()

			runtime.GC()
			var totalAllocDelta uint64

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var before runtime.MemStats
				runtime.ReadMemStats(&before)

				resultsCh, errCh := sidx.StreamingQuery(ctx, req)
				var count int
				for res := range resultsCh {
					if res == nil {
						continue
					}
					count += res.Len()
					time.Sleep(consumerDelay)
				}
				if err, ok := <-errCh; ok && err != nil {
					b.Fatalf("streaming query returned error: %v", err)
				}
				if count != expected {
					b.Fatalf("unexpected streaming result count: got %d, want %d", count, expected)
				}

				var after runtime.MemStats
				runtime.ReadMemStats(&after)
				if after.TotalAlloc > before.TotalAlloc {
					totalAllocDelta += after.TotalAlloc - before.TotalAlloc
				}
			}
			b.StopTimer()

			if b.N > 0 {
				perIter := float64(totalAllocDelta) / float64(b.N)
				b.ReportMetric(perIter, "bytes_totalalloc_per_iter")
			}
		})
	}
}

func BenchmarkSIDXStreamingQuery_WorkerScaling(b *testing.B) {
	const (
		numSeries        = 16
		pointsPerSeries  = 256
		streamElementCap = 128
	)

	workerConfigs := []int{1, 2, runtime.NumCPU()}
	for _, workers := range workerConfigs {
		workers := workers
		b.Run(fmt.Sprintf("%d_workers", workers), func(b *testing.B) {
			previous := runtime.GOMAXPROCS(workers)
			b.Cleanup(func() {
				runtime.GOMAXPROCS(previous)
			})

			sidx, req, expected := setupBenchmarkSIDX(b, numSeries, pointsPerSeries, streamElementCap)
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resultsCh, errCh := sidx.StreamingQuery(ctx, req)
				count := drainStreamingResults(b, resultsCh, errCh)
				if count != expected {
					b.Fatalf("unexpected streaming result count: got %d, want %d", count, expected)
				}
			}
		})
	}
}

func runStreamingAndBlockingBenchmarks(b *testing.B, numSeries, pointsPerSeries, streamElementCap int, reportAllocs bool) {
	b.Helper()

	b.Run("Streaming", func(b *testing.B) {
		sidx, req, expected := setupBenchmarkSIDX(b, numSeries, pointsPerSeries, streamElementCap)
		if reportAllocs {
			b.ReportAllocs()
		}
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resultsCh, errCh := sidx.StreamingQuery(ctx, req)
			count := drainStreamingResults(b, resultsCh, errCh)
			if count != expected {
				b.Fatalf("unexpected streaming result count: got %d, want %d", count, expected)
			}
		}
	})

	b.Run("Blocking", func(b *testing.B) {
		sidx, req, expected := setupBenchmarkSIDX(b, numSeries, pointsPerSeries, streamElementCap)
		if reportAllocs {
			b.ReportAllocs()
		}
		ctx := context.Background()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := sidx.Query(ctx, req)
			if err != nil {
				b.Fatalf("blocking query failed: %v", err)
			}
			if resp.Len() != expected {
				b.Fatalf("unexpected blocking result count: got %d, want %d", resp.Len(), expected)
			}
		}
	})
}

func setupBenchmarkSIDX(b *testing.B, numSeries, pointsPerSeries, maxElementSize int) (SIDX, QueryRequest, int) {
	b.Helper()

	sidx := createBenchmarkSIDX(b)

	const batchSize = 512
	reqs := make([]WriteRequest, 0, batchSize)
	var (
		partID    uint64 = 1
		segmentID int64  = 1
		key       int64  = 1
	)

	for series := 0; series < numSeries; series++ {
		seriesID := common.SeriesID(series + 1)
		for point := 0; point < pointsPerSeries; point++ {
			reqs = append(reqs, WriteRequest{
				SeriesID: seriesID,
				Key:      key,
				Data:     []byte(fmt.Sprintf("series-%d-point-%d", seriesID, point)),
			})
			key++
			if len(reqs) == batchSize {
				introduceBenchmarkMemPart(b, sidx, reqs, segmentID, partID)
				reqs = reqs[:0]
				partID++
				if partID%16 == 0 {
					segmentID++
				}
			}
		}
	}

	if len(reqs) > 0 {
		introduceBenchmarkMemPart(b, sidx, reqs, segmentID, partID)
	}

	waitForIntroducerLoop()

	seriesIDs := make([]common.SeriesID, numSeries)
	for i := range seriesIDs {
		seriesIDs[i] = common.SeriesID(i + 1)
	}

	request := QueryRequest{
		SeriesIDs:      seriesIDs,
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: maxElementSize,
	}
	expected := numSeries * pointsPerSeries
	return sidx, request, expected
}

func createBenchmarkSIDX(b *testing.B) SIDX {
	b.Helper()

	fileSystem := fs.NewLocalFileSystem()
	opts := NewDefaultOptions()
	opts.Memory = protector.NewMemory(observability.NewBypassRegistry())
	opts.Path = b.TempDir()

	sidx, err := NewSIDX(fileSystem, opts)
	if err != nil {
		b.Fatalf("failed to create SIDX: %v", err)
	}

	b.Cleanup(func() {
		if err := sidx.Close(); err != nil {
			b.Fatalf("failed to close SIDX: %v", err)
		}
	})

	return sidx
}

func introduceBenchmarkMemPart(tb testing.TB, sidx SIDX, reqs []WriteRequest, segmentID int64, partID uint64) {
	tb.Helper()

	memPart, err := sidx.ConvertToMemPart(reqs, segmentID)
	if err != nil {
		tb.Fatalf("failed to convert requests to memPart: %v", err)
	}
	sidx.IntroduceMemPart(partID, memPart)
}

func drainStreamingResults(tb testing.TB, resultsCh <-chan *QueryResponse, errCh <-chan error) int {
	tb.Helper()

	var total int
	for res := range resultsCh {
		if res == nil {
			continue
		}
		total += res.Len()
	}

	if err, ok := <-errCh; ok && err != nil {
		tb.Fatalf("streaming query returned error: %v", err)
	}

	return total
}
