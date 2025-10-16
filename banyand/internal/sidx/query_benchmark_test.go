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

	runStreamingQueryBenchmark(b, numSeries, pointsPerSeries, streamElementCap, true)
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

		firstLatency, count := measureFirstResultLatency(resultsCh, start)
		validateStreamingQueryResult(b, errCh, count, expected)

		if firstLatency == 0 {
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
				count := drainStreamingResultsWithDelay(resultsCh, consumerDelay)
				validateStreamingQueryResult(b, errCh, count, expected)

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
		b.Run(fmt.Sprintf("%d_workers", workers), func(b *testing.B) {
			previous := runtime.GOMAXPROCS(workers)
			b.Cleanup(func() {
				runtime.GOMAXPROCS(previous)
			})

			runStreamingQueryBenchmark(b, numSeries, pointsPerSeries, streamElementCap, false)
		})
	}
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

func runStreamingQueryBenchmark(b *testing.B, numSeries, pointsPerSeries, streamElementCap int, reportAllocs bool) {
	b.Helper()

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
}

func validateStreamingQueryResult(tb testing.TB, errCh <-chan error, count, expected int) {
	tb.Helper()

	if err, ok := <-errCh; ok && err != nil {
		tb.Fatalf("streaming query returned error: %v", err)
	}

	if count != expected {
		tb.Fatalf("unexpected streaming result count: got %d, want %d", count, expected)
	}
}

func measureFirstResultLatency(resultsCh <-chan *QueryResponse, start time.Time) (time.Duration, int) {
	var firstLatency time.Duration
	var count int
	firstObserved := false

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

	return firstLatency, count
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

func drainStreamingResultsWithDelay(resultsCh <-chan *QueryResponse, delay time.Duration) int {
	var total int
	for res := range resultsCh {
		if res == nil {
			continue
		}
		total += res.Len()
		time.Sleep(delay)
	}
	return total
}

func BenchmarkSIDXStreamingQuery_LimitedMemoryUsage(b *testing.B) {
	const (
		numElementsToRetrieve = 20
		elementSizeKB         = 100
		expectedMemoryMB      = 2  // 20 elements * 100KB = 2MB
		memoryThresholdMB     = 10 // 5x expected, accounts for Go runtime overhead and metadata
	)

	testCases := []struct {
		name          string
		totalElements int
	}{
		{"100MB_dataset", 1000},
		{"500MB_dataset", 5000},
		{"1GB_dataset", 10000},
		{"2GB_dataset", 20000},
	}

	for _, tc := range testCases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()

			// Setup large dataset
			sidx, req := setupLargeDatasetSIDX(b, tc.totalElements, elementSizeKB, numElementsToRetrieve)
			ctx := context.Background()

			// Force GC and wait for it to complete
			runtime.GC()
			runtime.GC()
			time.Sleep(100 * time.Millisecond)

			var totalMemoryDelta uint64
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Measure memory before query
				var before runtime.MemStats
				runtime.ReadMemStats(&before)

				// Execute query with MaxElementSize limiting to numElementsToRetrieve
				resultsCh, errCh := sidx.StreamingQuery(ctx, req)
				count := drainStreamingResults(b, resultsCh, errCh)

				// Measure memory after query
				var after runtime.MemStats
				runtime.ReadMemStats(&after)

				// Verify we got the expected number of elements
				if count != numElementsToRetrieve {
					b.Fatalf("Expected %d elements, got %d", numElementsToRetrieve, count)
				}

				// Calculate memory delta
				var memoryDelta uint64
				if after.HeapAlloc > before.HeapAlloc {
					memoryDelta = after.HeapAlloc - before.HeapAlloc
				}
				totalMemoryDelta += memoryDelta

				// Verify memory usage is within threshold
				memoryDeltaMB := float64(memoryDelta) / (1024 * 1024)
				if memoryDeltaMB > memoryThresholdMB {
					b.Fatalf("Memory usage exceeded threshold: %.2f MB > %d MB (expected ~%d MB for %d elements)",
						memoryDeltaMB, memoryThresholdMB, expectedMemoryMB, numElementsToRetrieve)
				}

				// Force GC between iterations to avoid accumulation
				runtime.GC()
			}

			b.StopTimer()

			// Report metrics
			if b.N > 0 {
				avgMemoryMB := float64(totalMemoryDelta) / float64(b.N) / (1024 * 1024)
				b.ReportMetric(avgMemoryMB, "MB_per_query")
				b.ReportMetric(float64(tc.totalElements)*float64(elementSizeKB)/1024, "total_dataset_MB")
			}
		})
	}
}

func setupLargeDatasetSIDX(b *testing.B, totalElements, elementSizeKB, maxElementsToRetrieve int) (SIDX, QueryRequest) {
	b.Helper()

	sidx := createBenchmarkSIDX(b)

	const (
		batchSize = 512
		numSeries = 10
	)

	// Create data payload of specified size
	dataPayload := make([]byte, elementSizeKB*1024)
	for i := range dataPayload {
		dataPayload[i] = byte(i % 256)
	}

	reqs := make([]WriteRequest, 0, batchSize)
	var (
		partID    uint64 = 1
		segmentID int64  = 1
		key       int64  = 1
	)

	// Distribute elements across series
	elementsPerSeries := totalElements / numSeries
	if elementsPerSeries == 0 {
		elementsPerSeries = 1
	}

	for series := 0; series < numSeries; series++ {
		seriesID := common.SeriesID(series + 1)
		elementsForThisSeries := elementsPerSeries
		if series == numSeries-1 {
			// Last series gets any remainder
			elementsForThisSeries = totalElements - (series * elementsPerSeries)
		}

		for elem := 0; elem < elementsForThisSeries; elem++ {
			// Create a copy of the data payload for each element
			dataCopy := make([]byte, len(dataPayload))
			copy(dataCopy, dataPayload)

			reqs = append(reqs, WriteRequest{
				SeriesID: seriesID,
				Key:      key,
				Data:     dataCopy,
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

	// Create query request that retrieves only maxElementsToRetrieve elements
	// Use key range to limit results: keys 1 to maxElementsToRetrieve
	minKey := int64(1)
	maxKey := int64(maxElementsToRetrieve)
	request := QueryRequest{
		SeriesIDs:      []common.SeriesID{common.SeriesID(1)},
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: maxElementsToRetrieve,
		MinKey:         &minKey,
		MaxKey:         &maxKey,
	}

	return sidx, request
}
