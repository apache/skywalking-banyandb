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

package trace

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// Benchmark_mergeBlocks_FastRawMerge benchmarks the fast path where trace IDs don't overlap.
// This uses br.mustReadRaw and bw.mustWriteRawBlock to copy blocks without unmarshaling.
//
// Performance characteristics:
// - Lower memory usage (up to 1400x less for large datasets)
// - Fewer allocations
// - Slightly slower or comparable time performance
// - Optimal when trace IDs are unique across parts (common case).
func Benchmark_mergeBlocks_FastRawMerge(b *testing.B) {
	benchmarkCases := []struct {
		tracesGen func() *traces
		name      string
		numParts  int
	}{
		{
			name:     "small_parts_non_overlapping",
			numParts: 10,
			tracesGen: func() *traces {
				return &traces{
					traceIDs:   []string{"trace1", "trace2", "trace3"},
					timestamps: []int64{1, 2, 3},
					tags: [][]*tagValue{
						{{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val1"), valueArr: nil}},
						{{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val2"), valueArr: nil}},
						{{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val3"), valueArr: nil}},
					},
					spans:   [][]byte{[]byte("span1"), []byte("span2"), []byte("span3")},
					spanIDs: []string{"span1", "span2", "span3"},
				}
			},
		},
		{
			name:     "medium_parts_non_overlapping",
			numParts: 50,
			tracesGen: func() *traces {
				return generateHugeTraces(100)
			},
		},
		{
			name:     "large_parts_non_overlapping",
			numParts: 100,
			tracesGen: func() *traces {
				return generateHugeTraces(500)
			},
		},
	}

	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			tmpPath, defFn := test.Space(require.New(b))
			defer defFn()

			fileSystem := fs.NewLocalFileSystem()

			// Create parts with non-overlapping trace IDs (fast path)
			var parts []*part
			for i := 0; i < bc.numParts; i++ {
				mp := generateMemPart()
				traces := bc.tracesGen()
				// Make trace IDs unique per part to trigger fast path
				for j := range traces.traceIDs {
					traces.traceIDs[j] = traces.traceIDs[j] + "_part" + string(rune(i))
				}
				mp.mustInitFromTraces(traces)
				mp.mustFlush(fileSystem, partPath(tmpPath, uint64(i)))
				p := mustOpenFilePart(uint64(i), tmpPath, fileSystem)
				parts = append(parts, p)
				releaseMemPart(mp)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Create new iterators for each benchmark iteration
				var pmi []*partMergeIter
				for _, p := range parts {
					iter := generatePartMergeIter()
					iter.mustInitFromPart(p)
					pmi = append(pmi, iter)
				}

				br := generateBlockReader()
				br.init(pmi)
				bw := generateBlockWriter()
				dstPath := partPath(tmpPath, uint64(10000+i))
				bw.mustInitForFilePart(fileSystem, dstPath, false)

				closeCh := make(chan struct{})

				_, _, _, err := mergeBlocks(closeCh, bw, br)
				if err != nil {
					b.Fatal(err)
				}

				close(closeCh)
				releaseBlockWriter(bw)
				releaseBlockReader(br)

				// Release iterators
				for _, iter := range pmi {
					releasePartMergeIter(iter)
				}
			}
		})
	}
}

// Benchmark_mergeBlocks_OriginalMerge benchmarks the slow path where trace IDs overlap.
// This requires unmarshaling, merging blocks, and then writing them.
//
// Performance characteristics:
// - Higher memory usage (up to 1400x more for large datasets)
// - More allocations
// - Can be slightly faster for some workloads due to batching
// - Required when trace IDs overlap across parts (needs merging).
func Benchmark_mergeBlocks_OriginalMerge(b *testing.B) {
	benchmarkCases := []struct {
		tracesGen func() *traces
		name      string
		numParts  int
	}{
		{
			name:     "small_parts_overlapping",
			numParts: 10,
			tracesGen: func() *traces {
				return &traces{
					traceIDs:   []string{"trace1", "trace2", "trace3"},
					timestamps: []int64{1, 2, 3},
					tags: [][]*tagValue{
						{{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val1"), valueArr: nil}},
						{{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val2"), valueArr: nil}},
						{{tag: "tag1", valueType: pbv1.ValueTypeStr, value: []byte("val3"), valueArr: nil}},
					},
					spans:   [][]byte{[]byte("span1"), []byte("span2"), []byte("span3")},
					spanIDs: []string{"span1", "span2", "span3"},
				}
			},
		},
		{
			name:     "medium_parts_overlapping",
			numParts: 50,
			tracesGen: func() *traces {
				return generateHugeTraces(100)
			},
		},
		{
			name:     "large_parts_overlapping",
			numParts: 100,
			tracesGen: func() *traces {
				return generateHugeTraces(500)
			},
		},
	}

	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			tmpPath, defFn := test.Space(require.New(b))
			defer defFn()

			fileSystem := fs.NewLocalFileSystem()

			// Create parts with overlapping trace IDs (slow path)
			var parts []*part
			for i := 0; i < bc.numParts; i++ {
				mp := generateMemPart()
				traces := bc.tracesGen()
				// Keep same trace IDs across parts to trigger slow path (merging)
				mp.mustInitFromTraces(traces)
				mp.mustFlush(fileSystem, partPath(tmpPath, uint64(i)))
				p := mustOpenFilePart(uint64(i), tmpPath, fileSystem)
				parts = append(parts, p)
				releaseMemPart(mp)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Create new iterators for each benchmark iteration
				var pmi []*partMergeIter
				for _, p := range parts {
					iter := generatePartMergeIter()
					iter.mustInitFromPart(p)
					pmi = append(pmi, iter)
				}

				br := generateBlockReader()
				br.init(pmi)
				bw := generateBlockWriter()
				dstPath := partPath(tmpPath, uint64(20000+i))
				bw.mustInitForFilePart(fileSystem, dstPath, false)

				closeCh := make(chan struct{})

				_, _, _, err := mergeBlocks(closeCh, bw, br)
				if err != nil {
					b.Fatal(err)
				}

				close(closeCh)
				releaseBlockWriter(bw)
				releaseBlockReader(br)

				// Release iterators
				for _, iter := range pmi {
					releasePartMergeIter(iter)
				}
			}
		})
	}
}

// Benchmark_mergeBlocks_Comparison provides a direct comparison benchmark
// between fast raw merge and original merge with the SAME test data.
//
// This properly compares the two merge strategies by using identical data
// (non-overlapping trace IDs) and toggling the forceSlowMerge flag.
//
// Uses generateRealisticTraces with:
// - More than 10,000 traces per part
// - Each trace has 3-5 spans
// - Each span is more than 100KB
//
// Usage:
//
//	go test -bench=Benchmark_mergeBlocks_Comparison -run='^$' ./banyand/trace -benchtime=10x
//
// This benchmark clearly demonstrates the memory vs. time tradeoff:
// - Fast raw merge: Lower memory, uses raw block copy
// - Slow merge (forced): Higher memory, decompresses and processes blocks.
func Benchmark_mergeBlocks_Comparison(b *testing.B) {
	numParts := 20
	tracesPerPart := 10000 // More than 10,000 traces per part

	// Create shared test data with non-overlapping trace IDs
	tmpPath, defFn := test.Space(require.New(b))
	defer defFn()

	fileSystem := fs.NewLocalFileSystem()

	b.Logf("Creating %d parts with %d traces each (3-5 spans per trace, >100KB per span)", numParts, tracesPerPart)

	var parts []*part
	for i := 0; i < numParts; i++ {
		mp := generateMemPart()
		// Use new realistic trace generator
		traces := generateRealisticTraces(tracesPerPart)
		// Make trace IDs unique per part to avoid merging
		for j := range traces.traceIDs {
			traces.traceIDs[j] = traces.traceIDs[j] + "_part" + string(rune(i))
		}
		mp.mustInitFromTraces(traces)
		mp.mustFlush(fileSystem, partPath(tmpPath, uint64(i)))
		p := mustOpenFilePart(uint64(i), tmpPath, fileSystem)
		parts = append(parts, p)
		releaseMemPart(mp)

		if i == 0 {
			// Log stats for first part
			totalSpans := len(traces.spans)
			totalSize := int64(0)
			for _, span := range traces.spans {
				totalSize += int64(len(span))
			}
			b.Logf("Part 0 stats: %d spans, total size: %.2f MB, avg span size: %.2f KB",
				totalSpans, float64(totalSize)/(1024*1024), float64(totalSize)/float64(totalSpans)/1024)
		}
	}

	b.Run("fast_raw_merge", func(b *testing.B) {
		forceSlowMerge = false // Enable fast path
		defer func() { forceSlowMerge = false }()

		// Create separate temp dir for fast merge output
		tmpPathFast, defFnFast := test.Space(require.New(b))
		defer defFnFast()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Create new iterators for each benchmark iteration
			var pmi []*partMergeIter
			for _, p := range parts {
				iter := generatePartMergeIter()
				iter.mustInitFromPart(p)
				pmi = append(pmi, iter)
			}

			br := generateBlockReader()
			br.init(pmi)
			bw := generateBlockWriter()
			dstPath := partPath(tmpPathFast, uint64(30000+i))
			bw.mustInitForFilePart(fileSystem, dstPath, false)

			closeCh := make(chan struct{})

			_, _, _, err := mergeBlocks(closeCh, bw, br)
			if err != nil {
				b.Fatal(err)
			}

			close(closeCh)
			releaseBlockWriter(bw)
			releaseBlockReader(br)

			// Release iterators
			for _, iter := range pmi {
				releasePartMergeIter(iter)
			}
		}
	})

	b.Run("slow_merge_forced", func(b *testing.B) {
		forceSlowMerge = true // Force slow path (original merge)
		defer func() { forceSlowMerge = false }()

		// Create separate temp dir for slow merge output
		tmpPathSlow, defFnSlow := test.Space(require.New(b))
		defer defFnSlow()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Create new iterators for each benchmark iteration
			var pmi []*partMergeIter
			for _, p := range parts {
				iter := generatePartMergeIter()
				iter.mustInitFromPart(p)
				pmi = append(pmi, iter)
			}

			br := generateBlockReader()
			br.init(pmi)
			bw := generateBlockWriter()
			dstPath := partPath(tmpPathSlow, uint64(40000+i))
			bw.mustInitForFilePart(fileSystem, dstPath, false)

			closeCh := make(chan struct{})

			_, _, _, err := mergeBlocks(closeCh, bw, br)
			if err != nil {
				b.Fatal(err)
			}

			close(closeCh)
			releaseBlockWriter(bw)
			releaseBlockReader(br)

			// Release iterators
			for _, iter := range pmi {
				releasePartMergeIter(iter)
			}
		}
	})
}
