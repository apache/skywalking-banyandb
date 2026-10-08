// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

//go:build unix

package nativeice

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"syscall"
	"testing"
	"time"

	roaringpkg "github.com/RoaringBitmap/roaring"
)

// seriesShapedGeneration is a series-index-shaped input: a unique identifier,
// a few low- and medium-cardinality entity tags, a stored source blob and a
// timestamp doc value.
func seriesShapedGeneration(prefix string, documentCount int) Generation {
	documents := make([]EncodeDocument, documentCount)
	for index := range documents {
		series := fmt.Sprintf("%s-%08d", prefix, index)
		field := func(name, value string) EncodeField {
			return EncodeField{Name: name, Value: []byte(value), Index: true, Store: true}
		}
		documents[index] = EncodeDocument{Identifier: []byte("service_instance_cpm/" + series), Fields: []EncodeField{
			field("_group", "sw_metricsMinute"),
			field("service", fmt.Sprintf("service-%d", index%10)),
			field("instance", fmt.Sprintf("instance-%d", index%1000)),
			field("entity", series),
			{Name: "_source", Value: []byte(`{"entity":"` + series + `","layer":"GENERAL","labels":{"k":"v"}}`), Store: true},
			{Name: "_timestamp", Value: []byte(fmt.Sprintf("%016x", 1_700_000_000_000+int64(index))), Sort: true},
		}}
	}
	return Generation{Documents: documents, SegmentID: 1, SnapshotID: 1}
}

func openSeriesShapedInputs(t testing.TB, segments, documentsPerSegment int) []MergeInput {
	t.Helper()
	inputs := make([]MergeInput, segments)
	for segment := range inputs {
		directory := t.TempDir()
		if encodeErr := Encode(directory, seriesShapedGeneration(fmt.Sprintf("s%02d", segment), documentsPerSegment)); encodeErr != nil {
			t.Fatal(encodeErr)
		}
		reader, openErr := OpenStrict(directory)
		if openErr != nil {
			t.Fatal(openErr)
		}
		t.Cleanup(func() { _ = reader.Close() })
		// A few dropped documents keep the renumbering path honest.
		drop := roaringpkg.New()
		drop.AddRange(uint64(documentsPerSegment/2), uint64(documentsPerSegment/2+10))
		inputs[segment] = MergeInput{Reader: reader, Drop: drop}
		// Warm the inputs' dictionaries the way live segments are warmed, so
		// the measurement isolates the merge's own memory.
		for _, field := range []string{identifierField, "_group", "service", "instance", "entity"} {
			if _, termErr := reader.TermExists(field, []byte("absent")); termErr != nil {
				t.Fatal(termErr)
			}
		}
	}
	return inputs
}

// mergeMeasurement is one merge's cost.
//
//nolint:govet // report fields grouped for readability.
type mergeMeasurement struct {
	wall             time.Duration
	cpu              time.Duration
	peakHeapGrowth   uint64
	peakLiveGrowth   uint64
	allocatedBytes   uint64
	allocatedObjects uint64
	outputBytes      uint64
}

func (m mergeMeasurement) String() string {
	return fmt.Sprintf("wall=%v cpu=%v peakHeapGrowth=%.1fMiB peakLiveHeapGrowth=%.1fMiB alloc=%.1fMiB objects=%d output=%.1fMiB",
		m.wall.Round(time.Millisecond), m.cpu.Round(time.Millisecond), float64(m.peakHeapGrowth)/(1<<20),
		float64(m.peakLiveGrowth)/(1<<20), float64(m.allocatedBytes)/(1<<20), m.allocatedObjects, float64(m.outputBytes)/(1<<20))
}

func processCPU() time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

// measureMerge runs merge while sampling heap object bytes (live plus not yet
// collected garbage) and the live heap marked by each GC. A low GC target
// keeps both close to the merge's true working set; the default target
// measures the CPU a merge costs in a normally tuned process.
func measureMerge(t testing.TB, gcPercent int, merge func() uint64) mergeMeasurement {
	t.Helper()
	previousPercent := debug.SetGCPercent(gcPercent)
	defer debug.SetGCPercent(previousPercent)
	samples := []metrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/gc/heap/live:bytes"},
		{Name: "/gc/heap/allocs:bytes"},
		{Name: "/gc/heap/allocs:objects"},
	}
	runtime.GC()
	runtime.GC()
	metrics.Read(samples)
	baseHeap, baseLive := samples[0].Value.Uint64(), samples[1].Value.Uint64()
	baseAllocBytes, baseAllocObjects := samples[2].Value.Uint64(), samples[3].Value.Uint64()
	var peakHeap, peakLive uint64
	var mu sync.Mutex
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		local := make([]metrics.Sample, 2)
		local[0].Name, local[1].Name = samples[0].Name, samples[1].Name
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			metrics.Read(local)
			mu.Lock()
			peakHeap = max(peakHeap, local[0].Value.Uint64())
			peakLive = max(peakLive, local[1].Value.Uint64())
			mu.Unlock()
			select {
			case <-done:
				return
			case <-ticker.C:
			}
		}
	}()
	startCPU, start := processCPU(), time.Now()
	output := merge()
	wall, cpu := time.Since(start), processCPU()-startCPU
	close(done)
	<-stopped
	metrics.Read(samples)
	growth := func(peak, base uint64) uint64 {
		if peak < base {
			return 0
		}
		return peak - base
	}
	return mergeMeasurement{
		wall: wall, cpu: cpu, outputBytes: output,
		peakHeapGrowth: growth(peakHeap, baseHeap), peakLiveGrowth: growth(peakLive, baseLive),
		allocatedBytes: samples[2].Value.Uint64() - baseAllocBytes, allocatedObjects: samples[3].Value.Uint64() - baseAllocObjects,
	}
}

func streamingMergeMeasurement(t testing.TB, gcPercent int, inputs []MergeInput) mergeMeasurement {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".native-merge-memory")
	measurement := measureMerge(t, gcPercent, func() uint64 {
		stats, mergeErr := MergeSegmentsToFile(context.Background(), inputs, path)
		if mergeErr != nil {
			t.Fatal(mergeErr)
		}
		return stats.Size
	})
	if removeErr := os.Remove(path); removeErr != nil {
		t.Fatal(removeErr)
	}
	return measurement
}

func materializedMergeMeasurement(t testing.TB, gcPercent int, inputs []MergeInput) mergeMeasurement {
	t.Helper()
	return measureMerge(t, gcPercent, func() uint64 {
		result, mergeErr := materializedMergeSegments(context.Background(), inputs)
		if mergeErr != nil {
			t.Fatal(mergeErr)
		}
		return uint64(len(result.Payload))
	})
}

// streamingMergeHeapBound is the fixed peak heap growth a streaming merge may
// reach regardless of its size: a few chunk buffers, the 4 MiB spill
// thresholds of the staged sections, one term's posting bitmap, the
// dictionary builder's registry, and garbage awaiting the next GC cycle.
// streamingMergeLiveBound is the same budget for the live heap each GC marks,
// which excludes that garbage.
const (
	streamingMergeHeapBound = 48 << 20
	streamingMergeLiveBound = 24 << 20
)

// TestStreamingMergeMemoryIsBounded merges 10 inputs of 1M documents in
// total, then twice that, and requires the streaming merge's peak heap growth
// to stay under a fixed bound and its live heap not to grow with the merge.
// Heap object bytes include garbage, whose headroom the GC sizes from the
// whole heap -- inputs included -- so only the live heap is compared across
// sizes. The fixtures take a while to build, so it is skipped in -short mode.
// NIDX_MERGE_COMPARE=1 additionally measures the previous materializing merge
// on the same inputs for comparison.
func TestStreamingMergeMemoryIsBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("builds 3M documents of merge fixtures")
	}
	compare := os.Getenv("NIDX_MERGE_COMPARE") != ""
	var streamed []mergeMeasurement
	for _, documentsPerSegment := range []int{100_000, 200_000} {
		inputs := openSeriesShapedInputs(t, 10, documentsPerSegment)
		measurement := streamingMergeMeasurement(t, 10, inputs)
		t.Logf("streaming merge of %d documents (GOGC=10): %v", 10*documentsPerSegment, measurement)
		if compare {
			t.Logf("streaming merge of %d documents (GOGC=100): %v", 10*documentsPerSegment, streamingMergeMeasurement(t, 100, inputs))
			t.Logf("materialized merge of %d documents (GOGC=10): %v", 10*documentsPerSegment, materializedMergeMeasurement(t, 10, inputs))
			t.Logf("materialized merge of %d documents (GOGC=100): %v", 10*documentsPerSegment, materializedMergeMeasurement(t, 100, inputs))
		}
		if measurement.peakHeapGrowth > streamingMergeHeapBound || measurement.peakLiveGrowth > streamingMergeLiveBound {
			t.Errorf("streaming merge of %d documents grew the heap by %d bytes (live %d), bounds %d (live %d)",
				10*documentsPerSegment, measurement.peakHeapGrowth, measurement.peakLiveGrowth, streamingMergeHeapBound, streamingMergeLiveBound)
		}
		streamed = append(streamed, measurement)
		for _, input := range inputs {
			_ = input.Reader.Close()
		}
	}
	// Doubling the merge must not double its live memory. Allow noise, but a
	// merge whose memory tracked its size would grow by close to 2x here.
	if small, large := streamed[0].peakLiveGrowth, streamed[1].peakLiveGrowth; large > small*3/2+(4<<20) {
		t.Errorf("streaming merge live memory scales with size: %d bytes at 1M documents, %d at 2M", small, large)
	}
}

func BenchmarkStreamingMergeSeriesShaped(b *testing.B) {
	inputs := openSeriesShapedInputs(b, 10, 20_000)
	path := filepath.Join(b.TempDir(), ".native-merge-bench")
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, mergeErr := MergeSegmentsToFile(context.Background(), inputs, path); mergeErr != nil {
			b.Fatal(mergeErr)
		}
	}
}
