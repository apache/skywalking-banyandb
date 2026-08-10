// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package trace

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

const dropSetBenchmarkTraceCount = 33353

func TestDroppedTraceIDsKeepsEncodedValuesByExactID(t *testing.T) {
	dropped := acquireDroppedTraceIDs()
	t.Cleanup(func() { releaseDroppedTraceIDs(dropped) })

	dropped.add("trace-a")
	dropped.add("trace-b")
	dropped.add("trace-b")
	dropped.add("trace-c")

	require.Equal(t, 3, dropped.len())
	require.False(t, dropped.keepEncoded(append([]byte{byte(idFormatV1)}, []byte("trace-b")...)))
	require.True(t, dropped.keepEncoded(append([]byte{byte(idFormatV1)}, []byte("trace")...)), "prefixes must not match")
	require.True(t, dropped.keepEncoded(append([]byte{byte(idFormatV1)}, []byte("trace-b-tail")...)), "extensions must not match")
	require.True(t, dropped.keepEncoded(nil), "malformed values must fail open")
	require.True(t, dropped.keepEncoded([]byte{0xff, 't'}), "unknown encodings must fail open")
}

func BenchmarkDroppedTraceIDLookup(b *testing.B) {
	traceIDs := make([]string, dropSetBenchmarkTraceCount)
	encoded := make([][]byte, dropSetBenchmarkTraceCount)
	for traceIdx := range traceIDs {
		traceIDs[traceIdx] = fmt.Sprintf("service-a-%032x", traceIdx)
	}
	for traceIdx := range encoded {
		queryIdx := traceIdx * 7919 % len(traceIDs)
		encoded[traceIdx] = append([]byte{byte(idFormatV1)}, traceIDs[queryIdx]...)
	}
	for _, ratio := range []int{1, 35, 99} {
		droppedTraceIDs := make([]string, 0, len(traceIDs)*ratio/100)
		for traceIdx, traceID := range traceIDs {
			if traceIdx%100 < ratio {
				droppedTraceIDs = append(droppedTraceIDs, traceID)
			}
		}
		b.Run(fmt.Sprintf("ratio-%d/compact-hash", ratio), func(b *testing.B) {
			dropped := acquireDroppedTraceIDs()
			for _, traceID := range droppedTraceIDs {
				dropped.add(traceID)
			}
			defer releaseDroppedTraceIDs(dropped)
			benchmarkDropSetLookup(b, encoded, dropped.keepEncoded)
		})
		b.Run(fmt.Sprintf("ratio-%d/sorted-slice", ratio), func(b *testing.B) {
			dropped := droppedTraceIDs
			benchmarkDropSetLookup(b, encoded, func(data []byte) bool {
				traceID := convert.BytesToString(data[1:])
				matchIdx := sort.SearchStrings(dropped, traceID)
				return matchIdx == len(dropped) || dropped[matchIdx] != traceID
			})
		})
		b.Run(fmt.Sprintf("ratio-%d/go-map", ratio), func(b *testing.B) {
			dropped := make(map[string]struct{}, len(droppedTraceIDs))
			for _, traceID := range droppedTraceIDs {
				dropped[traceID] = struct{}{}
			}
			benchmarkDropSetLookup(b, encoded, func(data []byte) bool {
				_, exists := dropped[string(data[1:])]
				return !exists
			})
		})
	}
}

func benchmarkDropSetLookup(b *testing.B, encoded [][]byte, keep func([]byte) bool) {
	b.Helper()
	b.ReportMetric(float64(len(encoded)), "lookups/op")
	b.ReportAllocs()
	b.ResetTimer()
	var retained int
	for range b.N {
		for _, data := range encoded {
			if keep(data) {
				retained++
			}
		}
	}
	if retained == 0 {
		b.Fatal("lookup result was not consumed")
	}
}
