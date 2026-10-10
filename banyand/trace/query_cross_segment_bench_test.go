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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations
// under the License.

package trace

import (
	"context"
	"testing"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	vtrace "github.com/apache/skywalking-banyandb/pkg/query/vectorized/trace"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// BenchmarkTraceQuerySegmentBoundary compares a trace-ID query whose range is
// far from any segment boundary (no extra segment should ever open, fixed or
// not) against one within traceCompletionWindow of a boundary (which opens
// one extra segment once the Phase 2 widening fix is applied). Run this
// benchmark against origin/main and against the fix to compare; both share
// the exact same fixture/query code, since it only calls the stable
// (*trace).Query entry point.
func BenchmarkTraceQuerySegmentBoundary(b *testing.B) {
	b.Run("FarFromBoundary", func(b *testing.B) {
		benchmarkCrossSegmentTraceIDQuery(b, false)
	})
	b.Run("NearBoundary", func(b *testing.B) {
		benchmarkCrossSegmentTraceIDQuery(b, true)
	})
}

func benchmarkCrossSegmentTraceIDQuery(b *testing.B, nearBoundary bool) {
	fixture := newCrossSegmentFixture(b, "bench-cross-segment")
	defer fixture.cleanup()

	fixture.writeSpan(b, fixture.boundary.Add(-30*time.Second), spanDayA)
	fixture.writeSpan(b, fixture.boundary.Add(30*time.Second), spanDayB)

	tr := &trace{
		pm:         protector.Nop{},
		vectorized: vtrace.VectorizedConfig{BatchSize: 10, QueryMemoryMiB: 100},
		schema: &databasev1.Trace{
			Metadata:         &commonv1.Metadata{Name: "bench-cross-segment", Group: "bench-cross-segment"},
			TraceIdTagName:   "trace_id",
			SpanIdTagName:    "span_id",
			TimestampTagName: "timestamp",
		},
	}
	tr.tsdb.Store(fixture.tsdb)

	var timeRange timestamp.TimeRange
	if nearBoundary {
		// Matches narrowRange(): [boundary-60s, boundary) -- within
		// traceCompletionWindow of day A/day B's shared edge.
		timeRange = fixture.narrowRange()
	} else {
		// Deep inside day A, >1h from either of its edges: widening must find
		// no new segment (the widened lookup re-resolves day A itself).
		timeRange = timestamp.NewTimeRange(fixture.boundary.Add(-20*time.Hour), fixture.boundary.Add(-19*time.Hour), true, false)
	}
	tqo := model.TraceQueryOptions{
		TraceIDs:  []string{fixture.traceID},
		TimeRange: &timeRange,
	}

	ctx := context.Background()
	cpuBefore := traceBenchProcessCPU()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := tr.Query(ctx, tqo)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		for result.Pull() != nil {
		}
		result.Release()
	}
	b.StopTimer()
	cpu := traceBenchProcessCPU() - cpuBefore
	b.ReportMetric(float64(cpu.Nanoseconds())/float64(b.N), "cpu-ns/op")
}
