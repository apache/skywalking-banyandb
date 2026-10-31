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

package plan

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
)

func traceSpanNamed(spans []*commonv1.Span, name string) *commonv1.Span {
	for _, span := range spans {
		if span.GetMessage() == name {
			return span
		}
		if child := traceSpanNamed(span.GetChildren(), name); child != nil {
			return child
		}
	}
	return nil
}

func traceTagOf(span *commonv1.Span, key string) string {
	for _, tag := range span.GetTags() {
		if tag.GetKey() == key {
			return tag.GetValue()
		}
	}
	return ""
}

// TestStreamedTraceStaysBoundedWithManyDataNodes pins that the per-node sub-traces of a traced query are capped, and
// the surplus summarized, when the nodes stream their results, as they were when the nodes answered with one frame.
func TestStreamedTraceStaysBoundedWithManyDataNodes(t *testing.T) {
	previous := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	t.Cleanup(func() { data.SetMeasureWireModeRaw(previous) })
	const nodes = 25
	cluster := &scriptedCluster{}
	for node := range nodes {
		body, err := encodeTestFrame(rowsBatch(node))
		require.NoError(t, err)
		sub := &commonv1.Trace{Spans: []*commonv1.Span{{Message: fmt.Sprintf("data-%d", node)}}}
		cluster.streams = append(cluster.streams, nodeStreamOf(fmt.Sprintf("n%d", node)))
		cluster.streams[node].chunks = []any{&measurev1.InternalQueryResponse{RawFrameBody: body, Trace: sub}}
	}
	tracer, ctx := query.NewTracer(t.Context(), "bounded")
	root, ctx := tracer.StartSpan(ctx, "distributed")
	ctx = executor.WithDistributedExecutionContext(ctx, cluster)
	result, err := batchBoundaryPlan(t).ExecuteBatches(ctx)
	require.NoError(t, err)
	for {
		batch, pullErr := result.NextBatch(ctx)
		require.NoError(t, pullErr)
		if batch == nil {
			break
		}
	}
	require.NoError(t, result.Close())
	root.Stop()
	spans := tracer.ToProto().GetSpans()
	broadcast := traceSpanNamed(spans, "broadcast-rows")
	require.NotNil(t, broadcast)
	require.NotNil(t, broadcast.GetEndTime(), "the scan span ends when the result is closed")
	require.Equal(t, "25", traceTagOf(broadcast, "response_count"))
	require.NotEmpty(t, traceTagOf(broadcast, "frame_bytes_total"))
	require.LessOrEqual(t, len(broadcast.GetChildren()), maxIndividualSubTraces+1, "sub-traces are capped, the surplus is summarized")
	require.NotNil(t, traceSpanNamed(broadcast.GetChildren(), "data-summary"))
	require.NotNil(t, traceSpanNamed(spans, "decode-frame-summary"), "decode costs past the cap are summarized")
}
