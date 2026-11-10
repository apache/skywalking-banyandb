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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
)

func TestDistributedIndexFramesRequireOrderKey(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	t.Cleanup(func() { data.SetStreamWireModeRaw(false) })
	for _, rows := range [][]nodeRow{nil, {{ts: 1, elementID: 1}}} {
		plan := newDistributedWindowPlan(false, 0, 1)
		plan.sortByTime = false
		got, err := executePlan(t, plan, []any{frameChunk(t, rows)})
		require.ErrorContains(t, err, "order key")
		require.Nil(t, got)
	}
}

func TestDistributedMaterializerCoversMixedReply(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	t.Cleanup(func() { data.SetStreamWireModeRaw(false) })
	plan := newDistributedWindowPlan(false, 0, 2)
	rows := 0
	plan.buildElement = func(batch *vectorized.RecordBatch, projection []model.TagProjection, row int) (*streamv1.Element, error) {
		rows++
		return vstream.BuildElementFromBatchRow(batch, projection, row)
	}
	got, err := executePlan(t, plan,
		[]any{frameChunk(t, []nodeRow{{ts: 1, elementID: 1}})},
		[]any{protoResponseForRows([]nodeRow{{ts: 2, elementID: 2}})})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, 1, rows, "the frame fallback must use the injected materializer too")
}

func distributedSpanTags(spans []*commonv1.Span) map[string]string {
	for _, span := range spans {
		if span.Message == "distributed-client" {
			tags := make(map[string]string)
			for _, tag := range span.Tags {
				tags[tag.Key] = tag.Value
			}
			return tags
		}
		if tags := distributedSpanTags(span.Children); tags != nil {
			return tags
		}
	}
	return nil
}

func TestDistributedFrameSpanCountsBeforeWindow(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	t.Cleanup(func() { data.SetStreamWireModeRaw(false) })
	tracer, ctx := query.NewTracer(context.Background(), "root")
	fake := &fakeDistributedContext{streams: []*fakeNodeStream{
		{name: "one", chunks: []any{frameChunk(t, []nodeRow{{ts: 1, elementID: 1}, {ts: 3, elementID: 3}})}},
		{name: "two", chunks: []any{frameChunk(t, []nodeRow{{ts: 1, elementID: 1}, {ts: 2, elementID: 2}})}},
	}}
	got, err := newDistributedWindowPlan(false, 1, 1).Execute(executor.WithDistributedExecutionContext(ctx, fake))
	require.NoError(t, err)
	require.Equal(t, []string{hexElementID(2)}, elementIDSeq(got))
	tags := distributedSpanTags(tracer.ToProto().Spans)
	require.Equal(t, "2", tags["response_count"])
	require.Equal(t, "3", tags["element_id_count"])
}
