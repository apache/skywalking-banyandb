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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

func TestInternalTraceToResultPreservesTagOrderAndSpanAlignment(t *testing.T) {
	got := internalTraceToResult(&tracev1.InternalTrace{
		TraceId: "trace-1",
		Spans: []*tracev1.Span{
			{
				Span:   []byte("span-1"),
				SpanId: "span-1",
				Tags: []*modelv1.Tag{
					{Key: "service_id", Value: strTagValueForDistributedTest("svc")},
					{Key: "trace_id", Value: pbv1.NullTagValue},
					{Key: "trace_id", Value: strTagValueForDistributedTest("trace-1")},
					{Key: "span_id", Value: strTagValueForDistributedTest("span-1")},
				},
			},
			{
				Span:   []byte("span-2"),
				SpanId: "span-2",
				Tags: []*modelv1.Tag{
					{Key: "service_id", Value: strTagValueForDistributedTest("svc")},
					{Key: "trace_id", Value: strTagValueForDistributedTest("trace-1")},
					{Key: "span_id", Value: strTagValueForDistributedTest("span-2")},
				},
			},
		},
	})

	require.Len(t, got.Tags, 3)
	require.Equal(t, "service_id", got.Tags[0].Name)
	require.Equal(t, "trace_id", got.Tags[1].Name)
	require.Equal(t, "span_id", got.Tags[2].Name)
	require.Equal(t, "trace-1", got.Tags[1].Values[0].GetStr().GetValue())
	require.Equal(t, "trace-1", got.Tags[1].Values[1].GetStr().GetValue())
	require.Equal(t, "span-1", got.Tags[2].Values[0].GetStr().GetValue())
	require.Equal(t, "span-2", got.Tags[2].Values[1].GetStr().GetValue())
}

func TestInternalTraceToResultNullFillsMissingSpanTags(t *testing.T) {
	got := internalTraceToResult(&tracev1.InternalTrace{
		TraceId: "trace-1",
		Spans: []*tracev1.Span{
			{
				Span:   []byte("span-1"),
				SpanId: "span-1",
				Tags: []*modelv1.Tag{
					{Key: "service_id", Value: strTagValueForDistributedTest("svc")},
					{Key: "only_on_first", Value: strTagValueForDistributedTest("first")},
				},
			},
			{
				Span:   []byte("span-2"),
				SpanId: "span-2",
				Tags: []*modelv1.Tag{
					{Key: "service_id", Value: strTagValueForDistributedTest("svc")},
					{Key: "only_on_second", Value: strTagValueForDistributedTest("second")},
				},
			},
		},
	})

	spanCount := len(got.Spans)
	require.Equal(t, 2, spanCount)

	values := make(map[string][]*modelv1.TagValue, len(got.Tags))
	for _, tag := range got.Tags {
		require.Lenf(t, tag.Values, spanCount, "tag %q must have one value per span", tag.Name)
		values[tag.Name] = tag.Values
	}

	require.Equal(t, "svc", values["service_id"][0].GetStr().GetValue())
	require.Equal(t, "svc", values["service_id"][1].GetStr().GetValue())

	// only_on_first is present on span-1 and forward-filled with NULL on span-2.
	require.Equal(t, "first", values["only_on_first"][0].GetStr().GetValue())
	require.Same(t, pbv1.NullTagValue, values["only_on_first"][1])

	// only_on_second is back-filled with NULL on span-1 and present on span-2.
	require.Same(t, pbv1.NullTagValue, values["only_on_second"][0])
	require.Equal(t, "second", values["only_on_second"][1].GetStr().GetValue())
}

func TestMergeTraceResultSpansUnionsSrcOnlyTagColumns(t *testing.T) {
	// dst (from node A) holds span s1 with only service_id.
	dst := model.TraceResult{
		TID:     "t1",
		Spans:   [][]byte{[]byte("s1")},
		SpanIDs: []string{"s1"},
		Tags: []model.Tag{
			{Name: "service_id", Values: []*modelv1.TagValue{strTagValueForDistributedTest("svc")}},
		},
	}
	// src (from node B) re-reports s1 (duplicate) and adds s2, plus a tag column
	// http_method that dst never saw.
	src := model.TraceResult{
		TID:     "t1",
		Spans:   [][]byte{[]byte("s1"), []byte("s2")},
		SpanIDs: []string{"s1", "s2"},
		Tags: []model.Tag{
			{Name: "service_id", Values: []*modelv1.TagValue{strTagValueForDistributedTest("svc"), strTagValueForDistributedTest("svc")}},
			{Name: "http_method", Values: []*modelv1.TagValue{strTagValueForDistributedTest("GET"), strTagValueForDistributedTest("POST")}},
		},
	}

	mergeTraceResultSpans(&dst, &src)

	// Duplicate span s1 is skipped; only s2 is appended.
	require.Equal(t, []string{"s1", "s2"}, dst.SpanIDs)
	require.Len(t, dst.Spans, 2)

	// The src-only http_method column is unioned in (not dropped), and every
	// column stays aligned with the span count.
	values := make(map[string][]*modelv1.TagValue, len(dst.Tags))
	for _, tag := range dst.Tags {
		require.Lenf(t, tag.Values, len(dst.SpanIDs), "tag %q must have one value per span", tag.Name)
		values[tag.Name] = tag.Values
	}
	require.Contains(t, values, "service_id")
	require.Contains(t, values, "http_method")

	require.Equal(t, "svc", values["service_id"][0].GetStr().GetValue())
	require.Equal(t, "svc", values["service_id"][1].GetStr().GetValue())

	// s1 predates the src-only column, so it is NULL-backfilled; the newly
	// appended s2 carries its real value.
	require.Same(t, pbv1.NullTagValue, values["http_method"][0])
	require.Equal(t, "POST", values["http_method"][1].GetStr().GetValue())
}

func strTagValueForDistributedTest(value string) *modelv1.TagValue {
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_Str{
			Str: &modelv1.Str{Value: value},
		},
	}
}
