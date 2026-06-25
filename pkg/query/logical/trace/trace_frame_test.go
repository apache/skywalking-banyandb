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
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

func strTV(s string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s}}}
}

func intTV(i int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: i}}}
}

func nullTV() *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
}

func binTV(b []byte) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: b}}
}

func intArrTV(vs ...int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: vs}}}
}

func sampleTraceResults() []model.TraceResult {
	return []model.TraceResult{
		{
			TID:     "trace-1",
			Key:     -42,
			Spans:   [][]byte{[]byte("payload-1"), []byte("payload-2")},
			SpanIDs: []string{"span-1", "span-2"},
			Tags: []model.Tag{
				{Name: "service_id", Values: []*modelv1.TagValue{strTV("svc-0"), strTV("svc-1")}},
				{Name: "duration", Values: []*modelv1.TagValue{intTV(123), intTV(456)}},
				{Name: "missing", Values: []*modelv1.TagValue{nullTV(), nullTV()}},
				{Name: "binary", Values: []*modelv1.TagValue{binTV([]byte{0x1, 0x2, 0x3}), nullTV()}},
				{Name: "ints", Values: []*modelv1.TagValue{intArrTV(7, 8, 9), intArrTV(10)}},
			},
		},
		{
			TID:     "trace-2",
			Key:     100,
			Spans:   [][]byte{[]byte("p")},
			SpanIDs: []string{"s"},
			Tags:    nil,
		},
	}
}

func requireTraceResultsEqual(t *testing.T, want, got []model.TraceResult) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, want[i].TID, got[i].TID, "trace %d TID", i)
		require.Equal(t, want[i].Key, got[i].Key, "trace %d Key", i)
		require.Equal(t, want[i].Spans, got[i].Spans, "trace %d Spans", i)
		require.Equal(t, want[i].SpanIDs, got[i].SpanIDs, "trace %d SpanIDs", i)
		require.Len(t, got[i].Tags, len(want[i].Tags), "trace %d tag count", i)
		for j := range want[i].Tags {
			require.Equal(t, want[i].Tags[j].Name, got[i].Tags[j].Name, "trace %d tag %d name", i, j)
			require.Len(t, got[i].Tags[j].Values, len(want[i].Tags[j].Values), "trace %d tag %d value count", i, j)
			for k := range want[i].Tags[j].Values {
				require.True(t, proto.Equal(want[i].Tags[j].Values[k], got[i].Tags[j].Values[k]),
					"trace %d tag %d value %d: want %v got %v", i, j, k, want[i].Tags[j].Values[k], got[i].Tags[j].Values[k])
			}
		}
	}
}

func TestTraceResultFrame_RoundTrip(t *testing.T) {
	orig := sampleTraceResults()
	body := EncodeTraceResultFrame(orig)
	require.Greater(t, len(body), 0)
	require.Equal(t, data.RawFrameMagicLeadingByte, body[0], "frame must carry the raw-frame magic")
	require.Equal(t, traceFrameVersion, body[1], "frame must carry the version byte")

	got, err := DecodeTraceResultFrame(body)
	require.NoError(t, err)
	requireTraceResultsEqual(t, orig, got)
}

// TestTraceResultFrame_ExactPreSize locks the invariant that traceFrameSize
// equals the encoded length, so the encode buffer is pre-sized exactly (no
// realloc, no over-allocation).
func TestTraceResultFrame_ExactPreSize(t *testing.T) {
	orig := sampleTraceResults()
	body := EncodeTraceResultFrame(orig)
	require.Equal(t, traceFrameSize(orig), len(body), "traceFrameSize must equal the encoded length")
}

func TestTraceResultFrame_Empty(t *testing.T) {
	body := EncodeTraceResultFrame(nil)
	require.Equal(t, data.RawFrameMagicLeadingByte, body[0])
	got, err := DecodeTraceResultFrame(body)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestTraceResultFrame_BadMagic(t *testing.T) {
	_, err := DecodeTraceResultFrame([]byte{0xFF, traceFrameVersion})
	require.Error(t, err)
	_, err = DecodeTraceResultFrame([]byte{data.RawFrameMagicLeadingByte, 0x09})
	require.Error(t, err)
}
