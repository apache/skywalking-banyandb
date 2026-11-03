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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
	streamframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream/frame"
)

func TestDistributedBatchMergeSplitFrameOracle(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	defer data.SetStreamWireModeRaw(false)

	for _, tc := range []struct {
		name   string
		byTime bool
		desc   bool
	}{
		{name: "time ascending", byTime: true},
		{name: "time descending", byTime: true, desc: true},
		{name: "hidden key ascending"},
		{name: "hidden key descending", desc: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A appears on both nodes; each node sends one row per frame.
			// Time order and hidden-key order both give A,C,B before windowing.
			var first, second []any
			if tc.byTime {
				first = []any{frameChunk(t, []nodeRow{{ts: 1, elementID: 1}}), frameChunk(t, []nodeRow{{ts: 3, elementID: 3}})}
				second = []any{frameChunk(t, []nodeRow{{ts: 1, elementID: 1}}), frameChunk(t, []nodeRow{{ts: 2, elementID: 2}})}
			} else {
				// Timestamp order disagrees with key order, so timestamp fallback fails.
				first = []any{
					orderedFrameChunk(t, []orderedRow{{ts: 100, elementID: 1, seq: "1"}}),
					orderedFrameChunk(t, []orderedRow{{ts: 50, elementID: 3, seq: "3"}}),
				}
				second = []any{
					orderedFrameChunk(t, []orderedRow{{ts: 100, elementID: 1, seq: "1"}}),
					orderedFrameChunk(t, []orderedRow{{ts: 10, elementID: 2, seq: "2"}}),
				}
			}
			offset := uint32(1)
			want := []string{hexElementID(2), hexElementID(3)}
			if tc.desc {
				first[0], first[1] = first[1], first[0]
				second[0], second[1] = second[1], second[0]
				offset = 0
				want = []string{hexElementID(3), hexElementID(2)}
			}
			plan := newBatchMergePlan(tc.desc, offset, 2)
			plan.sortByTime = tc.byTime
			got, err := executePlan(t, plan, first, second)
			require.NoError(t, err)
			require.Equal(t, want, elementIDSeq(got))
			if !tc.byTime {
				for _, element := range got {
					require.Len(t, element.TagFamilies, 1)
					require.Equal(t, "fam", element.TagFamilies[0].Name)
					require.Len(t, element.TagFamilies[0].Tags, 1)
					require.Equal(t, "svc", element.TagFamilies[0].Tags[0].Key)
				}
			}
		})
	}
}

func TestDistributedBatchMergePreservesSourceProjection(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	defer data.SetStreamWireModeRaw(false)

	// The first node projects a tag that the second does not define. Alignment
	// must not turn the missing tag into a client-visible null on node two.
	withExtra := projectedFrameChunk(t, []nodeRow{{ts: 1, elementID: 1}, {ts: 3, elementID: 3}},
		[]string{"svc", "extra"})
	withoutExtra := projectedFrameChunk(t, []nodeRow{{ts: 2, elementID: 2}, {ts: 4, elementID: 4}},
		[]string{"svc"})
	got, err := executePlan(t, newBatchMergePlan(false, 0, 10), []any{withExtra}, []any{withoutExtra})
	require.NoError(t, err)
	require.Equal(t, []string{hexElementID(1), hexElementID(2), hexElementID(3), hexElementID(4)}, elementIDSeq(got))
	for idx, element := range got {
		require.Len(t, element.TagFamilies, 1)
		require.Equal(t, "fam", element.TagFamilies[0].Name)
		tags := element.TagFamilies[0].Tags
		if idx%2 == 0 {
			require.Len(t, tags, 2)
			require.Equal(t, "extra", tags[1].Key)
			require.Equal(t, "extra-value", tags[1].Value.GetStr().GetValue())
		} else {
			require.Len(t, tags, 1)
		}
		require.Equal(t, "svc", tags[0].Key)
		require.Equal(t, "svc-value", tags[0].Value.GetStr().GetValue())
	}
}

func TestDistributedBatchMergeEqualKeysWindowUsesScatterOrder(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	defer data.SetStreamWireModeRaw(false)

	for _, desc := range []bool{false, true} {
		plan := newBatchMergePlan(desc, 1, 2)
		// All timestamps tie. Fixed Scatter order stages node one's two frames
		// before node two, giving IDs 1,3,2,4 and therefore window 3,2.
		for range 5 {
			got, err := executePlan(t, plan,
				[]any{frameChunk(t, []nodeRow{{ts: 1, elementID: 1}}), frameChunk(t, []nodeRow{{ts: 1, elementID: 3}})},
				[]any{frameChunk(t, []nodeRow{{ts: 1, elementID: 2}}), frameChunk(t, []nodeRow{{ts: 1, elementID: 4}})})
			require.NoError(t, err)
			require.Equal(t, []string{hexElementID(3), hexElementID(2)}, elementIDSeq(got))
		}
	}
}

func projectedFrameChunk(t *testing.T, rows []nodeRow, names []string) []byte {
	t.Helper()
	schema := vstream.BuildStreamBatchSchema([]model.TagProjection{{Family: "fam", Names: names}}, "", "")
	batch := buildNodeBatch(schema, rows)
	for _, name := range names {
		idx, ok := schema.TagIndex("fam", name)
		require.True(t, ok)
		column := batch.Columns[idx].(*vectorized.TypedColumn[*modelv1.TagValue])
		for range rows {
			column.Append(&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: name + "-value"}}})
		}
	}
	frame, err := streamframe.Encode(batch)
	require.NoError(t, err)
	return frame
}
