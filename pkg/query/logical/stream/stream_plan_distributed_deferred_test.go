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

package stream

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
	streamframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream/frame"
)

// frameChunk encodes rows as a native columnar frame chunk, the body a data node replies with
// when the raw wire mode is on and the query is not traced.
func frameChunk(t *testing.T, rows []nodeRow) []byte {
	t.Helper()
	schema := vstream.BuildStreamBatchSchema(nil, "", "")
	frame, encodeErr := streamframe.Encode(buildNodeBatch(schema, rows))
	require.NoError(t, encodeErr)
	return frame
}

// executePlan runs the plan against one fake node stream per chunk list, through the real
// Scatter + read + merge path.
func executePlan(t *testing.T, plan *distributedPlan, chunks ...[]any) ([]*streamv1.Element, error) {
	t.Helper()
	fake := &fakeDistributedContext{}
	for idx, chunk := range chunks {
		fake.streams = append(fake.streams, &fakeNodeStream{name: fmt.Sprintf("node-%d", idx), chunks: chunk})
	}
	return plan.Execute(executor.WithDistributedExecutionContext(context.Background(), fake))
}

func newDistributedWindowPlan(desc bool, offset, limit uint32) *distributedPlan {
	return &distributedPlan{
		queryTemplate: &streamv1.QueryRequest{Name: "s"},
		sortByTime:    true,
		desc:          desc,
		offset:        offset,
		limit:         limit,
	}
}

// TestDistributedDeferredMergeHandCalculatedOracle is the issue's concrete example, run through
// the real broadcast/read/merge path with frame replies only.
//
// Node 1 holds A (timestamp 1) and B (timestamp 3); node 2 holds A again (the same ElementID,
// a replica) and C (timestamp 2). Ascending the merged, de-duplicated order is A, C, B, so
// offset 1 limit 2 returns C, B; descending offset 0 limit 2 returns B, C.
func TestDistributedDeferredMergeHandCalculatedOracle(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	defer data.SetStreamWireModeRaw(false)

	nodeOne := []nodeRow{{ts: 1, elementID: 1}, {ts: 3, elementID: 3}}
	nodeTwo := []nodeRow{{ts: 1, elementID: 1}, {ts: 2, elementID: 2}}

	ascending, err := executePlan(t, newDistributedWindowPlan(false, 1, 2),
		[]any{frameChunk(t, nodeOne)}, []any{frameChunk(t, nodeTwo)})
	require.NoError(t, err)
	require.Equal(t, []string{hexElementID(2), hexElementID(3)}, elementIDSeq(ascending),
		"ascending offset 1 limit 2 must return C, B")

	// The row heap consumes each node in the order requested from that node.
	descending, err := executePlan(t, newDistributedWindowPlan(true, 0, 2),
		[]any{frameChunk(t, []nodeRow{nodeOne[1]}), frameChunk(t, []nodeRow{nodeOne[0]})},
		[]any{frameChunk(t, []nodeRow{nodeTwo[1]}), frameChunk(t, []nodeRow{nodeTwo[0]})})
	require.NoError(t, err)
	require.Equal(t, []string{hexElementID(3), hexElementID(2)}, elementIDSeq(descending),
		"descending offset 0 limit 2 must return B, C")
}

// TestDistributedDeferredMergeMaterializesOnlyTheWindow is the path-sensitive assertion the
// issue asks for. Comparing answers alone cannot show the optimization, because the old row
// merge returns the same answers; so count the rows that reach the columnar→proto egress and
// require exactly the client window, while the merge consumed more rows than that. Restoring
// per-reply Element materialization leaves the counter at zero and fails the test.
func TestDistributedDeferredMergeMaterializesOnlyTheWindow(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	defer data.SetStreamWireModeRaw(false)

	// Six input rows across two nodes, one duplicate, five distinct elements.
	nodeOne := []nodeRow{{ts: 1, elementID: 1}, {ts: 3, elementID: 3}, {ts: 5, elementID: 5}}
	nodeTwo := []nodeRow{{ts: 1, elementID: 1}, {ts: 2, elementID: 2}, {ts: 4, elementID: 4}}

	plan := newDistributedWindowPlan(false, 2, 2)
	materializedRows := 0
	plan.buildElement = func(batch *vectorized.RecordBatch, projection []model.TagProjection, row int) (*streamv1.Element, error) {
		materializedRows++
		return vstream.BuildElementFromBatchRow(batch, projection, row)
	}

	// Distinct order is 1, 2, 3, 4, 5; offset 2 limit 2 must return 3 and 4.
	got, err := executePlan(t, plan,
		[]any{frameChunk(t, nodeOne)}, []any{frameChunk(t, nodeTwo)})
	require.NoError(t, err)
	require.Equal(t, []string{hexElementID(3), hexElementID(4)}, elementIDSeq(got))
	require.Equal(t, 2, materializedRows,
		"only the rows of the client window may be materialized into Elements")
}

// TestDistributedMixedProtoAndFrameRepliesMatchAllProto keeps the rolling-upgrade contract:
// a broadcast that mixes a proto reply with a frame reply must return exactly what an all-proto broadcast returns.
func TestDistributedMixedProtoAndFrameRepliesMatchAllProto(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	defer data.SetStreamWireModeRaw(false)

	nodeOne := []nodeRow{{ts: 1, elementID: 100}, {ts: 40, elementID: 101}}
	nodeTwo := []nodeRow{{ts: 20, elementID: 200}, {ts: 50, elementID: 201}}
	nodeThree := []nodeRow{{ts: 30, elementID: 100}, {ts: 60, elementID: 300}}

	mixed, err := executePlan(t, newDistributedWindowPlan(false, 1, 3),
		[]any{protoResponseForRows(nodeOne)},
		[]any{frameChunk(t, nodeTwo)},
		[]any{protoResponseForRows(nodeThree)})
	require.NoError(t, err)

	allProto, err := executePlan(t, newDistributedWindowPlan(false, 1, 3),
		[]any{protoResponseForRows(nodeOne)},
		[]any{protoResponseForRows(nodeTwo)},
		[]any{protoResponseForRows(nodeThree)})
	require.NoError(t, err)

	require.Equal(t, elementIDSeq(allProto), elementIDSeq(mixed),
		"a mixed proto/frame broadcast must match the all-proto merge")
	require.Equal(t, []string{hexElementID(200), hexElementID(101), hexElementID(201)}, elementIDSeq(mixed))
}

// TestDistributedDeferredMergeOrdersByTagThroughOrderKey covers the ordered-tag case: the frames
// carry the order key as a hidden column, the merge orders by it instead of the timestamp, and
// the order tag stays out of the projected response.
func TestDistributedDeferredMergeOrdersByTagThroughOrderKey(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	defer data.SetStreamWireModeRaw(false)

	nodeOne := []orderedRow{{ts: 100, elementID: 1, seq: "1"}, {ts: 50, elementID: 3, seq: "3"}}
	nodeTwo := []orderedRow{{ts: 200, elementID: 1, seq: "1"}, {ts: 10, elementID: 2, seq: "2"}}

	plan := &distributedPlan{
		queryTemplate: &streamv1.QueryRequest{Name: "s"},
		sortByTime:    false,
		offset:        1,
		limit:         2,
	}
	got, err := executePlan(t, plan,
		[]any{orderedFrameChunk(t, nodeOne)}, []any{orderedFrameChunk(t, nodeTwo)})
	require.NoError(t, err)
	require.Equal(t, []string{hexElementID(2), hexElementID(3)}, elementIDSeq(got),
		"ordered by seq 1,1,2,3 then offset 1 limit 2 must return the elements with seq 2 and 3")
	for _, element := range got {
		require.Len(t, element.TagFamilies, 1)
		require.Len(t, element.TagFamilies[0].Tags, 1)
		require.Equal(t, "svc", element.TagFamilies[0].Tags[0].Key,
			"the order key must not leak into the response projection")
	}
}

func TestDistributedDeferredMergeMixedIndexReplies(t *testing.T) {
	originalRaw := data.StreamWireModeRaw()
	data.SetStreamWireModeRaw(true)
	t.Cleanup(func() { data.SetStreamWireModeRaw(originalRaw) })

	plan := newDistributedWindowPlan(false, 1, 2)
	plan.sortByTime = false
	middle := scatterElement(1, 2)
	middle.TagFamilies = []*modelv1.TagFamily{{
		Name: "fam",
		Tags: []*modelv1.Tag{{Key: "seq", Value: &modelv1.TagValue{
			Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "2"}},
		}}},
	}}
	got, err := executePlan(t, plan,
		[]any{orderedFrameChunk(t, []orderedRow{{ts: 4, elementID: 1, seq: "1"}, {ts: 2, elementID: 3, seq: "3"}})},
		[]any{&streamv1.QueryResponse{Elements: []*streamv1.Element{middle}}})
	require.NoError(t, err)
	require.Equal(t, []string{hexElementID(2), hexElementID(3)}, elementIDSeq(got))
	require.Same(t, middle, got[0], "an existing proto Element is reused")
	require.Equal(t, "svc", got[1].GetTagFamilies()[0].GetTags()[0].GetKey())
}

// TestDistributedDeferredMergeOffsetBeyondResult returns an empty result instead of panicking
// when the offset is past the merged set.
func TestDistributedDeferredMergeOffsetBeyondResult(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	defer data.SetStreamWireModeRaw(false)

	got, err := executePlan(t, newDistributedWindowPlan(false, 5, 2),
		[]any{frameChunk(t, []nodeRow{{ts: 1, elementID: 1}})}, []any{frameChunk(t, []nodeRow{{ts: 2, elementID: 2}})})
	require.NoError(t, err)
	require.Empty(t, got)
}

// orderedRow is one element on a simulated data node for an index-ordered query.
type orderedRow struct {
	seq       string
	ts        int64
	elementID uint64
}

// orderedFrameChunk builds a frame carrying a projected tag and the hidden order-key column,
// the shape a data node emits for an ordered-tag query.
func orderedFrameChunk(t *testing.T, rows []orderedRow) []byte {
	t.Helper()
	projection := []model.TagProjection{{Family: "fam", Names: []string{"svc"}}}
	schema := vstream.BuildStreamBatchSchema(projection, "fam", "seq")
	batch := vectorized.NewRecordBatch(schema, len(rows))
	tsCol := batch.Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64])
	elemCol := batch.Columns[schema.ElementIDIndex()].(*vectorized.TypedColumn[int64])
	seriesCol := batch.Columns[schema.SeriesIDIndex()].(*vectorized.TypedColumn[int64])
	tagIdx, ok := schema.TagIndex("fam", "svc")
	require.True(t, ok)
	tagCol := batch.Columns[tagIdx].(*vectorized.TypedColumn[*modelv1.TagValue])
	orderIdx := schema.OrderKeyIndex()
	require.GreaterOrEqual(t, orderIdx, 0)
	orderCol := batch.Columns[orderIdx].(*vectorized.TypedColumn[[]byte])
	service := &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc-a"}}}
	for _, row := range rows {
		tsCol.Append(row.ts)
		elemCol.Append(vstream.ElementIDToColumn(row.elementID))
		seriesCol.Append(vstream.SeriesIDToColumn(0))
		tagCol.Append(service)
		seqValue := &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: row.seq}}}
		key, marshalErr := pbv1.MarshalTagValue(seqValue)
		require.NoError(t, marshalErr)
		orderCol.Append(key)
	}
	batch.Len = len(rows)
	frame, encodeErr := streamframe.Encode(batch)
	require.NoError(t, encodeErr)
	return frame
}
