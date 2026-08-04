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
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
	streamframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream/frame"
)

// nodeRow is one element on a simulated data node (time-ordered): (ts, elementID).
type nodeRow struct {
	ts        int64
	elementID uint64
}

func hexElementID(id uint64) string {
	return hex.EncodeToString(convert.Uint64ToBytes(id))
}

// buildNodeBatch builds a columnar batch for a set of rows using the (no-tag,
// no-order-key) time-order schema, so the frame round-trips through the stream
// codec exactly as a data node would emit it.
func buildNodeBatch(schema *vectorized.BatchSchema, rows []nodeRow) *vectorized.RecordBatch {
	b := vectorized.NewRecordBatch(schema, len(rows))
	ts := b.Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64])
	eid := b.Columns[schema.ElementIDIndex()].(*vectorized.TypedColumn[int64])
	sid := b.Columns[schema.SeriesIDIndex()].(*vectorized.TypedColumn[int64])
	for _, r := range rows {
		ts.Append(r.ts)
		eid.Append(vstream.ElementIDToColumn(r.elementID))
		sid.Append(vstream.SeriesIDToColumn(0))
	}
	b.Len = len(rows)
	return b
}

// protoResponseForRows builds a *streamv1.QueryResponse mirroring the same rows,
// so a node can present either a proto body or a frame body for the same data.
func protoResponseForRows(rows []nodeRow) *streamv1.QueryResponse {
	elements := make([]*streamv1.Element, 0, len(rows))
	for _, r := range rows {
		elements = append(elements, &streamv1.Element{
			Timestamp: timestamppb.New(time.Unix(0, r.ts)),
			ElementId: hexElementID(r.elementID),
		})
	}
	return &streamv1.QueryResponse{Elements: elements}
}

// runLiaisonMerge reproduces distributedPlan.Execute's post-broadcast merge over
// a set of pre-decoded node responses: decode each (proto or []byte frame) via
// decodeNodeElements, k-way merge by time, first-seen dedup by ElementId, then the
// distributedLimit offset:offset+limit slice. It is the exact production merge
// stack minus the Broadcaster.
func runLiaisonMerge(t *testing.T, plan *distributedPlan, nodeResponses []any, offset, limit uint32) []*streamv1.Element {
	t.Helper()
	var see []sort.Iterator[*comparableElement]
	for _, d := range nodeResponses {
		elements, decErr := plan.decodeNodeElements(d, nil)
		require.NoError(t, decErr)
		see = append(see, newSortableElements(elements, plan.sortByTime, plan.sortTagSpec))
	}
	iter := sort.NewItemIter(see, plan.desc)
	var merged []*streamv1.Element
	seen := make(map[string]bool)
	for iter.Next() {
		element := iter.Val().Element
		if !seen[element.ElementId] {
			seen[element.ElementId] = true
			merged = append(merged, element)
		}
	}
	start := int(offset)
	if start > len(merged) {
		return []*streamv1.Element{}
	}
	end := start + int(limit)
	if end > len(merged) {
		end = len(merged)
	}
	return merged[start:end]
}

func elementIDSeq(elements []*streamv1.Element) []string {
	out := make([]string, len(elements))
	for i, e := range elements {
		out[i] = e.ElementId
	}
	return out
}

// TestLiaisonMerge_MixedProtoAndFrame_MatchesAllProto is the M6 liaison
// decode+merge unit test. Three data nodes each present a set of rows; one node's
// body is a native columnar frame ([]byte), the others are proto responses. The
// merged/deduped/limited result (with offset>0 and cross-node duplicate
// ElementIDs) MUST be identical to feeding the equivalent all-proto bodies.
//
// This is a UNIT-level test of the liaison merge stack (decodeNodeElements +
// k-way merge + seen-dedup + distributedLimit). A full multi-data-node broadcast
// harness is out of scope in-session; see the report notes.
func TestLiaisonMerge_MixedProtoAndFrame_MatchesAllProto(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	defer data.SetStreamWireModeRaw(false)

	// Time-order schema, no tags / no order key — matches a projection-free query.
	schema := vstream.BuildStreamBatchSchema(nil, "", "")

	// Node A and Node C share elementID 100 (cross-node duplicate); first-seen in
	// time order must win. Timestamps interleave across nodes to exercise the merge.
	nodeA := []nodeRow{{ts: 10, elementID: 100}, {ts: 40, elementID: 101}}
	nodeB := []nodeRow{{ts: 20, elementID: 200}, {ts: 50, elementID: 201}}
	nodeC := []nodeRow{{ts: 30, elementID: 100}, {ts: 60, elementID: 300}}

	plan := &distributedPlan{sortByTime: true}

	// Encode node B as a native frame; A and C stay proto (mixed rolling deploy).
	frameB, encErr := streamframe.Encode(buildNodeBatch(schema, nodeB))
	require.NoError(t, encErr)

	const offset, limit = uint32(1), uint32(3)

	mixed := []any{
		protoResponseForRows(nodeA),
		frameB,
		protoResponseForRows(nodeC),
	}
	allProto := []any{
		protoResponseForRows(nodeA),
		protoResponseForRows(nodeB),
		protoResponseForRows(nodeC),
	}

	mixedResult := runLiaisonMerge(t, plan, mixed, offset, limit)
	protoResult := runLiaisonMerge(t, plan, allProto, offset, limit)

	require.Equal(t, elementIDSeq(protoResult), elementIDSeq(mixedResult),
		"mixed proto/frame merge diverged from all-proto merge")

	// Sanity: the merged full order is 100(ts10),200(ts20),[100@ts30 dropped dup],
	// 101(ts40),201(ts50),300(ts60) → deduped [100,200,101,201,300]; offset 1 limit
	// 3 → [200,101,201].
	require.Equal(t, []string{
		hexElementID(200), hexElementID(101), hexElementID(201),
	}, elementIDSeq(mixedResult))
}

// TestLiaisonMerge_FrameDecodeProducesProjectedTags verifies that a frame carrying
// projected tag columns decodes back into Elements with the same tag families/tags
// the proto path would carry, so mixed-body merges stay value-identical.
func TestLiaisonMerge_FrameDecodeProducesProjectedTags(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	defer data.SetStreamWireModeRaw(false)

	proj := []model.TagProjection{{Family: "fam", Names: []string{"svc"}}}
	schema := vstream.BuildStreamBatchSchema(proj, "", "")
	b := vectorized.NewRecordBatch(schema, 1)
	b.Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64]).Append(10)
	b.Columns[schema.ElementIDIndex()].(*vectorized.TypedColumn[int64]).Append(vstream.ElementIDToColumn(7))
	b.Columns[schema.SeriesIDIndex()].(*vectorized.TypedColumn[int64]).Append(vstream.SeriesIDToColumn(0))
	tagColIdx, ok := schema.TagIndex("fam", "svc")
	require.True(t, ok)
	b.Columns[tagColIdx].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(
		&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc-A"}}})
	b.Len = 1

	frame, encErr := streamframe.Encode(b)
	require.NoError(t, encErr)

	plan := &distributedPlan{sortByTime: true}
	elements, decErr := plan.decodeNodeElements(frame, nil)
	require.NoError(t, decErr)
	require.Len(t, elements, 1)
	require.Equal(t, hexElementID(7), elements[0].ElementId)
	require.Len(t, elements[0].TagFamilies, 1)
	require.Equal(t, "fam", elements[0].TagFamilies[0].Name)
	require.Equal(t, "svc", elements[0].TagFamilies[0].Tags[0].Key)
	require.Equal(t, "svc-A", elements[0].TagFamilies[0].Tags[0].Value.GetStr().GetValue())
}
