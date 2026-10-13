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

package measure

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// pinOwner is an Owner that counts its references, standing in for storage memory a
// drained batch borrows.
type pinOwner struct{ refs int }

func (o *pinOwner) Retain()  { o.refs++ }
func (o *pinOwner) Release() { o.refs-- }

type batchesSource struct {
	schema  *vectorized.BatchSchema
	batches []*vectorized.RecordBatch
	next    int
}

func (s *batchesSource) Init(context.Context) error            { return nil }
func (s *batchesSource) OutputSchema() *vectorized.BatchSchema { return s.schema }
func (s *batchesSource) Close() error                          { return nil }
func (s *batchesSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	if s.next >= len(s.batches) {
		return nil, nil
	}
	b := s.batches[s.next]
	s.next++
	return b, nil
}

func drainSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTimestamp, Name: "_timestamp", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "entity", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeFloat64},
	})
}

// drainBatch builds rows of drainSchema; svc borrows from owner-backed memory in the sense of
// the pin only, and entity is a constant column.
func drainBatch(schema *vectorized.BatchSchema, firstTS int64, rows int, owner vectorized.Owner) *vectorized.RecordBatch {
	b := vectorized.NewRecordBatch(schema, rows)
	for i := range rows {
		b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
		b.Columns[1].(*vectorized.TypedColumn[int64]).Append(firstTS + int64(i))
		svc := b.Columns[2].(*vectorized.TypedColumn[string])
		if i%5 == 3 {
			svc.AppendNull()
		} else {
			svc.Append(fmt.Sprintf("svc-%d", (int(firstTS)+i)%4))
		}
		v := b.Columns[4].(*vectorized.TypedColumn[float64])
		if i%7 == 6 {
			v.AppendNull()
		} else {
			v.Append(float64(i) * 1.5)
		}
	}
	b.Columns[3] = vectorized.NewConstString("the-entity", rows)
	b.Len = rows
	if owner != nil {
		b.Owners.Pin(owner)
	}
	return b
}

func drainWithVersion(t *testing.T, version uint8, batches ...*vectorized.RecordBatch) []byte {
	t.Helper()
	schema := drainSchema()
	pipeline, buildErr := vectorized.NewPipelineBuilder().From(&batchesSource{schema: schema, batches: batches}).Build()
	if buildErr != nil {
		t.Fatalf("Build: %v", buildErr)
	}
	ctx := ContextWithFrameVersion(context.Background(), version)
	if initErr := pipeline.Init(ctx); initErr != nil {
		t.Fatalf("Init: %v", initErr)
	}
	body, drainErr := DrainPipelineToFrame(ctx, pipeline, schema)
	if drainErr != nil {
		t.Fatalf("DrainPipelineToFrame v%d: %v", version, drainErr)
	}
	if closeErr := pipeline.Close(); closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
	return body
}

func decodedProtos(t *testing.T, body []byte) []*measurev1.InternalDataPoint {
	t.Helper()
	b, err := frame.Decode(body)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	defer b.ReleaseOwners()
	return serializeBatchToProto(b, nil)
}

func requireSameFrameRows(t *testing.T, want, got []*measurev1.InternalDataPoint) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("rows: want %d, got %d", len(want), len(got))
	}
	for i := range want {
		if !proto.Equal(want[i], got[i]) {
			t.Fatalf("row %d differs:\n v1 %v\n v2 %v", i, want[i], got[i])
		}
	}
}

// TestDrainPipelineToFrame_VersionsAgree drains the same batches into a version 3 and a version 4
// frame: the decoded rows must be identical, for one batch (encoded directly, constant column
// kept) and for several (coalesced), with and without a selection, and every owner a drained
// batch pins must be released.
func TestDrainPipelineToFrame_VersionsAgree(t *testing.T) {
	schema := drainSchema()
	cases := map[string]func(owner vectorized.Owner) []*vectorized.RecordBatch{
		"one batch": func(o vectorized.Owner) []*vectorized.RecordBatch {
			return []*vectorized.RecordBatch{drainBatch(schema, 100, 40, o)}
		},
		"one batch with selection": func(o vectorized.Owner) []*vectorized.RecordBatch {
			b := drainBatch(schema, 100, 40, o)
			for i := 0; i < 40; i += 3 {
				b.Selection = append(b.Selection, uint16(i))
			}
			return []*vectorized.RecordBatch{b}
		},
		"one batch with empty selection": func(o vectorized.Owner) []*vectorized.RecordBatch {
			b := drainBatch(schema, 100, 40, o)
			b.Selection = []uint16{}
			return []*vectorized.RecordBatch{b}
		},
		"three batches": func(o vectorized.Owner) []*vectorized.RecordBatch {
			return []*vectorized.RecordBatch{drainBatch(schema, 0, 10, o), drainBatch(schema, 50, 20, o), drainBatch(schema, 90, 5, nil)}
		},
		"no batches": func(vectorized.Owner) []*vectorized.RecordBatch { return nil },
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			bodies := map[uint8][]byte{}
			for _, version := range []uint8{frame.WireVersion, frame.WireVersionColumnar} {
				owner := &pinOwner{}
				bodies[version] = drainWithVersion(t, version, build(owner)...)
				if owner.refs != 0 {
					t.Fatalf("v%d: %d references on the drained batches' owner were not released", version, owner.refs)
				}
				if got := bodies[version][frame.MagicLen]; got != version {
					t.Fatalf("frame carries version %d, want %d", got, version)
				}
			}
			requireSameFrameRows(t, decodedProtos(t, bodies[frame.WireVersion]), decodedProtos(t, bodies[frame.WireVersionColumnar]))
		})
	}
}

// TestDrainPipelineToFrame_DefaultIsOriginalVersion pins that a drain without a negotiated version
// emits exactly what the original coalescing encoder produced.
func TestDrainPipelineToFrame_DefaultIsOriginalVersion(t *testing.T) {
	schema := drainSchema()
	legacy := vectorized.NewRecordBatch(schema, 0)
	for _, b := range []*vectorized.RecordBatch{drainBatch(schema, 0, 10, nil), drainBatch(schema, 50, 20, nil)} {
		if err := appendActive(legacy, b); err != nil {
			t.Fatal(err)
		}
	}
	want, encErr := frame.Encode(legacy)
	if encErr != nil {
		t.Fatal(encErr)
	}
	source := &batchesSource{schema: schema, batches: []*vectorized.RecordBatch{drainBatch(schema, 0, 10, nil), drainBatch(schema, 50, 20, nil)}}
	pipeline, buildErr := vectorized.NewPipelineBuilder().From(source).Build()
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	if initErr := pipeline.Init(context.Background()); initErr != nil {
		t.Fatal(initErr)
	}
	got, drainErr := DrainPipelineToFrame(context.Background(), pipeline, schema)
	if drainErr != nil {
		t.Fatal(drainErr)
	}
	_ = pipeline.Close()
	if !bytes.Equal(got, want) {
		t.Fatalf("default drain differs from the original encoding (%d vs %d bytes)", len(got), len(want))
	}
}

// TestReduceAndDecodeFrames_VersionsAgree runs the liaison receive paths over the same partials sent
// as version 3 and as version 4 frames, mixed within one call as a half-upgraded cluster would.
func TestReduceAndDecodeFrames_VersionsAgree(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
	rowsFor := func(node int) []literalRow {
		out := make([]literalRow, 0, 30)
		for i := range 30 {
			out = append(out, literalRow{shard: int64(node), g: fmt.Sprintf("group-%d", (i*(node+1))%11), v: int64(i + node)})
		}
		return out
	}
	frames := func(versions ...uint8) [][]byte {
		var out [][]byte
		for node, version := range versions {
			source := newLiteralBatchSource(t, schema, rowsFor(node+1))
			agg := NewBatchAggregation(schema, []int{1}, []AggSpec{{Func: AggSum, InputCol: 2, Output: "sum_v"}},
				AggModeMap, 1024, vectorized.NewMemoryTracker(1<<30), 0)
			pipeline, buildErr := vectorized.NewPipelineBuilder().From(source).Break(agg).Build()
			if buildErr != nil {
				t.Fatal(buildErr)
			}
			ctx := ContextWithFrameVersion(context.Background(), version)
			if initErr := pipeline.Init(ctx); initErr != nil {
				t.Fatal(initErr)
			}
			body, drainErr := DrainPipelineToFrame(ctx, pipeline, agg.OutputSchema())
			if drainErr != nil {
				t.Fatal(drainErr)
			}
			_ = pipeline.Close()
			agg.Close()
			out = append(out, body)
		}
		return out
	}
	v3, v4 := frame.WireVersion, frame.WireVersionColumnar
	reduce := func(frames [][]byte) []*measurev1.InternalDataPoint {
		specs := []AggReduceSpec{{OutputName: "sum_v", Func: AggSum}}
		got, err := ReduceFramesToInternalDataPoints(frames, "default", []string{"g"}, specs, nil, 1024, vectorized.NewMemoryTracker(1<<30))
		if err != nil {
			t.Fatalf("ReduceFramesToInternalDataPoints: %v", err)
		}
		return got
	}
	want := reduce(frames(v3, v3, v3))
	if len(want) == 0 {
		t.Fatal("empty reduce result")
	}
	for name, versions := range map[string][]uint8{"all v4": {v4, v4, v4}, "mixed": {v4, v3, v4}, "mixed reversed": {v3, v4, v3}} {
		t.Run(name, func(t *testing.T) {
			requireSameFrameRows(t, sortedByGroupKey(want), sortedByGroupKey(reduce(frames(versions...))))
		})
	}
	perSourceWant, perSourceErr := DecodeFramesPerSource(frames(v3, v3, v3))
	if perSourceErr != nil {
		t.Fatal(perSourceErr)
	}
	perSourceGot, perSourceErr2 := DecodeFramesPerSource(frames(v4, v3, v4))
	if perSourceErr2 != nil {
		t.Fatal(perSourceErr2)
	}
	if len(perSourceWant) != len(perSourceGot) {
		t.Fatalf("sources: want %d, got %d", len(perSourceWant), len(perSourceGot))
	}
	for i := range perSourceWant {
		requireSameFrameRows(t, perSourceWant[i], perSourceGot[i])
	}
	flatWant, _ := DecodeFramesToInternalDataPoints(frames(v3, v3))
	flatGot, _ := DecodeFramesToInternalDataPoints(frames(v4, v4))
	requireSameFrameRows(t, flatWant, flatGot)
}

func sortedByGroupKey(in []*measurev1.InternalDataPoint) []*measurev1.InternalDataPoint {
	out := append([]*measurev1.InternalDataPoint(nil), in...)
	key := func(i int) string {
		return out[i].GetDataPoint().GetTagFamilies()[0].GetTags()[0].GetValue().GetStr().GetValue()
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && key(j) < key(j-1); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
