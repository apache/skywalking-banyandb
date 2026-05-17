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
	"context"
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// TestDrainPipelineToFrame_AggModeMap proves G9f.5.b's data-node emit
// path end-to-end: build a real BatchAggregation(AggModeMap) pipeline
// over a synthetic source, drain it via DrainPipelineToFrame, and decode
// the resulting bytes back via frame.Decode. The recovered batch must
// match the AggModeMap output schema (shard_id + g + sum_v) and the row
// values must equal what an in-process drain would have produced.
func TestDrainPipelineToFrame_AggModeMap(t *testing.T) {
	inputSchema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
	source := newLiteralBatchSource(t, inputSchema, []literalRow{
		{shard: 1, g: "a", v: 10},
		{shard: 1, g: "b", v: 20},
		{shard: 1, g: "a", v: 30},
	})
	agg := NewBatchAggregation(inputSchema, []int{1},
		[]AggSpec{{Func: AggSum, InputCol: 2, Output: "sum_v"}},
		AggModeMap, 1024, vectorized.NewMemoryTracker(1<<30), 0)
	defer agg.Close()
	pipeline, buildErr := vectorized.NewPipelineBuilder().From(source).Break(agg).Build()
	if buildErr != nil {
		t.Fatalf("Build: %v", buildErr)
	}
	if initErr := pipeline.Init(context.Background()); initErr != nil {
		t.Fatalf("Init: %v", initErr)
	}
	terminalSchema := agg.OutputSchema()

	body, drainErr := DrainPipelineToFrame(context.Background(), pipeline, terminalSchema)
	if drainErr != nil {
		t.Fatalf("DrainPipelineToFrame: %v", drainErr)
	}
	if closeErr := pipeline.Close(); closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
	if len(body) == 0 || body[0] != 0x00 {
		t.Fatalf("body magic = %#x, want 0x00 leading byte", body[:1])
	}

	decoded, decodeErr := frame.Decode(body)
	if decodeErr != nil {
		t.Fatalf("frame.Decode: %v", decodeErr)
	}
	if decoded.Len != 2 {
		t.Fatalf("decoded.Len = %d, want 2 (groups a,b)", decoded.Len)
	}
	if len(decoded.Schema.Columns) != 3 {
		t.Fatalf("decoded columns = %d, want 3 (shard_id + g + sum_v)", len(decoded.Schema.Columns))
	}
	if decoded.Schema.Columns[0].Role != vectorized.RoleShardID {
		t.Fatalf("col 0 role = %v, want RoleShardID", decoded.Schema.Columns[0].Role)
	}
	if decoded.Schema.Columns[1].Name != "g" {
		t.Fatalf("col 1 name = %q, want g", decoded.Schema.Columns[1].Name)
	}
	if decoded.Schema.Columns[2].Name != "sum_v" {
		t.Fatalf("col 2 name = %q, want sum_v", decoded.Schema.Columns[2].Name)
	}
	sums := decoded.Columns[2].(*vectorized.TypedColumn[int64]).Data()
	groups := decoded.Columns[1].(*vectorized.TypedColumn[string]).Data()
	for i, g := range groups {
		switch g {
		case "a":
			if sums[i] != 40 {
				t.Fatalf("group a sum = %d, want 40 (10+30)", sums[i])
			}
		case "b":
			if sums[i] != 20 {
				t.Fatalf("group b sum = %d, want 20", sums[i])
			}
		default:
			t.Fatalf("unexpected group %q", g)
		}
	}
}

// TestReduceFramesToInternalDataPoints_AggRoundTrip is the receive-side
// counterpart of TestDrainPipelineToFrame_AggModeMap: it pipes a vec
// pipeline's frame body through the liaison's vmeasure receive path
// (frame.Decode → AggModeReduce + (shard,group) dedup → optional Top →
// serialise to InternalDataPoint) and asserts the final values match an
// AggModeAll oracle.
//
// Replica duplication (the frame is appended twice for shard=1) verifies
// the dedup contract holds end-to-end on the wire path.
func TestReduceFramesToInternalDataPoints_AggRoundTrip(t *testing.T) {
	inputSchema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
	// Shard 1 contributes (a=10) and (b=20); shard 2 contributes (a=30).
	shard1Body := buildAggMapFrame(t, inputSchema, 1, []literalRow{
		{shard: 1, g: "a", v: 10},
		{shard: 1, g: "b", v: 20},
	})
	shard2Body := buildAggMapFrame(t, inputSchema, 2, []literalRow{
		{shard: 2, g: "a", v: 30},
	})
	// Duplicate shard1 to exercise replica dedup on the wire path.
	frames := [][]byte{shard1Body, shard1Body, shard2Body, nil}

	idps, reduceErr := ReduceFramesToInternalDataPoints(frames,
		[]string{"g"},
		[]AggReduceSpec{{OutputName: "sum_v", Func: AggSum}},
		nil, 1024, vectorized.NewMemoryTracker(1<<30))
	if reduceErr != nil {
		t.Fatalf("ReduceFramesToInternalDataPoints: %v", reduceErr)
	}
	if len(idps) != 2 {
		t.Fatalf("idp count = %d, want 2 (groups a,b)", len(idps))
	}
	seen := make(map[string]int64)
	for _, idp := range idps {
		dp := idp.GetDataPoint()
		var groupVal string
		for _, fam := range dp.GetTagFamilies() {
			for _, tag := range fam.GetTags() {
				if tag.GetKey() == "g" {
					groupVal = tag.GetValue().GetStr().GetValue()
				}
			}
		}
		var sumVal int64
		for _, f := range dp.GetFields() {
			if f.GetName() == "sum_v" {
				sumVal = f.GetValue().GetInt().GetValue()
			}
		}
		seen[groupVal] = sumVal
	}
	if seen["a"] != 40 {
		t.Fatalf("group a sum = %d, want 40 (shard1=10 + shard2=30; shard1 replica must be dedup'd)", seen["a"])
	}
	if seen["b"] != 20 {
		t.Fatalf("group b sum = %d, want 20 (single shard1 contribution)", seen["b"])
	}
}

// TestDecodeFramesToInternalDataPoints_NonAgg covers G9f.5.c's non-agg
// path: each data node sends a raw frame carrying plain (sid + tag +
// value) rows; the liaison decodes + concats into InternalDataPoint
// protos for the sortableElements/sortedMIterator merge.
func TestDecodeFramesToInternalDataPoints_NonAgg(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
	b1 := vectorized.NewRecordBatch(schema, 2)
	b1.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	b1.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	b1.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	b1.Columns[1].(*vectorized.TypedColumn[string]).Append("b")
	b1.Columns[2].(*vectorized.TypedColumn[int64]).Append(10)
	b1.Columns[2].(*vectorized.TypedColumn[int64]).Append(20)
	b1.Len = 2
	body1, err := frame.Encode(b1)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	idps, decodeErr := DecodeFramesToInternalDataPoints([][]byte{body1, nil})
	if decodeErr != nil {
		t.Fatalf("DecodeFramesToInternalDataPoints: %v", decodeErr)
	}
	if len(idps) != 2 {
		t.Fatalf("idp count = %d, want 2", len(idps))
	}
	if idps[0].GetShardId() != 1 {
		t.Fatalf("row 0 shard_id = %d, want 1", idps[0].GetShardId())
	}
}

// TestDrainPipelineToFrame_PassthroughColumnsConverted exercises the
// throughput-and-efficiency path the hidden_tags / non-native scan flow
// depends on: the storage adapter emits ColumnTypeTagValue and
// ColumnTypeFieldValue passthrough columns (zero allocation in-process)
// and DrainPipelineToFrame must transparently decode them into typed
// columns the wire frame can carry. The round-trip then reconstructs the
// original TagValue / FieldValue protos at the receive-side, so the
// downstream InternalDataPoint is byte-equivalent.
func TestDrainPipelineToFrame_PassthroughColumnsConverted(t *testing.T) {
	inputSchema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeTagValue},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "n", Type: vectorized.ColumnTypeTagValue},
		{Role: vectorized.RoleField, Name: "value", Type: vectorized.ColumnTypeFieldValue},
	})
	b := vectorized.NewRecordBatch(inputSchema, 2)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(7)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(7)
	b.Columns[1].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(
		&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc-a"}}})
	b.Columns[1].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(
		&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc-b"}}})
	b.Columns[2].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(
		&modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 42}}})
	b.Columns[2].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(
		&modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 43}}})
	b.Columns[3].(*vectorized.TypedColumn[*modelv1.FieldValue]).Append(
		&modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 3.14}}})
	b.Columns[3].(*vectorized.TypedColumn[*modelv1.FieldValue]).Append(
		&modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 2.72}}})
	b.Len = 2

	converted, err := convertPassthroughForFrame(b)
	if err != nil {
		t.Fatalf("convertPassthroughForFrame: %v", err)
	}
	if converted.Schema.Columns[1].Type != vectorized.ColumnTypeString {
		t.Fatalf("svc col type = %v, want String (Str-variant TagValue)", converted.Schema.Columns[1].Type)
	}
	if converted.Schema.Columns[2].Type != vectorized.ColumnTypeInt64 {
		t.Fatalf("n col type = %v, want Int64 (Int-variant TagValue)", converted.Schema.Columns[2].Type)
	}
	if converted.Schema.Columns[3].Type != vectorized.ColumnTypeFloat64 {
		t.Fatalf("value col type = %v, want Float64 (Float-variant FieldValue)", converted.Schema.Columns[3].Type)
	}

	body, encodeErr := frame.Encode(converted)
	if encodeErr != nil {
		t.Fatalf("frame.Encode: %v", encodeErr)
	}
	decoded, decodeErr := frame.Decode(body)
	if decodeErr != nil {
		t.Fatalf("frame.Decode: %v", decodeErr)
	}
	if decoded.Len != 2 {
		t.Fatalf("decoded.Len = %d, want 2", decoded.Len)
	}
	svcs := decoded.Columns[1].(*vectorized.TypedColumn[string]).Data()
	if svcs[0] != "svc-a" || svcs[1] != "svc-b" {
		t.Fatalf("svc round trip = %v, want [svc-a svc-b]", svcs)
	}
	ns := decoded.Columns[2].(*vectorized.TypedColumn[int64]).Data()
	if ns[0] != 42 || ns[1] != 43 {
		t.Fatalf("n round trip = %v, want [42 43]", ns)
	}
	vs := decoded.Columns[3].(*vectorized.TypedColumn[float64]).Data()
	if vs[0] != 3.14 || vs[1] != 2.72 {
		t.Fatalf("value round trip = %v, want [3.14 2.72]", vs)
	}
}

// TestConvertPassthroughForFrame_NullCellsPreserved pins the validity
// bitmap as the source of truth: a TagValue column with a null in the
// middle must produce a typed column that reports IsNull(1) and emits
// a typed TagValue null when the receive side reconstructs the proto.
func TestConvertPassthroughForFrame_NullCellsPreserved(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "svc", Type: vectorized.ColumnTypeTagValue},
	})
	b := vectorized.NewRecordBatch(schema, 3)
	tc := b.Columns[0].(*vectorized.TypedColumn[*modelv1.TagValue])
	tc.Append(&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "first"}}})
	tc.AppendNull()
	tc.Append(&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "third"}}})
	b.Len = 3

	converted, err := convertPassthroughForFrame(b)
	if err != nil {
		t.Fatalf("convertPassthroughForFrame: %v", err)
	}
	outCol := converted.Columns[0].(*vectorized.TypedColumn[string])
	if outCol.IsNull(0) {
		t.Fatal("row 0 should be non-null")
	}
	if !outCol.IsNull(1) {
		t.Fatal("row 1 should be null (middle-of-batch null cell)")
	}
	if outCol.IsNull(2) {
		t.Fatal("row 2 should be non-null")
	}
}

// TestConvertPassthroughForFrame_NoOpForNativeColumns confirms the fast
// path: when no passthrough column appears, the function returns the
// input pointer unchanged (no allocation, no schema rebuild). This keeps
// agg/GroupBy queries — which already build native typed key + value
// columns — free of conversion overhead.
func TestConvertPassthroughForFrame_NoOpForNativeColumns(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "sum_v", Type: vectorized.ColumnTypeInt64},
	})
	b := vectorized.NewRecordBatch(schema, 1)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[2].(*vectorized.TypedColumn[int64]).Append(10)
	b.Len = 1

	converted, err := convertPassthroughForFrame(b)
	if err != nil {
		t.Fatalf("convertPassthroughForFrame: %v", err)
	}
	if converted != b {
		t.Fatal("native-columns-only batch should be returned unchanged (fast-path pointer equality)")
	}
}

// literalRow is the synthetic source row used by newLiteralBatchSource.
type literalRow struct {
	g     string
	shard int64
	v     int64
}

// newLiteralBatchSource builds a PullOperator that emits a single batch
// of the supplied rows under the given schema. It is the minimal source
// adapter needed to exercise BatchAggregation in tests without a real
// MeasureQueryResult — same role buildAdapterPipeline plays for the
// MIterator tests, but here the source emits a vec RecordBatch directly
// (no extract/serialize round trip).
func newLiteralBatchSource(t *testing.T, schema *vectorized.BatchSchema, rows []literalRow) vectorized.PullOperator {
	t.Helper()
	b := vectorized.NewRecordBatch(schema, len(rows))
	for _, r := range rows {
		b.Columns[0].(*vectorized.TypedColumn[int64]).Append(r.shard)
		b.Columns[1].(*vectorized.TypedColumn[string]).Append(r.g)
		b.Columns[2].(*vectorized.TypedColumn[int64]).Append(r.v)
	}
	b.Len = len(rows)
	return &literalBatchSource{schema: schema, batch: b}
}

type literalBatchSource struct {
	schema *vectorized.BatchSchema
	batch  *vectorized.RecordBatch
	emit   bool
	closed bool
}

func (l *literalBatchSource) Init(_ context.Context) error          { return nil }
func (l *literalBatchSource) OutputSchema() *vectorized.BatchSchema { return l.schema }
func (l *literalBatchSource) NextBatch(_ context.Context) (*vectorized.RecordBatch, error) {
	if l.emit {
		return nil, nil
	}
	l.emit = true
	return l.batch, nil
}

func (l *literalBatchSource) Close() error {
	l.closed = true
	return nil
}

// buildAggMapFrame builds a one-shard input batch, runs AggModeMap, and
// returns the frame-encoded partial body. Used to fabricate per-shard
// data-node responses for receive-side tests.
func buildAggMapFrame(t *testing.T, schema *vectorized.BatchSchema, _ int64, rows []literalRow) []byte {
	t.Helper()
	source := newLiteralBatchSource(t, schema, rows)
	agg := NewBatchAggregation(schema, []int{1},
		[]AggSpec{{Func: AggSum, InputCol: 2, Output: "sum_v"}},
		AggModeMap, 1024, vectorized.NewMemoryTracker(1<<30), 0)
	defer agg.Close()
	pipeline, buildErr := vectorized.NewPipelineBuilder().From(source).Break(agg).Build()
	if buildErr != nil {
		t.Fatalf("Build: %v", buildErr)
	}
	if initErr := pipeline.Init(context.Background()); initErr != nil {
		t.Fatalf("Init: %v", initErr)
	}
	body, drainErr := DrainPipelineToFrame(context.Background(), pipeline, agg.OutputSchema())
	if drainErr != nil {
		t.Fatalf("Drain: %v", drainErr)
	}
	_ = pipeline.Close()
	return body
}
