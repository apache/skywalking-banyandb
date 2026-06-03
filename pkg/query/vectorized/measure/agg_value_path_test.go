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
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// TestReduceRawFramesTracing_Path_Typed asserts that when the partial schema
// carries a native int64 value column, bindAggReduceSpecs and ReducePartialBatches
// report AggValuePathTyped — the value that the reduce-raw-frames span must tag
// as agg_value_path="typed".
func TestReduceRawFramesTracing_Path_Typed(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "f", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "sum_v", Type: vectorized.ColumnTypeInt64},
	})
	b := vectorized.NewRecordBatch(schema, 1)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[2].(*vectorized.TypedColumn[int64]).Append(42)
	b.Len = 1

	_, path, reduceErr := ReducePartialBatches(
		[]*vectorized.RecordBatch{b},
		[]string{"g"},
		[]AggReduceSpec{{OutputName: "sum_v", Func: AggSum}},
		64,
		vectorized.NewMemoryTracker(1<<30),
	)
	if reduceErr != nil {
		t.Fatalf("ReducePartialBatches: %v", reduceErr)
	}
	if path != AggValuePathTyped {
		t.Fatalf("agg_value_path = %q, want %q", path, AggValuePathTyped)
	}
}

// TestReduceRawFramesTracing_Path_FieldValueFallback asserts that when the
// partial schema carries only a ColumnTypeFieldValue passthrough column (no
// native int64/float64 column), bindAggReduceSpecs falls back to it and
// ReducePartialBatches reports AggValuePathFieldValueFallback — the value that
// the reduce-raw-frames span must tag as agg_value_path="fieldvalue-fallback".
func TestReduceRawFramesTracing_Path_FieldValueFallback(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "f", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "sum_v", Type: vectorized.ColumnTypeFieldValue},
	})
	b := vectorized.NewRecordBatch(schema, 1)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[2].(*vectorized.TypedColumn[*modelv1.FieldValue]).Append(
		&modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 7}}},
	)
	b.Len = 1

	_, path, reduceErr := ReducePartialBatches(
		[]*vectorized.RecordBatch{b},
		[]string{"g"},
		[]AggReduceSpec{{OutputName: "sum_v", Func: AggSum}},
		64,
		vectorized.NewMemoryTracker(1<<30),
	)
	if reduceErr != nil {
		t.Fatalf("ReducePartialBatches: %v", reduceErr)
	}
	if path != AggValuePathFieldValueFallback {
		t.Fatalf("agg_value_path = %q, want %q", path, AggValuePathFieldValueFallback)
	}
}

// TestReduceRawFramesTracing_Path_Unresolved asserts that when the partial
// schema has no column matching the requested agg output name, ReducePartialBatches
// returns an error and reports AggValuePathUnresolved — corresponding to the
// reduce-raw-frames span being marked Error=true with agg_value_path="unresolved".
func TestReduceRawFramesTracing_Path_Unresolved(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: shardIDOutputName, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "f", Name: "g", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "other_col", Type: vectorized.ColumnTypeInt64},
	})
	b := vectorized.NewRecordBatch(schema, 1)
	b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1)
	b.Columns[1].(*vectorized.TypedColumn[string]).Append("a")
	b.Columns[2].(*vectorized.TypedColumn[int64]).Append(99)
	b.Len = 1

	_, path, reduceErr := ReducePartialBatches(
		[]*vectorized.RecordBatch{b},
		[]string{"g"},
		[]AggReduceSpec{{OutputName: "sum_v", Func: AggSum}},
		64,
		vectorized.NewMemoryTracker(1<<30),
	)
	if reduceErr == nil {
		t.Fatal("ReducePartialBatches: expected error for missing agg column, got nil")
	}
	if path != AggValuePathUnresolved {
		t.Fatalf("agg_value_path = %q, want %q", path, AggValuePathUnresolved)
	}
}
