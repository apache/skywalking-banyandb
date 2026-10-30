// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package vectorized

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func TestEstimatedRetainedBytesIncludesByteCapacity(t *testing.T) {
	column := NewBytesColumn(1)
	backing := make([]byte, 1, 1<<20)
	column.Append(backing)
	batch := &RecordBatch{Columns: []Column{column}, Len: 1}
	if estimated := EstimatedRetainedBytes(batch); estimated < 1<<20 {
		t.Fatalf("estimated retained bytes %d did not include backing capacity", estimated)
	}
}

func TestEstimatedTagValueBytesLargeBinaryWithinOnePercent(t *testing.T) {
	const payload = 1_500_000
	tag := &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: make([]byte, payload)}}
	got := estimatedTagValueBytes(tag)
	require.Equal(t, uint64(payload)+valueOverhead, got)
	require.InDelta(t, float64(payload), float64(got), float64(payload)/100)
}

func TestEstimatedValueBytesPerKind(t *testing.T) {
	tagCases := map[string]struct {
		value *modelv1.TagValue
		want  uint64
	}{
		"str":       {&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "hello"}}}, 5 + valueOverhead},
		"binary":    {&modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte("abc")}}, 3 + valueOverhead},
		"int":       {&modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 1}}}, valueOverhead},
		"null":      {&modelv1.TagValue{Value: &modelv1.TagValue_Null{}}, valueOverhead},
		"timestamp": {&modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.Now()}}, valueOverhead},
		"empty":     {&modelv1.TagValue{}, valueOverhead},
		"str array": {&modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"ab", "cde"}}}}, 5 + 2*stringHeaderBytes + valueOverhead},
		"int array": {&modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{1, 2, 3}}}}, 3*int64Bytes + valueOverhead},
	}
	for name, tc := range tagCases {
		require.Equal(t, tc.want, estimatedTagValueBytes(tc.value), "tag "+name)
	}
	fieldCases := map[string]struct {
		value *modelv1.FieldValue
		want  uint64
	}{
		"str":    {&modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: "hello"}}}, 5 + valueOverhead},
		"binary": {&modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: []byte("abcd")}}, 4 + valueOverhead},
		"int":    {&modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 1}}}, valueOverhead},
		"float":  {&modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 1}}}, valueOverhead},
		"null":   {&modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}, valueOverhead},
	}
	for name, tc := range fieldCases {
		require.Equal(t, tc.want, estimatedFieldValueBytes(tc.value), "field "+name)
	}
}

func TestEstimatedRetainedBytesChargesLargeValuesOnce(t *testing.T) {
	const payload = 1_500_000
	tags := NewTagValueColumn(1)
	tags.Append(&modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte(strings.Repeat("x", payload))}})
	fields := NewFieldValueColumn(1)
	fields.Append(&modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: strings.Repeat("y", payload)}}})
	batch := &RecordBatch{Columns: []Column{tags, fields}, Len: 1}
	got := EstimatedRetainedBytes(batch)
	require.GreaterOrEqual(t, got, uint64(2*payload))
	require.Less(t, got, uint64(2*payload+4096), "batch charge must be payload once plus small fixed overhead, not a multiple")
}
