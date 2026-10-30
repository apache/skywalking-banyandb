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

func TestEstimatedValueBytesByKind(t *testing.T) {
	const object = valueObjectBytes
	tag := estimatedTagValueBytes
	require.Equal(t, 5+object, tag(&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "hello"}}}))
	require.Equal(t, 7+object, tag(&modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: make([]byte, 7)}}))
	require.Equal(t, object, tag(&modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 1 << 40}}}))
	require.Equal(t, object, tag(&modelv1.TagValue{Value: &modelv1.TagValue_Null{}}))
	require.Equal(t, object, tag(&modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.Now()}}))
	require.Equal(t, object, tag(&modelv1.TagValue{}), "an unset value still costs its object")
	require.Equal(t, (3+stringHeaderBytes)+(4+stringHeaderBytes)+object,
		tag(&modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"abc", "defg"}}}}))
	require.Equal(t, 3*intElementBytes+object,
		tag(&modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{1, 2, 3}}}}))

	field := estimatedFieldValueBytes
	require.Equal(t, 2+object, field(&modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: "ab"}}}))
	require.Equal(t, 9+object, field(&modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: make([]byte, 9)}}))
	require.Equal(t, object, field(&modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 1}}}))
	require.Equal(t, object, field(&modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 1.5}}}))
	require.Equal(t, object, field(&modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}))
}

func TestEstimatedRetainedBytesChargesLargeValueOnce(t *testing.T) {
	const payload = 1500 * 1000
	body := &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: make([]byte, payload)}}
	require.LessOrEqual(t, estimatedTagValueBytes(body), uint64(payload)+valueObjectBytes)
	require.GreaterOrEqual(t, estimatedTagValueBytes(body), uint64(payload))

	tags := NewTagValueColumn(1)
	tags.Append(body)
	fields := NewFieldValueColumn(1)
	fields.Append(&modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: make([]byte, payload)}})

	empty := EstimatedRetainedBytes(&RecordBatch{Columns: []Column{NewTagValueColumn(1)}, Len: 1})
	withTag := EstimatedRetainedBytes(&RecordBatch{Columns: []Column{tags}, Len: 1})
	withField := EstimatedRetainedBytes(&RecordBatch{Columns: []Column{fields}, Len: 1})
	for name, got := range map[string]uint64{"tag": withTag, "field": withField} {
		delta := float64(got-empty) - payload
		require.Greater(t, delta, float64(0), name)
		require.Less(t, delta/payload, 0.01, "%s: a %d byte value must be charged within 1%% of its size, got %d", name, payload, got-empty)
	}
	require.Less(t, withTag, uint64(2*payload), "a 1.5 MB value must not be charged several times over")
}
