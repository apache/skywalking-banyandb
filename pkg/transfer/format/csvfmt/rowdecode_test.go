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

package csvfmt

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func nullRow(l *Layout, resource string) []Value {
	row := make([]Value, len(l.Columns))
	for i := range row {
		row[i] = Value{Null: true}
	}
	row[0] = Value{Str: resource}
	return row
}

func TestCheckRow(t *testing.T) {
	l := roundTripLayout(t)
	col := func(header string) int {
		for i, c := range l.Columns {
			if c.Header == header {
				return i
			}
		}
		require.Failf(t, "no column", "%s", header)
		return -1
	}

	row := nullRow(l, "m1")
	row[col("_timestamp")] = Value{Int: 1}
	row[col("f.s")] = Value{Str: "shared"}
	row[col("_field.fi")] = Value{Int: 3}
	require.NoError(t, l.CheckRow(row))

	row = nullRow(l, "m2")
	row[col("f.s")] = Value{Str: "shared"}
	row[col("g.x")] = Value{Int: 3}
	require.NoError(t, l.CheckRow(row))

	row = nullRow(l, "m2")
	row[col("_field.fi")] = Value{Int: 3}
	err := l.CheckRow(row)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `column "_field.fi"`)

	row = nullRow(l, "m1")
	row[col("g.x")] = Value{Int: 3}
	require.ErrorContains(t, l.CheckRow(row), `column "g.x"`)

	require.ErrorContains(t, l.CheckRow(nullRow(l, "m3")), `resource "m3"`)

	row = nullRow(l, "m1")
	row[0] = Value{Null: true}
	require.ErrorContains(t, l.CheckRow(row), "null")

	require.ErrorContains(t, l.CheckRow(nullRow(l, "m1")[:3]), "cells")
}

func TestCheckRowSplitColumns(t *testing.T) {
	// A type conflict splits f.v into one column per resource; each resource owns only its own.
	l, err := StreamLayout([]*databasev1.Stream{
		{Metadata: md("a"), TagFamilies: []*databasev1.TagFamilySpec{family("f", tag("v", databasev1.TagType_TAG_TYPE_STRING))}},
		{Metadata: md("b"), TagFamilies: []*databasev1.TagFamilySpec{family("f", tag("v", databasev1.TagType_TAG_TYPE_INT))}},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"_name", "_element_id", "_timestamp", "a::f.v", "b::f.v"}, l.Header())
	row := nullRow(l, "a")
	row[3] = Value{Str: "x"}
	require.NoError(t, l.CheckRow(row))
	row = nullRow(l, "a")
	row[4] = Value{Int: 1}
	require.ErrorContains(t, l.CheckRow(row), `column "b::f.v"`)
}

func TestValueTagValue(t *testing.T) {
	ts := time.Unix(1700000000, 123456789).UnixNano()
	cases := []struct {
		want *modelv1.TagValue
		v    Value
		t    ValueType
	}{
		{v: Value{Null: true}, t: ValueString, want: &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}},
		{v: Value{Str: "a"}, t: ValueString, want: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "a"}}}},
		{v: Value{Int: math.MinInt64}, t: ValueInt, want: &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: math.MinInt64}}}},
		{v: Value{Bytes: []byte{}}, t: ValueBinary, want: &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte{}}}},
		{v: Value{Strs: []string{"x"}}, t: ValueStringArray, want: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"x"}}}}},
		{v: Value{Ints: []int64{}}, t: ValueIntArray, want: &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{}}}}},
		{v: Value{Int: ts}, t: ValueTimestamp, want: &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(time.Unix(0, ts))}}},
		{v: Value{Float: 1}, t: ValueFloat, want: &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}},
	}
	for _, tc := range cases {
		got := tc.v.TagValue(tc.t)
		assert.Empty(t, cmpDiff(tc.want, got), "%v", tc.t)
		if !tc.v.Null && tc.t != ValueFloat {
			// The conversion is the inverse of EncodeTagValue.
			var back Value
			require.NoError(t, decodeCell(EncodeTagValue(got), tc.t, &back))
			assert.Equal(t, keyOf(tc.v), keyOf(back))
		}
	}
	// An empty binary stays distinct from null.
	assert.NotEqual(t, NullCell, EncodeTagValue(Value{Bytes: []byte{}}.TagValue(ValueBinary)))
}

func TestValueFieldValue(t *testing.T) {
	cases := []struct {
		want *modelv1.FieldValue
		v    Value
		t    ValueType
	}{
		{v: Value{Null: true}, t: ValueFloat, want: &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}},
		{v: Value{Str: "a"}, t: ValueString, want: &modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: "a"}}}},
		{v: Value{Int: 7}, t: ValueInt, want: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 7}}}},
		{v: Value{Float: math.Inf(-1)}, t: ValueFloat, want: &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: math.Inf(-1)}}}},
		{v: Value{Bytes: []byte{1}}, t: ValueBinary, want: &modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: []byte{1}}}},
		{v: Value{Strs: []string{"x"}}, t: ValueStringArray, want: &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}},
	}
	for _, tc := range cases {
		got := tc.v.FieldValue(tc.t)
		assert.Empty(t, cmpDiff(tc.want, got), "%v", tc.t)
		if !tc.v.Null && tc.t != ValueStringArray {
			var back Value
			require.NoError(t, decodeCell(EncodeFieldValue(got), tc.t, &back))
			assert.Equal(t, keyOf(tc.v), keyOf(back))
		}
	}
}

func cmpDiff(want, got proto.Message) string {
	if proto.Equal(want, got) {
		return ""
	}
	return fmt.Sprintf("%v != %v", want, got)
}

func TestRowSize(t *testing.T) {
	tags := []*modelv1.TagValue{
		Value{Str: "abc"}.TagValue(ValueString),
		Value{Bytes: make([]byte, 300)}.TagValue(ValueBinary),
		Value{Null: true}.TagValue(ValueInt),
		Value{Ints: []int64{1, 2, 3}}.TagValue(ValueIntArray),
	}
	fam := &modelv1.TagFamilyForWrite{}
	sum := 0
	for _, tv := range tags {
		fam.Tags = append(fam.Tags, tv)
		sum += protowire.SizeTag(1) + RowSize(tv)
		// Each element adds its tag, its length prefix and its encoded size to the container.
		assert.Equal(t, sum, proto.Size(fam))
	}
	size := proto.Size(tags[1])
	assert.Equal(t, protowire.SizeVarint(uint64(size))+size, RowSize(tags[1]))
}

func TestValueEncodeRoundTrip(t *testing.T) {
	cases := []struct {
		v Value
		t ValueType
	}{
		{t: ValueString, v: Value{Str: "plain"}},
		{t: ValueString, v: Value{Str: `\N`}},
		{t: ValueString, v: Value{Str: "a\r\nb"}},
		{t: ValueString, v: Value{}},
		{t: ValueInt, v: Value{Int: math.MinInt64}},
		{t: ValueTimestamp, v: Value{Int: 1700000000123456789}},
		{t: ValueFloat, v: Value{Float: -1.25}},
		{t: ValueFloat, v: Value{Float: math.Inf(1)}},
		{t: ValueBinary, v: Value{Bytes: []byte{0, 1, 0xff}}},
		{t: ValueBinary, v: Value{Bytes: []byte{}}},
		{t: ValueStringArray, v: Value{Strs: []string{"x", `"y",z`}}},
		{t: ValueStringArray, v: Value{Strs: []string{}}},
		{t: ValueIntArray, v: Value{Ints: []int64{1, -2, math.MaxInt64}}},
		{t: ValueIntArray, v: Value{Ints: []int64{}}},
		{t: ValueString, v: Value{Null: true}},
		{t: ValueIntArray, v: Value{Null: true}},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/%v", tc.t, tc.v), func(t *testing.T) {
			cell := tc.v.Encode(tc.t)
			var back Value
			require.NoError(t, decodeCell(cell, tc.t, &back))
			assert.Equal(t, tc.v, back)
			assert.Equal(t, cell, back.Encode(tc.t))
		})
	}
	assert.Equal(t, NullCell, Value{Str: "x"}.Encode(ValueType(99)))
}
