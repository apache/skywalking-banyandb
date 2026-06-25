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

package sdk_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

func strArr(values ...string) []byte {
	var dest []byte
	for _, v := range values {
		dest = encoding.MarshalVarArray(dest, []byte(v))
	}
	return dest
}

func int64Arr(values ...int64) []byte {
	var dest []byte
	for _, v := range values {
		dest = append(dest, convert.Int64ToBytes(v)...)
	}
	return dest
}

func TestDecodeTagValue(t *testing.T) {
	tests := []struct {
		check func(*testing.T, sdk.Value)
		name  string
		raw   []byte
		vt    pbv1.ValueType
	}{
		{
			name: "str", vt: pbv1.ValueTypeStr, raw: []byte("PostgreSQL"),
			check: func(t *testing.T, v sdk.Value) { assert.Equal(t, "PostgreSQL", v.Str()) },
		},
		{
			name: "int64", vt: pbv1.ValueTypeInt64, raw: convert.Int64ToBytes(-42),
			check: func(t *testing.T, v sdk.Value) { assert.Equal(t, int64(-42), v.Int64()) },
		},
		{
			name: "float64", vt: pbv1.ValueTypeFloat64, raw: convert.Float64ToBytes(3.5),
			check: func(t *testing.T, v sdk.Value) { assert.InDelta(t, 3.5, v.Float64(), 1e-9) },
		},
		{
			name: "binary", vt: pbv1.ValueTypeBinaryData, raw: []byte{0x01, 0x02, 0x03},
			check: func(t *testing.T, v sdk.Value) { assert.Equal(t, []byte{0x01, 0x02, 0x03}, v.Bytes()) },
		},
		{
			name: "timestamp", vt: pbv1.ValueTypeTimestamp, raw: convert.Int64ToBytes(1_700_000_000_000_000_000),
			check: func(t *testing.T, v sdk.Value) { assert.Equal(t, int64(1_700_000_000_000_000_000), v.Int64()) },
		},
		{
			name: "int64_arr", vt: pbv1.ValueTypeInt64Arr, raw: int64Arr(1, 2, 3),
			check: func(t *testing.T, v sdk.Value) { assert.Equal(t, []int64{1, 2, 3}, v.Int64Arr()) },
		},
		{
			name: "str_arr", vt: pbv1.ValueTypeStrArr, raw: strArr("a", "bb", "ccc"),
			check: func(t *testing.T, v sdk.Value) { assert.Equal(t, []string{"a", "bb", "ccc"}, v.StrArr()) },
		},
		{
			name: "nil_is_null", vt: pbv1.ValueTypeStr, raw: nil,
			check: func(t *testing.T, v sdk.Value) { assert.True(t, v.IsNull()) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := sdk.DecodeTagValue(tt.vt, tt.raw)
			require.NoError(t, err)
			assert.Equal(t, tt.vt, v.ValueType())
			tt.check(t, v)
		})
	}
}

func TestDecodeTagValueErrors(t *testing.T) {
	_, err := sdk.DecodeTagValue(pbv1.ValueTypeInt64Arr, []byte{0x01, 0x02})
	require.Error(t, err, "int64 array length not a multiple of 8 must error")

	_, err = sdk.DecodeTagValue(pbv1.ValueType(255), []byte("x"))
	require.Error(t, err, "unknown value type must error")

	for _, vt := range []pbv1.ValueType{pbv1.ValueTypeInt64, pbv1.ValueTypeFloat64, pbv1.ValueTypeTimestamp} {
		_, err = sdk.DecodeTagValue(vt, []byte{0x01, 0x02, 0x03})
		require.Errorf(t, err, "fixed-width type %d with fewer than 8 bytes must error", vt)
	}
}

func TestTagColumnAt(t *testing.T) {
	col := sdk.TagColumn{
		Name:      "db.type",
		ValueType: pbv1.ValueTypeStr,
		Values:    [][]byte{[]byte("PostgreSQL"), nil, []byte("MySQL")},
	}
	v0, err := col.At(0)
	require.NoError(t, err)
	assert.Equal(t, "PostgreSQL", v0.Str())

	v1, err := col.At(1)
	require.NoError(t, err)
	assert.True(t, v1.IsNull())

	_, err = col.At(3)
	require.Error(t, err, "row out of range must error")
}

func TestTraceBlockHelpers(t *testing.T) {
	b := sdk.TraceBlock{
		TraceID: "t-1",
		MinTS:   100,
		MaxTS:   2_802_000_000, // 2802ms in nanos
		Tags: []sdk.TagColumn{
			{Name: "db.type", ValueType: pbv1.ValueTypeStr, Values: [][]byte{[]byte("PostgreSQL"), nil}},
		},
		SpanIDs: []string{"s-1", "s-2"},
	}
	assert.Equal(t, 2, b.Len(), "Len comes from the span-id column")
	require.NotNil(t, b.Tag("db.type"))
	assert.Nil(t, b.Tag("missing"))

	// Len falls back to a projected tag column when only tags are present.
	tagsOnly := sdk.TraceBlock{Tags: []sdk.TagColumn{{Name: "x", Values: [][]byte{nil, nil, nil}}}}
	assert.Equal(t, 3, tagsOnly.Len())

	// Metadata-only block reports 0 rows.
	metaOnly := sdk.TraceBlock{TraceID: "t-2", MinTS: 1, MaxTS: 2}
	assert.Equal(t, 0, metaOnly.Len())
}

type fakeSampler struct{}

func (fakeSampler) Kind() sdk.Kind                              { return sdk.KindSampler }
func (fakeSampler) Project() sdk.Projection                     { return sdk.Projection{} }
func (fakeSampler) Decide(*sdk.TraceBatch) (sdk.Verdict, error) { return sdk.Verdict{}, nil }
func (fakeSampler) Close() error                                { return nil }

func TestSamplerIsPlugin(t *testing.T) {
	var s sdk.Sampler = fakeSampler{}
	var p sdk.Plugin = s // a Sampler is a Plugin
	assert.Equal(t, sdk.KindSampler, p.Kind())
	assert.NotEqual(t, sdk.KindUnspecified, p.Kind())
}
