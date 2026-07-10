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

	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// TestEncodeTagValueRoundTrip proves EncodeTagValue is the true inverse of
// DecodeTagValue for every valuetype.ValueType: encode a raw Go value, decode
// the result, and assert the decoded Value equals the original input.
func TestEncodeTagValueRoundTrip(t *testing.T) {
	tests := []struct {
		goVal  any
		verify func(*testing.T, sdk.Value)
		name   string
		vt     pbv1.ValueType
	}{
		{
			name: "str", vt: pbv1.ValueTypeStr, goVal: "PostgreSQL",
			verify: func(t *testing.T, v sdk.Value) { assert.Equal(t, "PostgreSQL", v.Str()) },
		},
		{
			name: "int64", vt: pbv1.ValueTypeInt64, goVal: int64(-42),
			verify: func(t *testing.T, v sdk.Value) { assert.Equal(t, int64(-42), v.Int64()) },
		},
		{
			name: "timestamp", vt: pbv1.ValueTypeTimestamp, goVal: int64(1_700_000_000_000_000_000),
			verify: func(t *testing.T, v sdk.Value) {
				assert.Equal(t, int64(1_700_000_000_000_000_000), v.Int64())
			},
		},
		{
			name: "float64", vt: pbv1.ValueTypeFloat64, goVal: 3.5,
			verify: func(t *testing.T, v sdk.Value) { assert.InDelta(t, 3.5, v.Float64(), 1e-9) },
		},
		{
			name: "binary", vt: pbv1.ValueTypeBinaryData, goVal: []byte{0x01, 0x02, 0x03},
			verify: func(t *testing.T, v sdk.Value) { assert.Equal(t, []byte{0x01, 0x02, 0x03}, v.Bytes()) },
		},
		{
			name: "str_arr", vt: pbv1.ValueTypeStrArr, goVal: []string{"a", "bb", "ccc"},
			verify: func(t *testing.T, v sdk.Value) { assert.Equal(t, []string{"a", "bb", "ccc"}, v.StrArr()) },
		},
		{
			name: "str_arr_with_delimiters", vt: pbv1.ValueTypeStrArr, goVal: []string{"a|b", `c\d`, ""},
			verify: func(t *testing.T, v sdk.Value) { assert.Equal(t, []string{"a|b", `c\d`, ""}, v.StrArr()) },
		},
		{
			name: "int64_arr", vt: pbv1.ValueTypeInt64Arr, goVal: []int64{1, -2, 3},
			verify: func(t *testing.T, v sdk.Value) { assert.Equal(t, []int64{1, -2, 3}, v.Int64Arr()) },
		},
		{
			name: "nil_is_null", vt: pbv1.ValueTypeStr, goVal: nil,
			verify: func(t *testing.T, v sdk.Value) { assert.True(t, v.IsNull()) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, encErr := sdk.EncodeTagValue(tt.vt, tt.goVal)
			require.NoError(t, encErr)
			decoded, decErr := sdk.DecodeTagValue(tt.vt, raw)
			require.NoError(t, decErr)
			assert.Equal(t, tt.vt, decoded.ValueType())
			tt.verify(t, decoded)
		})
	}
}

// TestEncodeTagValueTypeMismatch asserts a Go value whose type does not match
// vt is rejected rather than silently mis-encoded.
func TestEncodeTagValueTypeMismatch(t *testing.T) {
	tests := []struct {
		goVal any
		name  string
		vt    pbv1.ValueType
	}{
		{name: "str_wants_string", vt: pbv1.ValueTypeStr, goVal: 42},
		{name: "int64_wants_int64", vt: pbv1.ValueTypeInt64, goVal: "42"},
		{name: "timestamp_wants_int64", vt: pbv1.ValueTypeTimestamp, goVal: 42},
		{name: "float64_wants_float64", vt: pbv1.ValueTypeFloat64, goVal: int64(1)},
		{name: "binary_wants_bytes", vt: pbv1.ValueTypeBinaryData, goVal: "not bytes"},
		{name: "str_arr_wants_slice", vt: pbv1.ValueTypeStrArr, goVal: "not a slice"},
		{name: "int64_arr_wants_slice", vt: pbv1.ValueTypeInt64Arr, goVal: []string{"1"}},
		{name: "unsupported_value_type", vt: pbv1.ValueType(255), goVal: "x"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := sdk.EncodeTagValue(tt.vt, tt.goVal)
			require.Error(t, err)
		})
	}
}

// TestEncodeTagValueNilAlwaysNull proves a nil Go value encodes to a nil raw
// value (the native "tag absent" representation) regardless of vt.
func TestEncodeTagValueNilAlwaysNull(t *testing.T) {
	for _, vt := range []pbv1.ValueType{
		pbv1.ValueTypeStr, pbv1.ValueTypeInt64, pbv1.ValueTypeFloat64,
		pbv1.ValueTypeBinaryData, pbv1.ValueTypeStrArr, pbv1.ValueTypeInt64Arr, pbv1.ValueTypeTimestamp,
	} {
		raw, err := sdk.EncodeTagValue(vt, nil)
		require.NoError(t, err)
		assert.Nil(t, raw)
	}
}

// TestEncodeTagValueEmptyCollectionsAreNotNull proves that a present-but-empty
// collection ([]byte{}, []string{}, []int64{}) encodes to a NON-nil raw value
// consistently across all three types, so it decodes back as an empty (not
// null) value — distinguishing "tag present, empty" from "tag absent". Only a
// genuinely nil Go value (covered by TestEncodeTagValueNilAlwaysNull) is null.
func TestEncodeTagValueEmptyCollectionsAreNotNull(t *testing.T) {
	cases := []struct {
		goVal any
		name  string
		vt    pbv1.ValueType
	}{
		{name: "empty_bytes", vt: pbv1.ValueTypeBinaryData, goVal: []byte{}},
		{name: "empty_str_arr", vt: pbv1.ValueTypeStrArr, goVal: []string{}},
		{name: "empty_int64_arr", vt: pbv1.ValueTypeInt64Arr, goVal: []int64{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, encErr := sdk.EncodeTagValue(tc.vt, tc.goVal)
			require.NoError(t, encErr)
			assert.NotNil(t, raw, "an empty collection must encode to a non-nil raw value")

			decoded, decErr := sdk.DecodeTagValue(tc.vt, raw)
			require.NoError(t, decErr)
			assert.False(t, decoded.IsNull(), "an empty collection must decode as present (not null)")
			assert.Equal(t, tc.vt, decoded.ValueType())
		})
	}
}
