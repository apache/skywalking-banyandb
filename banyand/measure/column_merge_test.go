// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package measure

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func encodeMergeColumn(c *column) []byte {
	var buf bytes.Buffer
	var cw writer
	cw.init(&buf)
	c.mustWriteTo(&columnMetadata{}, &cw)
	return buf.Buf
}

func TestMergeColumnTypedEncodingMatchesBytes(t *testing.T) {
	cases := []struct {
		name   string
		ints   []int64
		floats []float64
	}{
		{name: "integer extremes", ints: []int64{math.MinInt64, -1, 0, 1, math.MaxInt64}},
		{name: "integer constant", ints: []int64{42, 42, 42, 42}},
		{name: "integer counter", ints: []int64{10, 11, 12, 13}},
		{name: "decimal floats", floats: []float64{-123.25, 0, 0.01, 99.5}},
		{name: "float plain fallback", floats: []float64{math.Inf(-1), math.Float64frombits(0x7ff8000000000123), math.Copysign(0, -1), math.Inf(1)}},
		{name: "float scale overflow", floats: []float64{math.SmallestNonzeroFloat64, math.MaxFloat64}},
		{name: "negative zero", floats: []float64{math.Copysign(0, -1), 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			typed := column{name: "value"}
			byteBacked := column{name: "value"}
			if tc.ints != nil {
				typed.valueType, byteBacked.valueType = pbv1.ValueTypeInt64, pbv1.ValueTypeInt64
				typed.form, typed.int64s = numericFormInt64, tc.ints
				for _, v := range tc.ints {
					byteBacked.values = append(byteBacked.values, convert.Int64ToBytes(v))
				}
			} else {
				typed.valueType, byteBacked.valueType = pbv1.ValueTypeFloat64, pbv1.ValueTypeFloat64
				typed.form, typed.float64s = numericFormFloat64, tc.floats
				for _, v := range tc.floats {
					byteBacked.values = append(byteBacked.values, convert.Float64ToBytes(v))
				}
			}
			require.Equal(t, encodeMergeColumn(&byteBacked), encodeMergeColumn(&typed))
		})
	}
}

func TestMergeColumnAppendPreservesRowsAndOwnership(t *testing.T) {
	for _, vt := range []pbv1.ValueType{pbv1.ValueTypeInt64, pbv1.ValueTypeFloat64} {
		t.Run(fmt.Sprint(vt), func(t *testing.T) {
			source := column{valueType: vt}
			if vt == pbv1.ValueTypeInt64 {
				source.form = numericFormInt64
				source.int64s = []int64{math.MinInt64, -1, 0, math.MaxInt64}
			} else {
				source.form = numericFormFloat64
				source.float64s = []float64{math.Copysign(0, -1), math.Inf(-1), math.Float64frombits(0x7ff8000000000123), 1.25}
			}
			want := make([][]byte, source.rowCount())
			for i := range want {
				want[i] = source.rowBytes(i)
			}

			dest := column{valueType: vt}
			dest.appendRowsFrom(&source, 0, 2)
			dest.appendRowsFrom(&source, 2, 4)
			require.Equal(t, source.form, dest.form)
			require.Empty(t, dest.values)
			// Reusing input storage must not change the retained numeric rows.
			if vt == pbv1.ValueTypeInt64 {
				source.int64s[0] = 42
			} else {
				source.float64s[0] = 42
			}
			for i := range want {
				require.Equal(t, want[i], dest.rowBytes(i))
			}

			// A missing column forces the same nullable representation as old merges.
			dest.appendNulls(2)
			want = append(want, nil, nil)
			require.Equal(t, numericFormBytes, dest.form)
			require.Empty(t, dest.int64s)
			require.Empty(t, dest.float64s)
			require.Equal(t, want, dest.values)

			dest.appendRowsFrom(&source, 1, 3)
			want = append(want, source.rowBytes(1), source.rowBytes(2))
			plain := column{valueType: vt, values: [][]byte{nil, {}, []byte("null")}}
			dest.appendRowsFrom(&plain, 0, 3)
			want = append(want, nil, []byte{}, []byte("null"))
			require.Equal(t, want, dest.values)
			require.NotNil(t, dest.values[len(dest.values)-2])
			require.Equal(t, len(want), dest.rowCount())

			retained := dest.values
			dest.truncateRows(4)
			require.Equal(t, want[:4], dest.values)
			for _, removed := range retained[4:] {
				require.Nil(t, removed)
			}
			dest.reset()
			dest.valueType = vt
			dest.appendRowsFrom(&source, 0, 4)
			require.Equal(t, source.form, dest.form)
			dest.truncateRows(1)
			require.Equal(t, source.rowBytes(0), dest.rowBytes(0))
			require.Equal(t, 1, dest.rowCount())
		})
	}
}

func TestMergeColumnMixedFormsMatchByteEncoding(t *testing.T) {
	for _, vt := range []pbv1.ValueType{pbv1.ValueTypeInt64, pbv1.ValueTypeFloat64} {
		for _, nullsFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/nullsFirst=%v", vt, nullsFirst), func(t *testing.T) {
				source := column{name: "value", valueType: vt, values: genTypedValues(vt, "counter", 32, false)}
				var typed column
				readBack(t, &source, &typed, true)
				plain := column{valueType: vt, values: [][]byte{nil, []byte("null")}}
				merged := column{name: "value", valueType: vt}
				old := column{name: "value", valueType: vt}
				if nullsFirst {
					merged.appendRowsFrom(&plain, 0, 2)
					old.values = append(old.values, plain.values...)
				}
				merged.appendRowsFrom(&typed, 3, 24)
				old.values = append(old.values, source.values[3:24]...)
				if !nullsFirst {
					merged.appendRowsFrom(&plain, 0, 2)
					old.values = append(old.values, plain.values...)
				}
				require.Equal(t, encodeMergeColumn(&old), encodeMergeColumn(&merged))
			})
		}
	}
}

// Sequential reads are shared by merge readers and byte-only part iterators.
func TestMergeColumnSequentialReadForms(t *testing.T) {
	for _, vt := range []pbv1.ValueType{pbv1.ValueTypeInt64, pbv1.ValueTypeFloat64} {
		for _, nullable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/nullable=%v", vt, nullable), func(t *testing.T) {
				source := column{name: "value", valueType: vt, values: genTypedValues(vt, "gauge", 4096, nullable)}
				var stored bytes.Buffer
				var cw writer
				cw.init(&stored)
				var metadata columnMetadata
				source.mustWriteTo(&metadata, &cw)
				var reader seqReader
				defer reader.reset()
				var decoder encoding.BytesBlockDecoder
				var old, typed column
				reader.init(&stored)
				old.mustSeqReadValues(&decoder, &reader, metadata, uint64(source.rowCount()), false)
				require.Equal(t, numericFormBytes, old.form)
				require.Equal(t, source.values, old.values)
				reader.init(&stored)
				typed.mustSeqReadValues(&decoder, &reader, metadata, uint64(source.rowCount()), true)
				if nullable {
					require.Equal(t, numericFormBytes, typed.form)
				} else {
					require.NotEqual(t, numericFormBytes, typed.form)
					require.Empty(t, typed.values)
				}
				require.Equal(t, old.rowCount(), typed.rowCount())
				for i := 0; i < old.rowCount(); i++ {
					require.Equal(t, old.rowBytes(i), typed.rowBytes(i))
				}
				require.Equal(t, encodeMergeColumn(&old), encodeMergeColumn(&typed))
			})
		}
	}
}

func TestMergeColumnAppendReusesNumericStorage(t *testing.T) {
	ints := column{valueType: pbv1.ValueTypeInt64, form: numericFormInt64, int64s: []int64{10, 20, 30, 40}}
	floats := column{valueType: pbv1.ValueTypeFloat64, form: numericFormFloat64, float64s: []float64{1.25, 2.5, 3.75, 4.125}}
	var merged column
	merged.appendRowsFrom(&ints, 0, 4)
	intBuffer := &merged.int64s[0]
	merged.reset()
	merged.appendRowsFrom(&floats, 0, 4)
	floatBuffer := &merged.float64s[0]
	merged.reset()
	merged.appendRowsFrom(&ints, 0, 4)
	require.Same(t, intBuffer, &merged.int64s[0])
	require.Empty(t, merged.float64s)
	merged.reset()
	merged.appendRowsFrom(&floats, 0, 4)
	require.Same(t, floatBuffer, &merged.float64s[0])
	require.Empty(t, merged.int64s)
	merged.appendRowsFrom(&ints, 1, 1)
	merged.appendNulls(0)
	require.Equal(t, numericFormFloat64, merged.form)
	require.Equal(t, floats.float64s, merged.float64s)
}
