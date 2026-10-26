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
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

func TestEncodeStringSentinels(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{in: "", want: ""},
		{in: "plain", want: "plain"},
		{in: `\N`, want: `\\N`},
		{in: `\\N`, want: `\\\N`},
		{in: `\\\\N`, want: `\\\\\N`},
		{in: `\Bx`, want: `\\Bx`},
		{in: `\\Bx`, want: `\\\Bx`},
		{in: `\`, want: `\\`},
		{in: `\\`, want: `\\\`},
		{in: `a\N`, want: `a\N`},
		{in: "line\nbreak", want: "line\nbreak"},
		{in: "a\rb", want: `\B` + "YQ1i"},
		{in: "a\r\nb", want: `\B` + "YQ0KYg=="},
		{in: "\\\r", want: `\B` + "XA0="},
	}
	for _, c := range cases {
		got := EncodeString(c.in)
		assert.Equal(t, c.want, got, "encode %q", c.in)
		v, isNull, err := DecodeCell(got, ValueString)
		require.NoError(t, err)
		assert.False(t, isNull)
		assert.Equal(t, c.in, v, "round trip %q", c.in)
	}
}

func TestDecodeStringPlainBackslashCell(t *testing.T) {
	// A cell starting with a single backslash that is neither a sentinel nor an escape is kept as is.
	v, isNull, err := DecodeCell(`\x`, ValueString)
	require.NoError(t, err)
	assert.False(t, isNull)
	assert.Equal(t, `\x`, v)
}

func TestNullCellRoundTripAllTypes(t *testing.T) {
	for _, vt := range []ValueType{ValueString, ValueInt, ValueFloat, ValueBinary, ValueStringArray, ValueIntArray, ValueTimestamp} {
		v, isNull, err := DecodeCell(NullCell, vt)
		require.NoError(t, err)
		assert.True(t, isNull, vt.String())
		assert.Nil(t, v)
	}
}

// The element id cell is the hex form a stream query returns for element_id: the
// big-endian bytes of the id.
func TestElementIDRoundTrip(t *testing.T) {
	for _, id := range []uint64{0, 1, 0x0102030405060708, math.MaxUint64} {
		cell := EncodeElementID(id)
		assert.Equal(t, hex.EncodeToString(convert.Uint64ToBytes(id)), cell)
		got, err := DecodeElementID(cell)
		require.NoError(t, err)
		assert.Equal(t, id, got)
	}
	for _, cell := range []string{"", "1", "00000000000000001", "000000000000000g", NullCell} {
		_, err := DecodeElementID(cell)
		assert.Error(t, err, cell)
	}
}

// A value fits only a column of its own type, a null fits every column, and a timestamp
// fits only inside the int64 nanosecond range a timestamp cell holds.
func TestFitsColumn(t *testing.T) {
	str := &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "a"}}}
	assert.True(t, FitsColumn(str, ValueString))
	assert.False(t, FitsColumn(str, ValueInt))
	assert.True(t, FitsColumn(nil, ValueInt))
	assert.True(t, FitsColumn(&modelv1.TagValue{Value: &modelv1.TagValue_Null{}}, ValueBinary))
	ts := func(tm time.Time) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(tm)}}
	}
	assert.True(t, FitsColumn(ts(time.Unix(0, 0)), ValueTimestamp))
	assert.True(t, FitsColumn(ts(time.Date(2262, 1, 1, 0, 0, 0, 0, time.UTC)), ValueTimestamp))
	assert.False(t, FitsColumn(ts(time.Date(2262, 4, 12, 0, 0, 0, 0, time.UTC)), ValueTimestamp))
	assert.False(t, FitsColumn(ts(time.Time{}), ValueTimestamp), "year 1 has no nanosecond form")
	assert.False(t, FitsColumn(ts(time.Unix(0, 0)), ValueInt))
}

func TestIntRoundTrip(t *testing.T) {
	for _, x := range []int64{0, 1, -1, math.MaxInt64, math.MinInt64, 1 << 53, 1<<53 + 1} {
		cell := EncodeInt(x)
		v, _, err := DecodeCell(cell, ValueInt)
		require.NoError(t, err)
		assert.Equal(t, x, v)
		ts := EncodeTimestamp(x)
		v, _, err = DecodeCell(ts, ValueTimestamp)
		require.NoError(t, err)
		assert.Equal(t, x, v)
	}
	_, _, err := DecodeCell("1.5", ValueInt)
	require.Error(t, err)
}

func TestFloatEncoding(t *testing.T) {
	assert.Equal(t, "1000000", EncodeFloat(1000000))
	assert.Equal(t, "3.14159", EncodeFloat(3.14159))
	assert.Equal(t, "NaN", EncodeFloat(math.NaN()))
	assert.Equal(t, "+Inf", EncodeFloat(math.Inf(1)))
	assert.Equal(t, "-Inf", EncodeFloat(math.Inf(-1)))
	assert.Equal(t, "-0", EncodeFloat(math.Copysign(0, -1)))
	assert.Len(t, EncodeFloat(1e300), 301)

	for _, x := range []float64{
		0, math.Copysign(0, -1), 1e300, -1e300, math.SmallestNonzeroFloat64, math.MaxFloat64, 0.1, 1.0 / 3, 1000000,
		math.Inf(1), math.Inf(-1),
	} {
		v, isNull, err := DecodeCell(EncodeFloat(x), ValueFloat)
		require.NoError(t, err)
		assert.False(t, isNull)
		assert.Equal(t, math.Float64bits(x), math.Float64bits(v.(float64)), "round trip %v", x)
	}
	v, _, err := DecodeCell(EncodeFloat(math.NaN()), ValueFloat)
	require.NoError(t, err)
	assert.True(t, math.IsNaN(v.(float64)))
}

func TestBinaryEncoding(t *testing.T) {
	cases := [][]byte{{0x00, 0x01, 0xff, 0xfe}, []byte("hello"), {}, {0xff, 0xfb}, bytes.Repeat([]byte{0x5c, 0x0d, 0x0a, ','}, 100)}
	for _, b := range cases {
		cell := EncodeBinary(b)
		// The cell must equal the protojson rendering of the same bytes.
		js, err := protojson.Marshal(&modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: b}})
		require.NoError(t, err)
		assert.JSONEq(t, `{"binaryData":"`+cell+`"}`, string(js))
		v, isNull, err := DecodeCell(cell, ValueBinary)
		require.NoError(t, err)
		assert.False(t, isNull)
		assert.Equal(t, b, v)
	}
	assert.Equal(t, "AAH//g==", EncodeBinary([]byte{0x00, 0x01, 0xff, 0xfe}))
	// Empty binary is "" and stays distinct from null.
	assert.Equal(t, "", EncodeBinary([]byte{}))
	assert.Equal(t, "", EncodeBinary(nil))
	assert.NotEqual(t, NullCell, EncodeBinary(nil))
	v, isNull, err := DecodeCell("", ValueBinary)
	require.NoError(t, err)
	assert.False(t, isNull)
	assert.Equal(t, []byte{}, v)
}

func TestStringArrayEncoding(t *testing.T) {
	assert.Equal(t, `["a","b"]`, EncodeStringArray([]string{"a", "b"}))
	assert.Equal(t, `[]`, EncodeStringArray(nil))
	assert.Equal(t, `[]`, EncodeStringArray([]string{}))
	// CR inside elements is escaped by JSON, so STRING_ARRAY never uses the \B downgrade.
	withCR := EncodeStringArray([]string{"a\r\nb", `\N`, "<&>", ""})
	assert.NotContains(t, withCR, "\r")
	assert.False(t, strings.HasPrefix(withCR, `\B`))
	assert.Contains(t, withCR, "<&>")
	for _, in := range [][]string{{"a", "b"}, {}, {"a\r\nb", `\N`, "<&>", "", ",\""}} {
		v, isNull, err := DecodeCell(EncodeStringArray(in), ValueStringArray)
		require.NoError(t, err)
		assert.False(t, isNull)
		assert.Equal(t, in, v)
	}
	_, _, err := DecodeCell("null", ValueStringArray)
	require.Error(t, err)
}

func TestIntArrayEncoding(t *testing.T) {
	big := []int64{9007199254740993, -1, math.MaxInt64, math.MinInt64}
	assert.Equal(t, "9007199254740993;-1;9223372036854775807;-9223372036854775808", EncodeIntArray(big))
	assert.Equal(t, "", EncodeIntArray(nil))
	assert.Equal(t, "", EncodeIntArray([]int64{}))
	for _, in := range [][]int64{big, {}, {0}, {42}} {
		v, isNull, err := DecodeCell(EncodeIntArray(in), ValueIntArray)
		require.NoError(t, err)
		assert.False(t, isNull)
		assert.Equal(t, in, v)
	}
	_, _, err := DecodeCell("1;x", ValueIntArray)
	require.Error(t, err)
}

func TestEncodeTagValue(t *testing.T) {
	ts := time.Date(2026, 9, 7, 12, 0, 0, 123, time.UTC)
	cases := []struct {
		in   *modelv1.TagValue
		want string
	}{
		{in: nil, want: NullCell},
		{in: &modelv1.TagValue{}, want: NullCell},
		{in: &modelv1.TagValue{Value: &modelv1.TagValue_Null{Null: structpb.NullValue_NULL_VALUE}}, want: NullCell},
		{in: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: `\N`}}}, want: `\\N`},
		{in: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: ""}}}, want: ""},
		{in: &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: -7}}}, want: "-7"},
		{in: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"x"}}}}, want: `["x"]`},
		{in: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{}}}, want: `[]`},
		{in: &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{1, 2}}}}, want: "1;2"},
		{in: &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte("data")}}, want: "ZGF0YQ=="},
		{in: &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte{}}}, want: ""},
		{in: &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(ts)}}, want: EncodeTimestamp(ts.UnixNano())},
		{in: &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{}}, want: NullCell},
	}
	for i, c := range cases {
		assert.Equal(t, c.want, EncodeTagValue(c.in), "case %d", i)
	}
}

func TestEncodeFieldValue(t *testing.T) {
	cases := []struct {
		in   *modelv1.FieldValue
		want string
	}{
		{in: nil, want: NullCell},
		{in: &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}, want: NullCell},
		{in: &modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: "a\rb"}}}, want: `\BYQ1i`},
		{in: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 87}}}, want: "87"},
		{in: &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 1000000}}}, want: "1000000"},
		{in: &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: math.NaN()}}}, want: "NaN"},
		{in: &modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: []byte("data")}}, want: "ZGF0YQ=="},
	}
	for i, c := range cases {
		assert.Equal(t, c.want, EncodeFieldValue(c.in), "case %d", i)
	}
}

func TestCSVReadBackOfEncodedCells(t *testing.T) {
	values := []string{"a,b", `say "hi"`, "multi\nline", "crlf\r\nline", `\N`, "", "  spaced  ", "trailing\r"}
	record := make([]string, 0, len(values)+1)
	for _, v := range values {
		record = append(record, EncodeString(v))
	}
	record = append(record, NullCell)
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	require.NoError(t, w.Write(record))
	w.Flush()
	require.NoError(t, w.Error())

	got, err := csv.NewReader(&buf).Read()
	require.NoError(t, err)
	require.Len(t, got, len(values)+1)
	for i, v := range values {
		dv, isNull, decodeErr := DecodeCell(got[i], ValueString)
		require.NoError(t, decodeErr)
		assert.False(t, isNull)
		assert.Equal(t, v, dv, "column %d", i)
	}
	_, isNull, err := DecodeCell(got[len(values)], ValueString)
	require.NoError(t, err)
	assert.True(t, isNull)
}

func TestEscapePathComponent(t *testing.T) {
	assert.Equal(t, "sw_metric", EscapePathComponent("sw_metric"))
	assert.Equal(t, "a.b-c_D9", EscapePathComponent("a.b-c_D9"))
	assert.Equal(t, "a%2Fb", EscapePathComponent("a/b"))
	assert.Equal(t, "a%25b", EscapePathComponent("a%b"))
	assert.Equal(t, "%20%3A%C3%A9", EscapePathComponent(" :é"))
	assert.Equal(t, "%2E", EscapePathComponent("."))
	assert.Equal(t, "%2E%2E", EscapePathComponent(".."))
	assert.Equal(t, "", EscapePathComponent(""))
}
