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
	"compress/gzip"
	"encoding/base64"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// typesLayout has one column per value type after _name.
func typesLayout() *Layout {
	return &Layout{Columns: []Column{
		{Header: "_name", Kind: KindName, Type: ValueString},
		{Header: "s", Kind: KindTag, Type: ValueString},
		{Header: "i", Kind: KindTag, Type: ValueInt},
		{Header: "f", Kind: KindField, Type: ValueFloat},
		{Header: "b", Kind: KindTag, Type: ValueBinary},
		{Header: "sa", Kind: KindTag, Type: ValueStringArray},
		{Header: "ia", Kind: KindTag, Type: ValueIntArray},
		{Header: "ts", Kind: KindTag, Type: ValueTimestamp},
	}}
}

const (
	typesHeader = "_name,s,i,f,b,sa,ia,ts\n"
	typesLine   = `r,s,1,1.5,QUJD,"[""x""]",1;2,5`
)

// typesRow returns the decoded typesLine with the given cells replaced.
func typesRow(repl map[int]Value) []Value {
	row := []Value{
		{Str: "r"},
		{Str: "s"},
		{Int: 1},
		{Float: 1.5},
		{Bytes: []byte("ABC")},
		{Strs: []string{"x"}},
		{Ints: []int64{1, 2}},
		{Int: 5},
	}
	for i, v := range repl {
		row[i] = v
	}
	return row
}

// valueKey makes a Value comparable with assert.Equal, NaN included.
type valueKey struct {
	v         Value
	floatBits uint64
}

func keyOf(v Value) valueKey {
	bits := math.Float64bits(v.Float)
	v.Float = 0
	return valueKey{v: v, floatBits: bits}
}

func keysOf(rows [][]Value) [][]valueKey {
	out := make([][]valueKey, 0, len(rows))
	for _, row := range rows {
		k := make([]valueKey, len(row))
		for i, v := range row {
			k[i] = keyOf(v)
		}
		out = append(out, k)
	}
	return out
}

// readAll reads every record; rows holds copies since Next reuses its slice.
func readAll(t *testing.T, r io.Reader, l *Layout, opts ReaderOptions) ([][]Value, error) {
	rd, err := NewReader(r, l, opts)
	if err != nil {
		return nil, err
	}
	defer func() { require.NoError(t, rd.Close()) }()
	var rows [][]Value
	for {
		row, err := rd.Next()
		if errors.Is(err, io.EOF) {
			require.Equal(t, uint64(len(rows)), rd.RecordNo())
			return rows, nil
		}
		if err != nil {
			return rows, err
		}
		rows = append(rows, append([]Value(nil), row...))
		require.Equal(t, uint64(len(rows)), rd.RecordNo())
	}
}

func gzipBytes(t *testing.T, data []byte) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(data)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

// readBoth reads data with the field reader (also one byte per Read call) and the
// encoding/csv reader, requires all of them to agree, and returns the field reader's result.
func readBoth(t *testing.T, data []byte, l *Layout, gz bool) ([][]Value, error) {
	if gz {
		data = gzipBytes(t, data)
	}
	fieldRows, fieldErr := readAll(t, bytes.NewReader(data), l, ReaderOptions{Gzip: gz})
	oneRows, oneErr := fieldRows, fieldErr
	// One byte per Read call moves every buffer boundary, but is too slow for large inputs.
	if len(data) < 1<<20 {
		oneRows, oneErr = readAll(t, iotest.OneByteReader(bytes.NewReader(data)), l, ReaderOptions{Gzip: gz})
	}
	stdRows, stdErr := stdReadAll(t, bytes.NewReader(data), l, gz)
	assert.Equal(t, stdErr != nil, fieldErr != nil, "field error %v, std error %v", fieldErr, stdErr)
	assert.Equal(t, fieldErr != nil, oneErr != nil, "field error %v, one-byte error %v", fieldErr, oneErr)
	assert.Equal(t, keysOf(stdRows), keysOf(fieldRows))
	assert.Equal(t, keysOf(fieldRows), keysOf(oneRows))
	return fieldRows, fieldErr
}

func TestReadersAgree(t *testing.T) {
	crCell := base64.StdEncoding.EncodeToString([]byte("x\r\ny"))
	cases := []struct {
		name    string
		data    string
		wantErr string
		want    [][]Value
	}{
		{
			name: "quoted comma",
			data: typesHeader + `r,"a,b",1,1.5,QUJD,"[""x"",""y,z""]",1;2,5` + "\n",
			want: [][]Value{typesRow(map[int]Value{1: {Str: "a,b"}, 5: {Strs: []string{"x", "y,z"}}})},
		},
		{
			name: "doubled quote",
			data: typesHeader + `r,"say ""hi""",1,1.5,QUJD,"[""x""]",1;2,5` + "\n",
			want: [][]Value{typesRow(map[int]Value{1: {Str: `say "hi"`}})},
		},
		{
			name: "newline in field",
			data: typesHeader + "r,\"l1\nl2\n\",1,1.5,QUJD,\"[\"\"x\"\"]\",1;2,5\n" + typesLine + "\n",
			want: [][]Value{typesRow(map[int]Value{1: {Str: "l1\nl2\n"}}), typesRow(nil)},
		},
		{
			name: "crlf line endings",
			data: strings.ReplaceAll(typesHeader, "\n", "\r\n") + "r,\"a\r\nb\",1,1.5,QUJD,\"[\"\"x\"\"]\",1;2,5\r\n" + typesLine + "\r\n",
			want: [][]Value{typesRow(map[int]Value{1: {Str: "a\nb"}}), typesRow(nil)},
		},
		{
			name: "lone carriage return kept",
			data: typesHeader + "r,\"a\rb\",1,1.5,QUJD,\"[\"\"x\"\"]\",1;2,5\nr,c\rd,1,1.5,QUJD,\"[\"\"x\"\"]\",1;2,5\n",
			want: [][]Value{typesRow(map[int]Value{1: {Str: "a\rb"}}), typesRow(map[int]Value{1: {Str: "c\rd"}})},
		},
		{
			name: "empty fields",
			data: typesHeader + "r,,0,0,,[],,0\n",
			want: [][]Value{{{Str: "r"}, {}, {}, {}, {Bytes: []byte{}}, {Strs: []string{}}, {Ints: []int64{}}, {}}},
		},
		{
			name: "nulls",
			data: typesHeader + `r,\N,\N,\N,\N,\N,\N,\N` + "\n",
			want: [][]Value{{{Str: "r"}, {Null: true}, {Null: true}, {Null: true}, {Null: true}, {Null: true}, {Null: true}, {Null: true}}},
		},
		{
			name: "escaped backslash strings",
			data: typesHeader + `r,\\N,1,1.5,QUJD,"[""x""]",1;2,5` + "\n" + `r,"\\a,b",1,1.5,QUJD,"[""x""]",1;2,5` + "\n",
			want: [][]Value{typesRow(map[int]Value{1: {Str: `\N`}}), typesRow(map[int]Value{1: {Str: `\a,b`}})},
		},
		{
			name: "binary-downgraded string",
			data: typesHeader + `r,\B` + crCell + `,1,1.5,QUJD,"[""x""]",1;2,5` + "\n",
			want: [][]Value{typesRow(map[int]Value{1: {Str: "x\r\ny"}})},
		},
		{
			name: "large integers",
			data: typesHeader + `r,s,9007199254740993,1.5,QUJD,"[""x""]",9223372036854775807;-9223372036854775808;9007199254740993,-1` + "\n",
			want: [][]Value{typesRow(map[int]Value{
				2: {Int: 9007199254740993}, 6: {Ints: []int64{math.MaxInt64, math.MinInt64, 9007199254740993}}, 7: {Int: -1},
			})},
		},
		{
			name: "special floats",
			data: typesHeader + `r,s,1,NaN,QUJD,"[""x""]",1;2,5` + "\n" + `r,s,1,+Inf,QUJD,"[""x""]",1;2,5` + "\n" + `r,s,1,-Inf,QUJD,"[""x""]",1;2,5`,
			want: [][]Value{
				typesRow(map[int]Value{3: {Float: math.NaN()}}),
				typesRow(map[int]Value{3: {Float: math.Inf(1)}}),
				typesRow(map[int]Value{3: {Float: math.Inf(-1)}}),
			},
		},
		{
			name: "empty lines and trailing carriage return",
			data: "\n" + typesHeader + "\n\r\n" + typesLine + "\n\n" + typesLine + "\r",
			want: [][]Value{typesRow(nil), typesRow(nil)},
		},
		{
			name: "datatype line",
			data: "#datatype,string,string,long,double,base64Binary,stringArray,longArray,timestamp\n" + typesHeader + typesLine + "\n",
			want: [][]Value{typesRow(nil)},
		},
		{name: "empty file", data: "", wantErr: "no header"},
		{name: "datatype line only", data: "#datatype,string,string,long,double,base64Binary,stringArray,longArray,timestamp\n", wantErr: "no header"},
		{
			name:    "datatype token mismatch",
			data:    "#datatype,string,string,long,double,base64Binary,stringArray,longArray,long\n" + typesHeader,
			wantErr: "#datatype line cell 8",
		},
		{name: "datatype length mismatch", data: "#datatype,string\n" + typesHeader, wantErr: "#datatype line has 2 cells"},
		{name: "header mismatch", data: "_name,s,i,f,b,sa,ia,tx\n", wantErr: `header cell 7 is "tx"`},
		{name: "header quoted cells match", data: `"_name","s",i,f,b,sa,ia,ts` + "\n" + typesLine + "\n", want: [][]Value{typesRow(nil)}},
		{name: "header column count", data: "_name,s\n", wantErr: "header has 2 cells"},
		{name: "too few columns", data: typesHeader + typesLine + "\nr,s,1\n", wantErr: "record 2: record has 3 columns"},
		{name: "too many columns", data: typesHeader + typesLine + "\n" + typesLine + ",x\n", wantErr: "record 2:"},
		{name: "invalid UTF-8", data: typesHeader + "r,a\xffb,1,1.5,QUJD,\"[\"\"x\"\"]\",1;2,5\n", wantErr: "record 1: column \"s\": string is not valid UTF-8"},
		{name: "invalid UTF-8 in binary-downgraded string", data: typesHeader + `r,\B/w==,1,1.5,QUJD,"[""x""]",1;2,5` + "\n", wantErr: "UTF-8"},
		{name: "bare quote", data: typesHeader + `r,a"b,1,1.5,QUJD,"[""x""]",1;2,5` + "\n", wantErr: "record 1:"},
		{name: "text after closing quote", data: typesHeader + `r,"a"b,1,1.5,QUJD,"[""x""]",1;2,5` + "\n", wantErr: "record 1:"},
		{name: "unterminated quote", data: typesHeader + `r,"abc`, wantErr: "record 1:"},
		{name: "invalid int", data: typesHeader + `r,s,1x,1.5,QUJD,"[""x""]",1;2,5` + "\n", wantErr: `record 1: column "i"`},
		{name: "invalid base64", data: typesHeader + `r,s,1,1.5,QUJ,"[""x""]",1;2,5` + "\n", wantErr: `record 1: column "b"`},
		{name: "invalid string array", data: typesHeader + `r,s,1,1.5,QUJD,null,1;2,5` + "\n", wantErr: `record 1: column "sa"`},
		{name: "invalid int array", data: typesHeader + `r,s,1,1.5,QUJD,[],1;;2,5` + "\n", wantErr: `record 1: column "ia"`},
	}
	for _, tc := range cases {
		for _, gz := range []bool{false, true} {
			name := tc.name
			if gz {
				name += "/gzip"
			}
			t.Run(name, func(t *testing.T) {
				rows, err := readBoth(t, []byte(tc.data), typesLayout(), gz)
				if tc.wantErr != "" {
					require.Error(t, err)
					assert.Contains(t, err.Error(), tc.wantErr)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, keysOf(tc.want), keysOf(rows))
			})
		}
	}
}

func TestReaderStdErrorsCarryRecordNumber(t *testing.T) {
	data := typesHeader + typesLine + "\nr,a\xffb,1,1.5,QUJD,\"[\"\"x\"\"]\",1;2,5\n"
	_, err := readAll(t, strings.NewReader(data), typesLayout(), ReaderOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "record 2:")
	_, err = stdReadAll(t, strings.NewReader(data), typesLayout(), false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "record 2:")
}

func TestReaderTruncatedGzip(t *testing.T) {
	var body strings.Builder
	body.WriteString(typesHeader)
	for range 2000 {
		body.WriteString(typesLine + "\n")
	}
	full := gzipBytes(t, []byte(body.String()))
	for _, cut := range []int{4, 8, len(full) / 2} {
		data := full[:len(full)-cut]
		_, err := readAll(t, bytes.NewReader(data), typesLayout(), ReaderOptions{Gzip: true})
		require.Error(t, err, "cut %d", cut)
		_, err = stdReadAll(t, bytes.NewReader(data), typesLayout(), true)
		require.Error(t, err, "cut %d std", cut)
	}
	_, err := NewReader(strings.NewReader("not gzip"), typesLayout(), ReaderOptions{Gzip: true})
	require.Error(t, err)
}

func TestReaderErrorIsSticky(t *testing.T) {
	rd, err := NewReader(strings.NewReader(typesHeader+"r,s\n"+typesLine+"\n"), typesLayout(), ReaderOptions{})
	require.NoError(t, err)
	_, err = rd.Next()
	require.Error(t, err)
	_, err2 := rd.Next()
	assert.Equal(t, err, err2)
	assert.Zero(t, rd.RecordNo())
	require.NoError(t, rd.Close())
}
