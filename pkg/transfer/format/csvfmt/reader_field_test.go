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
	"encoding/base64"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// roundTripLayout covers every value type: m1 owns tags and fields of each type, m2 shares
// f.s and owns g.x.
func roundTripLayout(t *testing.T) *Layout {
	l, err := MeasureLayout([]*databasev1.Measure{
		{
			Metadata: md("m1"),
			TagFamilies: []*databasev1.TagFamilySpec{family("f",
				tag("s", databasev1.TagType_TAG_TYPE_STRING),
				tag("i", databasev1.TagType_TAG_TYPE_INT),
				tag("sa", databasev1.TagType_TAG_TYPE_STRING_ARRAY),
				tag("ia", databasev1.TagType_TAG_TYPE_INT_ARRAY),
				tag("b", databasev1.TagType_TAG_TYPE_DATA_BINARY),
			)},
			Fields: []*databasev1.FieldSpec{
				field("fs", databasev1.FieldType_FIELD_TYPE_STRING),
				field("fi", databasev1.FieldType_FIELD_TYPE_INT),
				field("ff", databasev1.FieldType_FIELD_TYPE_FLOAT),
				field("fb", databasev1.FieldType_FIELD_TYPE_DATA_BINARY),
			},
		},
		{
			Metadata:    md("m2"),
			TagFamilies: []*databasev1.TagFamilySpec{family("f", tag("s", databasev1.TagType_TAG_TYPE_STRING)), family("g", tag("x", databasev1.TagType_TAG_TYPE_INT))},
		},
	})
	require.NoError(t, err)
	return l
}

var stringPieces = []string{
	"", "a", ",", `"`, `""`, "\n", "\r", "\r\n", `\`, `\N`, `\B`, `\\`, " lead", `\.`, "é", "中文", "QUJD", "#datatype", "\t", "=",
}

type valueGen struct {
	rnd *rand.Rand
	// large enables cells longer than the field reader's input window.
	large bool
}

func (g *valueGen) str() string {
	if g.large && g.rnd.Intn(20) == 0 {
		n := fieldSourceBuffer + g.rnd.Intn(3*fieldSourceBuffer)
		s := strings.Repeat("x", n)
		switch g.rnd.Intn(4) {
		case 0:
			return s + "\r" // `\B` downgrade, streamed
		case 1:
			return s + "," // quoted
		case 2:
			return `\` + s // escaped backslash
		}
		return s
	}
	var b strings.Builder
	for range g.rnd.Intn(6) {
		b.WriteString(stringPieces[g.rnd.Intn(len(stringPieces))])
	}
	return b.String()
}

func (g *valueGen) int() int64 {
	switch g.rnd.Intn(5) {
	case 0:
		return math.MaxInt64
	case 1:
		return math.MinInt64
	case 2:
		return 1<<53 + 1
	}
	return g.rnd.Int63() - g.rnd.Int63()
}

func (g *valueGen) float() float64 {
	switch g.rnd.Intn(8) {
	case 0:
		return math.NaN()
	case 1:
		return math.Inf(1)
	case 2:
		return math.Inf(-1)
	case 3:
		return math.MaxFloat64
	case 4:
		return math.SmallestNonzeroFloat64
	case 5:
		return 0
	}
	return g.rnd.NormFloat64() * 1e6
}

func (g *valueGen) bytes() []byte {
	n := g.rnd.Intn(40)
	if g.large && g.rnd.Intn(15) == 0 {
		n = fieldSourceBuffer + g.rnd.Intn(4*fieldSourceBuffer)
	}
	b := make([]byte, n)
	g.rnd.Read(b)
	return b
}

// cell returns a random value of type t and its encoded cell; one in six is null.
func (g *valueGen) cell(t ValueType) (Value, string) {
	if g.rnd.Intn(6) == 0 {
		return Value{Null: true}, NullCell
	}
	switch t {
	case ValueString:
		s := g.str()
		return Value{Str: s}, EncodeString(s)
	case ValueInt:
		v := g.int()
		return Value{Int: v}, EncodeInt(v)
	case ValueTimestamp:
		v := g.int()
		return Value{Int: v}, EncodeTimestamp(v)
	case ValueFloat:
		v := g.float()
		return Value{Float: v}, EncodeFloat(v)
	case ValueBinary:
		b := g.bytes()
		return Value{Bytes: b}, EncodeBinary(b)
	case ValueStringArray:
		v := make([]string, g.rnd.Intn(4))
		for i := range v {
			v[i] = g.str()
		}
		return Value{Strs: v}, EncodeStringArray(v)
	case ValueIntArray:
		v := make([]int64, g.rnd.Intn(4))
		for i := range v {
			v[i] = g.int()
		}
		return Value{Ints: v}, EncodeIntArray(v)
	}
	panic(fmt.Sprintf("unexpected type %v", t))
}

// randomRows returns rows of m1 and m2: cells of columns the resource owns are random,
// all others are null.
func randomRows(l *Layout, g *valueGen, n int) (resources []string, want [][]Value, cells [][]string) {
	for range n {
		resource := "m1"
		if g.rnd.Intn(3) == 0 {
			resource = "m2"
		}
		row := l.NewRow()
		vals := make([]Value, len(l.Columns))
		vals[0] = Value{Str: resource}
		for i := 1; i < len(l.Columns); i++ {
			c := &l.Columns[i]
			owned := c.Kind != KindTag && c.Kind != KindField
			for _, r := range c.Resources {
				owned = owned || r == resource
			}
			if !owned {
				vals[i] = Value{Null: true}
				continue
			}
			vals[i], row[i] = g.cell(c.Type)
		}
		resources = append(resources, resource)
		want = append(want, vals)
		cells = append(cells, row)
	}
	return resources, want, cells
}

func TestReaderRoundTripRandom(t *testing.T) {
	l := roundTripLayout(t)
	for seed := int64(1); seed <= 6; seed++ {
		for _, opts := range []WriterOptions{
			{Gzip: false, Annotations: false},
			{Gzip: true, Annotations: true, MaxFileSize: 64 << 10},
			{Gzip: false, Annotations: true, MaxFileSize: 4 << 10},
		} {
			name := fmt.Sprintf("seed%d/gzip=%v/annotations=%v/max=%d", seed, opts.Gzip, opts.Annotations, opts.MaxFileSize)
			t.Run(name, func(t *testing.T) {
				g := &valueGen{rnd: rand.New(rand.NewSource(seed)), large: seed%2 == 0} //nolint:gosec // a fixed seed keeps failures reproducible.
				resources, want, cells := randomRows(l, g, 300)
				sink := &memSink{}
				opts.Layout = l
				opts.PathPrefix = "p"
				w := NewWriter(sink, opts)
				for i := range cells {
					require.NoError(t, w.WriteRow(resources[i], cells[i]))
				}
				require.NoError(t, w.Close())
				var got [][]Value
				for _, f := range sink.files {
					var rows [][]Value
					var err error
					if opts.Gzip {
						rows, err = readGzipBoth(t, f.data, l)
					} else {
						rows, err = readBoth(t, f.data, l, false)
					}
					require.NoError(t, err, f.path)
					got = append(got, rows...)
				}
				require.Equal(t, keysOf(want), keysOf(got))
				for i, row := range got {
					require.NoError(t, l.CheckRow(row), "row %d", i)
				}
			})
		}
	}
}

// readGzipBoth is readBoth for data that is already gzip-compressed.
func readGzipBoth(t *testing.T, data []byte, l *Layout) ([][]Value, error) {
	fieldRows, fieldErr := readAll(t, bytes.NewReader(data), l, ReaderOptions{Gzip: true})
	stdRows, stdErr := stdReadAll(t, bytes.NewReader(data), l, true)
	require.Equal(t, stdErr != nil, fieldErr != nil, "field error %v, std error %v", fieldErr, stdErr)
	require.Equal(t, keysOf(stdRows), keysOf(fieldRows))
	return fieldRows, fieldErr
}

func blobLayout() *Layout {
	return &Layout{Columns: []Column{
		{Header: "_name", Kind: KindName, Type: ValueString},
		{Header: "b", Kind: KindTag, Type: ValueBinary},
		{Header: "s", Kind: KindTag, Type: ValueString},
	}}
}

func TestFieldReaderLongCells(t *testing.T) {
	blob := make([]byte, 3*csvReadBuffer+12345)
	rand.New(rand.NewSource(7)).Read(blob) //nolint:gosec // deterministic fixture.
	text := base64.StdEncoding.EncodeToString(blob)
	longStr := strings.Repeat("y", 2*fieldSourceBuffer) + "\r"
	badUTF8 := base64.StdEncoding.EncodeToString(append(bytes.Repeat([]byte{'a'}, 2*fieldSourceBuffer), 0xff))
	cases := []struct {
		name    string
		line    string
		wantErr bool
	}{
		{name: "long binary then null", line: "r," + text + `,\N`},
		{name: "long \\B string", line: "r,\\N," + EncodeString(longStr)},
		{name: "long binary with crlf", line: "r," + text + ",x\r"},
		{name: "data after padding", line: "r," + text[:len(text)-8] + "AA==" + text[len(text)-4:] + ",x", wantErr: true},
		{name: "data after padding on decoder boundary", line: "r," + text[:1020] + "AA==" + text[1024:] + ",x", wantErr: true},
		{name: "bare quote in long binary", line: "r," + text[:len(text)/2] + `"` + text[len(text)/2:] + ",x", wantErr: true},
		{name: "invalid character in long binary", line: "r," + text[:len(text)/2] + "!" + text[len(text)/2:] + ",x", wantErr: true},
		{name: "long \\B string with invalid UTF-8", line: "r,\\N,\\B" + badUTF8, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := readBoth(t, []byte("_name,b,s\n"+tc.line+"\n"), blobLayout(), false)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, rows, 1)
		})
	}
	rows, err := readBoth(t, []byte("_name,b,s\nr,"+text+","+EncodeString(longStr)), blobLayout(), true)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, blob, rows[0][1].Bytes)
	assert.Equal(t, longStr, rows[0][2].Str)
}

// allocatedBy returns the bytes allocated while reading the single record of data, with
// the field reader or with the encoding/csv oracle.
func allocatedBy(t *testing.T, data []byte, std bool) (uint64, []byte) {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	var got []byte
	if std {
		rows, err := stdReadAll(t, bytes.NewReader(data), blobLayout(), false)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		got = rows[0][1].Bytes
	} else {
		rd, err := NewReader(bytes.NewReader(data), blobLayout(), ReaderOptions{})
		require.NoError(t, err)
		row, err := rd.Next()
		require.NoError(t, err)
		got = row[1].Bytes
		require.NoError(t, rd.Close())
	}
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc, got
}

func TestFieldReaderBinaryMemory(t *testing.T) {
	blob := make([]byte, 8<<20)
	rand.New(rand.NewSource(1)).Read(blob) //nolint:gosec // deterministic fixture.
	data := []byte("_name,b,s\nr," + EncodeBinary(blob) + ",x\n")

	fieldAlloc, got := allocatedBy(t, data, false)
	require.Equal(t, blob, got)
	stdAlloc, got := allocatedBy(t, data, true)
	require.Equal(t, blob, got)
	fieldRatio := float64(fieldAlloc) / float64(len(blob))
	stdRatio := float64(stdAlloc) / float64(len(blob))
	t.Logf("allocated per blob byte: field reader %.2f, encoding/csv reader %.2f", fieldRatio, stdRatio)
	// The decoded chunks plus the joined result: about 2x, far below the ~5x of encoding/csv.
	assert.Less(t, fieldRatio, 3.0)
}

func TestDecodeNumberMatchesDecodeCell(t *testing.T) {
	cells := []string{"0", "-1", "+7", "9223372036854775807", "9223372036854775808", "1x", "", " 1", "1.5", "NaN", "+Inf", "-inf", "1e400", "0x10", "1_000"}
	for _, typ := range []ValueType{ValueInt, ValueTimestamp, ValueFloat} {
		for _, cell := range cells {
			var got, want Value
			gotErr := decodeNumber([]byte(cell), typ, &got)
			wantErr := decodeCell(cell, typ, &want)
			if wantErr != nil {
				require.Error(t, gotErr, "%s %q", typ, cell)
				assert.Equal(t, wantErr.Error(), gotErr.Error())
				continue
			}
			require.NoError(t, gotErr, "%s %q", typ, cell)
			assert.Equal(t, keyOf(want), keyOf(got), "%s %q", typ, cell)
		}
	}
}
