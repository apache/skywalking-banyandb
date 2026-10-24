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
	"math"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	storagemeasure "github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// readAllRows builds a part from rows through the engine flush path, opens it
// with OpenPart and returns every row with its own copy of tags and fields.
func readAllRows(t *testing.T, rows []storagemeasure.DumpRow) []Row {
	tmpPath, defFn := test.Space(require.New(t))
	t.Cleanup(defFn)

	fileSystem := fs.NewLocalFileSystem()
	partPath, _, cleanup := storagemeasure.BuildPartForDump(tmpPath, fileSystem, 12345, rows)
	t.Cleanup(cleanup)

	partID, err := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	require.NoError(t, err)
	p, err := OpenPart(partID, filepath.Dir(partPath), fileSystem)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })

	it := p.Iterator()
	defer it.Close()
	var out []Row
	for it.Next() {
		r := it.Row()
		cp := Row{
			Timestamp:  r.Timestamp,
			Version:    r.Version,
			SeriesID:   r.SeriesID,
			Tags:       make(map[string][]byte, len(r.Tags)),
			TagTypes:   r.TagTypes,
			Fields:     make(map[string][]byte, len(r.Fields)),
			FieldTypes: r.FieldTypes,
		}
		for k, v := range r.Tags {
			cp.Tags[k] = append([]byte(nil), v...)
		}
		for k, v := range r.Fields {
			cp.Fields[k] = append([]byte(nil), v...)
		}
		out = append(out, cp)
	}
	require.NoError(t, it.Err())
	return out
}

// TestMeasureFloatFieldNaNInf verifies that a FLOAT field column holding NaN and
// ±Inf, which the engine cannot encode as decimal ints and therefore writes with
// the plain-marker-wrapped default (dictionary) encoding, reads back exactly.
func TestMeasureFloatFieldNaNInf(t *testing.T) {
	values := []float64{1.5, math.NaN(), math.Inf(1), math.Inf(-1), -2.25, 0}
	rows := make([]storagemeasure.DumpRow, 0, len(values))
	for i, v := range values {
		rows = append(rows, storagemeasure.DumpRow{
			SeriesID:  1,
			Timestamp: 1700000000000000000 + int64(i),
			Version:   1,
			Fields:    []storagemeasure.DumpField{{Name: "value", Value: v}},
		})
	}

	got := readAllRows(t, rows)
	require.Len(t, got, len(values))
	for i, r := range got {
		require.Equal(t, pbv1.ValueTypeFloat64, r.FieldTypes["value"])
		fv := DecodeFieldValue(r.FieldTypes["value"], r.Fields["value"])
		require.NotNil(t, fv.GetFloat(), "row %d must decode as float", i)
		gotVal := fv.GetFloat().GetValue()
		if math.IsNaN(values[i]) {
			require.True(t, math.IsNaN(gotVal), "row %d: want NaN, got %v", i, gotVal)
			continue
		}
		require.Equal(t, math.Float64bits(values[i]), math.Float64bits(gotVal), "row %d", i)
	}
}

// TestMeasureFloatFieldNaNInfManyDistinct forces the inner default encoding past
// the dictionary limit so the wrapped block is a plain bytes block rather than a
// dictionary.
func TestMeasureFloatFieldNaNInfManyDistinct(t *testing.T) {
	const n = 1000
	values := make([]float64, n)
	for i := range values {
		values[i] = float64(i) + 0.5
	}
	values[7] = math.NaN()
	values[8] = math.Inf(1)
	rows := make([]storagemeasure.DumpRow, 0, n)
	for i, v := range values {
		rows = append(rows, storagemeasure.DumpRow{
			SeriesID:  1,
			Timestamp: 1700000000000000000 + int64(i),
			Version:   1,
			Fields:    []storagemeasure.DumpField{{Name: "value", Value: v}},
		})
	}

	got := readAllRows(t, rows)
	require.Len(t, got, n)
	for i, r := range got {
		gotVal := DecodeFieldValue(r.FieldTypes["value"], r.Fields["value"]).GetFloat().GetValue()
		if math.IsNaN(values[i]) {
			require.True(t, math.IsNaN(gotVal), "row %d: want NaN, got %v", i, gotVal)
			continue
		}
		require.Equal(t, math.Float64bits(values[i]), math.Float64bits(gotVal), "row %d", i)
	}
}

// TestMeasureIntTagWithNulls verifies an INT tag column that is absent on some
// rows of a block (null values force the plain-marker-wrapped default encoding)
// reads back with the present values intact and the missing ones null.
func TestMeasureIntTagWithNulls(t *testing.T) {
	rows := []storagemeasure.DumpRow{
		{
			SeriesID: 1, Timestamp: 1700000000000000000, Version: 1,
			Tags:   []storagemeasure.DumpTag{{Family: "default", Name: "code", Value: int64(200)}},
			Fields: []storagemeasure.DumpField{{Name: "value", Value: int64(1)}},
		},
		{
			SeriesID: 1, Timestamp: 1700000000000000001, Version: 1,
			Fields: []storagemeasure.DumpField{{Name: "value", Value: int64(2)}},
		},
		{
			SeriesID: 1, Timestamp: 1700000000000000002, Version: 1,
			Tags:   []storagemeasure.DumpTag{{Family: "default", Name: "code", Value: int64(500)}},
			Fields: []storagemeasure.DumpField{{Name: "value", Value: int64(3)}},
		},
	}

	got := readAllRows(t, rows)
	require.Len(t, got, 3)
	want := []*int64{ptr(200), nil, ptr(500)}
	for i, r := range got {
		require.Equal(t, int64(i+1), DecodeFieldValue(r.FieldTypes["value"], r.Fields["value"]).GetInt().GetValue())
		raw, ok := r.Tags["default.code"]
		if want[i] == nil {
			if ok {
				require.Nil(t, dump.DecodeTagValue(r.TagTypes["default.code"], raw, nil).GetInt(), "row %d must be null", i)
			}
			continue
		}
		require.True(t, ok, "row %d", i)
		require.Equal(t, *want[i], dump.DecodeTagValue(r.TagTypes["default.code"], raw, nil).GetInt().GetValue(), "row %d", i)
	}
}

func ptr(v int64) *int64 { return &v }
