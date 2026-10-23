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

package csv

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumpmeasure "github.com/apache/skywalking-banyandb/banyand/internal/dump/measure"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// TestSetStoredTags reads stored tag columns the way a query does: a merge's typed column
// serves its tag when the type is the schema's, a value of another type stays null.
func TestSetStoredTags(t *testing.T) {
	layout, err := csvfmt.StreamLayout([]*databasev1.Stream{{
		Metadata: &commonv1.Metadata{Name: "log"},
		Entity:   &databasev1.Entity{TagNames: []string{"svc"}},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "f", Tags: []*databasev1.TagSpec{
			{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "code", Type: databasev1.TagType_TAG_TYPE_INT},
			{Name: "msg", Type: databasev1.TagType_TAG_TYPE_STRING},
		}}},
	}})
	require.NoError(t, err)
	columns := tagColumns(layout, "log", []*databasev1.TagFamilySpec{{Name: "f", Tags: []*databasev1.TagSpec{
		{Name: "svc"}, {Name: "code"}, {Name: "msg"},
	}}})
	cell := func(cells []string, tag string) string { return cells[columns[tag]] }
	dropped := func(run *unitRun) []string {
		var out []string
		for tag := range run.dropped["log"] {
			out = append(out, tag)
		}
		return out
	}

	cells := layout.NewRow()
	run := newUnitRun(context.Background(), nil, &streamSettings{})
	setStoredTags(cells, layout,
		map[string][]byte{
			"f.code#int": convert.Int64ToBytes(200), // the schema's type: kept
			"f.code#str": []byte("old"),             // another type: null, like a query
			"f.msg":      []byte("hello"),
			"f.gone":     []byte("x"), // deleted from the schema
			"f.svc":      nil,         // no value in this row
		},
		map[string]pbv1.ValueType{
			"f.code#int": pbv1.ValueTypeInt64, "f.code#str": pbv1.ValueTypeStr,
			"f.msg": pbv1.ValueTypeStr, "f.gone": pbv1.ValueTypeStr, "f.svc": pbv1.ValueTypeStr,
		},
		columns, run, "log")
	require.Equal(t, "200", cell(cells, "f.code"))
	require.Equal(t, "hello", cell(cells, "f.msg"))
	require.Equal(t, csvfmt.NullCell, cell(cells, "f.svc"))
	require.ElementsMatch(t, []string{"f.code#str", "f.gone"}, dropped(run))

	// An unmerged part that stored code as a string before the schema made it an int.
	cells = layout.NewRow()
	run = newUnitRun(context.Background(), nil, &streamSettings{})
	setStoredTags(cells, layout, map[string][]byte{"f.code": []byte("old")}, map[string]pbv1.ValueType{"f.code": pbv1.ValueTypeStr},
		columns, run, "log")
	require.Equal(t, csvfmt.NullCell, cell(cells, "f.code"))
	require.Equal(t, []string{"f.code"}, dropped(run))

	// A value whose stored type is unknown reads as null in a query, and is not a drop.
	cells = layout.NewRow()
	run = newUnitRun(context.Background(), nil, &streamSettings{})
	setStoredTags(cells, layout, map[string][]byte{"f.msg": []byte("x")}, map[string]pbv1.ValueType{"f.msg": pbv1.ValueTypeUnknown},
		columns, run, "log")
	require.Equal(t, csvfmt.NullCell, cell(cells, "f.msg"))
	require.Empty(t, dropped(run))
}

// A measure field stored with another type than its column's (a field dropped and declared
// again with a new type) is dropped like a mistyped tag, and a value the series index stores
// for an indexed tag wins over the part's column, as in a query.
func TestMeasureStoredValues(t *testing.T) {
	layout, err := csvfmt.MeasureLayout([]*databasev1.Measure{{
		Metadata: &commonv1.Metadata{Name: "m"},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "f", Tags: []*databasev1.TagSpec{
			{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
		}}},
		Fields: []*databasev1.FieldSpec{{Name: "total", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}})
	require.NoError(t, err)
	totalIdx, ok := layout.FieldIndex("m", "total")
	require.True(t, ok)
	svcIdx, ok := layout.TagIndex("m", "f", "svc")
	require.True(t, ok)

	cells := layout.NewRow()
	run := newUnitRun(context.Background(), nil, &streamSettings{})
	// A null field value is no stored value, whatever its type.
	setFields(cells, layout, map[string][]byte{"total": nil}, map[string]pbv1.ValueType{"total": pbv1.ValueTypeStr},
		map[string]int{"total": totalIdx}, run, "m")
	require.Empty(t, run.dropped)
	setFields(cells, layout, map[string][]byte{"total": []byte("abc")}, map[string]pbv1.ValueType{"total": pbv1.ValueTypeStr},
		map[string]int{"total": totalIdx}, run, "m")
	require.Equal(t, csvfmt.NullCell, cells[totalIdx])
	require.Contains(t, run.dropped["m"], "_field.total")

	setFields(cells, layout, map[string][]byte{"total": convert.Int64ToBytes(7)}, map[string]pbv1.ValueType{"total": pbv1.ValueTypeInt64},
		map[string]int{"total": totalIdx}, run, "m")
	require.Equal(t, "7", cells[totalIdx])

	cells[svcIdx] = "from-column"
	res := &measureResource{
		tags:    map[string]int{"f.svc": svcIdx},
		indexed: map[uint32]dump.IndexedTagSpec{1: {Family: "f", Name: "svc", Type: pbv1.ValueTypeStr}},
	}
	fillIndexed(cells, res, map[uint32][][]byte{1: {[]byte("from-index")}}, run, "m")
	require.Equal(t, "from-index", cells[svcIdx])

	// An index value written while the tag had another type is dropped, not decoded.
	cells[svcIdx] = "from-column"
	res.indexed[1] = dump.IndexedTagSpec{Family: "f", Name: "svc", Type: pbv1.ValueTypeInt64}
	fillIndexed(cells, res, map[uint32][][]byte{1: {[]byte("abc")}}, run, "m")
	require.Equal(t, "from-column", cells[svcIdx])
	require.Contains(t, run.dropped["m"], "f.svc")
}

// The blocks of a series in one part may overlap in time (a merged block too large to join
// the next is written as it is), and each block's values alias a buffer the next block
// reuses. The cursor still serves the series in timestamp and descending version order,
// with the values each row was decoded with.
func TestMeasureCursorSortsOverlappingBlocks(t *testing.T) {
	type point struct {
		sid     common.SeriesID
		ts, ver int64
	}
	blocks := [][]point{
		{{1, 1, 1}, {1, 3, 1}},
		{{1, 2, 1}, {1, 3, 5}},
		{{2, 1, 1}},
	}
	shared := make([]byte, 8)
	it := dump.NewIterator(len(blocks), func(i int) ([]dumpmeasure.Row, error) {
		rows := make([]dumpmeasure.Row, 0, len(blocks[i]))
		for j, p := range blocks[i] {
			value := shared[j*4 : j*4+4]
			copy(value, fmt.Sprintf("%d%d%02d", p.sid, p.ts, p.ver))
			rows = append(rows, dumpmeasure.Row{SeriesID: p.sid, Timestamp: p.ts, Version: p.ver, Tags: map[string][]byte{"f.v": value}})
		}
		return rows, nil
	})
	c := &measureCursor{part: &dumpmeasure.PartReader{}, it: it}
	var got []string
	ok, err := c.next("shard")
	for ; ok; ok, err = c.next("shard") {
		r := c.row()
		got = append(got, fmt.Sprintf("%d/%d/%d=%s", r.SeriesID, r.Timestamp, r.Version, r.Tags["f.v"]))
	}
	require.NoError(t, err)
	require.Equal(t, []string{"1/1/1=1101", "1/2/1=1201", "1/3/5=1305", "1/3/1=1301", "2/1/1=2101"}, got)
}
