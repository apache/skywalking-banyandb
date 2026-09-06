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

package stream

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

const filterWantState = "open"

// filterProjection is the client projection the accessor's (family, tag)
// coordinate space is defined by. It matches filterSchema's columns.
func filterProjection() []model.TagProjection {
	return []model.TagProjection{{Family: testTagFamily, Names: []string{testTagName, testFilterTagName}}}
}

// filterRegistry maps the criteria tag onto the coordinates the projection
// defines, exactly as CommonSchema.ProjTags numbers a projected schema.
func filterRegistry() logical.TagSpecMap {
	registry := logical.TagSpecMap{}
	registry.RegisterTagFamilies([]*databasev1.TagFamilySpec{{
		Name: testTagFamily,
		Tags: []*databasev1.TagSpec{
			{Name: testTagName, Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: testFilterTagName, Type: databasev1.TagType_TAG_TYPE_STRING},
		},
	}})
	return registry
}

// eqStateFilter builds `state == open`, the criteria every case below shares
// unless it needs a different operator.
func eqStateFilter(t *testing.T) logical.TagFilter {
	t.Helper()
	filter, err := logical.BuildSimpleTagFilter(&modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name:  testFilterTagName,
		Op:    modelv1.Condition_BINARY_OP_EQ,
		Value: strTagValue(filterWantState),
	}}})
	require.NoError(t, err)
	return filter
}

func TestTagFilter_Process(t *testing.T) {
	openRow := func(id uint64) filterRow {
		return filterRow{elemID: id, state: strTagValue(filterWantState)}
	}
	closedRow := func(id uint64) filterRow {
		return filterRow{elemID: id, state: strTagValue("closed")}
	}
	tests := []struct {
		name   string
		rows   []filterRow
		selIn  []uint16
		want   []uint16
		nilSel bool
	}{
		{
			name:   "nil selection keeps only survivors",
			rows:   []filterRow{openRow(1), closedRow(2), openRow(3), closedRow(4)},
			nilSel: true,
			want:   []uint16{0, 2},
		},
		{
			name:  "non-nil selection narrows within itself",
			rows:  []filterRow{openRow(1), closedRow(2), openRow(3), openRow(4)},
			selIn: []uint16{1, 2},
			want:  []uint16{2},
		},
		{
			name:   "all rows rejected yields an empty non-nil selection",
			rows:   []filterRow{closedRow(1), closedRow(2)},
			nilSel: true,
			want:   []uint16{},
		},
		{
			name:   "all rows kept",
			rows:   []filterRow{openRow(1), openRow(2)},
			nilSel: true,
			want:   []uint16{0, 1},
		},
		{
			// A null validity bit must win over the cell, which AppendColumnRange can
			// leave holding a stale pointer to a value that WOULD have matched.
			name:   "null bit beats a stale matching pointer",
			rows:   []filterRow{{elemID: 1, state: strTagValue(filterWantState), stateNull: true}, openRow(2)},
			nilSel: true,
			want:   []uint16{1},
		},
		{
			// A producer that leaves a cell empty without marking it null must not
			// crash the filter, and must not match a string criteria either.
			name:   "unmarked nil cell is treated as null",
			rows:   []filterRow{{elemID: 1}, openRow(2)},
			nilSel: true,
			want:   []uint16{1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := filterSchema()
			batch := buildFilterBatch(schema, tt.rows)
			if !tt.nilSel {
				batch.Selection = tt.selIn
			}
			op := NewTagFilter(schema, filterProjection(), eqStateFilter(t), filterRegistry())
			require.NoError(t, op.Init(context.Background()))
			require.Equal(t, schema, op.OutputSchema())
			require.NoError(t, op.Process(context.Background(), batch))
			// Equal against a non-nil empty slice also pins that the filter always
			// WRITES a selection: a nil here would leave every row active.
			require.Equal(t, tt.want, batch.Selection)
			require.Equal(t, len(tt.want), batch.ActiveLen())
			require.NoError(t, op.Close())
			require.NoError(t, op.Close(), "Close is idempotent")
		})
	}
}

// TestTagFilter_ProjectedTagWithoutColumn covers the egress case people miss: a
// tag is in the projection but the executed batch carries no column for it. The
// egress substitutes NullTagValue rather than failing, so the filter must too —
// erroring here would abort a query the row path answers.
func TestTagFilter_ProjectedTagWithoutColumn(t *testing.T) {
	// The batch schema omits the criteria tag entirely; the projection keeps it.
	schema := BuildStreamBatchSchema(
		[]model.TagProjection{{Family: testTagFamily, Names: []string{testTagName}}},
		testTagFamily, testTagName,
	)
	batch := vectorized.NewRecordBatch(schema, 1)
	batch.Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64]).Append(0)
	batch.Columns[schema.ElementIDIndex()].(*vectorized.TypedColumn[int64]).Append(ElementIDToColumn(1))
	batch.Columns[schema.SeriesIDIndex()].(*vectorized.TypedColumn[int64]).Append(0)
	serviceIdx, _ := schema.TagIndex(testTagFamily, testTagName)
	batch.Columns[serviceIdx].(*vectorized.TypedColumn[*modelv1.TagValue]).Append(strTagValue("svc"))
	streamOrderKeys(batch).Append([]byte{0})
	batch.Len = 1

	op := NewTagFilter(schema, filterProjection(), eqStateFilter(t), filterRegistry())
	require.NoError(t, op.Process(context.Background(), batch))
	require.Equal(t, []uint16{}, batch.Selection, "a null-substituted cell cannot equal a string criteria")
}

// TestTagFilter_CoordinateOutOfRange pins the accessor's out-of-range contract:
// it returns a bare nil, so tagExpr raises ErrTagNotDefined and the query errors,
// exactly as the row path does. Silently failing to match would return a wrong
// result set instead of an error.
func TestTagFilter_CoordinateOutOfRange(t *testing.T) {
	registry := logical.TagSpecMap{}
	registry.RegisterTag(7, 0, &databasev1.TagSpec{Name: testFilterTagName, Type: databasev1.TagType_TAG_TYPE_STRING})
	schema := filterSchema()
	batch := buildFilterBatch(schema, []filterRow{{elemID: 1, state: strTagValue(filterWantState)}})

	op := NewTagFilter(schema, filterProjection(), eqStateFilter(t), registry)
	err := op.Process(context.Background(), batch)
	require.ErrorIs(t, err, logical.ErrTagNotDefined)
}

// analyzedIndexChecker reports the criteria tag as indexed with a keyword
// analyzer, which is the shape a `match` criteria normally has in production.
type analyzedIndexChecker struct{}

func (analyzedIndexChecker) IndexDefined(tagName string) (bool, *databasev1.IndexRule) {
	return analyzedIndexChecker{}.IndexRuleDefined(tagName)
}

func (analyzedIndexChecker) IndexRuleDefined(ruleName string) (bool, *databasev1.IndexRule) {
	if ruleName != testFilterTagName {
		return false, nil
	}
	return true, &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: testFilterTagName},
		Tags:     []string{testFilterTagName},
		Analyzer: index.AnalyzerKeyword,
	}
}

// TestTagFilter_MatchOverNullCell records what a `match` criteria does to a null
// cell, and asserts it rather than working around it. The behavior splits on
// whether the tag carries an analyzer, and BOTH arms are reachable, so the filter
// must not short-circuit nulls: a short-circuit would make the vec path answer a
// query the row path rejects, and reject a row the row path also rejects but for
// a reason the client can see.
func TestTagFilter_MatchOverNullCell(t *testing.T) {
	criteria := &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name:  testFilterTagName,
		Op:    modelv1.Condition_BINARY_OP_MATCH,
		Value: strTagValue(filterWantState),
	}}}

	t.Run("analyzed tag errors the query", func(t *testing.T) {
		// parseExpr takes its analyzer branch, which accepts only TagValue_Str, so
		// the null raises ErrUnsupportedConditionValue and the whole query fails.
		filter, err := logical.BuildTagFilter(criteria, nil, nil, analyzedIndexChecker{}, false)
		require.NoError(t, err)
		schema := filterSchema()
		batch := buildFilterBatch(schema, []filterRow{{elemID: 1, stateNull: true}})

		op := NewTagFilter(schema, filterProjection(), filter, filterRegistry())
		require.ErrorIs(t, op.Process(context.Background(), batch), logical.ErrUnsupportedConditionValue)
	})

	t.Run("unanalyzed tag rejects the row", func(t *testing.T) {
		// With no index rule the analyzer is nil, so parseExpr returns the null
		// literal, whose Contains is always false — the row drops, without an error.
		filter, err := logical.BuildSimpleTagFilter(criteria)
		require.NoError(t, err)
		schema := filterSchema()
		batch := buildFilterBatch(schema, []filterRow{{elemID: 1, stateNull: true}})

		op := NewTagFilter(schema, filterProjection(), filter, filterRegistry())
		require.NoError(t, op.Process(context.Background(), batch))
		require.Equal(t, []uint16{}, batch.Selection)
	})
}
