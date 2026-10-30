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

package bydbql_test

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestMeasureComputedGroupingAndOrdinalShareResults(t *testing.T) {
	expression := "CASE WHEN v >= 4 THEN 'high' ELSE 'low' END"
	for _, group := range []string{expression, "1"} {
		t.Run(group, func(t *testing.T) {
			execution := typedExecution(t, "SELECT "+expression+" AS band, SUM(v) AS total FROM g.m GROUP BY "+group)
			require.Nil(t, execution.Plan.NativeAggregate)
			require.Nil(t, execution.Request.Agg)
			source := computedGroupFixture(execution.Plan.Slots)
			ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
			op, err := ql.WrapRaw(ctx, source, execution.Plan)
			require.NoError(t, err)
			iterator, err := ql.RawIterator(ctx, op)
			require.NoError(t, err)
			totals := make(map[string]int64)
			for iterator.Next() {
				for _, row := range iterator.Current() {
					totals[row.DataPoint.Fields[0].Value.GetStr().Value] = row.DataPoint.Fields[1].Value.GetInt().Value
				}
			}
			require.NoError(t, iterator.Close())
			require.Equal(t, map[string]int64{"high": 10, "low": 2}, totals)
			require.Equal(t, 1, source.closes)
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		})
	}
}

type computedGroupSource struct {
	schema     *vectorized.BatchSchema
	batch      *vectorized.RecordBatch
	beforePull func()
	onClose    func()
	sent       bool
	closes     int
}

func computedGroupFixture(slots []ql.Slot) *computedGroupSource {
	defs := make([]vectorized.ColumnDef, 0, len(slots)+1)
	columns := make([]vectorized.Column, 0, len(slots)+1)
	for _, slot := range slots {
		if slot.Source.Role == vectorized.RoleTimestamp {
			continue
		}
		defs = append(defs, slot.Source)
		switch slot.Source.Name {
		case "v":
			col := vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, []int64{0, 2, 4, 0, 6, 0})
			col.MarkNullAt(3)
			col.MarkNullAt(5)
			columns = append(columns, col)
		case "f":
			col := vectorized.NewColumnFromSlice(vectorized.ColumnTypeFloat64, []float64{0.25, 0.75, 1.5, 0, 2.5, 4})
			col.MarkNullAt(3)
			columns = append(columns, col)
		default:
			columns = append(columns, vectorized.NewColumnFromSlice(vectorized.ColumnTypeString, []string{"a", "a", "b", "b", "c", "c"}))
		}
	}
	defs = append(defs, vectorized.ColumnDef{Name: "time", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64})
	columns = append(columns, vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, []int64{0, 1e9, 2e9, 3e9, 4e9, 5e9}))
	schema := vectorized.NewBatchSchema(defs)
	return &computedGroupSource{schema: schema, batch: &vectorized.RecordBatch{Schema: schema, Columns: columns, Len: 6}}
}

func (s *computedGroupSource) OutputSchema() *vectorized.BatchSchema { return s.schema }
func (s *computedGroupSource) Init(context.Context) error            { return nil }
func (s *computedGroupSource) Close() error {
	s.closes++
	if s.onClose != nil {
		s.onClose()
	}
	return nil
}

func (s *computedGroupSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	if s.beforePull != nil {
		s.beforePull()
	}
	if s.sent {
		return nil, nil
	}
	s.sent = true
	return s.batch, nil
}

func TestMeasureComputedGroupingParameterBindingsAndMultipleKeys(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT CASE WHEN v >= ? THEN CAST(? AS STRING) ELSE 'low' END AS band, " +
		"CAST(f AS INT) AS whole, SUM(v) AS total FROM g.m GROUP BY 1,2,1")
	require.NoError(t, err)
	require.Equal(t, 2, statement.NumPlaceholders())
	template, err := typedTransformer().CompileMeasure(t.Context(), statement)
	require.NoError(t, err)
	originalSize := template.EstimatedSize()
	var workers sync.WaitGroup
	for _, limit := range []int64{4, 6} {
		workers.Go(func() {
			execution, bindErr := template.BindParams([]*modelv1.TagValue{intParam(limit), strParam("high")}, time.Unix(0, 0))
			require.NoError(t, bindErr)
			require.Len(t, execution.Plan.Aggregate.GroupBy, 2)
			require.Len(t, execution.Plan.Aggregate.GroupExpressions.Outputs, 2)
			require.Equal(t, limit, execution.Plan.Aggregate.GroupExpressions.Outputs[0].Expr.Args[0].Args[1].Literal.Int64)
			execution.Plan.Aggregate.GroupExpressions.Outputs[0].Expr.Args[0].Args[1].Literal.Int64 = 99
		})
	}
	workers.Wait()
	require.Equal(t, originalSize, template.EstimatedSize())
	groupOnly := typedExecution(t, "SELECT CAST(NULL AS STRING) AS missing FROM g.m GROUP BY 1")
	require.Empty(t, groupOnly.Plan.Aggregate.Calls)
	source := computedGroupFixture(groupOnly.Plan.Slots)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err := ql.WrapRaw(ctx, source, groupOnly.Plan)
	require.NoError(t, err)
	iterator, err := ql.RawIterator(ctx, op)
	require.NoError(t, err)
	require.True(t, iterator.Next())
	require.NotNil(t, iterator.Current()[0].DataPoint.Fields[0].Value.GetNull())
	require.False(t, iterator.Next())
	require.NoError(t, iterator.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestMeasureGroupingOrdinalsExpandStarAndRejectInvalidTargets(t *testing.T) {
	schema := &databasev1.Measure{Fields: []*databasev1.FieldSpec{
		{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		{Name: "other", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
	}}
	transformer := bydbql.NewTransformerWithSource(nil, nil)
	for _, query := range []string{"SELECT * FROM g.m GROUP BY 1,2", "SELECT *, SUM(v) AS total FROM g.m GROUP BY 1,2"} {
		statement, err := bydbql.Prepare(query)
		require.NoError(t, err)
		template, err := transformer.CompileMeasureWithSchemas(t.Context(), statement, map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: schema})
		require.NoError(t, err)
		execution, err := template.BindParams(nil, time.Unix(0, 0))
		require.NoError(t, err)
		require.Len(t, execution.Plan.Aggregate.GroupBy, 2)
	}
	for _, query := range []string{
		"SELECT v FROM g.m GROUP BY 0", "SELECT v FROM g.m GROUP BY -1", "SELECT v FROM g.m GROUP BY 2",
		"SELECT SUM(v) FROM g.m GROUP BY 1", "SELECT v + 1 FROM g.m GROUP BY v + 2",
		"SELECT v + ? FROM g.m GROUP BY v + ?",
	} {
		statement, err := bydbql.Prepare(query)
		require.NoError(t, err)
		_, err = typedTransformer().CompileMeasure(t.Context(), statement)
		require.ErrorIs(t, err, bydbql.ErrInvalidMeasureQuery, query)
	}
}

func TestMeasureSelectableTimeBucketUsesNativeTimestampMapping(t *testing.T) {
	for _, group := range []string{"TIME_BUCKET('2s')", "1"} {
		execution := typedExecution(t, "SELECT TIME_BUCKET('2s') AS bucket, SUM(v) AS total FROM g.m GROUP BY "+group)
		source := computedGroupFixture(execution.Plan.Slots)
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		op, err := ql.WrapRaw(ctx, source, execution.Plan)
		require.NoError(t, err)
		iterator, err := ql.RawIterator(ctx, op)
		require.NoError(t, err)
		totals := make(map[int64]int64)
		for iterator.Next() {
			for _, row := range iterator.Current() {
				point := row.DataPoint
				require.Len(t, point.TagFamilies, 1)
				require.Equal(t, "__ql", point.TagFamilies[0].Name)
				require.Equal(t, "bucket", point.TagFamilies[0].Tags[0].Key)
				instant := point.TagFamilies[0].Tags[0].Value.GetTimestamp()
				require.NotNil(t, instant)
				require.Equal(t, instant.AsTime(), point.Timestamp.AsTime())
				totals[instant.AsTime().Unix()] = point.Fields[0].Value.GetInt().Value
			}
		}
		require.NoError(t, iterator.Close())
		require.Equal(t, map[int64]int64{0: 2, 2: 4, 4: 6}, totals)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
	only := typedExecution(t, "SELECT TIME_BUCKET('2s') AS bucket FROM g.m GROUP BY 1")
	require.Empty(t, only.Plan.Aggregate.Calls)
	require.Nil(t, only.Plan.NativeAggregate)
}

func TestMeasureComputedGroupingResourcesAndMaskedWork(t *testing.T) {
	execution := typedExecution(t, "SELECT CAST(v AS STRING) AS key, SUM(v) AS total FROM g.m WHERE v > 0 GROUP BY 1 HAVING CAST(v AS STRING) != '4'")
	for _, mode := range []string{"success", "cancel", "budget"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			size := int64(1 << 20)
			if mode == "budget" {
				size = 256
			}
			ctx = vectorized.WithQueryMemoryTracker(ctx, size)
			source := computedGroupFixture(execution.Plan.Slots)
			if mode == "cancel" {
				source.beforePull = cancel
			}
			op, err := ql.WrapRaw(ctx, source, execution.Plan)
			if mode == "budget" {
				require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
			} else {
				require.NoError(t, err)
				require.NoError(t, op.Init(ctx))
				batch, nextErr := op.NextBatch(ctx)
				if mode == "cancel" {
					require.ErrorIs(t, nextErr, context.Canceled)
				} else {
					require.NoError(t, nextErr)
					require.Equal(t, 2, batch.ActiveLen())
				}
				require.NoError(t, op.Close())
			}
			require.Equal(t, 1, source.closes)
			require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		})
	}
}

func TestMeasureTimeBucketDefaultWidthAndNativeEligibility(t *testing.T) {
	schema := &databasev1.Measure{
		Interval: "2s", Entity: &databasev1.Entity{TagNames: []string{"host"}},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING}}}},
		Fields:      []*databasev1.FieldSpec{{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT}, {Name: "f", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT}},
	}
	transformer := bydbql.NewTransformerWithSource(nil, nil)
	for _, width := range []string{"", "'2s'", "'1d'", "'1ns'"} {
		statement, err := bydbql.Prepare("SELECT TIME_BUCKET(" + width + ") AS bucket, host, MIN(v) AS lo, MAX(v) AS hi, COUNT(v) AS c, AVG(f) AS avg FROM MEASURE m IN g " +
			"WHERE time >= '1970-01-01T00:00:01Z' AND time < '1970-01-01T00:00:07Z' AND host IN ('a','b','c') GROUP BY 1,host")
		require.NoError(t, err)
		template, err := transformer.CompileMeasureWithSchemas(t.Context(), statement, map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: schema})
		require.NoError(t, err)
		execution, err := template.BindParams(nil, time.Unix(0, 0))
		require.NoError(t, err)
		require.NotNil(t, execution.Plan.NativeAggregate)
		require.Len(t, execution.Plan.NativeAggregate.Leaves, 5)
		require.Equal(t, vectorized.RoleTimestamp, execution.Plan.NativeAggregate.Aggregate.GroupBy[0].Role)
		require.True(t, execution.Plan.Aggregate.Outputs[0].Timestamp)
		for _, leaf := range execution.Plan.NativeAggregate.Leaves {
			require.Equal(t, execution.Plan.Aggregate.TimeBucketWidth, leaf.Request.GetGroupBy().GetTimeBucket().GetWidth())
		}
	}
	for _, width := range []string{"'0s'", "'-1s'", "'100000000000000000000d'"} {
		statement, err := bydbql.Prepare("SELECT TIME_BUCKET(" + width + ") AS bucket FROM g.m GROUP BY 1")
		require.NoError(t, err)
		_, err = transformer.CompileMeasureWithSchemas(t.Context(), statement, map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: schema})
		require.ErrorIs(t, err, bydbql.ErrInvalidMeasureQuery)
	}
	other := &databasev1.Measure{Interval: "3s", TagFamilies: schema.TagFamilies, Fields: schema.Fields}
	statement, err := bydbql.Prepare("SELECT TIME_BUCKET() AS bucket FROM MEASURE m IN (g,h) GROUP BY 1")
	require.NoError(t, err)
	_, err = transformer.CompileMeasureWithSchemas(t.Context(), statement,
		map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: schema, {Group: "h", Name: "m"}: other})
	require.ErrorContains(t, err, "intervals differ")
}

func TestMeasureComputedGroupingOwnsBorrowedStringKeys(t *testing.T) {
	execution := typedExecution(t, "SELECT CAST(host AS STRING) AS node, COUNT(v) AS count FROM g.m GROUP BY 1")
	source := computedGroupFixture(execution.Plan.Slots)
	source.onClose = func() {
		for _, column := range source.batch.Columns {
			if text, ok := column.(*vectorized.TypedColumn[string]); ok {
				for index := range text.Data() {
					text.Data()[index] = "poison"
				}
			}
		}
	}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err := ql.WrapRaw(ctx, source, execution.Plan)
	require.NoError(t, err)
	iterator, err := ql.RawIterator(ctx, op)
	require.NoError(t, err)
	counts := map[string]int64{}
	for iterator.Next() {
		for _, row := range iterator.Current() {
			counts[row.DataPoint.Fields[0].Value.GetStr().Value] = row.DataPoint.Fields[1].Value.GetInt().Value
		}
	}
	require.NoError(t, iterator.Close())
	require.Equal(t, map[string]int64{"a": 2, "b": 1, "c": 1}, counts)
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestComputedGroupingSyntaxStaysMeasureOnly(t *testing.T) {
	for _, group := range []string{"host + 1", "1"} {
		_, err := bydbql.ParseQuery("SELECT host FROM STREAM m IN g GROUP BY " + group)
		require.Error(t, err)
	}
}

func TestMeasureRawTimeAndBucketRemainIndependentGroupingKeys(t *testing.T) {
	execution := typedExecution(t, "SELECT time AS original, TIME_BUCKET('2s') AS bucket, SUM(v) AS total FROM g.m GROUP BY time, TIME_BUCKET('2s')")
	require.Nil(t, execution.Plan.NativeAggregate)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	source := computedGroupFixture(execution.Plan.Slots)
	op, err := ql.WrapRaw(ctx, source, execution.Plan)
	require.NoError(t, err)
	iterator, err := ql.RawIterator(ctx, op)
	require.NoError(t, err)
	buckets := map[int64]int64{}
	for iterator.Next() {
		for _, row := range iterator.Current() {
			point := row.DataPoint
			tags := point.TagFamilies[0].Tags
			require.Equal(t, "original", tags[0].Key)
			require.Equal(t, "bucket", tags[1].Key)
			original := tags[0].Value.GetTimestamp().AsTime().Unix()
			bucket := tags[1].Value.GetTimestamp().AsTime().Unix()
			buckets[original] = bucket
			require.Equal(t, bucket, point.Timestamp.AsTime().Unix())
			if original == 3 || original == 5 {
				require.NotNil(t, point.Fields[0].Value.GetNull())
			}
		}
	}
	require.NoError(t, iterator.Close())
	require.Equal(t, map[int64]int64{0: 0, 1: 0, 2: 2, 3: 2, 4: 4, 5: 4}, buckets)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestMeasureTimeBucketFloorsBeforeEpochAndChecksRange(t *testing.T) {
	execution := typedExecution(t, "SELECT TIME_BUCKET('2s') AS bucket, COUNT(v) AS count FROM g.m GROUP BY 1")
	for _, overflow := range []bool{false, true} {
		source := computedGroupFixture(execution.Plan.Slots)
		instants := []int64{-1, 0, 1, 1999999999, 2000000000, 3999999999}
		if overflow {
			instants[0] = math.MinInt64
		}
		source.batch.Columns[len(source.batch.Columns)-1] = vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, instants)
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		op, err := ql.WrapRaw(ctx, source, execution.Plan)
		require.NoError(t, err)
		if overflow {
			require.NoError(t, op.Init(ctx))
			_, err = op.NextBatch(ctx)
			require.ErrorIs(t, err, ql.ErrInvalidExpression)
			require.NoError(t, op.Close())
		} else {
			iterator, iteratorErr := ql.RawIterator(ctx, op)
			require.NoError(t, iteratorErr)
			counts := map[int64]int64{}
			for iterator.Next() {
				for _, row := range iterator.Current() {
					counts[row.DataPoint.Timestamp.AsTime().UnixNano()] = row.DataPoint.Fields[0].Value.GetInt().Value
				}
			}
			require.NoError(t, iterator.Close())
			require.Equal(t, map[int64]int64{-2000000000: 1, 0: 2, 2000000000: 1}, counts)
		}
		require.Equal(t, 1, source.closes)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}

func TestMeasureComputedGroupingAvoidsSourceCollisionsAndAccountsCachedRecipes(t *testing.T) {
	schema := &databasev1.Measure{Fields: []*databasev1.FieldSpec{
		{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		{Name: "__ql_group_0", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		{Name: "__ql_group_1", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
	}}
	transformer := bydbql.NewTransformerWithSource(nil, nil)
	sizes := make([]int, 0, 2)
	for _, query := range []string{
		"SELECT CAST(v AS STRING) AS key FROM g.m GROUP BY 1",
		"SELECT CASE WHEN v > 0 THEN 'large literal retained by the compiled grouping recipe' ELSE 'otherwise' END AS key FROM g.m GROUP BY 1",
	} {
		statement, err := bydbql.Prepare(query)
		require.NoError(t, err)
		template, err := transformer.CompileMeasureWithSchemas(t.Context(), statement, map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: schema})
		require.NoError(t, err)
		sizes = append(sizes, template.EstimatedSize())
		execution, err := template.BindParams(nil, time.Unix(0, 0))
		require.NoError(t, err)
		require.NotContains(t, []string{"v", "__ql_group_0", "__ql_group_1"}, execution.Plan.Aggregate.GroupBy[0].Name)
		require.Equal(t, "key", execution.Plan.Aggregate.Outputs[0].Column.Name)
		execution.Plan.Aggregate.GroupExpressions.Outputs[0].Expr.Literal.String = "poison"
		require.Equal(t, sizes[len(sizes)-1], template.EstimatedSize())
	}
	require.Greater(t, sizes[1], sizes[0])
}
