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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestMeasureHavingBucketsRequireGroupedScope(t *testing.T) {
	for _, query := range []string{
		"SELECT TIME_BUCKET('2s') AS bucket, SUM(v) AS total FROM g.m GROUP BY 1 HAVING TIME_BUCKET('3s') = CAST('1970-01-01T00:00:03Z' AS TIMESTAMP)",
		"SELECT host, SUM(v) AS total FROM g.m GROUP BY host HAVING TIME_BUCKET('2s') IS NOT NULL",
		"SELECT SUM(v) AS total FROM g.m HAVING TIME_BUCKET('2s') IS NOT NULL",
		"SELECT TIME_BUCKET('2s') AS bucket FROM g.m GROUP BY 1 HAVING CAST(TIME_BUCKET('3s') AS BIGINT) > 0",
		"SELECT TIME_BUCKET('2s') AS bucket FROM g.m GROUP BY 1 HAVING COALESCE(TIME_BUCKET('3s'),TIME_BUCKET('2s')) IS NOT NULL",
		"SELECT v FROM g.m WHERE TIME_BUCKET('2s') = CAST('1970-01-01T00:00:00Z' AS TIMESTAMP)",
		"SELECT TIME_BUCKET('2s') AS bucket FROM g.m WHERE TIME_BUCKET('2s') IS NOT NULL GROUP BY 1",
		"SELECT TIME_BUCKET('2s') AS bucket FROM g.m",
	} {
		t.Run(query, func(t *testing.T) {
			statement, err := bydbql.Prepare(query)
			if err == nil {
				_, err = typedTransformer().CompileMeasure(t.Context(), statement)
			}
			require.ErrorContains(t, err, "TIME_BUCKET")
		})
	}
	execution := typedExecution(t, "SELECT TIME_BUCKET('2s') AS bucket, SUM(v) AS total FROM g.m GROUP BY 1 "+
		"HAVING CAST(TIME_BUCKET('2s') AS BIGINT) >= 2000000000")
	require.Equal(t, map[int64]int64{2: 4, 4: 6}, bucketScopeTotals(t, execution))
}

func bucketScopeTotals(t *testing.T, execution *bydbql.MeasureExecution) map[int64]int64 {
	t.Helper()
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	source := computedGroupFixture(execution.Plan.Slots)
	op, err := ql.WrapRaw(ctx, source, execution.Plan)
	require.NoError(t, err)
	iterator, err := ql.RawIterator(ctx, op)
	require.NoError(t, err)
	totals := map[int64]int64{}
	for iterator.Next() {
		for _, row := range iterator.Current() {
			point := row.DataPoint
			require.NotNil(t, point.Timestamp)
			require.Equal(t, point.Timestamp.AsTime(), point.TagFamilies[0].Tags[0].Value.GetTimestamp().AsTime())
			totals[point.Timestamp.AsTime().Unix()] = point.Fields[0].Value.GetInt().Value
		}
	}
	require.NoError(t, iterator.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	return totals
}

func TestMeasureEquivalentBucketWidthsMatchGroupingAndHaving(t *testing.T) {
	schema := &databasev1.Measure{Interval: "2s", Fields: []*databasev1.FieldSpec{{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT}}}
	transformer := bydbql.NewTransformerWithSource(nil, nil)
	for _, width := range []string{"", "'2s'", "'2000ms'"} {
		t.Run(width, func(t *testing.T) {
			statement, err := bydbql.Prepare("SELECT TIME_BUCKET(" + width + ") AS bucket, SUM(v) AS total FROM g.m " +
				"GROUP BY TIME_BUCKET('2s') HAVING TIME_BUCKET(" + width + ") >= CAST('1970-01-01T00:00:02Z' AS TIMESTAMP)")
			require.NoError(t, err)
			template, err := transformer.CompileMeasureWithSchemas(t.Context(), statement,
				map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: schema})
			require.NoError(t, err)
			execution, err := template.BindParams(nil, time.Unix(0, 0))
			require.NoError(t, err)
			require.Equal(t, map[int64]int64{2: 4, 4: 6}, bucketScopeTotals(t, execution))
		})
	}
}

func TestMeasureHavingBucketUsesGenuinelyGroupedRawTime(t *testing.T) {
	for _, group := range []string{"time", "time, TIME_BUCKET('2s')"} {
		execution := typedExecution(t, "SELECT time AS original, SUM(v) AS total FROM g.m GROUP BY "+group+
			" HAVING TIME_BUCKET('3s') >= CAST('1970-01-01T00:00:03Z' AS TIMESTAMP)")
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		source := computedGroupFixture(execution.Plan.Slots)
		op, err := ql.WrapRaw(ctx, source, execution.Plan)
		require.NoError(t, err)
		iterator, err := ql.RawIterator(ctx, op)
		require.NoError(t, err)
		var originals []int64
		for iterator.Next() {
			for _, row := range iterator.Current() {
				original := row.DataPoint.TagFamilies[0].Tags[0].Value.GetTimestamp().AsTime().Unix()
				originals = append(originals, original)
				if original == 3 || original == 5 {
					require.NotNil(t, row.DataPoint.Fields[0].Value.GetNull())
				}
			}
		}
		require.NoError(t, iterator.Close())
		require.ElementsMatch(t, []int64{3, 4, 5}, originals)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}

func TestMeasureBucketAndAggregateCanShareAliasAcrossContainers(t *testing.T) {
	for _, projection := range []string{"TIME_BUCKET('2s') AS total, SUM(v) AS total", "SUM(v) AS total, TIME_BUCKET('2s') AS total"} {
		ordinal := "1"
		if projection[0] == 'S' {
			ordinal = "2"
		}
		statement, err := bydbql.Prepare("SELECT " + projection + " FROM g.m GROUP BY " + ordinal + " HAVING SUM(v) > ?")
		require.NoError(t, err)
		template, err := typedTransformer().CompileMeasure(t.Context(), statement)
		require.NoError(t, err)
		originalSize := template.EstimatedSize()
		var workers sync.WaitGroup
		for _, threshold := range []int64{3, 5} {
			workers.Go(func() {
				execution, bindErr := template.BindParams([]*modelv1.TagValue{intParam(threshold)}, time.Unix(0, 0))
				require.NoError(t, bindErr)
				expected := map[int64]int64{2: 4, 4: 6}
				if threshold == 5 {
					expected = map[int64]int64{4: 6}
				}
				require.Equal(t, expected, bucketScopeTotals(t, execution))
				require.Equal(t, vectorized.RoleField, execution.Plan.Having.Predicate.Args[0].Source.Role)
				execution.Plan.Having.Predicate.Args[0].Source.Name = "poison"
				execution.Plan.Having.Outputs[0].Source.Name = "poison"
				execution.Plan.Aggregate.Outputs[0].Column.Name = "poison"
			})
		}
		workers.Wait()
		require.Equal(t, originalSize, template.EstimatedSize())
	}
	for _, query := range []string{
		"SELECT TIME_BUCKET('2s') AS total, TIME_BUCKET('2000ms') AS total, SUM(v) AS sum FROM g.m GROUP BY 1,2",
		"SELECT TIME_BUCKET('2s') AS bucket, SUM(v) AS total, COUNT(v) AS total FROM g.m GROUP BY 1",
		"SELECT CAST(v AS STRING) AS total, SUM(v) AS total FROM g.m GROUP BY 1",
	} {
		statement, err := bydbql.Prepare(query)
		require.NoError(t, err)
		template, err := typedTransformer().CompileMeasure(t.Context(), statement)
		if err == nil {
			_, err = template.BindParams(nil, time.Unix(0, 0))
		}
		require.ErrorContains(t, err, "duplicate output name")
	}
}

func TestMeasureHavingUsesOutputSlotsWhenParameterChangesContainer(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT ? AS total, SUM(v) AS total FROM g.m GROUP BY 1 HAVING SUM(v) > ?")
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(t.Context(), statement)
	require.NoError(t, err)
	originalSize := template.EstimatedSize()
	for _, value := range []string{"1970-01-01T00:00:00Z", "1970-01-01T00:00:02Z"} {
		execution, bindErr := template.BindParams([]*modelv1.TagValue{
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(mustBucketTime(t, value))}}, intParam(11),
		}, time.Unix(0, 0))
		require.NoError(t, bindErr)
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
		source := computedGroupFixture(execution.Plan.Slots)
		op, operatorErr := ql.WrapRaw(ctx, source, execution.Plan)
		require.NoError(t, operatorErr)
		iterator, iteratorErr := ql.RawIterator(ctx, op)
		require.NoError(t, iteratorErr)
		require.True(t, iterator.Next())
		point := iterator.Current()[0].DataPoint
		require.Equal(t, value, point.TagFamilies[0].Tags[0].Value.GetTimestamp().AsTime().Format(time.RFC3339))
		require.EqualValues(t, 12, point.Fields[0].Value.GetInt().Value)
		require.False(t, iterator.Next())
		require.NoError(t, iterator.Close())
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
		execution.Plan.Having.Predicate.Args[0].Source.Name = "poison"
	}
	_, err = template.BindParams([]*modelv1.TagValue{intParam(1), intParam(11)}, time.Unix(0, 0))
	require.ErrorContains(t, err, "duplicate output name")
	require.Equal(t, originalSize, template.EstimatedSize())
}

func mustBucketTime(t *testing.T, value string) time.Time {
	t.Helper()
	instant, err := time.Parse(time.RFC3339, value)
	require.NoError(t, err)
	return instant
}

func TestMeasureHavingBucketCanReadHiddenGroupedRawTime(t *testing.T) {
	execution := typedExecution(t, "SELECT SUM(v) AS total FROM g.m GROUP BY time "+
		"HAVING TIME_BUCKET('3s') >= CAST('1970-01-01T00:00:03Z' AS TIMESTAMP)")
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	op, err := ql.WrapRaw(ctx, computedGroupFixture(execution.Plan.Slots), execution.Plan)
	require.NoError(t, err)
	iterator, err := ql.RawIterator(ctx, op)
	require.NoError(t, err)
	var totals []int64
	nulls := 0
	for iterator.Next() {
		for _, row := range iterator.Current() {
			require.Empty(t, row.DataPoint.TagFamilies)
			require.Nil(t, row.DataPoint.Timestamp)
			require.Len(t, row.DataPoint.Fields, 1)
			if _, isNull := row.DataPoint.Fields[0].Value.Value.(*modelv1.FieldValue_Null); isNull {
				nulls++
			} else {
				totals = append(totals, row.DataPoint.Fields[0].Value.GetInt().Value)
			}
		}
	}
	require.NoError(t, iterator.Close())
	require.Equal(t, 2, nulls)
	require.Equal(t, []int64{6}, totals)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}

func TestMeasureComputedGroupErrorsNameTheirOriginalScope(t *testing.T) {
	t.Run("aggregate argument", func(t *testing.T) {
		_, err := bydbql.Prepare("SELECT SUM(v) FROM g.m GROUP BY SUM(v + 1)")
		require.ErrorContains(t, err, "in GROUP BY")
		require.NotContains(t, err.Error(), "in HAVING")
	})
	t.Run("original column", func(t *testing.T) {
		_, err := bydbql.Prepare("SELECT host FROM MEASURE m IN g GROUP BY host LIMIT ten")
		require.ErrorContains(t, err, "1:53:")
	})
	t.Run("boolean key", func(t *testing.T) {
		statement, err := bydbql.Prepare("SELECT SUM(v) AS total FROM g.m GROUP BY v > 4")
		require.NoError(t, err)
		template, err := typedTransformer().CompileMeasure(t.Context(), statement)
		if err == nil {
			_, err = template.BindParams(nil, time.Unix(0, 0))
		}
		require.ErrorContains(t, err, "boolean GROUP BY")
		require.NotContains(t, err.Error(), "output type 3")
	})
}
