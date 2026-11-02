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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestMeasureLargeStringMembershipFitsBudgetAndRebinds(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT CAST(host::TAG IN (?) AS INT) AS member FROM g.m")
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(t.Context(), statement)
	require.NoError(t, err)
	for _, selected := range []string{"a", "b"} {
		values := make([]string, 10000)
		values[0] = selected
		for index := 1; index < len(values); index++ {
			values[index] = fmt.Sprintf("absent-%05d", index)
		}
		execution, bindErr := template.BindParams([]*modelv1.TagValue{{
			Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: values}},
		}}, time.Unix(0, 0))
		require.NoError(t, bindErr)
		values[0] = "mutated after binding"
		source := computedGroupFixture(execution.Plan.Slots)
		ctx := vectorized.WithQueryMemoryTracker(t.Context(), 2<<20)
		operator, wrapErr := ql.WrapRaw(ctx, source, execution.Plan)
		require.NoError(t, wrapErr)
		result, pullErr := operator.NextBatch(ctx)
		require.NoError(t, pullErr)
		require.Equal(t, 6, result.Len)
		want := []int64{1, 1, 0, 0, 0, 0}
		if selected == "b" {
			want = []int64{0, 0, 1, 1, 0, 0}
		}
		require.Equal(t, want, result.Columns[0].(*vectorized.TypedColumn[int64]).Data())
		require.NoError(t, operator.Close())
		require.Equal(t, 1, source.closes)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}
}

func TestMeasureStringMembershipPreservesNullsDuplicatesAndCaseOrder(t *testing.T) {
	for _, test := range []struct {
		name, projection string
		want             []int64
		nulls            []int
	}{
		{name: "in", projection: "CAST(host::TAG IN (?, 'a', NULL) AS INT)", want: []int64{1, 1, 0, 0, 0, 0}, nulls: []int{2, 3, 4, 5}},
		{name: "not-in", projection: "CAST(host::TAG NOT IN (?, 'a', NULL) AS INT)", want: []int64{0, 0, 0, 0, 0, 0}, nulls: []int{2, 3, 4, 5}},
		{name: "row-value", projection: "CAST(host::TAG IN (?, host::TAG, NULL) AS INT)", want: []int64{1, 1, 1, 1, 1, 0}, nulls: []int{5}},
		{
			name: "case", projection: "CASE WHEN host::TAG IN (?) THEN 1 WHEN host::TAG IN ('a','b') THEN 2 ELSE 3 END",
			want: []int64{1, 1, 2, 2, 3, 3},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			statement, err := bydbql.Prepare("SELECT " + test.projection + " AS member FROM g.m")
			require.NoError(t, err)
			template, err := typedTransformer().CompileMeasure(t.Context(), statement)
			require.NoError(t, err)
			for _, size := range []int{2, 10000} {
				values := make([]string, size)
				values[0], values[1] = "a", "a"
				for index := 2; index < size; index++ {
					values[index] = fmt.Sprintf("missing-%d", index)
				}
				execution, bindErr := template.BindParams([]*modelv1.TagValue{{
					Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: values}},
				}}, time.Unix(0, 0))
				require.NoError(t, bindErr)
				source := computedGroupFixture(execution.Plan.Slots)
				source.batch.Columns[0].(*vectorized.TypedColumn[string]).MarkNullAt(5)
				ctx := vectorized.WithQueryMemoryTracker(t.Context(), 2<<20)
				operator, wrapErr := ql.WrapRaw(ctx, source, execution.Plan)
				require.NoError(t, wrapErr)
				result, pullErr := operator.NextBatch(ctx)
				require.NoError(t, pullErr)
				column := result.Columns[0].(*vectorized.TypedColumn[int64])
				for row, want := range test.want {
					isNull := false
					for _, nullRow := range test.nulls {
						isNull = isNull || row == nullRow
					}
					require.Equal(t, isNull, column.IsNull(row), "row %d", row)
					if !isNull {
						require.Equal(t, want, column.Data()[row], "row %d", row)
					}
				}
				require.NoError(t, operator.Close())
				require.Equal(t, 1, source.closes)
				require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
			}
		})
	}
}

func TestMeasureStringMembershipReservesLookupBeforeAllocation(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT CAST(host::TAG IN (?) AS INT) AS member FROM g.m")
	require.NoError(t, err)
	template, err := typedTransformer().CompileMeasure(t.Context(), statement)
	require.NoError(t, err)
	values := make([]string, 10000)
	for index := range values {
		values[index] = fmt.Sprintf("missing-%d", index)
	}
	execution, err := template.BindParams([]*modelv1.TagValue{{
		Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: values}},
	}}, time.Unix(0, 0))
	require.NoError(t, err)
	source := computedGroupFixture(execution.Plan.Slots)
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 256<<10)
	operator, err := ql.WrapRaw(ctx, source, execution.Plan)
	require.NoError(t, err)
	result, err := operator.NextBatch(ctx)
	require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded)
	require.Nil(t, result)
	require.NoError(t, operator.Close())
	require.Equal(t, 1, source.closes)
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
}
