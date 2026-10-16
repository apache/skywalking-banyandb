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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type expressionContractSource struct {
	schema       *vectorized.BatchSchema
	batches      []*vectorized.RecordBatch
	next, closes int
}

func (*expressionContractSource) Init(context.Context) error              { return nil }
func (s *expressionContractSource) OutputSchema() *vectorized.BatchSchema { return s.schema }
func (s *expressionContractSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	if s.next == len(s.batches) {
		return nil, nil
	}
	batch := s.batches[s.next]
	s.next++
	return batch, nil
}
func (s *expressionContractSource) Close() error { s.closes++; return nil }

func TestMeasureExpressionKeepsNativeInputSlicesReadOnly(t *testing.T) {
	statement, err := bydbql.Prepare("SELECT COALESCE(s::TAG, 'missing') AS value FROM g.m")
	require.NoError(t, err)
	measure := &databasev1.Measure{TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{{
		Name: "s", Type: databasev1.TagType_TAG_TYPE_STRING,
	}}}}}
	template, err := bydbql.NewTransformerWithSource(nil, nil).CompileMeasureWithSchemas(t.Context(), statement,
		map[bydbql.MeasureSource]*databasev1.Measure{{Group: "g", Name: "m"}: measure})
	require.NoError(t, err)
	bound, err := statement.Bind(nil)
	require.NoError(t, err)
	execution, err := template.Bind(bound, time.Unix(0, 0))
	require.NoError(t, err)
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Name: "s", TagFamily: "default", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeTagValue},
		{Name: "time", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Name: "sid", Role: vectorized.RoleSeriesID, Type: vectorized.ColumnTypeInt64},
		{Name: "version", Role: vectorized.RoleVersion, Type: vectorized.ColumnTypeInt64},
	})
	makeBatch := func(column vectorized.Column) *vectorized.RecordBatch {
		return &vectorized.RecordBatch{Schema: schema, Len: 1, Columns: []vectorized.Column{
			column, vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, []int64{0}),
			vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, []int64{1}),
			vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, []int64{1}),
		}}
	}
	first := makeBatch(vectorized.NewColumnFromSlice(vectorized.ColumnTypeTagValue, []*modelv1.TagValue{
		{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "one"}}},
	}))
	inputStrings := []string{"two"}
	second := makeBatch(vectorized.NewColumnFromSlice(vectorized.ColumnTypeString, inputStrings))
	source := &expressionContractSource{schema: schema, batches: []*vectorized.RecordBatch{first, second, second}}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	projected, err := ql.WrapRaw(ctx, source, execution.Plan)
	require.NoError(t, err)
	iterator, err := ql.RawIterator(ctx, projected)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, iterator.Close())
		require.Equal(t, 1, source.closes)
		require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	}()
	for _, expected := range []string{"one", "two", "two"} {
		require.True(t, iterator.Next())
		require.Equal(t, expected, iterator.Current()[0].DataPoint.Fields[0].Value.GetStr().GetValue())
	}
	require.Equal(t, []string{"two"}, inputStrings)
}

func TestMeasureExpressionPreservesNonMeasureBooleanNamedColumns(t *testing.T) {
	for _, source := range []string{"STREAM s IN g", "TRACE t IN g", "PROPERTY p IN g"} {
		grammar, err := bydbql.ParseQuery("SELECT TRUE, FALSE FROM " + source)
		require.NoError(t, err)
		require.Nil(t, grammar.Select.Projection.Columns[0].Expression)
		require.Nil(t, grammar.Select.Projection.Columns[1].Expression)
	}
}
