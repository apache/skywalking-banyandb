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

package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	logical_measure "github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// budgetScanContext serves one stored TopN row holding the given entities, each with the value 1.
type budgetScanContext struct {
	entities []string
	released int
}

func (c *budgetScanContext) Query(context.Context, model.MeasureQueryOptions) (model.MeasureQueryResult, error) {
	values := make([]int64, len(c.entities))
	encodedEntities := make([][]byte, len(c.entities))
	for i, entity := range c.entities {
		values[i] = 1
		encoded, err := pbv1.MarshalTagValues(nil, []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entity}}}})
		if err != nil {
			return nil, err
		}
		encodedEntities[i] = encoded
	}
	dst := encoding.EncodeBytes(nil, []byte("value"))
	dst = encoding.VarUint64ToBytes(dst, 1)
	dst = encoding.EncodeBytes(dst, []byte("entity"))
	dst = encoding.VarUint64ToBytes(dst, uint64(len(values)))
	buf, encodeType, firstValue := encoding.Int64ListToBytes(nil, values)
	dst = append(dst, byte(encodeType))
	dst = encoding.VarInt64ToBytes(dst, firstValue)
	dst = encoding.VarUint64ToBytes(dst, uint64(len(buf)))
	dst = append(dst, buf...)
	dst = encoding.EncodeBytesBlock(dst, encodedEntities)
	return &budgetScanResult{data: dst, onRelease: func() { c.released++ }}, nil
}

type budgetScanResult struct {
	onRelease func()
	data      []byte
	pulled    bool
}

func (r *budgetScanResult) Pull() *model.MeasureResult {
	if r.pulled {
		return nil
	}
	r.pulled = true
	return &model.MeasureResult{
		Timestamps: []int64{60_000_000_000},
		Versions:   []int64{1},
		ShardIDs:   []common.ShardID{0},
		Fields:     []model.Field{{Name: "value", Values: []*modelv1.FieldValue{{Value: &modelv1.FieldValue_BinaryData{BinaryData: r.data}}}}},
	}
}

func (r *budgetScanResult) Release() { r.onRelease() }

func executeTopNSum(t *testing.T, ec executor.MeasureExecutionContext) error {
	t.Helper()
	source := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "source", Group: "g"},
		Entity:   &databasev1.Entity{TagNames: []string{"entity"}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{{Name: "entity", Type: databasev1.TagType_TAG_TYPE_STRING}},
		}},
		Fields: []*databasev1.FieldSpec{{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}
	rule := &databasev1.TopNAggregation{FieldName: "value", GroupByTagNames: []string{"entity"}}
	request := &measurev1.TopNRequest{
		Name: "rule", TopN: 1, FieldValueSort: modelv1.Sort_SORT_DESC,
		Agg: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
	}
	plan, err := logical_measure.TopNAnalyze(request, 0, []*databasev1.Measure{source}, []*databasev1.TopNAggregation{rule},
		[]executor.MeasureExecutionContext{ec})
	require.NoError(t, err)
	it, err := plan.(executor.MeasureExecutable).Execute(context.Background())
	if err == nil {
		require.NoError(t, it.Close())
	}
	return err
}

func TestTopNAggregationStateBudget(t *testing.T) {
	saved := logical_measure.TopNAggregateStateBudget
	t.Cleanup(func() { logical_measure.TopNAggregateStateBudget = saved })

	entities := []string{"e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8"}
	logical_measure.TopNAggregateStateBudget = 1 << 20
	within := &budgetScanContext{entities: entities}
	require.NoError(t, executeTopNSum(t, within))
	assert.Equal(t, 1, within.released)

	logical_measure.TopNAggregateStateBudget = 512
	over := &budgetScanContext{entities: entities}
	err := executeTopNSum(t, over)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "per-item state")
	assert.Equal(t, 1, over.released, "the scan result must be released when the budget is exceeded")
}
