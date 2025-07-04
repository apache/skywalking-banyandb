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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
)

// Mock types for testing.
type mockDistributedContext struct{}

func (m *mockDistributedContext) NodeSelectors() map[string][]string {
	return nil
}

func (m *mockDistributedContext) TimeRange() *modelv1.TimeRange {
	return &modelv1.TimeRange{
		Begin: timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)),
		End:   timestamppb.New(time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC)),
	}
}

func (m *mockDistributedContext) Broadcast(_ time.Duration, _ bus.Topic, _ bus.Message) ([]bus.Future, error) {
	return nil, nil
}

type mockFuture struct {
	data interface{}
}

func (m *mockFuture) Get() (bus.Message, error) {
	return bus.NewMessage(1, m.data), nil
}

func (m *mockFuture) GetAll() ([]bus.Message, error) {
	return []bus.Message{bus.NewMessage(1, m.data)}, nil
}

func TestMeanFields(t *testing.T) {
	tests := []struct {
		name           string
		isDistributed  bool
		expectedFields int
	}{
		{
			name:           "distributed mode",
			isDistributed:  true,
			expectedFields: 2, // mean + count
		},
		{
			name:           "standalone mode",
			isDistributed:  false,
			expectedFields: 1, // only mean
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create context
			ctx := context.Background()
			if tt.isDistributed {
				ctx = context.WithValue(ctx, executor.DistributedExecutionContextKey{}, &mockDistributedContext{})
			}

			// Create mean function and add values
			meanFunc := &aggregation.MeanFunc[float64]{}
			meanFunc.In(10)
			meanFunc.In(20)

			// Create mean value
			meanValue := &modelv1.FieldValue{
				Value: &modelv1.FieldValue_Float{
					Float: &modelv1.Float{Value: 15.0},
				},
			}

			fields := BuildAggregationFields(ctx, meanFunc, "test_field", meanValue)

			// Verify results
			assert.Equal(t, tt.expectedFields, len(fields))

			// Check mean field
			assert.Equal(t, "test_field", fields[0].Name)
			assert.Equal(t, meanValue, fields[0].Value)

			if tt.isDistributed {
				// Check count field in distributed mode
				assert.Equal(t, "count", fields[1].Name)
				countValue := fields[1].Value.GetValue()
				assert.NotNil(t, countValue)
				intVal, ok := countValue.(*modelv1.FieldValue_Int)
				assert.True(t, ok)
				assert.Equal(t, int64(2), intVal.Int.Value)
			}
		})
	}
}

func TestDistributedMeanAggregation(t *testing.T) {
	// Create mock futures with mean results from different nodes
	futures := []bus.Future{
		&mockFuture{
			data: &measurev1.QueryResponse{
				DataPoints: []*measurev1.DataPoint{
					{
						Fields: []*measurev1.DataPoint_Field{
							{
								Name:  "value",
								Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 15.0}}},
							},
							{
								Name:  "count",
								Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 2}}},
							},
						},
					},
				},
			},
		},
		&mockFuture{
			data: &measurev1.QueryResponse{
				DataPoints: []*measurev1.DataPoint{
					{
						Fields: []*measurev1.DataPoint_Field{
							{
								Name:  "value",
								Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 25.0}}},
							},
							{
								Name:  "count",
								Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 3}}},
							},
						},
					},
				},
			},
		},
	}

	// Create distributed merger and call Merge
	merger, err := aggregation.NewResultCombiner(
		modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN,
		"value",
	)
	assert.NoError(t, err)

	dataPoint, err := merger.Combine(futures, nil)
	assert.NoError(t, err)
	assert.NotNil(t, dataPoint)

	iterator := aggregation.NewAggregationResultIterator(dataPoint)

	// Call Next() to prepare the result
	assert.True(t, iterator.Next())

	// Get result
	results := iterator.Current()
	assert.Equal(t, 1, len(results))

	// Verify the merged mean
	// Node 1: mean=15, count=2, sum=30
	// Node 2: mean=25, count=3, sum=75
	// Total: sum=105, count=5, final mean=21
	field := results[0].Fields[0]
	assert.Equal(t, "value", field.Name)
	meanValue, ok := field.Value.GetValue().(*modelv1.FieldValue_Float)
	assert.True(t, ok)
	assert.Equal(t, 21.0, meanValue.Float.Value)
}

func TestDistributedCountAggregation(t *testing.T) {
	futures := []bus.Future{
		&mockFuture{data: &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{
			{
				Fields: []*measurev1.DataPoint_Field{{Name: "value", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 2}}}}},
			},
		}}},
		&mockFuture{data: &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{
			{
				Fields: []*measurev1.DataPoint_Field{{Name: "value", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 3}}}}},
			},
		}}},
	}
	merger, err := aggregation.NewResultCombiner(
		modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT,
		"value",
	)
	assert.NoError(t, err)

	dataPoint, err := merger.Combine(futures, nil)
	assert.NoError(t, err)

	iterator := aggregation.NewAggregationResultIterator(dataPoint)
	assert.True(t, iterator.Next())
	results := iterator.Current()
	assert.Equal(t, 1, len(results))
	// For count aggregation, we expect the sum of all counts: 2 + 3 = 5
	countValue := results[0].Fields[0].Value.GetValue().(*modelv1.FieldValue_Int).Int.Value
	assert.Equal(t, int64(5), countValue)
}

func TestDistributedMaxAggregation(t *testing.T) {
	futures := []bus.Future{
		&mockFuture{data: &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{
			{
				Fields: []*measurev1.DataPoint_Field{{Name: "value", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 10}}}}},
			},
		}}},
		&mockFuture{data: &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{
			{
				Fields: []*measurev1.DataPoint_Field{{Name: "value", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 20}}}}},
			},
		}}},
	}
	merger, err := aggregation.NewResultCombiner(
		modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX,
		"value",
	)
	assert.NoError(t, err)

	dataPoint, err := merger.Combine(futures, nil)
	assert.NoError(t, err)

	iterator := aggregation.NewAggregationResultIterator(dataPoint)
	assert.True(t, iterator.Next())
	results := iterator.Current()
	assert.Equal(t, 1, len(results))
	// For max aggregation, we expect the global maximum: 20
	maxValue := results[0].Fields[0].Value.GetValue().(*modelv1.FieldValue_Float).Float.Value
	assert.Equal(t, 20.0, maxValue)
}

func TestDistributedMinAggregation(t *testing.T) {
	futures := []bus.Future{
		&mockFuture{data: &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{
			{
				Fields: []*measurev1.DataPoint_Field{{Name: "value", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 10}}}}},
			},
		}}},
		&mockFuture{data: &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{
			{
				Fields: []*measurev1.DataPoint_Field{{Name: "value", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 20}}}}},
			},
		}}},
	}
	merger, err := aggregation.NewResultCombiner(
		modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN,
		"value",
	)
	assert.NoError(t, err)

	dataPoint, err := merger.Combine(futures, nil)
	assert.NoError(t, err)

	iterator := aggregation.NewAggregationResultIterator(dataPoint)
	assert.True(t, iterator.Next())
	results := iterator.Current()
	assert.Equal(t, 1, len(results))
	// For min aggregation, we expect the global minimum: 10
	minValue := results[0].Fields[0].Value.GetValue().(*modelv1.FieldValue_Float).Float.Value
	assert.Equal(t, 10.0, minValue)
}

func TestDistributedSumAggregation(t *testing.T) {
	futures := []bus.Future{
		&mockFuture{data: &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{
			{
				Fields: []*measurev1.DataPoint_Field{{Name: "value", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 10}}}}},
			},
		}}},
		&mockFuture{data: &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{
			{
				Fields: []*measurev1.DataPoint_Field{{Name: "value", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 20}}}}},
			},
		}}},
	}
	merger, err := aggregation.NewResultCombiner(
		modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
		"value",
	)
	assert.NoError(t, err)

	dataPoint, err := merger.Combine(futures, nil)
	assert.NoError(t, err)

	iterator := aggregation.NewAggregationResultIterator(dataPoint)
	assert.True(t, iterator.Next())
	results := iterator.Current()
	assert.Equal(t, 1, len(results))
	// For sum aggregation, we expect the total sum: 10 + 20 = 30
	sumValue := results[0].Fields[0].Value.GetValue().(*modelv1.FieldValue_Float).Float.Value
	assert.Equal(t, 30.0, sumValue)
}
