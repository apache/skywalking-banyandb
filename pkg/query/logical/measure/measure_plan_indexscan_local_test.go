package measure

import (
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

type mockMeasureQueryResult struct {
	data []*model.MeasureResult
	idx  int
}

func (m *mockMeasureQueryResult) Pull() *model.MeasureResult {
	if m.idx >= len(m.data) {
		return nil
	}
	result := m.data[m.idx]
	m.idx++
	return result
}

func (m *mockMeasureQueryResult) Release() {}

func TestDeltaResultMIterator(t *testing.T) {
	testCases := []struct {
		name     string
		data     []*model.MeasureResult
		aggFuncs map[string]modelv1.MeasureAggregate
		want     []*measurev1.DataPoint
	}{
		{
			name: "empty data",
			data: []*model.MeasureResult{},
			aggFuncs: map[string]modelv1.MeasureAggregate{
				"_field1": modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN,
			},
			want: []*measurev1.DataPoint{},
		},
		{
			name: "single data point",
			data: []*model.MeasureResult{
				{
					SID:        1,
					Timestamps: []int64{1000000000},
					Versions:   []int64{1},
					Fields: []model.Field{
						{
							Name:   "_field1",
							Values: []*modelv1.FieldValue{{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 10.0}}}},
						},
					},
				},
			},
			aggFuncs: map[string]modelv1.MeasureAggregate{
				"_field1": modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN,
			},
			want: []*measurev1.DataPoint{
				{
					Timestamp: timestamppb.New(time.Unix(1, 0)),
					Fields: []*measurev1.DataPoint_Field{
						{
							Name:  "field1",
							Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 10.0}}},
						},
					},
				},
			},
		},
		{
			name: "multiple fields with different timestamps",
			data: []*model.MeasureResult{
				{
					SID:        1,
					Timestamps: []int64{1000000000, 2000000000, 3000000000},
					Versions:   []int64{1, 1, 1},
					Fields: []model.Field{
						{
							Name: "_field1",
							Values: []*modelv1.FieldValue{
								{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 10.0}}},
								{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 20.0}}},
								{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 30.0}}},
							},
						},
						{
							Name: "_field2",
							Values: []*modelv1.FieldValue{
								{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 1.5}}},
								{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 2.5}}},
								{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 3.5}}},
							},
						},
					},
				},
			},
			aggFuncs: map[string]modelv1.MeasureAggregate{
				"_field1": modelv1.MeasureAggregate_MEASURE_AGGREGATE_MAX,
				"_field2": modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN,
			},
			want: []*measurev1.DataPoint{
				{
					Timestamp: timestamppb.New(time.Unix(1, 0)),
					Fields: []*measurev1.DataPoint_Field{
						{
							Name:  "field1",
							Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 10.0}}},
						},
						{
							Name:  "field2",
							Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 1.5}}},
						},
					},
				},
				{
					Timestamp: timestamppb.New(time.Unix(2, 0)),
					Fields: []*measurev1.DataPoint_Field{
						{
							Name:  "field1",
							Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 20.0}}},
						},
						{
							Name:  "field2",
							Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 2.5}}},
						},
					},
				},
				{
					Timestamp: timestamppb.New(time.Unix(3, 0)),
					Fields: []*measurev1.DataPoint_Field{
						{
							Name:  "field1",
							Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 30.0}}},
						},
						{
							Name:  "field2",
							Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 3.5}}},
						},
					},
				},
			},
		},
		{
			name: "multiple fields with same timestamps",
			data: []*model.MeasureResult{
				{
					SID:        1,
					Timestamps: []int64{1000000000, 1000000000, 2000000000, 2000000000},
					Versions:   []int64{1, 1, 1, 1},
					Fields: []model.Field{
						{
							Name: "_field1",
							Values: []*modelv1.FieldValue{
								{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 10}}},
								{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 20}}},
								{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 30}}},
								{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 40}}},
							},
						},
						{
							Name: "_field2",
							Values: []*modelv1.FieldValue{
								{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 1.5}}},
								{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 2.5}}},
								{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 3.5}}},
								{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 4.5}}},
							},
						},
					},
				},
			},
			aggFuncs: map[string]modelv1.MeasureAggregate{
				"_field1": modelv1.MeasureAggregate_MEASURE_AGGREGATE_MAX,
				"_field2": modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN,
			},
			want: []*measurev1.DataPoint{
				{
					Timestamp: timestamppb.New(time.Unix(1, 0)),
					Fields: []*measurev1.DataPoint_Field{
						{
							Name:  "field1",
							Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 20}}},
						},
						{
							Name:  "field2",
							Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 1.5}}},
						},
					},
				},
				{
					Timestamp: timestamppb.New(time.Unix(2, 0)),
					Fields: []*measurev1.DataPoint_Field{
						{
							Name:  "field1",
							Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 40}}},
						},
						{
							Name:  "field2",
							Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 3.5}}},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			iter := &deltaResultMIterator{
				result:      &mockMeasureQueryResult{data: tc.data},
				aggregators: make(map[string]Aggregator),
			}

			for fieldName, aggFunc := range tc.aggFuncs {
				iter.aggregators[fieldName] = NewAggregator(aggFunc)
			}

			got := make([]*measurev1.DataPoint, 0)
			for iter.Next() {
				got = append(got, iter.Current()[0])
			}

			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("deltaResultMIterator mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
