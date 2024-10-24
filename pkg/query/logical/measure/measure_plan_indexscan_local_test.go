package measure

import (
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

func TestGroupAndAggregate(t *testing.T) {
	testCases := []struct {
		name     string
		input    *model.MeasureResult
		want     []*measurev1.DataPoint
		configs  []AggregatorConfig
		interval time.Duration
	}{
		{
			name: "simple aggregation",
			configs: []AggregatorConfig{
				{
					InputField:  FieldInfo{Name: "value", Type: databasev1.FieldType_FIELD_TYPE_INT},
					OutputField: FieldInfo{Name: "avg_value", Type: databasev1.FieldType_FIELD_TYPE_FLOAT},
					Function:    modelv1.MeasureAggregate_MEASURE_AGGREGATE_AVG,
				},
			},
			interval: time.Minute,
			input: &model.MeasureResult{
				Timestamps: []int64{
					time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
					time.Date(2023, 1, 1, 0, 0, 30, 0, time.UTC).UnixNano(),
					time.Date(2023, 1, 1, 0, 1, 0, 0, time.UTC).UnixNano(),
				},
				SID:      1,
				Versions: []int64{1, 1, 1},
				Fields: []model.Field{
					{
						Name: "value",
						Values: []*modelv1.FieldValue{
							{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 10}}},
							{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 20}}},
							{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 30}}},
						},
					},
				},
			},
			want: []*measurev1.DataPoint{
				{
					Timestamp: timestamppb.New(time.Date(2023, 1, 1, 0, 0, 30, 0, time.UTC)),
					Sid:       1,
					Version:   1,
					Fields: []*measurev1.DataPoint_Field{
						{
							Name:  "value",
							Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 20}}},
						},
						{
							Name:  "avg_value",
							Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 15}}},
						},
						{
							Name:  "avg_value_count",
							Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 2}}},
						},
					},
				},
				{
					Timestamp: timestamppb.New(time.Date(2023, 1, 1, 0, 1, 0, 0, time.UTC)),
					Sid:       1,
					Version:   1,
					Fields: []*measurev1.DataPoint_Field{
						{
							Name:  "value",
							Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 30}}},
						},
						{
							Name:  "avg_value",
							Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 30}}},
						},
						{
							Name:  "avg_value_count",
							Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 1}}},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := groupAndAggregate(tc.configs, tc.interval, tc.input)

			if diff := cmp.Diff(tc.want, got,
				protocmp.Transform(),
				protocmp.IgnoreFields(&measurev1.DataPoint{}, "tag_families"),
				cmp.Comparer(func(x, y float64) bool {
					return math.Abs(x-y) < 1e-5
				})); diff != "" {
				t.Errorf("groupAndAggregate mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
