package aggregate_function

import (
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMin(t *testing.T) {
	minInt64, _ := NewMeasureAggregateFunction[int64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN)
	err := minInt64.Combine([][]int64{{10, 20, 30}})
	assert.NoError(t, err)
	assert.Equal(t, int64(10), minInt64.Result())

	minFloat64, _ := NewMeasureAggregateFunction[float64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN)
	err = minFloat64.Combine([][]float64{{1.0, 2.0, 3.0}})
	assert.NoError(t, err)
	assert.Equal(t, 1.0, minFloat64.Result())

}
