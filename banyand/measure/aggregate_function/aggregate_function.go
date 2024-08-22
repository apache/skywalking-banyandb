package aggregate_function

import (
	"fmt"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"math"
)

type MAFInput interface {
	~int64 | ~float64 | ~string
}

type MAFInputNumber interface {
	~int64 | ~float64
}

type MAF[T MAFInput] interface {
	// todo [][]T 可能抽象出相关结构“参数数组”。
	Combine(arguments [][]T) error
	Result() T
}

func NewMeasureAggregateFunction[T MAFInput](aggregate modelv1.MeasureAggregate) (MAF[T], error) {
	var function MAF[T]
	switch aggregate {
	case modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN:
		function = &Min[T]{value: maxValue[T]()}
	case modelv1.MeasureAggregate_MEASURE_AGGREGATE_PERCENT:
		function = &Percent[T]{match: zeroValue[T](), total: zeroValue[T]()}
	default:
		return nil, fmt.Errorf("MeasureAggregate unknown")
	}

	return function, nil
}

func zeroValue[T MAFInput]() T {
	var z T
	return z
}

func minValue[N MAFInput]() (r N) {
	switch x := any(&r).(type) {
	case *int64:
		*x = math.MinInt64
	case *float64:
		*x = -math.MaxFloat64
	case *string:
		*x = ""
	default:
		panic("unreachable")
	}
	return
}

func maxValue[N MAFInput]() (r N) {
	switch x := any(&r).(type) {
	case *int64:
		*x = math.MaxInt64
	case *float64:
		*x = math.MaxFloat64
	case *string:
		*x = ""
	default:
		panic("unreachable")
	}
	return
}
