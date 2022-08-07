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
package aggregation

import (
	"github.com/pkg/errors"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

var ErrUnknownFunc = errors.New("unknown aggregation function")

type Int64Func interface {
	In(int64)
	Val() int64
	Reset()
}

func NewInt64Func(af modelv1.AggregationFunction) (Int64Func, error) {
	switch af {
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN:
		return &meanInt64Func{}, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT:
		return &countInt64Func{}, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX:
		return &maxInt64Func{}, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN:
		return &minInt64Func{}, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM:
		return &sumInt64Func{}, nil
	}
	return nil, ErrUnknownFunc
}
