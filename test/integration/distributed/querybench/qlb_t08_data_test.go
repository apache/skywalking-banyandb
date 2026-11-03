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

package querybench

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

type qlbT08Shape struct {
	name       string
	projection string
	branches   int
	lanes      uint64
}

var qlbT08Shapes = []qlbT08Shape{
	{name: "float_avg", projection: "AVG(f)::FIELD AS avg_value", branches: 2, lanes: 2},
	{
		name:       "float_shared",
		projection: "SUM(f)::FIELD AS sum_value, COUNT(f)::FIELD AS count_value, AVG(f)::FIELD AS avg_value, AVG(f)::FIELD AS avg_again",
		branches:   2, lanes: 2,
	},
	{name: "int_extrema", projection: "MIN(v)::FIELD AS min_value, MAX(v)::FIELD AS max_value, COUNT(v)::FIELD AS count_value", branches: 3, lanes: 3},
	{name: "int_avg_fallback", projection: "AVG(v)::FIELD AS avg_value", lanes: 2},
}

var qlbT08Modes = []string{"pushdown", "local", "individual"}

func qlbT08FindShape(name string) (qlbT08Shape, error) {
	for _, shape := range qlbT08Shapes {
		if shape.name == name {
			return shape, nil
		}
	}
	return qlbT08Shape{}, fmt.Errorf("unknown T08 shape %q", name)
}

func qlbT08Query(name string, cardinality int, base time.Time) (*bydbqlv1.QueryRequest, error) {
	shape, err := qlbT08FindShape(name)
	if err != nil {
		return nil, err
	}
	hosts := make([]string, 16)
	for index := range hosts {
		hosts[index] = fmt.Sprintf("'host_%02d'", index)
	}
	return &bydbqlv1.QueryRequest{
		Query: "SELECT host, " + shape.projection + " FROM " + benchGroupName + "." + qlbT07Measure +
			" WHERE time >= ? AND time < ? AND host IN (" + strings.Join(hosts, ",") + ") GROUP BY host",
		Params: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base)}},
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(cardinality) * time.Millisecond))}},
		},
	}, nil
}

func qlbT08NativeQueries(name string, cardinality int, base time.Time) ([]*measurev1.QueryRequest, error) {
	shape, err := qlbT08FindShape(name)
	if err != nil {
		return nil, err
	}
	if shape.branches == 0 {
		return nil, nil
	}
	field := "f"
	functions := []modelv1.AggregationFunction{modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT}
	if name == "int_extrema" {
		field = "v"
		functions = []modelv1.AggregationFunction{
			modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN,
			modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX, modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT,
		}
	}
	hosts := make([]string, 16)
	for index := range hosts {
		hosts[index] = fmt.Sprintf("host_%02d", index)
	}
	requests := make([]*measurev1.QueryRequest, 0, len(functions))
	for _, function := range functions {
		requests = append(requests, &measurev1.QueryRequest{
			Groups: []string{benchGroupName}, Name: qlbT07Measure,
			TimeRange: &modelv1.TimeRange{Begin: timestamppb.New(base), End: timestamppb.New(base.Add(time.Duration(cardinality-1) * time.Millisecond))},
			Criteria: &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
				Name: "host", Op: modelv1.Condition_BINARY_OP_IN, Value: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: hosts}}},
			}}},
			TagProjection: tagProjection("host"), FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{field}},
			GroupBy: &measurev1.QueryRequest_GroupBy{TagProjection: tagProjection("host")},
			Agg:     &measurev1.QueryRequest_Aggregation{Function: function, FieldName: field}, Limit: ^uint32(0),
		})
	}
	return requests, nil
}
