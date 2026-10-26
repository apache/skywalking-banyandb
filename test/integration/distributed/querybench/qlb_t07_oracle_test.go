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

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// The reference uses scalar fixture values and ordinary Go state, without the QL evaluator or group codec.
type qlbT07ScalarState struct {
	count int64
	sum   int64
	min   int64
	max   int64
}

func (s *qlbT07ScalarState) add(value int64) {
	if s.count == 0 || value < s.min {
		s.min = value
	}
	if s.count == 0 || value > s.max {
		s.max = value
	}
	s.sum += value
	s.count++
}

type qlbT07Oracle struct {
	expected    *measurev1.QueryResponse
	name        string
	cardinality int
}

func newQLBT07Oracle(name string, cardinality int) (qlbT07Oracle, error) {
	oracle := qlbT07Oracle{name: name, cardinality: cardinality, expected: &measurev1.QueryResponse{}}
	states := make([]qlbT07ScalarState, 1024)
	var scalar qlbT07ScalarState
	var floatSum float64
	var floatCount int64
	for row := 0; row < cardinality; row++ {
		integer, integerValid, floating, floatingValid := qlbT07Values(row)
		if integerValid {
			scalar.add(integer)
			states[row%1024].add(integer)
		}
		if floatingValid {
			floatSum += floating
			floatCount++
		}
	}
	switch name {
	case "ql_sum_scalar":
		oracle.expected.DataPoints = []*measurev1.DataPoint{{Fields: []*measurev1.DataPoint_Field{{Name: "sum_value", Value: intFieldValue(scalar.sum)}}}}
	case "ql_multi_1024_groups":
		for group := 0; group < min(cardinality, 1024); group++ {
			state := states[group]
			fields := []*measurev1.DataPoint_Field{
				{Name: "count_value", Value: intFieldValue(state.count)},
				{Name: "sum_value", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}},
				{Name: "min_value", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}},
				{Name: "max_value", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}},
			}
			if state.count > 0 {
				fields[1].Value, fields[2].Value, fields[3].Value = intFieldValue(state.sum), intFieldValue(state.min), intFieldValue(state.max)
			}
			oracle.expected.DataPoints = append(oracle.expected.DataPoints, qlbT07GroupedPoint("key1024", qlbT07Key(group), fields))
		}
	case "ql_avg_one_group":
		oracle.expected.DataPoints = []*measurev1.DataPoint{qlbT07GroupedPoint("one",
			&modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 1}}},
			[]*measurev1.DataPoint_Field{{Name: "avg_value", Value: qlbT07Float(floatSum / float64(floatCount))}})}
	case "ql_shared_n_groups":
		// LIMIT 1 chooses an unspecified group. Check each returned RID against its independently calculated value.
	default:
		return qlbT07Oracle{}, fmt.Errorf("unknown T07 oracle %q", name)
	}
	return oracle, nil
}

func qlbT07GroupedPoint(key string, value *modelv1.TagValue, fields []*measurev1.DataPoint_Field) *measurev1.DataPoint {
	return &measurev1.DataPoint{TagFamilies: []*modelv1.TagFamily{{
		Name: benchTagFamily,
		Tags: []*modelv1.Tag{{Key: key, Value: value}},
	}}, Fields: fields}
}

func (o qlbT07Oracle) rows() int {
	if o.name == "ql_shared_n_groups" {
		return 1
	}
	return len(o.expected.DataPoints)
}

func (o qlbT07Oracle) check(response *measurev1.QueryResponse) (uint64, error) {
	if len(response.GetDataPoints()) != o.rows() {
		return 0, fmt.Errorf("%s expected %d rows, received %d", o.name, o.rows(), len(response.GetDataPoints()))
	}
	expected := o.expected
	if o.name == "ql_shared_n_groups" {
		point := response.DataPoints[0]
		if len(point.TagFamilies) != 1 || point.TagFamilies[0].Name != benchTagFamily || len(point.TagFamilies[0].Tags) != 1 ||
			point.TagFamilies[0].Tags[0].Key != "rid" {
			return 0, fmt.Errorf("%s returned an unexpected grouping projection", o.name)
		}
		tag := point.TagFamilies[0].Tags[0].Value
		if _, ok := tag.GetValue().(*modelv1.TagValue_Int); !ok {
			return 0, fmt.Errorf("%s returned a noninteger RID", o.name)
		}
		rid := tag.GetInt().Value
		if rid < 0 || rid >= int64(o.cardinality) {
			return 0, fmt.Errorf("%s RID %d is outside the fixture", o.name, rid)
		}
		_, _, value, valid := qlbT07Values(int(rid))
		fields := []*measurev1.DataPoint_Field{
			{Name: "sum_value", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}},
			{Name: "count_value", Value: intFieldValue(0)},
			{Name: "avg_value", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}},
		}
		if valid {
			fields[0].Value, fields[1].Value, fields[2].Value = qlbT07Float(value), intFieldValue(1), qlbT07Float(value)
		}
		expected = &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{qlbT07GroupedPoint("rid",
			&modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: rid}}}, fields)}}
	}
	actualHash, expectedHash := qlbT01Hash(response), qlbT01Hash(expected)
	if actualHash != expectedHash {
		return 0, fmt.Errorf("%s differs from independent scalar fixture: got hash=%x expected=%x", o.name, actualHash, expectedHash)
	}
	return expectedHash, nil
}

func qlbT07GroupCount(name string, cardinality int) uint64 {
	switch name {
	case "ql_multi_1024_groups":
		return uint64(min(1024, cardinality))
	case "ql_shared_n_groups":
		return uint64(cardinality)
	default:
		return 1
	}
}
