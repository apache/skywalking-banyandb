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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package querybench

import (
	"fmt"
	"math"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

var qlbT10Variants = []string{"raw_control", "constant", "arithmetic", "parameter_cast", "masked_case_coalesce"}

func qlbT10Query(name string, cardinality int, base time.Time) (*bydbqlv1.QueryRequest, error) {
	projection := "v::FIELD AS result"
	var params []*modelv1.TagValue
	switch name {
	case "raw_control":
	case "constant":
		projection = "CAST(? AS DOUBLE) AS result"
		params = append(params, strTagValue("1.25"))
	case "arithmetic":
		projection = "v + 2 AS result"
	case "parameter_cast":
		projection = "v + CAST(? AS DOUBLE) AS result"
		params = append(params, strTagValue("1.25"))
	case "masked_case_coalesce":
		projection = "COALESCE(CASE WHEN v = 0 THEN 0 ELSE 8 / v END, -1) AS result"
	default:
		return nil, fmt.Errorf("unknown T10 variant %q", name)
	}
	params = append(params,
		&modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base)}},
		&modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(cardinality) * time.Millisecond))}},
	)
	return &bydbqlv1.QueryRequest{
		Query: "SELECT " + projection + " FROM " + benchGroupName + "." + qlbT07Measure + " WHERE time >= ? AND time < ?", Params: params,
	}, nil
}

// The oracle derives values from the documented fixture arithmetic rather than
// the production evaluator or any other query's result.
func qlbT10ExpectedValue(name string, row int) (*modelv1.FieldValue, error) {
	value, valid := int64(row%97-48), row%5 != 0
	switch name {
	case "constant":
		return qlbT07Float(1.25), nil
	case "masked_case_coalesce":
		result := int64(-1)
		if valid {
			result = 0
			if value != 0 {
				result = 8 / value
			}
		}
		return intFieldValue(result), nil
	case "raw_control", "arithmetic", "parameter_cast":
		if !valid {
			return &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}, nil
		}
		if name == "arithmetic" {
			value += 2
		}
		if name == "parameter_cast" {
			return qlbT07Float(float64(value) + 1.25), nil
		}
		return intFieldValue(value), nil
	default:
		return nil, fmt.Errorf("unknown T10 variant %q", name)
	}
}

func checkQLBT10Result(response *measurev1.QueryResponse, name string, cardinality int, base time.Time) (uint64, error) {
	if len(response.GetDataPoints()) != cardinality {
		return 0, fmt.Errorf("T10 %s returned %d rows, expected %d", name, len(response.GetDataPoints()), cardinality)
	}
	seen := make([]bool, cardinality)
	checksum := uint64(0xcbf29ce484222325)
	for _, point := range response.DataPoints {
		nanos := point.GetTimestamp().AsTime().UnixNano() - base.UnixNano()
		row := nanos / int64(time.Millisecond)
		if nanos < 0 || nanos%int64(time.Millisecond) != 0 || row >= int64(cardinality) || seen[row] {
			return 0, fmt.Errorf("T10 %s returned an unknown or duplicate timestamp", name)
		}
		seen[row] = true
		if len(point.Fields) != 1 || point.Fields[0].Name != "result" || len(point.TagFamilies) != 0 {
			return 0, fmt.Errorf("T10 %s returned unexpected visible columns", name)
		}
		want, err := qlbT10ExpectedValue(name, int(row))
		if err != nil {
			return 0, err
		}
		if err = checkQLBT10Value(point.Fields[0].Value, want); err != nil {
			return 0, fmt.Errorf("T10 %s row %d: %w", name, row, err)
		}
		checksum += qlbT10RowHash(uint64(row), point.Fields[0].Value)
	}
	return checksum, nil
}

func qlbT10ExpectedHash(name string, cardinality int) (uint64, error) {
	checksum := uint64(0xcbf29ce484222325)
	for row := range cardinality {
		value, err := qlbT10ExpectedValue(name, row)
		if err != nil {
			return 0, err
		}
		checksum += qlbT10RowHash(uint64(row), value)
	}
	return checksum, nil
}

func qlbT10RowHash(row uint64, value *modelv1.FieldValue) uint64 {
	var payload, kind uint64
	switch typed := value.GetValue().(type) {
	case *modelv1.FieldValue_Int:
		kind, payload = 1, uint64(typed.Int.Value)
	case *modelv1.FieldValue_Float:
		kind, payload = 2, math.Float64bits(typed.Float.Value)
	default:
	}
	// Row keys make this checksum insensitive to native row order while keeping
	// values, types and nulls distinct. Full verification separately rejects any
	// duplicate key, so cancellation in a commutative hash cannot hide lost rows.
	mixed := row ^ (payload + 0x9e3779b97f4a7c15 + kind*0x100000001b3)
	mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9
	mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111eb
	return mixed ^ (mixed >> 31)
}

func checkQLBT10Value(got, want *modelv1.FieldValue) error {
	switch value := want.GetValue().(type) {
	case *modelv1.FieldValue_Null:
		if _, ok := got.GetValue().(*modelv1.FieldValue_Null); ok {
			return nil
		}
	case *modelv1.FieldValue_Int:
		if actual, ok := got.GetValue().(*modelv1.FieldValue_Int); ok && actual.Int != nil && actual.Int.Value == value.Int.Value {
			return nil
		}
	case *modelv1.FieldValue_Float:
		if actual, ok := got.GetValue().(*modelv1.FieldValue_Float); ok && actual.Float != nil && math.Abs(actual.Float.Value-value.Float.Value) <= 1e-12 {
			return nil
		}
	default:
	}
	return fmt.Errorf("value or type differs: got %v, expected %v", got, want)
}
