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
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

const qlbT16Measure = "qlb_t16"

// Every control reads two equally typed native fields and returns the same
// duplicated visible key and SUM. The duplicate projection is a fixed witness
// that SELECT and GROUP BY share one computed key evaluation.
type qlbT16Spec struct {
	name                    string
	high, computed, ordinal bool
}

func qlbT16Specs() []qlbT16Spec {
	var specs []qlbT16Spec
	for _, high := range []bool{false, true} {
		level := "low"
		if high {
			level = "high"
		}
		specs = append(specs,
			qlbT16Spec{name: "simple_" + level, high: high},
			qlbT16Spec{name: "computed_explicit_" + level, high: high, computed: true},
			qlbT16Spec{name: "computed_ordinal_" + level, high: high, computed: true, ordinal: true},
		)
	}
	return specs
}

func (s qlbT16Spec) dataset() string {
	if s.high {
		return "high"
	}
	return "low"
}

func (s qlbT16Spec) groupCount(rows int) int {
	if s.high {
		return max(1, rows/2)
	}
	return min(8, rows)
}

type qlbT16Oracle struct {
	expected          *measurev1.QueryResponse
	spec              qlbT16Spec
	inputRows, groups int
	hash              uint64
}

func newQLBT16Oracle(spec qlbT16Spec, cardinality int) *qlbT16Oracle {
	o := &qlbT16Oracle{spec: spec, inputRows: cardinality, groups: spec.groupCount(cardinality), expected: &measurev1.QueryResponse{}}
	// Work directly from the documented row sequence and modulo partition. This
	// does not evaluate the CASE expression or inspect any production result.
	for key := 0; key < o.groups; key++ {
		var sum int64
		var present bool
		for row := key; row < cardinality; row += o.groups {
			if row%5 != 0 {
				sum += int64(row%97 - 48)
				present = true
			}
		}
		value := &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
		if present {
			value = intFieldValue(sum)
		}
		o.expected.DataPoints = append(o.expected.DataPoints, &measurev1.DataPoint{Fields: []*measurev1.DataPoint_Field{
			{Name: "key", Value: intFieldValue(int64(key))},
			{Name: "key_again", Value: intFieldValue(int64(key))},
			{Name: "total", Value: value},
		}})
	}
	o.hash = qlbT01Hash(o.expected)
	return o
}

func (o *qlbT16Oracle) query(cardinality int, base time.Time) *bydbqlv1.QueryRequest {
	key := "precomputed::FIELD"
	if o.spec.computed {
		key = "CASE WHEN raw::FIELD >= 0 THEN raw::FIELD + 1 ELSE 0 END"
	}
	group := key
	if o.spec.ordinal {
		group = "1"
	}
	return &bydbqlv1.QueryRequest{
		Query: "SELECT " + key + " AS key, " + key + " AS key_again, SUM(v)::FIELD AS total FROM " + benchGroupName + "." + qlbT16Measure +
			" WHERE variant = ? AND time >= ? AND time < ? GROUP BY " + group + fmt.Sprintf(" LIMIT %d", cardinality+1),
		Params: []*modelv1.TagValue{
			strTagValue(o.spec.dataset()),
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base)}},
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(cardinality) * time.Millisecond))}},
		},
	}
}

func (o *qlbT16Oracle) check(response *measurev1.QueryResponse) error {
	if response == nil || len(response.DataPoints) != o.groups {
		return fmt.Errorf("T16 %s returned wrong group count", o.spec.name)
	}
	for _, point := range response.DataPoints {
		if point.Timestamp != nil || point.Sid != 0 || point.Version != 0 || len(point.TagFamilies) != 0 || len(point.Fields) != 3 ||
			point.Fields[0].Name != "key" || point.Fields[1].Name != "key_again" || point.Fields[2].Name != "total" {
			return fmt.Errorf("T16 %s returned hidden identity or wrong visible columns", o.spec.name)
		}
	}
	if qlbT01Hash(response) != o.hash {
		return fmt.Errorf("T16 %s differs from independent grouped fixture", o.spec.name)
	}
	return nil
}
