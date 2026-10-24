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
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

const qlbT12Measure = "qlb_t12"

type qlbT12Spec struct {
	percent         int
	aggregate, wide bool
}

func qlbT12Variants() []string {
	names := make([]string, 0, 16)
	for _, percent := range []int{0, 1, 50, 100} {
		for _, mode := range []string{"raw", "aggregate"} {
			for _, width := range []string{"narrow", "wide"} {
				names = append(names, fmt.Sprintf("%s_%s_p%02d", mode, width, percent))
			}
		}
	}
	return names
}

func parseQLBT12Spec(name string) (qlbT12Spec, error) {
	parts := strings.Split(name, "_")
	if len(parts) != 3 || (parts[0] != "raw" && parts[0] != "aggregate") || (parts[1] != "narrow" && parts[1] != "wide") {
		return qlbT12Spec{}, fmt.Errorf("invalid T12 variant %q", name)
	}
	percent, err := strconv.Atoi(strings.TrimPrefix(parts[2], "p"))
	if err != nil || (percent != 0 && percent != 1 && percent != 50 && percent != 100) {
		return qlbT12Spec{}, fmt.Errorf("invalid T12 selectivity %q", name)
	}
	return qlbT12Spec{percent: percent, aggregate: parts[0] == "aggregate", wide: parts[1] == "wide"}, nil
}

func qlbT12Query(name string, cardinality int, base time.Time) (*bydbqlv1.QueryRequest, error) {
	spec, err := parseQLBT12Spec(name)
	if err != nil {
		return nil, err
	}
	projection := "v::FIELD AS result"
	if spec.aggregate {
		projection = "SUM(v)::FIELD AS sum_value, COUNT(v)::FIELD AS count_value"
	}
	predicate := "score::FIELD < ?"
	if spec.wide {
		predicate += " AND COALESCE(f::FIELD, 0) > -1000"
	}
	return &bydbqlv1.QueryRequest{
		Query: "SELECT " + projection + " FROM " + benchGroupName + "." + qlbT12Measure + " WHERE time >= ? AND time < ? AND " + predicate,
		Params: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base)}},
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(cardinality) * time.Millisecond))}},
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(spec.percent)}}},
		},
	}, nil
}

type qlbT12Oracle struct {
	spec                  qlbT12Spec
	cardinality, selected int
	sum, count            int64
	hash                  uint64
}

func newQLBT12Oracle(name string, cardinality int) (*qlbT12Oracle, error) {
	spec, err := parseQLBT12Spec(name)
	if err != nil {
		return nil, err
	}
	oracle := &qlbT12Oracle{spec: spec, cardinality: cardinality, hash: 0xcbf29ce484222325}
	for row := 0; row < cardinality; row++ {
		if row%100 >= spec.percent {
			continue
		}
		oracle.selected++
		// The independent fixture rule is v=row%97-48, with every fifth v null.
		value := &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
		if row%5 != 0 {
			integer := int64(row%97 - 48)
			oracle.sum += integer
			oracle.count++
			value = intFieldValue(integer)
		}
		if !spec.aggregate {
			oracle.hash += qlbT10RowHash(uint64(row), value)
		}
	}
	if spec.aggregate {
		oracle.hash += qlbT10RowHash(0, oracle.sumValue()) + qlbT10RowHash(1, intFieldValue(oracle.count))
	}
	return oracle, nil
}

func (o *qlbT12Oracle) outputRows() int {
	if o.spec.aggregate {
		return 1
	}
	return o.selected
}

func (o *qlbT12Oracle) sumValue() *modelv1.FieldValue {
	if o.count == 0 {
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
	}
	return intFieldValue(o.sum)
}

func (o *qlbT12Oracle) check(response *measurev1.QueryResponse, base time.Time) (uint64, error) {
	if len(response.GetDataPoints()) != o.outputRows() {
		return 0, fmt.Errorf("T12 returned %d rows, expected %d", len(response.GetDataPoints()), o.outputRows())
	}
	if o.spec.aggregate {
		point := response.DataPoints[0]
		if len(point.Fields) != 2 || len(point.TagFamilies) != 0 || point.Fields[0].Name != "sum_value" || point.Fields[1].Name != "count_value" {
			return 0, fmt.Errorf("T12 aggregate exposes unexpected columns")
		}
		if err := checkQLBT10Value(point.Fields[0].Value, o.sumValue()); err != nil {
			return 0, err
		}
		if err := checkQLBT10Value(point.Fields[1].Value, intFieldValue(o.count)); err != nil {
			return 0, err
		}
		return uint64(0xcbf29ce484222325) + qlbT10RowHash(0, point.Fields[0].Value) + qlbT10RowHash(1, point.Fields[1].Value), nil
	}
	seen := make([]bool, o.cardinality)
	hash := uint64(0xcbf29ce484222325)
	for _, point := range response.DataPoints {
		nanos := point.GetTimestamp().AsTime().UnixNano() - base.UnixNano()
		row := nanos / int64(time.Millisecond)
		if nanos < 0 || nanos%int64(time.Millisecond) != 0 || row >= int64(o.cardinality) || seen[row] || int(row)%100 >= o.spec.percent {
			return 0, fmt.Errorf("T12 raw returned duplicate or unselected row")
		}
		seen[row] = true
		if len(point.Fields) != 1 || point.Fields[0].Name != "result" || len(point.TagFamilies) != 0 {
			return 0, fmt.Errorf("T12 hidden predicate inputs leaked")
		}
		want, err := qlbT10ExpectedValue("raw_control", int(row))
		if err != nil {
			return 0, err
		}
		if err = checkQLBT10Value(point.Fields[0].Value, want); err != nil {
			return 0, err
		}
		hash += qlbT10RowHash(uint64(row), point.Fields[0].Value)
	}
	return hash, nil
}
