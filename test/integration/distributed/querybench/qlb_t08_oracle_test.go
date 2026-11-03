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

	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

type qlbT08Sample struct {
	ql     *measurev1.QueryResponse
	native []*measurev1.QueryResponse
}

func (s qlbT08Sample) rows() int {
	if s.ql != nil {
		return len(s.ql.DataPoints)
	}
	rows := 0
	for _, response := range s.native {
		rows += len(response.GetDataPoints())
	}
	return rows
}

type qlbT08Oracle struct {
	name     string
	expected *measurev1.QueryResponse
	native   []*measurev1.QueryResponse
}

// The oracle uses scalar arithmetic directly from the fixture definition and never queries another execution mode.
func newQLBT08Oracle(name string, cardinality int) (qlbT08Oracle, error) {
	shape, err := qlbT08FindShape(name)
	if err != nil {
		return qlbT08Oracle{}, err
	}
	oracle := qlbT08Oracle{name: name, expected: &measurev1.QueryResponse{}}
	for branch := 0; branch < shape.branches; branch++ {
		oracle.native = append(oracle.native, &measurev1.QueryResponse{})
	}
	for host := 0; host < min(16, cardinality); host++ {
		var integer qlbT07ScalarState
		var floatSum float64
		var floatCount int64
		for row := host; row < cardinality; row += 16 {
			if row%5 != 0 {
				integer.add(int64(row%97 - 48))
			}
			if row%7 != 0 {
				floatSum += float64(row%29-14) / 4
				floatCount++
			}
		}
		key := strTagValue(fmt.Sprintf("host_%02d", host))
		var fields []*measurev1.DataPoint_Field
		var nativeValues []*modelv1.FieldValue
		switch name {
		case "float_avg", "float_shared":
			avg := qlbT07Float(floatSum / float64(floatCount))
			fields = []*measurev1.DataPoint_Field{{Name: "avg_value", Value: avg}}
			if name == "float_shared" {
				fields = []*measurev1.DataPoint_Field{
					{Name: "sum_value", Value: qlbT07Float(floatSum)},
					{Name: "count_value", Value: intFieldValue(floatCount)},
					{Name: "avg_value", Value: avg},
					{Name: "avg_again", Value: qlbT07Float(floatSum / float64(floatCount))},
				}
			}
			nativeValues = []*modelv1.FieldValue{qlbT07Float(floatSum), qlbT07Float(float64(floatCount))}
		case "int_extrema":
			fields = []*measurev1.DataPoint_Field{
				{Name: "min_value", Value: intFieldValue(integer.min)},
				{Name: "max_value", Value: intFieldValue(integer.max)},
				{Name: "count_value", Value: intFieldValue(integer.count)},
			}
			nativeValues = []*modelv1.FieldValue{intFieldValue(integer.min), intFieldValue(integer.max), intFieldValue(integer.count)}
		case "int_avg_fallback":
			fields = []*measurev1.DataPoint_Field{{Name: "avg_value", Value: qlbT07Float(float64(integer.sum) / float64(integer.count))}}
		}
		oracle.expected.DataPoints = append(oracle.expected.DataPoints, qlbT07GroupedPoint("host", key, fields))
		fieldName := "f"
		if name == "int_extrema" {
			fieldName = "v"
		}
		for branch, value := range nativeValues {
			oracle.native[branch].DataPoints = append(oracle.native[branch].DataPoints, qlbT07GroupedPoint("host", key,
				[]*measurev1.DataPoint_Field{{Name: fieldName, Value: value}}))
		}
	}
	return oracle, nil
}

func (o qlbT08Oracle) check(sample qlbT08Sample) (uint64, error) {
	if sample.ql != nil {
		got, want := qlbT01Hash(sample.ql), qlbT01Hash(o.expected)
		if got != want {
			return 0, fmt.Errorf("T08 %s SQL result differs from independent fixture: got=%x want=%x", o.name, got, want)
		}
		return want, nil
	}
	if len(sample.native) != len(o.native) {
		return 0, fmt.Errorf("T08 %s expected %d individual branches, got %d", o.name, len(o.native), len(sample.native))
	}
	var checksum uint64 = 14695981039346656037
	for index, response := range sample.native {
		got, want := qlbT01Hash(response), qlbT01Hash(o.native[index])
		if got != want {
			return 0, fmt.Errorf("T08 %s native branch %d differs from independent fixture: got=%x want=%x", o.name, index, got, want)
		}
		checksum = (checksum ^ want) * 1099511628211
	}
	if len(sample.native) == 0 {
		return 0, fmt.Errorf("T08 individual control requires native branches")
	}
	// Finalize each independent SUM/COUNT pair by host, never by response position.
	if o.name == "float_avg" || o.name == "float_shared" {
		sums := make(map[string]float64, 16)
		for _, point := range sample.native[0].DataPoints {
			sums[point.TagFamilies[0].Tags[0].Value.GetStr().Value] = point.Fields[0].Value.GetFloat().Value
		}
		finalized := proto.Clone(o.expected).(*measurev1.QueryResponse)
		counts := make(map[string]float64, 16)
		for _, point := range sample.native[1].DataPoints {
			counts[point.TagFamilies[0].Tags[0].Value.GetStr().Value] = point.Fields[0].Value.GetFloat().Value
		}
		for _, point := range finalized.DataPoints {
			host := point.TagFamilies[0].Tags[0].Value.GetStr().Value
			if o.name == "float_avg" {
				point.Fields[0].Value = qlbT07Float(sums[host] / counts[host])
			} else {
				point.Fields[0].Value = qlbT07Float(sums[host])
				point.Fields[1].Value = intFieldValue(int64(counts[host]))
				point.Fields[2].Value = qlbT07Float(sums[host] / counts[host])
				point.Fields[3].Value = qlbT07Float(sums[host] / counts[host])
			}
		}
		if _, err := o.check(qlbT08Sample{ql: finalized}); err != nil {
			return 0, err
		}
	}
	return checksum, nil
}
