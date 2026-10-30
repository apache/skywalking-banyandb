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

type qlbT15Spec struct {
	name                   string
	percent                int
	wide, reference, empty bool
}

func (s qlbT15Spec) dataset() string { return fmt.Sprintf("p%d_w%t", s.percent, s.wide) }
func qlbT15Specs() []qlbT15Spec {
	var out []qlbT15Spec
	for _, percent := range []int{1, 50, 100} {
		for _, wide := range []bool{false, true} {
			for _, reference := range []bool{false, true} {
				algorithm, width := "distinct", "narrow"
				if reference {
					algorithm = "group_by"
				}
				if wide {
					width = "wide"
				}
				out = append(out, qlbT15Spec{name: fmt.Sprintf("%s_p%03d_%s", algorithm, percent, width), percent: percent, wide: wide, reference: reference})
			}
		}
	}
	for _, reference := range []bool{false, true} {
		name := "distinct_empty"
		if reference {
			name = "group_by_empty"
		}
		out = append(out, qlbT15Spec{name: name, percent: 1, empty: true, reference: reference})
	}
	return out
}

// These structural pairs deliberately collide under plain concatenation.
func qlbT15Tuple(unique int, wide bool) (string, string) {
	a, b := "a", "bc"
	if unique%2 == 1 {
		a, b = "ab", "c"
	}
	suffix := fmt.Sprintf("_%08d", unique/2)
	if wide {
		suffix += strings.Repeat("w", 256)
	}
	return a, b + suffix
}

type qlbT15Oracle struct {
	expected          *measurev1.QueryResponse
	spec              qlbT15Spec
	inputRows, groups int
	hash              uint64
}

func newQLBT15Oracle(spec qlbT15Spec, cardinality int) *qlbT15Oracle {
	o := &qlbT15Oracle{spec: spec, inputRows: cardinality, groups: max(1, cardinality*spec.percent/100), expected: &measurev1.QueryResponse{}}
	if spec.empty {
		o.inputRows, o.groups = 0, 0
	}
	// Enumerate independently specified unique fixture pairs, never query execution output.
	for unique := 0; unique < o.groups; unique++ {
		a, b := qlbT15Tuple(unique, spec.wide)
		point := qlbT07GroupedPoint("a", strTagValue(a), nil)
		point.TagFamilies[0].Tags = append(point.TagFamilies[0].Tags, &modelv1.Tag{Key: "b", Value: strTagValue(b)})
		o.expected.DataPoints = append(o.expected.DataPoints, point)
	}
	o.hash = qlbT01Hash(o.expected)
	return o
}

func (o *qlbT15Oracle) query(cardinality int, base time.Time) *bydbqlv1.QueryRequest {
	projection, group := "DISTINCT a, b", ""
	if o.spec.reference {
		projection, group = "a, b", " GROUP BY a, b"
	}
	dataset := o.spec.dataset()
	if o.spec.empty {
		dataset = "absent"
	}
	return &bydbqlv1.QueryRequest{
		Query: "SELECT " + projection + " FROM " + benchGroupName + "." + qlbT15Measure +
			" WHERE variant = ? AND time >= ? AND time < ?" + group + fmt.Sprintf(" LIMIT %d", cardinality+1),
		Params: []*modelv1.TagValue{
			strTagValue(dataset),
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base)}},
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(cardinality) * time.Millisecond))}},
		},
	}
}

func (o *qlbT15Oracle) check(response *measurev1.QueryResponse) error {
	if response == nil || len(response.DataPoints) != o.groups {
		return fmt.Errorf("T15 %s tuple count differs", o.spec.name)
	}
	for _, point := range response.DataPoints {
		if point.Timestamp != nil || point.Sid != 0 || point.Version != 0 || len(point.Fields) != 0 || len(point.TagFamilies) != 1 || len(point.TagFamilies[0].Tags) != 2 ||
			point.TagFamilies[0].Tags[0].Key != "a" || point.TagFamilies[0].Tags[1].Key != "b" {
			return fmt.Errorf("T15 exposed hidden input identity")
		}
	}
	if qlbT01Hash(response) != o.hash {
		return fmt.Errorf("T15 %s differs from independent tuples", o.spec.name)
	}
	return nil
}
