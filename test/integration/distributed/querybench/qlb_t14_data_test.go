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
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

const (
	qlbT14MembershipMeasure = "qlb_t14_membership"
	qlbT14MembershipRows    = 1024
)

type qlbT14Spec struct {
	name, mode                 string
	percent, length            int
	hidden, membership, legacy bool
}

func qlbT14Specs(cardinality int) []qlbT14Spec {
	var specs []qlbT14Spec
	for _, mode := range []string{"native", "local"} {
		specs = append(specs, qlbT14Spec{name: mode + "_base", mode: mode, percent: -1})
		for _, percent := range []int{0, 1, 50, 100} {
			specs = append(specs, qlbT14Spec{name: fmt.Sprintf("%s_p%02d", mode, percent), mode: mode, percent: percent})
		}
		specs = append(specs, qlbT14Spec{name: mode + "_hidden_p50", mode: mode, percent: 50, hidden: true})
	}
	if cardinality == 1024 {
		for _, length := range []int{0, 1, 16, 256} {
			for _, spelling := range []string{"contains", "legacy"} {
				specs = append(specs, qlbT14Spec{
					name: fmt.Sprintf("membership_%s_len%d", spelling, length), mode: "local", percent: 100, length: length,
					membership: true, legacy: spelling == "legacy",
				})
			}
		}
	}
	return specs
}

type qlbT14Oracle struct {
	expected                         *measurev1.QueryResponse
	spec                             qlbT14Spec
	threshold                        float64
	inputRows, aggregateRows, groups int
	hash                             uint64
}

// Fixture arithmetic is independent of the planner, aggregate operators and any query response.
func newQLBT14Oracle(spec qlbT14Spec, cardinality int) *qlbT14Oracle {
	oracle := &qlbT14Oracle{spec: spec, expected: &measurev1.QueryResponse{}, inputRows: cardinality, aggregateRows: cardinality, groups: min(16, cardinality)}
	if spec.membership {
		oracle.inputRows = qlbT14MembershipRows / 4
		oracle.aggregateRows, oracle.groups = 0, 0
		if spec.length > 0 {
			oracle.aggregateRows = qlbT14MembershipRows / 8
			oracle.groups = 8
			for host := 0; host < 16; host += 2 {
				oracle.expected.DataPoints = append(oracle.expected.DataPoints, qlbT07GroupedPoint("host", strTagValue(fmt.Sprintf("member_%02d", host)),
					[]*measurev1.DataPoint_Field{{Name: "samples", Value: intFieldValue(16)}}))
			}
		}
	} else {
		averages := make([]float64, oracle.groups)
		for host := range averages {
			var sum float64
			var count int64
			for row := host; row < cardinality; row += 16 {
				if row%7 != 0 {
					sum += float64(row%29-14) / 4
					count++
				}
			}
			averages[host] = sum / float64(count)
		}
		sorted := append([]float64(nil), averages...)
		sort.Float64s(sorted)
		switch spec.percent {
		case -1, 100:
			oracle.threshold = -math.MaxFloat64
		case 0:
			oracle.threshold = math.MaxFloat64
		default:
			wanted := (oracle.groups*spec.percent + 99) / 100
			oracle.threshold = sorted[oracle.groups-wanted]
		}
		for host, avg := range averages {
			if spec.percent >= 0 && avg < oracle.threshold {
				continue
			}
			var fields []*measurev1.DataPoint_Field
			if !spec.hidden {
				fields = []*measurev1.DataPoint_Field{{Name: "mean", Value: qlbT07Float(avg)}, {Name: "mean_again", Value: qlbT07Float(avg)}}
			}
			oracle.expected.DataPoints = append(oracle.expected.DataPoints, qlbT07GroupedPoint("host", strTagValue(fmt.Sprintf("host_%02d", host)), fields))
		}
	}
	oracle.hash = qlbT01Hash(oracle.expected)
	return oracle
}

func (o *qlbT14Oracle) query(cardinality int, base time.Time) *bydbqlv1.QueryRequest {
	params := []*modelv1.TagValue{
		{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base)}},
		{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(cardinality) * time.Millisecond))}},
	}
	projection := "host, AVG(f)::FIELD AS mean, AVG(f)::FIELD AS mean_again"
	if o.spec.hidden {
		projection = "host"
	}
	hosts := make([]string, 16)
	for host := range hosts {
		hosts[host] = fmt.Sprintf("'host_%02d'", host)
	}
	query := "SELECT " + projection + " FROM " + benchGroupName + "." + qlbT07Measure +
		" WHERE time >= ? AND time < ? AND host IN (" + strings.Join(hosts, ",") + ") GROUP BY host"
	if o.spec.percent >= 0 {
		query += " HAVING AVG(f::FIELD) >= CAST(? AS DOUBLE) AND AVG(f::FIELD) >= CAST(? AS DOUBLE)"
		params = append(params, qlbT14ThresholdParam(o.threshold), qlbT14ThresholdParam(o.threshold))
	}
	if o.spec.membership {
		params = params[:2]
		params[1] = &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(qlbT14MembershipRows * time.Millisecond))}}
		membership := "CONTAINS"
		if o.spec.legacy {
			membership = "HAVING"
		}
		query = "SELECT host, COUNT(f)::FIELD AS samples FROM " + benchGroupName + "." + qlbT14MembershipMeasure +
			" WHERE time >= ? AND time < ? AND width = ? AND labels " + membership + " ('needle') AND f::FIELD >= 0" +
			" GROUP BY host HAVING COUNT(f::FIELD) > 0"
		params = append(params, &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(o.spec.length)}}})
	}
	return &bydbqlv1.QueryRequest{Query: query, Params: params}
}

func qlbT14ThresholdParam(value float64) *modelv1.TagValue {
	return strTagValue(strconv.FormatFloat(value, 'g', -1, 64))
}

func (o *qlbT14Oracle) check(response *measurev1.QueryResponse) error {
	if response == nil || len(response.DataPoints) != len(o.expected.DataPoints) {
		return fmt.Errorf("T14 %s returned %d groups, expected %d", o.spec.name, len(response.GetDataPoints()), len(o.expected.DataPoints))
	}
	for _, point := range response.DataPoints {
		fields := 2
		if o.spec.hidden {
			fields = 0
		}
		if o.spec.membership {
			fields = 1
		}
		if point.Timestamp != nil || point.Sid != 0 || point.Version != 0 || len(point.Fields) != fields || len(point.TagFamilies) != 1 ||
			len(point.TagFamilies[0].Tags) != 1 || point.TagFamilies[0].Tags[0].Key != "host" {
			return fmt.Errorf("T14 %s exposed hidden values or raw identity", o.spec.name)
		}
	}
	if qlbT01Hash(response) != o.hash {
		return fmt.Errorf("T14 %s result differs from independent fixture", o.spec.name)
	}
	return nil
}
