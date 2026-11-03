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
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func waitForQLBT15Visibility(ctx context.Context, client bydbqlv1.BydbQLServiceClient, cardinality int, base time.Time) error {
	// Each entity-pruned window returns at most 1024 raw rows. Never use a failing
	// large DISTINCT or GROUP BY request as evidence of incomplete ingestion.
	for _, spec := range qlbT15Specs() {
		if spec.reference || spec.empty {
			continue
		}
		for first := 0; first < cardinality; first += 16384 {
			end := min(first+16384, cardinality)
			for host := 0; host < 16; host++ {
				request, expected := qlbT15RawChunk(first, end, host, cardinality, spec, base)
				if len(expected.DataPoints) == 0 {
					continue
				}
				if err := waitForQLBT07RawChunk(ctx, client, request, expected); err != nil {
					return fmt.Errorf("T15 %s input readiness: %w", spec.dataset(), err)
				}
			}
		}
	}
	return nil
}

func qlbT15RawChunk(first, end, host, cardinality int, spec qlbT15Spec, base time.Time) (*bydbqlv1.QueryRequest, *measurev1.QueryResponse) {
	request := &bydbqlv1.QueryRequest{
		Query: "SELECT rid, a, b, v::FIELD FROM " + benchGroupName + "." + qlbT15Measure +
			" WHERE host = ? AND variant = ? AND time >= ? AND time < ? LIMIT 1025",
		Params: []*modelv1.TagValue{
			strTagValue(fmt.Sprintf("host_%02d", host)), strTagValue(spec.dataset()),
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(first) * time.Millisecond))}},
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(end) * time.Millisecond))}},
		},
	}
	expected := &measurev1.QueryResponse{}
	for row := first + host; row < end; row += 16 {
		a, b := qlbT15Tuple(row%max(1, cardinality*spec.percent/100), spec.wide)
		point := qlbT07GroupedPoint("rid", &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(row)}}},
			[]*measurev1.DataPoint_Field{{Name: "v", Value: intFieldValue(int64(row))}})
		point.TagFamilies[0].Tags = append(point.TagFamilies[0].Tags, &modelv1.Tag{Key: "a", Value: strTagValue(a)}, &modelv1.Tag{Key: "b", Value: strTagValue(b)})
		point.Timestamp = timestamppb.New(base.Add(time.Duration(row) * time.Millisecond))
		expected.DataPoints = append(expected.DataPoints, point)
	}
	return request, expected
}
