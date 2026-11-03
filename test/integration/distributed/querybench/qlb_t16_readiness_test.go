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

func waitForQLBT16Visibility(ctx context.Context, client bydbqlv1.BydbQLServiceClient, cardinality int, base time.Time) error {
	// Each entity-pruned window returns at most 1024 raw rows. Never use a failing
	// full GROUP BY request as evidence of incomplete ingestion.
	for _, spec := range qlbT16Specs() {
		if spec.computed {
			continue
		}
		for first := 0; first < cardinality; first += 16384 {
			end := min(first+16384, cardinality)
			for host := 0; host < 16; host++ {
				request, expected := qlbT16RawChunk(first, end, host, cardinality, spec, base)
				if len(expected.DataPoints) == 0 {
					continue
				}
				if err := waitForQLBT07RawChunk(ctx, client, request, expected); err != nil {
					return fmt.Errorf("T16 %s input readiness: %w", spec.dataset(), err)
				}
			}
		}
	}
	return nil
}

func qlbT16RawChunk(first, end, host, cardinality int, spec qlbT16Spec, base time.Time) (*bydbqlv1.QueryRequest, *measurev1.QueryResponse) {
	request := &bydbqlv1.QueryRequest{
		Query: "SELECT rid, precomputed::FIELD, raw::FIELD, v::FIELD FROM " + benchGroupName + "." + qlbT16Measure +
			" WHERE host = ? AND variant = ? AND time >= ? AND time < ? LIMIT 1025",
		Params: []*modelv1.TagValue{
			strTagValue(fmt.Sprintf("host_%02d", host)), strTagValue(spec.dataset()),
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(first) * time.Millisecond))}},
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(end) * time.Millisecond))}},
		},
	}
	expected := &measurev1.QueryResponse{}
	for row := first + host; row < end; row += 16 {
		key := int64(row % spec.groupCount(cardinality))
		value := &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
		if row%5 != 0 {
			value = intFieldValue(int64(row%97 - 48))
		}
		point := qlbT07GroupedPoint("rid", &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(row)}}},
			[]*measurev1.DataPoint_Field{
				{Name: "precomputed", Value: intFieldValue(key)}, {Name: "raw", Value: intFieldValue(key - 1)}, {Name: "v", Value: value},
			})
		point.Timestamp = timestamppb.New(base.Add(time.Duration(row) * time.Millisecond))
		expected.DataPoints = append(expected.DataPoints, point)
	}
	return request, expected
}
