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

func waitForQLBT17Visibility(ctx context.Context, client bydbqlv1.BydbQLServiceClient, rows int, base time.Time) error {
	// Readiness checks every input tuple in <=1024-row entity/time windows;
	// successful paginated sorts cannot certify complete fixture ingestion.
	for first := 0; first < rows; first += 16384 {
		for host := 0; host < 16; host++ {
			request, expected := qlbT17RawChunk(first, min(first+16384, rows), host, base)
			if len(expected.DataPoints) == 0 {
				continue
			}
			if err := waitForQLBT07RawChunk(ctx, client, request, expected); err != nil {
				return fmt.Errorf("T17 raw input readiness: %w", err)
			}
		}
	}
	return nil
}

func qlbT17RawChunk(first, end, host int, base time.Time) (*bydbqlv1.QueryRequest, *measurev1.QueryResponse) {
	request := &bydbqlv1.QueryRequest{
		Query: "SELECT rid::FIELD, key0::FIELD, key1::FIELD, v::FIELD FROM " + benchGroupName + "." + qlbT17Measure +
			" WHERE host = ? AND time >= ? AND time < ? LIMIT 1025",
		Params: []*modelv1.TagValue{
			strTagValue(fmt.Sprintf("host_%02d", host)),
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(first) * time.Millisecond))}},
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(end) * time.Millisecond))}},
		},
	}
	expected := &measurev1.QueryResponse{}
	for row := first + host; row < end; row += 16 {
		firstKey, secondKey, firstValid, secondValid := qlbT17Keys(row)
		key0 := &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
		key1 := &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
		value := &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
		if firstValid {
			key0 = intFieldValue(firstKey)
		}
		if secondValid {
			key1 = intFieldValue(secondKey)
		}
		if row%5 != 0 {
			value = intFieldValue(int64(row%97 - 48))
		}
		point := &measurev1.DataPoint{
			Timestamp: timestamppb.New(base.Add(time.Duration(row) * time.Millisecond)),
			Fields: []*measurev1.DataPoint_Field{
				{Name: "rid", Value: intFieldValue(int64(row))}, {Name: "key0", Value: key0}, {Name: "key1", Value: key1}, {Name: "v", Value: value},
			},
		}
		expected.DataPoints = append(expected.DataPoints, point)
	}
	return request, expected
}
