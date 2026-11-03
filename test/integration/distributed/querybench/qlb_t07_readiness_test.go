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
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func waitForQLBT07RawVisibility(ctx context.Context, client bydbqlv1.BydbQLServiceClient, cardinality, chunkSize int, base time.Time) error {
	if cardinality <= 0 || chunkSize <= 0 {
		return fmt.Errorf("T07 readiness requires positive input and chunk cardinalities")
	}
	// A single entity avoids retaining decoder buffers for all sixteen hosts.
	// Wider time windows keep the number of setup queries proportional to rows/chunkSize.
	window := chunkSize * 16
	for first := 0; first < cardinality; first += window {
		end := min(first+window, cardinality)
		for host := 0; host < 16; host++ {
			request, expected := qlbT07RawVisibilityChunk(first, end, host, base)
			if len(expected.DataPoints) == 0 {
				continue
			}
			if err := waitForQLBT07RawChunk(ctx, client, request, expected); err != nil {
				return err
			}
		}
	}
	return nil
}

func qlbT07RawVisibilityChunk(first, end, host int, base time.Time) (*bydbqlv1.QueryRequest, *measurev1.QueryResponse) {
	request := &bydbqlv1.QueryRequest{
		Query: "SELECT rid, v::FIELD FROM " + benchGroupName + "." + qlbT07Measure + " WHERE host = ? AND time >= ? AND time < ?",
		Params: []*modelv1.TagValue{
			strTagValue(fmt.Sprintf("host_%02d", host)),
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(first) * time.Millisecond))}},
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(end) * time.Millisecond))}},
		},
	}
	expected := &measurev1.QueryResponse{}
	for row := first + host; row < end; row += 16 {
		point := qlbT07GroupedPoint("rid", &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(row)}}}, nil)
		point.Timestamp = timestamppb.New(base.Add(time.Duration(row) * time.Millisecond))
		value := &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
		if row%5 != 0 {
			value = intFieldValue(int64(row%97 - 48))
		}
		point.Fields = []*measurev1.DataPoint_Field{{Name: "v", Value: value}}
		expected.DataPoints = append(expected.DataPoints, point)
	}
	return request, expected
}

func waitForQLBT07RawChunk(ctx context.Context, client bydbqlv1.BydbQLServiceClient,
	request *bydbqlv1.QueryRequest, expected *measurev1.QueryResponse,
) error {
	expectedHash := qlbT01Hash(expected)
	deadline := time.Now().Add(2 * time.Minute)
	for {
		queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		response, queryErr := client.Query(queryCtx, request)
		cancel()
		if queryErr == nil && len(response.GetMeasureResult().GetDataPoints()) == len(expected.DataPoints) && qlbT01Hash(response.GetMeasureResult()) == expectedHash {
			return nil
		}
		switch status.Code(queryErr) {
		case codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated, codes.Unimplemented, codes.ResourceExhausted:
			return fmt.Errorf("T07 host-pruned visibility query cannot execute: %w", queryErr)
		default:
		}
		if queryErr == nil {
			queryErr = fmt.Errorf("expected rows=%d hash=%x, received rows=%d hash=%x; %s", len(expected.DataPoints), expectedHash,
				len(response.GetMeasureResult().GetDataPoints()), qlbT01Hash(response.GetMeasureResult()), qlbReadinessDifference(expected, response.GetMeasureResult()))
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("T07 host-pruned fixture did not become visible: %w", queryErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func qlbReadinessDifference(expected, actual *measurev1.QueryResponse) string {
	normalize := func(response *measurev1.QueryResponse) []*measurev1.DataPoint {
		if response == nil {
			return nil
		}
		points := proto.Clone(response).(*measurev1.QueryResponse).GetDataPoints()
		for _, point := range points {
			point.Sid, point.Version = 0, 0
		}
		slices.SortFunc(points, func(a, b *measurev1.DataPoint) int { return a.Timestamp.AsTime().Compare(b.Timestamp.AsTime()) })
		return points
	}
	want, got := normalize(expected), normalize(actual)
	for i := 0; i < min(len(want), len(got)); i++ {
		if !proto.Equal(want[i], got[i]) {
			return fmt.Sprintf("first differing tuple=%d expected={%s} actual={%s}", i, want[i], got[i])
		}
	}
	if len(want) > len(got) {
		return fmt.Sprintf("missing tuple=%d expected={%s}", len(got), want[len(got)])
	}
	if len(got) > len(want) {
		return fmt.Sprintf("unexpected tuple=%d actual={%s}", len(want), got[len(want)])
	}
	return "no differing tuple"
}
