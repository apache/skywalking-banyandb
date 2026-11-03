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
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

type qlbT07ReadinessClient func(context.Context, *bydbqlv1.QueryRequest, ...grpc.CallOption) (*bydbqlv1.QueryResponse, error)

func (query qlbT07ReadinessClient) Query(ctx context.Context, request *bydbqlv1.QueryRequest, options ...grpc.CallOption) (*bydbqlv1.QueryResponse, error) {
	return query(ctx, request, options...)
}

func TestQLBT07ReadinessPrunesHostsAndVisitsEveryRID(t *testing.T) {
	base := time.Unix(1700000000, 0)
	for _, cardinality := range []int{1, 15, 16, 17, 1024, 16384, 16385, 100000} {
		t.Run(strconv.Itoa(cardinality), func(t *testing.T) {
			seen := make([]bool, cardinality)
			calls := 0
			client := qlbT07ReadinessClient(func(_ context.Context, request *bydbqlv1.QueryRequest, _ ...grpc.CallOption) (*bydbqlv1.QueryResponse, error) {
				if len(request.Params) != 3 || !strings.Contains(request.Query, "host = ?") {
					return nil, status.Error(codes.InvalidArgument, "readiness must prune one host before its bounded time window")
				}
				statement, err := bydbql.Prepare(request.Query)
				if err != nil {
					return nil, err
				}
				if _, err = statement.Bind(request.Params); err != nil {
					return nil, err
				}
				host, err := strconv.Atoi(strings.TrimPrefix(request.Params[0].GetStr().GetValue(), "host_"))
				if err != nil || host < 0 || host >= 16 {
					return nil, fmt.Errorf("invalid fixture host %v", request.Params[0])
				}
				first := int(request.Params[1].GetTimestamp().AsTime().Sub(base) / time.Millisecond)
				end := int(request.Params[2].GetTimestamp().AsTime().Sub(base) / time.Millisecond)
				if first < 0 || first >= end || end > cardinality || end-first > 16384 {
					return nil, fmt.Errorf("invalid bounded readiness window [%d,%d)", first, end)
				}
				expected := &measurev1.QueryResponse{}
				// Enumerate the independent fixture, rather than repeating the chunk builder's stepping algorithm.
				for rid := 0; rid < cardinality; rid++ {
					if rid < first || rid >= end || rid%16 != host {
						continue
					}
					if seen[rid] {
						return nil, fmt.Errorf("RID %d appeared in more than one readiness query", rid)
					}
					seen[rid] = true
					value := &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
					if rid%5 != 0 {
						value = intFieldValue(int64(rid%97 - 48))
					}
					expected.DataPoints = append(expected.DataPoints, &measurev1.DataPoint{
						Timestamp: timestamppb.New(base.Add(time.Duration(rid) * time.Millisecond)),
						TagFamilies: []*modelv1.TagFamily{{Name: benchTagFamily, Tags: []*modelv1.Tag{
							{Key: "rid", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(rid)}}}},
						}}},
						Fields: []*measurev1.DataPoint_Field{{Name: "v", Value: value}},
					})
				}
				if len(expected.DataPoints) > 1024 {
					return nil, fmt.Errorf("readiness returned %d rows, exceeding the per-host bound", len(expected.DataPoints))
				}
				calls++
				return &bydbqlv1.QueryResponse{Result: &bydbqlv1.QueryResponse_MeasureResult{MeasureResult: expected}}, nil
			})
			if err := waitForQLBT07RawVisibility(t.Context(), client, cardinality, 1024, base); err != nil {
				t.Fatal(err)
			}
			for rid, visited := range seen {
				if !visited {
					t.Fatalf("RID %d was never checked", rid)
				}
			}
			if calls > (cardinality+1023)/1024+15 {
				t.Fatalf("readiness used %d queries instead of bounded host windows", calls)
			}
		})
	}
}

func TestQLBT07ReadinessStopsOnResourceRejection(t *testing.T) {
	calls := 0
	client := qlbT07ReadinessClient(func(context.Context, *bydbqlv1.QueryRequest, ...grpc.CallOption) (*bydbqlv1.QueryResponse, error) {
		calls++
		return nil, status.Error(codes.ResourceExhausted, "bounded readiness query exceeds native working memory")
	})
	err := waitForQLBT07RawVisibility(t.Context(), client, 100000, 1024, time.Unix(1700000000, 0))
	if status.Code(err) != codes.ResourceExhausted || calls != 1 {
		t.Fatalf("resource rejection must stop readiness immediately, calls=%d error=%v", calls, err)
	}
}
