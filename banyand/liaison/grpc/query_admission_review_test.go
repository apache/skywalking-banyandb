// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you
// under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package grpc

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

func TestQueryAdmissionBeforeSchemaAccess(t *testing.T) {
	// Empty services intentionally have no schema, pipeline, or metric dependencies.
	// Rejection must happen before accessing any of those dependencies.
	//
	// Cases cover nil requests and overflow windows. MaxUint32 list-all / max-limit
	// sentinels are admitted as scans and are covered separately.
	testCases := []struct {
		call func(context.Context) error
		name string
	}{
		{name: "property nil", call: func(ctx context.Context) error {
			_, queryErr := new(propertyServer).Query(ctx, nil)
			return queryErr
		}},
		{name: "stream nil", call: func(ctx context.Context) error {
			_, queryErr := new(streamService).Query(ctx, nil)
			return queryErr
		}},
		{name: "trace nil", call: func(ctx context.Context) error {
			_, queryErr := new(traceService).Query(ctx, nil)
			return queryErr
		}},
		{name: "stream overflow", call: func(ctx context.Context) error {
			_, queryErr := new(streamService).Query(ctx, &streamv1.QueryRequest{Limit: 1, Offset: math.MaxUint32})
			return queryErr
		}},
		{name: "trace overflow", call: func(ctx context.Context) error {
			_, queryErr := new(traceService).Query(ctx, &tracev1.QueryRequest{Limit: 1, Offset: math.MaxUint32})
			return queryErr
		}},
		{name: "stream above absolute window", call: func(ctx context.Context) error {
			_, queryErr := new(streamService).Query(ctx, &streamv1.QueryRequest{Limit: 100001})
			return queryErr
		}},
		{name: "trace above absolute window", call: func(ctx context.Context) error {
			_, queryErr := new(traceService).Query(ctx, &tracev1.QueryRequest{Limit: 100001})
			return queryErr
		}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			queryErr := testCase.call(context.Background())
			if status.Code(queryErr) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument, got %v", queryErr)
			}
		})
	}
}

func TestQueryAdmissionAllowsUnboundedLimitSentinel(t *testing.T) {
	for _, limit := range []uint32{math.MaxUint32, math.MaxInt32} {
		budget := protector.NewQueryBudget(nil)
		ctx, release, err := admitQuery(context.Background(), budget, limit, 0, 20)
		if err != nil {
			t.Fatalf("list-all sentinel %d must be admitted as a scan: %v", limit, err)
		}
		release()
		if _, ok := query.BudgetLeaseFromContext(ctx); !ok {
			t.Fatal("expected admitted scan lease")
		}
		if budget.Reserved() != 0 {
			t.Fatalf("release leaked %d reserved bytes", budget.Reserved())
		}
	}
}

func TestQueryAdmissionContextStatus(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	expiredCtx, expireCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expireCancel()
	testCases := []struct {
		ctx  context.Context
		name string
		code codes.Code
	}{
		{ctx: canceledCtx, name: "canceled", code: codes.Canceled},
		{ctx: expiredCtx, name: "expired", code: codes.DeadlineExceeded},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			budget := protector.NewQueryBudget(nil)
			service := &propertyServer{queryBudget: budget}
			_, queryErr := service.Query(testCase.ctx, &propertyv1.QueryRequest{Limit: 1})
			if status.Code(queryErr) != testCase.code {
				t.Fatalf("expected %s, got %v", testCase.code, queryErr)
			}
			if budget.Reserved() != 0 {
				t.Fatalf("canceled admission leaked %d reserved bytes", budget.Reserved())
			}
		})
	}
}

func TestQueryAdmissionTransportScopeClosesAtRPCEnd(t *testing.T) {
	budget := protector.NewQueryBudget(nil)
	handler := queryAdmissionStatsHandler{}
	ctx := handler.TagRPC(context.Background(), &stats.RPCTagInfo{FullMethodName: "/query"})
	_, release, admissionErr := admitQuery(ctx, budget, 1, 0, 20)
	if admissionErr != nil {
		t.Fatal(admissionErr)
	}
	release()
	if budget.Reserved() == 0 {
		t.Fatal("query reservation released before RPC serialization completed")
	}
	handler.HandleRPC(ctx, &stats.End{})
	if budget.Reserved() != 0 {
		t.Fatalf("query reservation remained after RPC end: %d", budget.Reserved())
	}
}

func TestPropertyMergeChargesPayloadBeforeDeduplication(t *testing.T) {
	budget := protector.NewQueryBudget(nil)
	ctx, release, admissionErr := budget.AdmitContext(context.Background(), 1, 0, 100)
	if admissionErr != nil {
		t.Fatal(admissionErr)
	}
	defer release()
	lease, _ := query.BudgetLeaseFromContext(ctx)
	if chargeErr := lease.Charge(lease.Limit() - lease.Used() - 1024); chargeErr != nil {
		t.Fatal(chargeErr)
	}
	properties := map[string][]*propertyWithMetadata{
		"node": {{Property: &propertyv1.Property{Id: strings.Repeat("x", 2048)}}},
	}
	mergeErr := chargePropertyMerge(ctx, properties)
	if status.Code(queryStatus(mergeErr)) != codes.ResourceExhausted {
		t.Fatalf("expected rejection before allocating entity and dedup maps, got %v", mergeErr)
	}
}

func TestQueryResponseEncodingBudget(t *testing.T) {
	budget := protector.NewQueryBudget(nil)
	ctx, release, admissionErr := budget.AdmitContext(context.Background(), 1, 0, 100)
	if admissionErr != nil {
		t.Fatal(admissionErr)
	}
	defer release()
	lease, _ := query.BudgetLeaseFromContext(ctx)
	if chargeErr := lease.Charge(lease.Limit() - lease.Used() - 1024); chargeErr != nil {
		t.Fatal(chargeErr)
	}
	response := &propertyv1.QueryResponse{Properties: []*propertyv1.Property{{Id: strings.Repeat("x", 1024)}}}
	encodingErr := query.ChargeResponse(ctx, response)
	if status.Code(queryStatus(encodingErr)) != codes.ResourceExhausted {
		t.Fatalf("expected encoding budget rejection, got %v", encodingErr)
	}
}
