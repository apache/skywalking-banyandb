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

package measure_test

import (
	"context"
	"sync/atomic"
	"time"

	gm "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	ql "github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func verifyGeneratedNativeAggregateLifecycle(base time.Time, scenario qlgen.NativeAggregateScenario) {
	c := scenario.Cases[0]
	if scenario.Kind == "authorization" {
		verifyGeneratedNativeAggregateAuthorization(base, c, scenario.Branches)
		return
	}
	for _, branch := range []int{0, 1} {
		if scenario.Kind == "group-cap" && branch > 0 {
			break
		}
		func() {
			var calls atomic.Uint64
			hooks := liaisongrpc.NativeAggregateTestHooks{}
			wantCode := codes.ResourceExhausted
			if scenario.Kind == "group-cap" {
				hooks.MaxGroups = 1
			} else {
				wantCode = codes.Unavailable
				if scenario.Kind == "cancellation" {
					wantCode = codes.Canceled
				}
				if scenario.Kind == "child-revision" {
					wantCode = codes.FailedPrecondition
				}
				hooks.BeforeBranch = func(_ context.Context, index int, request *measurev1.QueryRequest) error {
					calls.Add(1)
					if index != branch {
						return nil
					}
					if wantCode == codes.Canceled {
						return context.Canceled
					}
					if wantCode == codes.FailedPrecondition {
						request.GroupModRevisions = map[string]int64{qlgen.FixtureGroup: -1}
						return nil
					}
					return status.Error(wantCode, "generated T08 branch failure")
				}
			}
			restore := liaisongrpc.SetNativeAggregateHooksForTest(hooks)
			defer restore()
			verifyGeneratedNativeAggregateFailure(base, c, wantCode)
			if scenario.Kind != "group-cap" {
				gm.Expect(calls.Load()).To(gm.Equal(uint64(branch+1)), "later native branches must not execute after failure")
			}
		}()
		verifyGeneratedNativeAggregateCall(base, c, scenario.Branches, false)
	}
}

func verifyGeneratedNativeAggregateAuthorization(base time.Time, c qlgen.Case, branches uint64) {
	verifyGeneratedNativeAggregateCall(base, c, branches, false)
	var calls atomic.Uint64
	func() {
		restoreNative := liaisongrpc.SetNativeAggregateHooksForTest(liaisongrpc.NativeAggregateTestHooks{
			BeforeBranch: func(context.Context, int, *measurev1.QueryRequest) error {
				calls.Add(1)
				return nil
			},
		})
		defer restoreNative()
		restoreAuthorization := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{
			Context: generatedReuseAuthorization("revoked_scope"),
		})
		defer restoreAuthorization()
		before := vmeasure.BatchExecutionStats()
		verifyGeneratedNativeAggregateFailure(base, c, codes.PermissionDenied)
		gm.Expect(vmeasure.BatchExecutionStats().NativeLeafCalls).To(gm.Equal(before.NativeLeafCalls))
	}()
	gm.Expect(calls.Load()).To(gm.BeZero(), "authorization must reject a cached template before opening any native branch")
	verifyGeneratedNativeAggregateCall(base, c, branches, false)
}

func verifyGeneratedNativeAggregateFailure(base time.Time, c qlgen.Case, wantCode codes.Code) {
	before := vmeasure.BatchExecutionStats()
	memoryBefore := vectorized.QueryMemoryStats()
	localBefore := ql.AggregateExecutionStats()
	nativeBefore := ql.NativeAggregateExecutionStats()
	defer func() {
		after := vmeasure.BatchExecutionStats()
		gm.Expect(after.IntermediateDataPoints).To(gm.Equal(before.IntermediateDataPoints))
		gm.Expect(after.LiveResults).To(gm.Equal(before.LiveResults))
		expectLiveReservedBytes(memoryBefore.LiveReservedBytes)
		gm.Expect(ql.AggregateExecutionStats().LiveBytes).To(gm.Equal(localBefore.LiveBytes))
		gm.Expect(ql.NativeAggregateExecutionStats().LiveBytes).To(gm.Equal(nativeBefore.LiveBytes))
	}()
	query, _ := renderGeneratedQLCase(base, c)
	text, params, extractErr := helpers.ExtractQL(query)
	gm.Expect(extractErr).NotTo(gm.HaveOccurred())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	response, queryErr := bydbqlv1.NewBydbQLServiceClient(SharedContext.Connection).Query(ctx, &bydbqlv1.QueryRequest{Query: text, Params: params})
	gm.Expect(status.Code(queryErr)).To(gm.Equal(wantCode), c.Name)
	gm.Expect(response).To(gm.BeNil(), "failed aggregate queries must not return partial rows")
}

func verifyGeneratedNativeAggregateOverflow(base time.Time, c qlgen.Case, recovery qlgen.NativeAggregateScenario) {
	var calls atomic.Uint64
	func() {
		restore := liaisongrpc.SetNativeAggregateHooksForTest(liaisongrpc.NativeAggregateTestHooks{
			BeforeBranch: func(context.Context, int, *measurev1.QueryRequest) error {
				calls.Add(1)
				return nil
			},
		})
		defer restore()
		verifyGeneratedNativeAggregateFailure(base, c, c.WantCode)
	}()
	gm.Expect(calls.Load()).To(gm.BeZero(), "wide-range integer SUM must retain T07's checked arithmetic")
	verifyGeneratedNativeAggregateCall(base, recovery.Cases[0], recovery.Branches, false)
}
