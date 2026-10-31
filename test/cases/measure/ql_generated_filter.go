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
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	ql "github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	measuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func registerGeneratedQLFilterScenarios(suite qlgen.Suite, base *time.Time) {
	var recovery qlgen.Case
	for _, scenario := range suite.FilterScenarios {
		if scenario.Name == "t12_field_filter_F" {
			recovery = scenario.Cases[0]
			break
		}
	}
	g.Describe("generated raw field filtering", g.Ordered, g.Label("QLB-T12"), func() {
		var leaves uint64
		g.BeforeAll(func() {
			gm.Expect(vectorized.QueryMetricsEnabled).To(gm.BeTrue(), "QLB-T12 complete-input and ownership checks require -tags=querymetrics")
			gm.Expect(recovery.Name).NotTo(gm.BeEmpty(), "generated filter suite requires its F recovery witness")
			prepareGeneratedFilterFixture(*base, suite.FilterReadiness)
			// A raw readiness request observes the active topology's single scan fanout.
			// The fixture has one logical source, with one standalone or two data nodes.
			before := vmeasure.BatchExecutionStats()
			query, expected := renderGeneratedQLCase(*base, suite.FilterReadiness[0])
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			_, queryErr := measuredata.VerifyQL(ctx, SharedContext.Connection, query, expected, true)
			gm.Expect(queryErr).NotTo(gm.HaveOccurred())
			after := vmeasure.BatchExecutionStats()
			leaves = after.NativeLeafCalls - before.NativeLeafCalls
			gm.Expect(after.QLBatchExecutions - before.QLBatchExecutions).To(gm.Equal(uint64(1)))
			gm.Expect(leaves).To(gm.Or(gm.Equal(uint64(1)), gm.Equal(uint64(2))))
		})
		for _, scenario := range suite.FilterScenarios {
			g.It(scenario.Name, func() {
				liaisongrpc.ClearBydbQLPlanCachesForTest()
				if scenario.Kind != "query" {
					verifyGeneratedFilterLifecycle(*base, scenario, leaves)
					return
				}
				before := liaisongrpc.BydbQLPlanCacheStats()
				for _, c := range scenario.Cases {
					verifyGeneratedFilterCall(*base, c, leaves)
				}
				warm := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(warm.Compiles-before.Compiles).To(gm.Equal(uint64(1)), "changed predicate bindings reuse one immutable template")
				gm.Expect(warm.Hits - before.Hits).To(gm.BeNumerically(">=", 2))
				restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{ForceBypass: true})
				defer restore()
				for _, c := range scenario.Cases {
					verifyGeneratedFilterCall(*base, c, leaves)
				}
				uncached := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(uncached.Hits).To(gm.Equal(warm.Hits))
				gm.Expect(uncached.Bypass - warm.Bypass).To(gm.Equal(uint64(len(scenario.Cases))))
			})
		}
		for _, c := range suite.FilterRejections {
			g.It(c.Name, func() {
				verifyGeneratedFilterCall(*base, c, leaves)
				verifyGeneratedFilterCall(*base, recovery, leaves)
			})
		}
	})
}

func prepareGeneratedFilterFixture(base time.Time, readiness []qlgen.Case) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	registry := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection)
	schema := qlgen.T12FixtureSchema()
	_, createErr := registry.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: schema})
	gm.Expect(createErr).NotTo(gm.HaveOccurred())
	registered, getErr := registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: schema.GetMetadata()})
	gm.Expect(getErr).NotTo(gm.HaveOccurred())
	writes := qlgen.T12FixtureWrites(base)
	for ordinal, request := range writes {
		request.Metadata = nil
		if ordinal == 0 {
			request.Metadata = registered.GetMeasure().GetMetadata()
		}
	}
	gm.Eventually(func(inner gm.Gomega) {
		writeCtx, writeCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer writeCancel()
		inner.Expect(writeGeneratedQLFixture(writeCtx, SharedContext.Connection, writes)).To(gm.Succeed())
		for _, c := range readiness {
			query, expected := renderGeneratedQLCase(base, c)
			_, queryErr := measuredata.VerifyQL(writeCtx, SharedContext.Connection, query, expected, true)
			inner.Expect(queryErr).NotTo(gm.HaveOccurred())
		}
	}, flags.EventuallyTimeout, 100*time.Millisecond).Should(gm.Succeed())
}

func verifyGeneratedFilterCall(base time.Time, c qlgen.Case, leaves uint64) {
	before := vmeasure.BatchExecutionStats()
	memoryBefore := vectorized.QueryMemoryStats()
	filterBefore := ql.FilterExecutionStats()
	expressionBefore := ql.ExpressionExecutionStats()
	aggregateBefore := ql.AggregateExecutionStats()
	nativeBefore := ql.NativeAggregateExecutionStats()
	defer func() {
		after := vmeasure.BatchExecutionStats()
		filterAfter := ql.FilterExecutionStats()
		gm.Expect(after.IntermediateDataPoints).To(gm.Equal(before.IntermediateDataPoints))
		gm.Expect(after.LiveResults).To(gm.Equal(before.LiveResults))
		gm.Expect(after.NativeBatchExecutions).To(gm.Equal(before.NativeBatchExecutions), "the oracle never issues native Query RPCs")
		gm.Eventually(func() int64 { return vectorized.QueryMemoryStats().LiveReservedBytes }, flags.EventuallyTimeout, 10*time.Millisecond).Should(gm.Equal(memoryBefore.LiveReservedBytes))
		gm.Expect(filterAfter.LiveBytes).To(gm.Equal(filterBefore.LiveBytes))
		gm.Expect(filterAfter.ReservedBytes - filterBefore.ReservedBytes).To(gm.Equal(filterAfter.ReleasedBytes - filterBefore.ReleasedBytes))
		gm.Expect(ql.ExpressionExecutionStats().LiveBytes).To(gm.Equal(expressionBefore.LiveBytes))
		gm.Expect(ql.AggregateExecutionStats().LiveBytes).To(gm.Equal(aggregateBefore.LiveBytes))
		gm.Expect(ql.NativeAggregateExecutionStats().LiveBytes).To(gm.Equal(nativeBefore.LiveBytes))
		gm.Expect(ql.NativeAggregateExecutionStats().Executions).To(gm.Equal(nativeBefore.Executions), "residuals disable native aggregate leaves and merge")
		if c.NoScan {
			gm.Expect(after.NativeLeafCalls).To(gm.Equal(before.NativeLeafCalls))
		}
	}()
	query, expected := renderGeneratedQLCase(base, c)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if c.WantCode != codes.OK {
		gm.Expect(measuredata.VerifyQLError(ctx, SharedContext.Connection, query, c.WantCode)).To(gm.Succeed(), c.Name)
		return
	}
	response, queryErr := measuredata.VerifyQL(ctx, SharedContext.Connection, query, expected, c.Ordered)
	gm.Expect(queryErr).NotTo(gm.HaveOccurred(), c.Name)
	after := vmeasure.BatchExecutionStats()
	filterAfter := ql.FilterExecutionStats()
	if c.NoScan {
		gm.Expect(after.QLBatchExecutions).To(gm.Equal(before.QLBatchExecutions))
		gm.Expect(filterAfter.Executions).To(gm.Equal(filterBefore.Executions))
		return
	}
	gm.Expect(after.QLBatchExecutions-before.QLBatchExecutions).To(gm.Equal(uint64(1)), "one complete raw logical scan per residual query")
	gm.Expect(after.NativeLeafCalls-before.NativeLeafCalls).To(gm.Equal(leaves), "residuals retain the raw scan's data-node fanout")
	// Operator tests and K warmup prove borrowed-column identity. The RPC
	// counter only checks report consistency with that structural invariant.
	gm.Expect(filterAfter.CopiedBytes).To(gm.Equal(filterBefore.CopiedBytes), "copy counter stays consistent with borrowed columns")
	if c.Filter.Complete || c.Filter.Aggregate {
		gm.Expect(filterAfter.InputRows-filterBefore.InputRows).To(gm.Equal(c.Filter.InputRows), "all uncapped native candidates are examined")
		gm.Expect(filterAfter.TrueRows - filterBefore.TrueRows).To(gm.Equal(c.Filter.TrueRows))
		gm.Expect(filterAfter.FalseRows - filterBefore.FalseRows).To(gm.Equal(c.Filter.FalseRows))
		gm.Expect(filterAfter.UnknownRows - filterBefore.UnknownRows).To(gm.Equal(c.Filter.UnknownRows))
	}
	if c.Filter.Aggregate {
		aggregateAfter := ql.AggregateExecutionStats()
		gm.Expect(aggregateAfter.Executions - aggregateBefore.Executions).To(gm.Equal(uint64(1)))
		gm.Expect(aggregateAfter.InputRows-aggregateBefore.InputRows).To(gm.Equal(c.Filter.TrueRows), "aggregation consumes only TRUE rows")
		gm.Expect(aggregateAfter.Groups - aggregateBefore.Groups).To(gm.Equal(c.AggregateGroups))
		for _, point := range response.GetDataPoints() {
			gm.Expect(point.GetTimestamp()).To(gm.BeNil())
			gm.Expect(point.GetSid()).To(gm.BeZero())
			gm.Expect(point.GetVersion()).To(gm.BeZero())
		}
	} else {
		gm.Expect(ql.AggregateExecutionStats().Executions).To(gm.Equal(aggregateBefore.Executions))
	}
}

func verifyGeneratedFilterLifecycle(base time.Time, scenario qlgen.FilterScenario, leaves uint64) {
	func() {
		hooks := liaisongrpc.PlanCacheTestHooks{}
		want := codes.ResourceExhausted
		if scenario.Kind == "low-budget" {
			hooks.Context = func(ctx context.Context) context.Context { return vectorized.WithQueryMemoryTracker(ctx, 1) }
		} else {
			want = codes.Canceled
			var cancel context.CancelFunc
			hooks.Context = func(ctx context.Context) context.Context {
				var child context.Context
				child, cancel = context.WithCancel(ctx)
				return child
			}
			hooks.AfterAcquire = func(context.Context, string) { cancel() }
			defer func() {
				if cancel != nil {
					cancel()
				}
			}()
		}
		restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(hooks)
		defer restore()
		c := scenario.Cases[0]
		c.WantCode = want
		verifyGeneratedFilterCall(base, c, leaves)
	}()
	for _, c := range scenario.Cases {
		verifyGeneratedFilterCall(base, c, leaves)
	}
}
