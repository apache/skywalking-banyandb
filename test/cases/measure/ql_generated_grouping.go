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
	"slices"
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

func registerGeneratedQLGroupingScenarios(suite qlgen.Suite, base *time.Time) {
	g.Describe("generated computed and ordinal grouping", g.Ordered, g.Label("QLB-T16"), func() {
		var fixtureBase time.Time
		g.BeforeAll(func() {
			fixtureBase = base.Truncate(6 * time.Second)
			gm.Expect(vectorized.QueryMetricsEnabled).To(gm.BeTrue(), "QLB-T16 key evaluation and ownership checks require -tags=querymetrics")
			prepareGeneratedGroupingFixtures(fixtureBase, suite.GroupingReadiness)
		})
		for _, scenario := range suite.GroupingScenarios {
			g.It(scenario.Name, func() {
				liaisongrpc.ClearBydbQLPlanCachesForTest()
				before := liaisongrpc.BydbQLPlanCacheStats()
				for _, c := range scenario.Cases {
					verifyGeneratedGroupingCall(fixtureBase, c, scenario.Branches, false)
				}
				warm := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(warm.Compiles - before.Compiles).To(gm.Equal(uint64(1)))
				gm.Expect(warm.Hits - before.Hits).To(gm.Equal(uint64(len(scenario.Cases) - 1)))
				func() {
					restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{ForceBypass: true})
					defer restore()
					for _, c := range scenario.Cases {
						verifyGeneratedGroupingCall(fixtureBase, c, scenario.Branches, false)
					}
				}()
				uncached := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(uncached.Hits).To(gm.Equal(warm.Hits))
				gm.Expect(uncached.Bypass - warm.Bypass).To(gm.Equal(uint64(len(scenario.Cases))))
				if scenario.Branches > 0 {
					restore := liaisongrpc.SetNativeAggregateHooksForTest(liaisongrpc.NativeAggregateTestHooks{Disabled: true})
					defer restore()
					for _, c := range scenario.Cases {
						verifyGeneratedGroupingCall(fixtureBase, c, scenario.Branches, true)
					}
				}
			})
		}
		for _, c := range suite.GroupingRejections {
			g.It(c.Name, func() {
				verifyGeneratedGroupingCall(fixtureBase, c, 0, false)
				verifyGeneratedGroupingCall(fixtureBase, suite.GroupingScenarios[0].Cases[0], 0, false)
			})
		}
	})
}

func prepareGeneratedGroupingFixtures(base time.Time, fixtures []qlgen.AggregateFixture) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	registry := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection)
	for _, fixture := range fixtures {
		schema := qlgen.T16FixtureSchema(fixture.Name)
		_, createErr := registry.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: schema})
		gm.Expect(createErr).NotTo(gm.HaveOccurred())
		registered, getErr := registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: schema.GetMetadata()})
		gm.Expect(getErr).NotTo(gm.HaveOccurred())
		writes := qlgen.T16FixtureWrites(fixture.Name, base)
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
			for _, c := range fixture.Cases {
				query, expected := renderGeneratedQLCase(base, c)
				_, queryErr := measuredata.VerifyQL(writeCtx, SharedContext.Connection, query, expected, true)
				inner.Expect(queryErr).NotTo(gm.HaveOccurred())
			}
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(gm.Succeed())
	}
}

func verifyGeneratedGroupingCall(base time.Time, c qlgen.Case, branches uint64, forceLocal bool) {
	before := vmeasure.BatchExecutionStats()
	memoryBefore := vectorized.QueryMemoryStats()
	groupBefore, expressionBefore := ql.GroupExpressionExecutionStats(), ql.ExpressionExecutionStats()
	aggregateBefore, nativeBefore := ql.AggregateExecutionStats(), ql.NativeAggregateExecutionStats()
	filterBefore, havingBefore := ql.FilterExecutionStats(), ql.HavingExecutionStats()
	distinctBefore := ql.DistinctExecutionStats()
	defer func() {
		after := vmeasure.BatchExecutionStats()
		gm.Expect(after.IntermediateDataPoints).To(gm.Equal(before.IntermediateDataPoints))
		gm.Expect(after.LiveResults).To(gm.Equal(before.LiveResults))
		gm.Expect(after.NativeBatchExecutions).To(gm.Equal(before.NativeBatchExecutions), "fixture visibility and cases use only the QL endpoint")
		expectLiveReservedBytes(memoryBefore.LiveReservedBytes)
		for _, pair := range [][2]ql.ExpressionStats{
			{groupBefore, ql.GroupExpressionExecutionStats()}, {expressionBefore, ql.ExpressionExecutionStats()},
		} {
			gm.Expect(pair[1].LiveBytes).To(gm.Equal(pair[0].LiveBytes))
			gm.Expect(pair[1].ReservedBytes - pair[0].ReservedBytes).To(gm.Equal(pair[1].ReleasedBytes - pair[0].ReleasedBytes))
		}
		for _, pair := range [][2]ql.FilterStats{{filterBefore, ql.FilterExecutionStats()}, {havingBefore, ql.HavingExecutionStats()}} {
			gm.Expect(pair[1].LiveBytes).To(gm.Equal(pair[0].LiveBytes))
			gm.Expect(pair[1].ReservedBytes - pair[0].ReservedBytes).To(gm.Equal(pair[1].ReleasedBytes - pair[0].ReleasedBytes))
		}
		gm.Expect(ql.AggregateExecutionStats().LiveBytes).To(gm.Equal(aggregateBefore.LiveBytes))
		gm.Expect(ql.NativeAggregateExecutionStats().LiveBytes).To(gm.Equal(nativeBefore.LiveBytes))
		gm.Expect(ql.DistinctExecutionStats().LiveBytes).To(gm.Equal(distinctBefore.LiveBytes))
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
	aggregateAfter, nativeAfter := ql.AggregateExecutionStats(), ql.NativeAggregateExecutionStats()
	groupAfter := ql.GroupExpressionExecutionStats()
	if branches == 0 || forceLocal {
		gm.Expect(nativeAfter.Executions).To(gm.Equal(nativeBefore.Executions))
		gm.Expect(aggregateAfter.Executions - aggregateBefore.Executions).To(gm.Equal(uint64(1)))
		gm.Expect(aggregateAfter.InputRows - aggregateBefore.InputRows).To(gm.Equal(c.AggregateInputs))
		gm.Expect(aggregateAfter.Groups - aggregateBefore.Groups).To(gm.Equal(c.AggregateGroups))
		gm.Expect(after.QLBatchExecutions-before.QLBatchExecutions).To(gm.Equal(uint64(1)), "computed grouping reads one complete raw scan")
		gm.Expect(groupAfter.KeyEvaluations - groupBefore.KeyEvaluations).To(gm.Equal(c.Grouping.KeyEvaluations))
		gm.Expect(groupAfter.KeyRootCalls-groupBefore.KeyRootCalls).To(gm.Equal((groupAfter.Batches-groupBefore.Batches)*c.Grouping.KeyExpressions),
			"each independently modeled structural key root is invoked once per active batch")
		if c.Grouping.KeyExpressions > 0 {
			gm.Expect(groupAfter.InputRows-groupBefore.InputRows).To(gm.Equal(c.Grouping.InputRows), "keys see selected WHERE rows once")
		}
	} else {
		gm.Expect(aggregateAfter.Executions).To(gm.Equal(aggregateBefore.Executions))
		gm.Expect(nativeAfter.Executions - nativeBefore.Executions).To(gm.Equal(uint64(1)))
		gm.Expect(nativeAfter.Branches - nativeBefore.Branches).To(gm.Equal(branches))
		gm.Expect(nativeAfter.Groups - nativeBefore.Groups).To(gm.Equal(c.AggregateGroups))
		gm.Expect(after.QLBatchExecutions - before.QLBatchExecutions).To(gm.Equal(branches))
		gm.Expect(groupAfter.KeyEvaluations).To(gm.Equal(groupBefore.KeyEvaluations))
		gm.Expect(groupAfter.KeyRootCalls).To(gm.Equal(groupBefore.KeyRootCalls))
	}
	if c.Filter != nil {
		filterAfter := ql.FilterExecutionStats()
		gm.Expect(filterAfter.InputRows - filterBefore.InputRows).To(gm.Equal(c.Filter.InputRows))
		gm.Expect(filterAfter.TrueRows - filterBefore.TrueRows).To(gm.Equal(c.Filter.TrueRows))
		gm.Expect(filterAfter.FalseRows - filterBefore.FalseRows).To(gm.Equal(c.Filter.FalseRows))
		gm.Expect(filterAfter.UnknownRows - filterBefore.UnknownRows).To(gm.Equal(c.Filter.UnknownRows))
	}
	if c.Having != nil {
		havingAfter := ql.HavingExecutionStats()
		gm.Expect(havingAfter.InputRows - havingBefore.InputRows).To(gm.Equal(c.Having.InputGroups))
		gm.Expect(havingAfter.TrueRows - havingBefore.TrueRows).To(gm.Equal(c.Having.TrueGroups))
		gm.Expect(havingAfter.FalseRows - havingBefore.FalseRows).To(gm.Equal(c.Having.FalseGroups))
		gm.Expect(havingAfter.UnknownRows - havingBefore.UnknownRows).To(gm.Equal(c.Having.UnknownGroups))
	}
	if slices.Contains(c.Features, "t16.distinct") {
		gm.Expect(ql.DistinctExecutionStats().Groups - distinctBefore.Groups).To(gm.Equal(c.DistinctGroups))
	}
	for _, point := range response.GetDataPoints() {
		gm.Expect(point.GetSid()).To(gm.BeZero())
		gm.Expect(point.GetVersion()).To(gm.BeZero())
		if !slices.Contains(c.Features, "t16.bucket_projection") {
			gm.Expect(point.GetTimestamp()).To(gm.BeNil())
		}
	}
}
