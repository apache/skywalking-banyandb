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

func registerGeneratedQLHavingScenarios(suite qlgen.Suite, base *time.Time) {
	g.Describe("generated grouped HAVING and membership", g.Ordered, g.Label("QLB-T14"), func() {
		g.BeforeAll(func() {
			gm.Expect(vectorized.QueryMetricsEnabled).To(gm.BeTrue(), "QLB-T14 completed-group and ownership checks require -tags=querymetrics")
			prepareGeneratedHavingFixtures(*base, suite.HavingReadiness)
		})
		registerGeneratedHavingReviewScenarios(suite, base)
		for _, scenario := range suite.HavingScenarios {
			g.It(scenario.Name, func() {
				liaisongrpc.ClearBydbQLPlanCachesForTest()
				before := liaisongrpc.BydbQLPlanCacheStats()
				cacheAcquires := uint64(0)
				for _, c := range scenario.Cases {
					if !c.PreparedRejected {
						cacheAcquires++
					}
				}
				for _, c := range scenario.Cases {
					verifyGeneratedHavingCall(*base, c, scenario.Branches, false)
				}
				warm := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(warm.Compiles-before.Compiles).To(gm.Equal(uint64(1)), "changed bindings share one immutable template")
				gm.Expect(warm.Hits-before.Hits).To(gm.Equal(cacheAcquires-1), "only successfully prepared bindings acquire the compiled cache")
				func() {
					restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{ForceBypass: true})
					defer restore()
					for _, c := range scenario.Cases {
						verifyGeneratedHavingCall(*base, c, scenario.Branches, false)
					}
				}()
				uncached := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(uncached.Hits).To(gm.Equal(warm.Hits))
				gm.Expect(uncached.Bypass - warm.Bypass).To(gm.Equal(cacheAcquires))
				if scenario.Branches > 0 {
					restore := liaisongrpc.SetNativeAggregateHooksForTest(liaisongrpc.NativeAggregateTestHooks{Disabled: true})
					defer restore()
					for _, c := range scenario.Cases {
						verifyGeneratedHavingCall(*base, c, scenario.Branches, true)
					}
				}
			})
		}
		for _, scenario := range suite.HavingLegacyScenarios {
			g.It(scenario.Name, g.Label("QLB-T14-legacy"), func() {
				liaisongrpc.ClearBydbQLPlanCachesForTest()
				before := liaisongrpc.BydbQLPlanCacheStats()
				for _, c := range scenario.Cases {
					verifyGeneratedLegacyHavingCall(*base, c)
				}
				warm := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(warm.Compiles - before.Compiles).To(gm.Equal(uint64(1)))
				gm.Expect(warm.Hits - before.Hits).To(gm.BeNumerically(">=", 2))
				restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{ForceBypass: true})
				defer restore()
				for _, c := range scenario.Cases {
					verifyGeneratedLegacyHavingCall(*base, c)
				}
				uncached := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(uncached.Hits).To(gm.Equal(warm.Hits))
				gm.Expect(uncached.Bypass - warm.Bypass).To(gm.Equal(uint64(len(scenario.Cases))))
			})
		}
		for _, c := range suite.HavingRejections {
			g.It(c.Name, func() {
				verifyGeneratedHavingCall(*base, c, 0, false)
				verifyGeneratedHavingCall(*base, suite.HavingScenarios[0].Cases[0], 0, false)
			})
		}
	})
}

func prepareGeneratedHavingFixtures(base time.Time, fixtures []qlgen.AggregateFixture) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	registry := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection)
	for _, fixture := range fixtures {
		if fixture.Name == qlgen.T14ContainsFixtureName || fixture.Name == qlgen.T14IndexedFixtureName {
			prepareGeneratedHavingReviewIndexes(ctx, fixture.Name)
		}
		if fixture.Name == qlgen.T14LegacyFixtureName {
			indexRegistry := databasev1.NewIndexRuleRegistryServiceClient(SharedContext.Connection)
			for _, rule := range qlgen.T14LegacyIndexRules() {
				_, indexErr := indexRegistry.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{IndexRule: rule})
				gm.Expect(indexErr).NotTo(gm.HaveOccurred())
			}
			bindingRegistry := databasev1.NewIndexRuleBindingRegistryServiceClient(SharedContext.Connection)
			_, bindingErr := bindingRegistry.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{IndexRuleBinding: qlgen.T14LegacyIndexBinding()})
			gm.Expect(bindingErr).NotTo(gm.HaveOccurred())
		}
		schema := qlgen.T14FixtureSchema(fixture.Name)
		_, createErr := registry.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: schema})
		gm.Expect(createErr).NotTo(gm.HaveOccurred())
		registered, getErr := registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: schema.GetMetadata()})
		gm.Expect(getErr).NotTo(gm.HaveOccurred())
		writes := qlgen.T14FixtureWrites(fixture.Name, base)
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

func verifyGeneratedHavingCall(base time.Time, c qlgen.Case, branches uint64, forceLocal bool) {
	before := vmeasure.BatchExecutionStats()
	memoryBefore := vectorized.QueryMemoryStats()
	filterBefore, havingBefore := ql.FilterExecutionStats(), ql.HavingExecutionStats()
	expressionBefore := ql.ExpressionExecutionStats()
	aggregateBefore, nativeBefore := ql.AggregateExecutionStats(), ql.NativeAggregateExecutionStats()
	defer func() {
		after := vmeasure.BatchExecutionStats()
		gm.Expect(after.IntermediateDataPoints).To(gm.Equal(before.IntermediateDataPoints))
		gm.Expect(after.LiveResults).To(gm.Equal(before.LiveResults))
		gm.Expect(after.NativeBatchExecutions).To(gm.Equal(before.NativeBatchExecutions), "readiness and verification issue only QL query RPCs")
		gm.Expect(vectorized.QueryMemoryStats().LiveReservedBytes).To(gm.Equal(memoryBefore.LiveReservedBytes))
		for _, pair := range [][2]ql.FilterStats{{filterBefore, ql.FilterExecutionStats()}, {havingBefore, ql.HavingExecutionStats()}} {
			gm.Expect(pair[1].LiveBytes).To(gm.Equal(pair[0].LiveBytes))
			gm.Expect(pair[1].ReservedBytes - pair[0].ReservedBytes).To(gm.Equal(pair[1].ReleasedBytes - pair[0].ReleasedBytes))
			// Public operator and kernel tests prove input column identity. This
			// structural counter is a consistency check, not independent copy telemetry.
			gm.Expect(pair[1].CopiedBytes).To(gm.Equal(pair[0].CopiedBytes))
		}
		gm.Expect(ql.ExpressionExecutionStats().LiveBytes).To(gm.Equal(expressionBefore.LiveBytes))
		gm.Expect(ql.AggregateExecutionStats().LiveBytes).To(gm.Equal(aggregateBefore.LiveBytes))
		gm.Expect(ql.NativeAggregateExecutionStats().LiveBytes).To(gm.Equal(nativeBefore.LiveBytes))
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
	filterAfter, havingAfter := ql.FilterExecutionStats(), ql.HavingExecutionStats()
	aggregateAfter, nativeAfter := ql.AggregateExecutionStats(), ql.NativeAggregateExecutionStats()
	if c.NoScan {
		gm.Expect(after.QLBatchExecutions).To(gm.Equal(before.QLBatchExecutions))
		gm.Expect(filterAfter.Executions).To(gm.Equal(filterBefore.Executions))
		gm.Expect(havingAfter.Executions).To(gm.Equal(havingBefore.Executions))
		gm.Expect(aggregateAfter.Executions).To(gm.Equal(aggregateBefore.Executions))
		gm.Expect(nativeAfter.Executions).To(gm.Equal(nativeBefore.Executions))
		return
	}
	if c.Filter != nil {
		gm.Expect(filterAfter.InputRows-filterBefore.InputRows).To(gm.Equal(c.Filter.InputRows), "WHERE examines raw candidates")
		gm.Expect(filterAfter.TrueRows - filterBefore.TrueRows).To(gm.Equal(c.Filter.TrueRows))
		gm.Expect(filterAfter.FalseRows - filterBefore.FalseRows).To(gm.Equal(c.Filter.FalseRows))
		gm.Expect(filterAfter.UnknownRows - filterBefore.UnknownRows).To(gm.Equal(c.Filter.UnknownRows))
	} else {
		gm.Expect(filterAfter.InputRows).To(gm.Equal(filterBefore.InputRows))
	}
	if c.Having == nil {
		gm.Expect(havingAfter.Executions).To(gm.Equal(havingBefore.Executions))
		gm.Expect(aggregateAfter.Executions).To(gm.Equal(aggregateBefore.Executions))
		gm.Expect(nativeAfter.Executions).To(gm.Equal(nativeBefore.Executions))
		return
	}
	gm.Expect(havingAfter.InputRows-havingBefore.InputRows).To(gm.Equal(c.Having.InputGroups), "HAVING sees completed groups after the whole aggregate merge")
	gm.Expect(havingAfter.TrueRows - havingBefore.TrueRows).To(gm.Equal(c.Having.TrueGroups))
	gm.Expect(havingAfter.FalseRows - havingBefore.FalseRows).To(gm.Equal(c.Having.FalseGroups))
	gm.Expect(havingAfter.UnknownRows - havingBefore.UnknownRows).To(gm.Equal(c.Having.UnknownGroups))
	for _, point := range response.GetDataPoints() {
		gm.Expect(point.GetTimestamp()).To(gm.BeNil())
		gm.Expect(point.GetSid()).To(gm.BeZero())
		gm.Expect(point.GetVersion()).To(gm.BeZero())
	}
	if branches == 0 || forceLocal {
		gm.Expect(nativeAfter.Executions).To(gm.Equal(nativeBefore.Executions))
		gm.Expect(aggregateAfter.Executions - aggregateBefore.Executions).To(gm.Equal(uint64(1)))
		gm.Expect(aggregateAfter.InputRows - aggregateBefore.InputRows).To(gm.Equal(c.AggregateInputs))
		gm.Expect(aggregateAfter.Groups - aggregateBefore.Groups).To(gm.Equal(c.AggregateGroups))
		gm.Expect(after.QLBatchExecutions - before.QLBatchExecutions).To(gm.Equal(uint64(1)))
		if slices.Contains(c.Features, "t14.repeated_aggregate") {
			gm.Expect(aggregateAfter.StateLanes-aggregateBefore.StateLanes).To(gm.Equal(uint64(2)), "repeated calls retain one sum/count state")
		}
	} else {
		gm.Expect(aggregateAfter.Executions).To(gm.Equal(aggregateBefore.Executions))
		gm.Expect(nativeAfter.Executions - nativeBefore.Executions).To(gm.Equal(uint64(1)))
		gm.Expect(nativeAfter.Branches-nativeBefore.Branches).To(gm.Equal(branches), "repeated visible/hidden aggregate calls share native leaves")
		gm.Expect(nativeAfter.Groups - nativeBefore.Groups).To(gm.Equal(c.AggregateGroups))
		gm.Expect(after.QLBatchExecutions - before.QLBatchExecutions).To(gm.Equal(branches))
		if slices.Contains(c.Features, "t14.repeated_aggregate") {
			gm.Expect(nativeAfter.StateLanes-nativeBefore.StateLanes).To(gm.Equal(uint64(2)), "repeated calls retain one sum/count merge state")
		}
	}
}
