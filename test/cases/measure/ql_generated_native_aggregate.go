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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	ql "github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func registerGeneratedQLNativeAggregateScenarios(suite qlgen.Suite, base *time.Time) {
	g.Describe("generated native aggregate pushdown", g.Ordered, g.Label("QLB-T08"), func() {
		g.BeforeAll(func() {
			gm.Expect(vectorized.QueryMetricsEnabled).To(gm.BeTrue(), "QLB-T08 routing and ownership verification requires -tags=querymetrics")
			prepareGeneratedNativeAggregateFixtures(*base, suite.NativeAggregateReadiness)
		})
		for _, scenario := range suite.NativeAggregateScenarios {
			g.It(scenario.Name, func() {
				liaisongrpc.ClearBydbQLPlanCachesForTest()
				if scenario.Kind != "query" && scenario.Kind != "fallback" {
					verifyGeneratedNativeAggregateLifecycle(*base, scenario)
					return
				}
				before := liaisongrpc.BydbQLPlanCacheStats()
				for _, c := range scenario.Cases {
					verifyGeneratedNativeAggregateCall(*base, c, scenario.Branches, false)
				}
				warm := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(warm.Compiles-before.Compiles).To(gm.Equal(uint64(1)), "changed bindings share one immutable template")
				gm.Expect(warm.Hits - before.Hits).To(gm.BeNumerically(">=", 2))
				func() {
					restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{ForceBypass: true})
					defer restore()
					for _, c := range scenario.Cases {
						verifyGeneratedNativeAggregateCall(*base, c, scenario.Branches, false)
					}
				}()
				uncached := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(uncached.Hits).To(gm.Equal(warm.Hits))
				gm.Expect(uncached.Bypass - warm.Bypass).To(gm.Equal(uint64(len(scenario.Cases))))
				if scenario.Branches == 0 {
					return
				}
				restore := liaisongrpc.SetNativeAggregateHooksForTest(liaisongrpc.NativeAggregateTestHooks{Disabled: true})
				defer restore()
				for _, c := range scenario.Cases {
					verifyGeneratedNativeAggregateCall(*base, c, scenario.Branches, true)
				}
			})
		}
		for _, c := range suite.NativeAggregateRejections {
			g.It(c.Name, func() {
				verifyGeneratedNativeAggregateOverflow(*base, c, suite.NativeAggregateScenarios[0])
			})
		}
	})
}

func prepareGeneratedNativeAggregateFixtures(base time.Time, fixtures []qlgen.AggregateFixture) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	registry := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection)
	indexRegistry := databasev1.NewIndexRuleRegistryServiceClient(SharedContext.Connection)
	bindingRegistry := databasev1.NewIndexRuleBindingRegistryServiceClient(SharedContext.Connection)
	for _, fixture := range fixtures {
		_, indexErr := indexRegistry.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{IndexRule: qlgen.T08FixtureIndexRule(fixture.Name)})
		gm.Expect(indexErr).NotTo(gm.HaveOccurred())
		_, bindingErr := bindingRegistry.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
			IndexRuleBinding: qlgen.T08FixtureIndexBinding(fixture.Name),
		})
		gm.Expect(bindingErr).NotTo(gm.HaveOccurred())
		schema := qlgen.T08FixtureSchema(fixture.Name)
		_, createErr := registry.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: schema})
		gm.Expect(createErr).NotTo(gm.HaveOccurred())
		registered, getErr := registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: schema.GetMetadata()})
		gm.Expect(getErr).NotTo(gm.HaveOccurred())
		writes := qlgen.T08FixtureWrites(fixture.Name, base)
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
			inner.Expect(executeGeneratedReuseCall(writeCtx, base, fixture.Cases[len(fixture.Cases)-1])).To(gm.Succeed())
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(gm.Succeed())
		for _, c := range fixture.Cases {
			gm.Eventually(func() error {
				return executeGeneratedReuseCall(ctx, base, c)
			}, flags.EventuallyTimeout, 100*time.Millisecond).Should(gm.Succeed())
		}
	}
}

func verifyGeneratedNativeAggregateCall(base time.Time, c qlgen.Case, branches uint64, forceLocal bool) {
	before := vmeasure.BatchExecutionStats()
	memoryBefore := vectorized.QueryMemoryStats()
	localBefore := ql.AggregateExecutionStats()
	nativeBefore := ql.NativeAggregateExecutionStats()
	defer func() {
		after := vmeasure.BatchExecutionStats()
		gm.Expect(after.IntermediateDataPoints).To(gm.Equal(before.IntermediateDataPoints))
		gm.Expect(after.LiveResults).To(gm.Equal(before.LiveResults))
		gm.Expect(vectorized.QueryMemoryStats().LiveReservedBytes).To(gm.Equal(memoryBefore.LiveReservedBytes))
		gm.Expect(ql.AggregateExecutionStats().LiveBytes).To(gm.Equal(localBefore.LiveBytes))
		gm.Expect(ql.NativeAggregateExecutionStats().LiveBytes).To(gm.Equal(nativeBefore.LiveBytes))
		if c.NoScan {
			gm.Expect(after.NativeLeafCalls).To(gm.Equal(before.NativeLeafCalls))
		}
	}()
	query, expected := renderGeneratedQLCase(base, c)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	response, queryErr := verifyGeneratedNativeAggregateResult(ctx, query, expected, c.AggregateGroups)
	gm.Expect(queryErr).NotTo(gm.HaveOccurred(), c.Name)
	for _, point := range response.GetDataPoints() {
		gm.Expect(point.GetTimestamp()).To(gm.BeNil(), "SQL aggregate rows have no source timestamp")
		gm.Expect(point.GetSid()).To(gm.BeZero(), "SQL aggregate rows have no source series identity")
		gm.Expect(point.GetVersion()).To(gm.BeZero(), "SQL aggregate rows have no source version")
	}
	localAfter := ql.AggregateExecutionStats()
	nativeAfter := ql.NativeAggregateExecutionStats()
	if c.NoScan {
		gm.Expect(localAfter.Executions).To(gm.Equal(localBefore.Executions))
		gm.Expect(nativeAfter.Executions).To(gm.Equal(nativeBefore.Executions))
		gm.Expect(vmeasure.BatchExecutionStats().QLBatchExecutions).To(gm.Equal(before.QLBatchExecutions))
		return
	}
	if branches == 0 || forceLocal {
		gm.Expect(nativeAfter.Executions).To(gm.Equal(nativeBefore.Executions), "unsafe/disabled native aggregation uses the complete T07 path")
		gm.Expect(localAfter.Executions - localBefore.Executions).To(gm.Equal(uint64(1)))
		gm.Expect(localAfter.InputRows-localBefore.InputRows).To(gm.Equal(c.AggregateInputs), "local fallback consumes the complete deduplicated input")
		gm.Expect(localAfter.Groups - localBefore.Groups).To(gm.Equal(c.AggregateGroups))
		gm.Expect(vmeasure.BatchExecutionStats().QLBatchExecutions - before.QLBatchExecutions).To(gm.Equal(uint64(1)))
		return
	}
	gm.Expect(localAfter.Executions).To(gm.Equal(localBefore.Executions), "eligible QL aggregates use the keyed native merge")
	gm.Expect(nativeAfter.Executions - nativeBefore.Executions).To(gm.Equal(uint64(1)))
	gm.Expect(nativeAfter.Branches-nativeBefore.Branches).To(gm.Equal(branches), "matching SUM/COUNT/AVG calls reuse native leaves")
	gm.Expect(vmeasure.BatchExecutionStats().QLBatchExecutions - before.QLBatchExecutions).To(gm.Equal(branches))
	gm.Expect(nativeAfter.Groups-nativeBefore.Groups).To(gm.Equal(c.AggregateGroups), "native merge retains the complete typed key union")
	inputRows := nativeAfter.InputRows - nativeBefore.InputRows
	visits := nativeAfter.MergeVisits - nativeBefore.MergeVisits
	gm.Expect(visits).To(gm.BeNumerically("<=", 3*(inputRows+c.AggregateGroups*branches)), "keyed merge work grows linearly in branch rows and group states")
}
