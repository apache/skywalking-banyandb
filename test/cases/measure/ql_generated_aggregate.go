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
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	ql "github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	measuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func registerGeneratedQLAggregateScenarios(suite qlgen.Suite, base *time.Time) {
	g.Describe("generated complete SQL aggregation", g.Ordered, g.Label("QLB-T07"), func() {
		g.BeforeAll(func() {
			gm.Expect(vectorized.QueryMetricsEnabled).To(gm.BeTrue(), "QLB-T07 execution/state verification requires -tags=querymetrics")
			prepareGeneratedAggregateFixtures(*base, suite.AggregateReadiness)
		})
		for _, scenario := range suite.AggregateScenarios {
			g.It(scenario.Name, func() {
				liaisongrpc.ClearBydbQLPlanCachesForTest()
				before := liaisongrpc.BydbQLPlanCacheStats()
				for _, c := range scenario.Cases {
					verifyGeneratedAggregateCall(*base, c)
				}
				warm := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(warm.Compiles-before.Compiles).To(gm.Equal(uint64(1)), "cold/warm changed bindings share one immutable template")
				gm.Expect(warm.Hits - before.Hits).To(gm.BeNumerically(">=", 2))
				restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{ForceBypass: true})
				defer restore()
				for _, c := range scenario.Cases {
					verifyGeneratedAggregateCall(*base, c)
				}
				uncached := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(uncached.Hits).To(gm.Equal(warm.Hits))
				gm.Expect(uncached.Bypass - warm.Bypass).To(gm.Equal(uint64(len(scenario.Cases))))
			})
		}
		for _, c := range suite.AggregateRejections {
			g.It(c.Name, func() { verifyGeneratedAggregateCall(*base, c) })
		}
	})
}

func prepareGeneratedAggregateFixtures(base time.Time, fixtures []qlgen.AggregateFixture) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	registry := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection)
	for _, fixture := range fixtures {
		schema := qlgen.T07FixtureSchema(fixture.Name)
		_, createErr := registry.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: schema})
		gm.Expect(createErr).NotTo(gm.HaveOccurred())
		registered, getErr := registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: schema.GetMetadata()})
		gm.Expect(getErr).NotTo(gm.HaveOccurred())
		writes := qlgen.T07FixtureWrites(fixture.Name, base)
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
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(gm.Succeed())
		for _, c := range fixture.Cases {
			gm.Eventually(func() error {
				return executeGeneratedReuseCall(ctx, base, c)
			}, flags.EventuallyTimeout, 100*time.Millisecond).Should(gm.Succeed())
		}
	}
}

func verifyGeneratedAggregateCall(base time.Time, c qlgen.Case) {
	before := vmeasure.BatchExecutionStats()
	memoryBefore := vectorized.QueryMemoryStats()
	aggregateBefore := ql.AggregateExecutionStats()
	defer func() {
		after := vmeasure.BatchExecutionStats()
		gm.Expect(after.IntermediateDataPoints).To(gm.Equal(before.IntermediateDataPoints))
		gm.Expect(after.LiveResults).To(gm.Equal(before.LiveResults))
		gm.Expect(vectorized.QueryMemoryStats().LiveReservedBytes).To(gm.Equal(memoryBefore.LiveReservedBytes))
		gm.Expect(ql.AggregateExecutionStats().LiveBytes).To(gm.Equal(aggregateBefore.LiveBytes))
		if c.NoScan {
			gm.Expect(after.NativeLeafCalls).To(gm.Equal(before.NativeLeafCalls))
		}
	}()
	query, expected := renderGeneratedQLCase(base, c)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if c.WantCode != codes.OK {
		gm.Expect(measuredata.VerifyQLError(ctx, SharedContext.Connection, query, c.WantCode)).To(gm.Succeed())
		return
	}
	verify := func() (*measurev1.QueryResponse, error) {
		if c.AggregateGroups > 100 {
			return verifyGeneratedAggregateGroups(ctx, query, expected)
		}
		return measuredata.VerifyQL(ctx, SharedContext.Connection, query, expected, false)
	}
	response, queryErr := verify()
	gm.Expect(queryErr).NotTo(gm.HaveOccurred(), c.Name)
	for _, point := range response.GetDataPoints() {
		gm.Expect(point.GetTimestamp()).To(gm.BeNil(), "local aggregate rows have no source timestamp")
		gm.Expect(point.GetSid()).To(gm.BeZero(), "local aggregate rows have no source series identity")
		gm.Expect(point.GetVersion()).To(gm.BeZero(), "local aggregate rows have no source version")
	}
	if !c.NoScan {
		aggregateAfter := ql.AggregateExecutionStats()
		gm.Expect(aggregateAfter.Executions-aggregateBefore.Executions).To(gm.Equal(uint64(1)), "one local aggregate per QL RPC")
		gm.Expect(aggregateAfter.InputRows-aggregateBefore.InputRows).To(gm.Equal(c.AggregateInputs), "complete raw input after native deduplication")
		gm.Expect(aggregateAfter.Groups-aggregateBefore.Groups).To(gm.Equal(c.AggregateGroups), "typed group cardinality")
	}
}
