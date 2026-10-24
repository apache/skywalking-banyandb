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

func registerGeneratedQLDistinctScenarios(suite qlgen.Suite, base *time.Time) {
	g.Describe("generated complete visible tuple DISTINCT", g.Ordered, g.Label("QLB-T15"), func() {
		g.BeforeAll(func() {
			gm.Expect(vectorized.QueryMetricsEnabled).To(gm.BeTrue(), "QLB-T15 masking and ownership checks require -tags=querymetrics")
			prepareGeneratedDistinctFixture(*base, suite.DistinctReadiness)
		})
		for _, scenario := range suite.DistinctScenarios {
			g.It(scenario.Name, func() {
				liaisongrpc.ClearBydbQLPlanCachesForTest()
				before := liaisongrpc.BydbQLPlanCacheStats()
				for _, c := range scenario.Cases {
					verifyGeneratedDistinctCall(*base, c)
				}
				warm := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(warm.Compiles-before.Compiles).To(gm.Equal(uint64(1)), "changed bindings share one immutable expression template")
				gm.Expect(warm.Hits - before.Hits).To(gm.BeNumerically(">=", 2))
				restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{ForceBypass: true})
				defer restore()
				for _, c := range scenario.Cases {
					verifyGeneratedDistinctCall(*base, c)
				}
				uncached := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(uncached.Hits).To(gm.Equal(warm.Hits))
				gm.Expect(uncached.Bypass - warm.Bypass).To(gm.Equal(uint64(len(scenario.Cases))))
			})
		}
		for _, c := range suite.DistinctRejections {
			g.It(c.Name, func() { verifyGeneratedDistinctCall(*base, c) })
		}
	})
}

func prepareGeneratedDistinctFixture(base time.Time, readiness []qlgen.Case) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	registry := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection)
	schema := qlgen.T15FixtureSchema()
	_, createErr := registry.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: schema})
	gm.Expect(createErr).NotTo(gm.HaveOccurred())
	registered, getErr := registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: schema.GetMetadata()})
	gm.Expect(getErr).NotTo(gm.HaveOccurred())
	writes := qlgen.T15FixtureWrites(base)
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

func verifyGeneratedDistinctCall(base time.Time, c qlgen.Case) {
	before := vmeasure.BatchExecutionStats()
	memoryBefore := vectorized.QueryMemoryStats()
	distinctBefore := ql.DistinctExecutionStats()
	aggregateBefore := ql.AggregateExecutionStats()
	nativeBefore := ql.NativeAggregateExecutionStats()
	defer func() {
		after := vmeasure.BatchExecutionStats()
		distinctAfter := ql.DistinctExecutionStats()
		gm.Expect(after.IntermediateDataPoints).To(gm.Equal(before.IntermediateDataPoints))
		gm.Expect(after.LiveResults).To(gm.Equal(before.LiveResults))
		gm.Expect(after.NativeBatchExecutions).To(gm.Equal(before.NativeBatchExecutions), "all readiness and verification use the QL endpoint")
		gm.Expect(vectorized.QueryMemoryStats().LiveReservedBytes).To(gm.Equal(memoryBefore.LiveReservedBytes))
		gm.Expect(distinctAfter.LiveBytes).To(gm.Equal(distinctBefore.LiveBytes))
		gm.Expect(distinctAfter.ReservedBytes - distinctBefore.ReservedBytes).To(gm.Equal(distinctAfter.ReleasedBytes - distinctBefore.ReleasedBytes))
		gm.Expect(distinctAfter.StateLanes).To(gm.Equal(distinctBefore.StateLanes), "visible-tuple DISTINCT allocates no aggregate states")
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
	if !c.NoScan {
		after := ql.DistinctExecutionStats()
		gm.Expect(after.Executions - distinctBefore.Executions).To(gm.Equal(uint64(1)))
		gm.Expect(after.Groups-distinctBefore.Groups).To(gm.Equal(c.DistinctGroups), "key cardinality is measured before final pagination")
		if c.DistinctInputs != nil {
			gm.Expect(after.InputRows-distinctBefore.InputRows).To(gm.Equal(*c.DistinctInputs), "DISTINCT stops only after enough unique tuples")
		}
	}
	for _, point := range response.GetDataPoints() {
		gm.Expect(point.GetTimestamp()).To(gm.BeNil())
		gm.Expect(point.GetSid()).To(gm.BeZero())
		gm.Expect(point.GetVersion()).To(gm.BeZero())
	}
}
