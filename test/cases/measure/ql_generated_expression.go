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

func registerGeneratedQLExpressionScenarios(suite qlgen.Suite, base *time.Time) {
	g.Describe("generated typed expression projections", g.Ordered, g.Label("QLB-T10"), func() {
		g.BeforeAll(func() {
			gm.Expect(vectorized.QueryMetricsEnabled).To(gm.BeTrue(), "QLB-T10 masking and ownership checks require -tags=querymetrics")
			prepareGeneratedExpressionFixture(*base, suite.ExpressionReadiness)
		})
		for _, scenario := range suite.ExpressionScenarios {
			g.It(scenario.Name, func() {
				liaisongrpc.ClearBydbQLPlanCachesForTest()
				before := liaisongrpc.BydbQLPlanCacheStats()
				for _, c := range scenario.Cases {
					verifyGeneratedExpressionCall(*base, c)
				}
				warm := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(warm.Compiles-before.Compiles).To(gm.Equal(uint64(1)), "changed bindings share one immutable expression template")
				gm.Expect(warm.Hits - before.Hits).To(gm.BeNumerically(">=", 2))
				restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{ForceBypass: true})
				defer restore()
				for _, c := range scenario.Cases {
					verifyGeneratedExpressionCall(*base, c)
				}
				uncached := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(uncached.Hits).To(gm.Equal(warm.Hits))
				gm.Expect(uncached.Bypass - warm.Bypass).To(gm.Equal(uint64(len(scenario.Cases))))
			})
		}
		for _, c := range suite.ExpressionRejections {
			g.It(c.Name, func() { verifyGeneratedExpressionCall(*base, c) })
		}
	})
}

func prepareGeneratedExpressionFixture(base time.Time, readiness []qlgen.Case) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	registry := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection)
	schema := qlgen.T10FixtureSchema()
	_, createErr := registry.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: schema})
	gm.Expect(createErr).NotTo(gm.HaveOccurred())
	registered, getErr := registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: schema.GetMetadata()})
	gm.Expect(getErr).NotTo(gm.HaveOccurred())
	writes := qlgen.T10FixtureWrites(base)
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

func verifyGeneratedExpressionCall(base time.Time, c qlgen.Case) {
	before := vmeasure.BatchExecutionStats()
	memoryBefore := vectorized.QueryMemoryStats()
	expressionBefore := ql.ExpressionExecutionStats()
	defer func() {
		after := vmeasure.BatchExecutionStats()
		expressionAfter := ql.ExpressionExecutionStats()
		gm.Expect(after.IntermediateDataPoints).To(gm.Equal(before.IntermediateDataPoints))
		gm.Expect(after.LiveResults).To(gm.Equal(before.LiveResults))
		expectLiveReservedBytes(memoryBefore.LiveReservedBytes)
		gm.Expect(expressionAfter.LiveBytes).To(gm.Equal(expressionBefore.LiveBytes))
		gm.Expect(expressionAfter.ReservedBytes-expressionBefore.ReservedBytes).To(
			gm.Equal(expressionAfter.ReleasedBytes-expressionBefore.ReleasedBytes), "all expression allocation reservations are released")
		gm.Expect(after.NativeBatchExecutions).To(gm.Equal(before.NativeBatchExecutions), "QL expressions never issue a native Query RPC")
		if c.NoScan {
			gm.Expect(after.NativeLeafCalls).To(gm.Equal(before.NativeLeafCalls), "LIMIT 0 and compile failures skip scans")
		}
	}()
	query, expected := renderGeneratedQLCase(base, c)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if c.WantCode != codes.OK {
		gm.Expect(measuredata.VerifyQLError(ctx, SharedContext.Connection, query, c.WantCode)).To(gm.Succeed(), c.Name)
		return
	}
	response, queryErr := measuredata.VerifyQL(ctx, SharedContext.Connection, query, expected, true)
	gm.Expect(queryErr).NotTo(gm.HaveOccurred(), c.Name)
	expressionAfter := ql.ExpressionExecutionStats()
	if len(response.GetDataPoints()) > 0 {
		gm.Expect(expressionAfter.Executions-expressionBefore.Executions).To(gm.Equal(uint64(1)), "typed expression projection executes once per nonempty QL RPC")
	}
	if c.Name == "t10_unreachable_invalid_arm_binding_0" || c.Name == "t10_unreachable_invalid_arm_binding_1" {
		gm.Expect(expressionAfter.LaterArmRows).To(gm.Equal(expressionBefore.LaterArmRows), "first matching branches skip every later value and invalid condition")
	}
}
