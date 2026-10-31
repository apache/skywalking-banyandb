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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	ql "github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	measuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func registerGeneratedQLLegacyCompatibility(suite qlgen.Suite, base *time.Time) {
	g.Describe("generated legacy schema compatibility", g.Ordered, g.Label("QLB-T12", "QLB-T12-legacy-compatibility"), func() {
		g.BeforeAll(func() {
			gm.Expect(vectorized.QueryMetricsEnabled).To(gm.BeTrue(), "legacy compatibility checks require -tags=querymetrics")
			prepareGeneratedLegacyCompatibility(*base, suite.LegacyCompatibilityReadiness)
		})
		for _, scenario := range suite.LegacyCompatibilityScenarios {
			g.It(scenario.Name, func() {
				liaisongrpc.ClearBydbQLPlanCachesForTest()
				before := liaisongrpc.BydbQLPlanCacheStats()
				for _, c := range scenario.Cases {
					verifyGeneratedLegacyCompatibilityCall(*base, c)
				}
				warm := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(warm.Compiles-before.Compiles).To(gm.Equal(uint64(1)), "changed legacy tag bindings reuse one template")
				gm.Expect(warm.Hits - before.Hits).To(gm.BeNumerically(">=", 2))
				restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{ForceBypass: true})
				defer restore()
				for _, c := range scenario.Cases {
					verifyGeneratedLegacyCompatibilityCall(*base, c)
				}
				uncached := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(uncached.Hits).To(gm.Equal(warm.Hits))
				gm.Expect(uncached.Bypass - warm.Bypass).To(gm.Equal(uint64(len(scenario.Cases))))
			})
		}
	})
}

func prepareGeneratedLegacyCompatibility(base time.Time, readiness []qlgen.Case) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	groups := databasev1.NewGroupRegistryServiceClient(SharedContext.Connection)
	registry := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection)
	for _, group := range qlgen.T12LegacyFixtureGroups() {
		_, groupErr := groups.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{Group: &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: group}, Catalog: commonv1.Catalog_CATALOG_MEASURE,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum:        1,
				SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
				Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
			},
		}})
		gm.Expect(groupErr).NotTo(gm.HaveOccurred())
		schema := qlgen.T12LegacyFixtureSchema(group)
		_, createErr := registry.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: schema})
		gm.Expect(createErr).NotTo(gm.HaveOccurred())
		registered, getErr := registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: schema.GetMetadata()})
		gm.Expect(getErr).NotTo(gm.HaveOccurred())
		writes := qlgen.T12LegacyFixtureWrites(group, base)
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
	}
	gm.Eventually(func(inner gm.Gomega) {
		for _, c := range readiness {
			query, expected := renderGeneratedQLCase(base, c)
			readCtx, readCancel := context.WithTimeout(context.Background(), 20*time.Second)
			_, queryErr := measuredata.VerifyQL(readCtx, SharedContext.Connection, query, expected, false)
			readCancel()
			inner.Expect(queryErr).NotTo(gm.HaveOccurred())
		}
	}, flags.EventuallyTimeout, 100*time.Millisecond).Should(gm.Succeed())
}

func verifyGeneratedLegacyCompatibilityCall(base time.Time, c qlgen.Case) {
	before := vmeasure.BatchExecutionStats()
	memoryBefore := vectorized.QueryMemoryStats()
	filterBefore := ql.FilterExecutionStats()
	expressionBefore := ql.ExpressionExecutionStats()
	aggregateBefore := ql.AggregateExecutionStats()
	nativeBefore := ql.NativeAggregateExecutionStats()
	defer func() {
		after := vmeasure.BatchExecutionStats()
		gm.Expect(after.LiveResults).To(gm.Equal(before.LiveResults))
		gm.Eventually(func() int64 { return vectorized.QueryMemoryStats().LiveReservedBytes }, flags.EventuallyTimeout, 10*time.Millisecond).Should(gm.Equal(memoryBefore.LiveReservedBytes))
		gm.Expect(ql.FilterExecutionStats().LiveBytes).To(gm.Equal(filterBefore.LiveBytes))
		gm.Expect(ql.ExpressionExecutionStats().LiveBytes).To(gm.Equal(expressionBefore.LiveBytes))
		gm.Expect(ql.AggregateExecutionStats().LiveBytes).To(gm.Equal(aggregateBefore.LiveBytes))
		gm.Expect(ql.NativeAggregateExecutionStats().LiveBytes).To(gm.Equal(nativeBefore.LiveBytes))
	}()
	query, expected := renderGeneratedQLCase(base, c)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, queryErr := measuredata.VerifyQL(ctx, SharedContext.Connection, query, expected, false)
	gm.Expect(queryErr).NotTo(gm.HaveOccurred(), c.Name)
	gm.Expect(ql.FilterExecutionStats().Executions).To(gm.Equal(filterBefore.Executions), "legacy tag-only queries retain their native predicate path")
	gm.Expect(ql.ExpressionExecutionStats().Executions).To(gm.Equal(expressionBefore.Executions))
	gm.Expect(ql.AggregateExecutionStats().Executions).To(gm.Equal(aggregateBefore.Executions))
	gm.Expect(ql.NativeAggregateExecutionStats().Executions).To(gm.Equal(nativeBefore.Executions))
}
