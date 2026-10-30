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
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	measuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func registerGeneratedQLPatternScenarios(suite qlgen.Suite, base *time.Time) {
	g.Describe("generated indexed SQL patterns", g.Ordered, g.Label("QL-patterns"), func() {
		var fixtureBase time.Time
		var calls, completed int
		g.BeforeAll(func() {
			gm.Expect(vectorized.QueryMetricsEnabled).To(gm.BeTrue(), "pattern checks require querymetrics")
			fixtureBase = base.Truncate(6 * time.Second)
			prepareGeneratedPatternFixtures(fixtureBase, suite.PatternReadiness)
		})
		g.AfterAll(func() {
			g.GinkgoWriter.Printf("Pattern generated: completed_specs=%d registered_specs=%d verification_rpcs=%d fixture=%s\n",
				completed, len(suite.PatternScenarios), calls, suite.FixtureDigest)
		})
		for _, scenario := range suite.PatternScenarios {
			g.It(scenario.Name, func() {
				verify := func(c qlgen.Case) {
					calls++
					before := vmeasure.BatchExecutionStats()
					memoryBefore := vectorized.QueryMemoryStats()
					defer func() {
						after := vmeasure.BatchExecutionStats()
						gm.Expect(after.IntermediateDataPoints).To(gm.Equal(before.IntermediateDataPoints))
						gm.Expect(after.LiveResults).To(gm.Equal(before.LiveResults))
						gm.Expect(vectorized.QueryMemoryStats().LiveReservedBytes).To(gm.Equal(memoryBefore.LiveReservedBytes))
					}()
					query, expected := renderGeneratedQLCase(fixtureBase, c)
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()
					if c.WantCode != codes.OK {
						gm.Expect(measuredata.VerifyQLError(ctx, SharedContext.Connection, query, c.WantCode)).To(gm.Succeed(), c.Name)
						return
					}
					_, queryErr := measuredata.VerifyQL(ctx, SharedContext.Connection, query, expected, c.Ordered)
					gm.Expect(queryErr).NotTo(gm.HaveOccurred(), c.Name)
				}
				liaisongrpc.ClearBydbQLPlanCachesForTest()
				for _, c := range scenario.Cases {
					verify(c)
				}
				func() {
					restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{ForceBypass: true})
					defer restore()
					for _, c := range scenario.Cases {
						verify(c)
					}
				}()
				completed++
			})
		}
	})
}

func prepareGeneratedPatternFixtures(base time.Time, fixtures []qlgen.AggregateFixture) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	registry := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection)
	var readinessCalls int
	for _, fixture := range fixtures {
		schema := qlgen.PatternFixtureSchema(fixture.Name)
		if schema.Metadata.Group != qlgen.FixtureGroup {
			_, groupErr := databasev1.NewGroupRegistryServiceClient(SharedContext.Connection).Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{Group: &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: schema.Metadata.Group}, Catalog: commonv1.Catalog_CATALOG_MEASURE,
				ResourceOpts: &commonv1.ResourceOpts{
					ShardNum: 1, SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
					Ttl: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
				},
			}})
			gm.Expect(groupErr).NotTo(gm.HaveOccurred())
		}
		for _, rule := range qlgen.PatternIndexRules(fixture.Name) {
			_, indexErr := databasev1.NewIndexRuleRegistryServiceClient(SharedContext.Connection).Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{IndexRule: rule})
			gm.Expect(indexErr).NotTo(gm.HaveOccurred())
		}
		bindingRegistry := databasev1.NewIndexRuleBindingRegistryServiceClient(SharedContext.Connection)
		_, bindingErr := bindingRegistry.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
			IndexRuleBinding: qlgen.PatternIndexBinding(fixture.Name),
		})
		gm.Expect(bindingErr).NotTo(gm.HaveOccurred())
		registered, getErr := registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: schema.GetMetadata()})
		if status.Code(getErr) == codes.NotFound {
			_, createErr := registry.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: schema})
			gm.Expect(createErr).NotTo(gm.HaveOccurred())
			registered, getErr = registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: schema.GetMetadata()})
		}
		gm.Expect(getErr).NotTo(gm.HaveOccurred())
		writes := qlgen.PatternFixtureWrites(fixture.Name, base)
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
				readinessCalls++
				_, queryErr := measuredata.VerifyQL(writeCtx, SharedContext.Connection, query, expected, c.Ordered)
				inner.Expect(queryErr).NotTo(gm.HaveOccurred())
			}
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(gm.Succeed())
	}
	g.GinkgoWriter.Printf("Pattern visibility: pages=%d actual_readiness_rpcs=%d (retry-inclusive)\n", generatedSortingReadinessCount(fixtures), readinessCalls)
}
