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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	measuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func registerGeneratedQLParentChildScenarios(suite qlgen.Suite, base *time.Time) {
	g.Describe("generated parent and child query shapes", g.Ordered, g.Label("QL-parent-child"), func() {
		var fixtureBase time.Time
		var calls, completed int
		g.BeforeAll(func() {
			fixtureBase = base.Truncate(6 * time.Second)
			prepareGeneratedParentChildFixtures(fixtureBase, suite.ParentChildReadiness)
		})
		g.AfterAll(func() {
			g.GinkgoWriter.Printf("Parent/child generated: completed_specs=%d registered_specs=%d verification_rpcs=%d fixture=%s\n",
				completed, len(suite.ParentChildScenarios), calls, suite.FixtureDigest)
		})
		for _, scenario := range suite.ParentChildScenarios {
			g.It(scenario.Name, func() {
				verify := func(c qlgen.Case) {
					calls++
					query, expected := renderGeneratedQLCase(fixtureBase, c)
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()
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

func prepareGeneratedParentChildFixtures(base time.Time, fixtures []qlgen.AggregateFixture) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	registry := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection)
	var readinessCalls int
	for _, fixture := range fixtures {
		for _, rule := range qlgen.ParentChildIndexRules(fixture.Name) {
			_, indexErr := databasev1.NewIndexRuleRegistryServiceClient(SharedContext.Connection).Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{IndexRule: rule})
			gm.Expect(indexErr).NotTo(gm.HaveOccurred())
		}
		bindingRegistry := databasev1.NewIndexRuleBindingRegistryServiceClient(SharedContext.Connection)
		_, bindingErr := bindingRegistry.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
			IndexRuleBinding: qlgen.ParentChildIndexBinding(fixture.Name),
		})
		gm.Expect(bindingErr).NotTo(gm.HaveOccurred())
		schema := qlgen.ParentChildFixtureSchema(fixture.Name)
		registered, getErr := registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: schema.GetMetadata()})
		if status.Code(getErr) == codes.NotFound {
			_, createErr := registry.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: schema})
			gm.Expect(createErr).NotTo(gm.HaveOccurred())
			registered, getErr = registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: schema.GetMetadata()})
		}
		gm.Expect(getErr).NotTo(gm.HaveOccurred())
		writes := qlgen.ParentChildFixtureWrites(fixture.Name, base)
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
	g.GinkgoWriter.Printf("Parent/child visibility: pages=%d actual_readiness_rpcs=%d (retry-inclusive)\n", generatedSortingReadinessCount(fixtures), readinessCalls)
}
