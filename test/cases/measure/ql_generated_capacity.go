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
	"os"
	"strconv"
	"strings"
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

func registerGeneratedQLCapacityScenarios(suite qlgen.Suite, base *time.Time) {
	if os.Getenv("P1_VERIFY") != "1" {
		return
	}
	g.Describe("P1 directory lists and CASE maps", g.Ordered, g.Label("QL-P1"), func() {
		var fixtureBase time.Time
		var calls, completed int
		g.BeforeAll(func() {
			fixtureBase = base.Truncate(6 * time.Second)
			prepareGeneratedCapacityFixtures(fixtureBase, suite.CapacityReadiness)
		})
		g.AfterAll(func() {
			g.GinkgoWriter.Printf("P1 generated: completed_specs=%d registered_specs=%d verification_rpcs=%d fixture=%s\n",
				completed, len(suite.CapacityScenarios), calls, suite.CapacityFixtureDigest)
		})
		for _, scenario := range suite.CapacityScenarios {
			g.It(scenario.Name, func() {
				var capacityRejections int
				verify := func(c qlgen.Case) {
					calls++
					before := vectorized.QueryMemoryStats()
					batchBefore := vmeasure.BatchExecutionStats()
					defer func() {
						gm.Eventually(func() int64 { return vectorized.QueryMemoryStats().LiveReservedBytes }, 5*time.Second).Should(gm.Equal(before.LiveReservedBytes))
						gm.Expect(vmeasure.BatchExecutionStats().LiveResults).To(gm.Equal(batchBefore.LiveResults))
					}()
					query, expected := renderGeneratedQLCase(fixtureBase, c)
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()
					_, queryErr := measuredata.VerifyQL(ctx, SharedContext.Connection, query, expected, c.Ordered)
					if status.Code(queryErr) == codes.ResourceExhausted {
						capacityRejections++
						g.GinkgoWriter.Printf("P1 capacity rejection: case=%s error=%v\n", c.Name, queryErr)
						return
					}
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
				gm.Expect(capacityRejections).To(gm.BeZero(), "P1 full-cohort correctness remains unqualified when native admission rejects")
				completed++
			})
		}
	})
}

func registerGeneratedQLShardAggregateScenarios(suite qlgen.Suite, base *time.Time) {
	if os.Getenv("P2_VERIFY") != "1" {
		return
	}
	g.Describe("P2 shard aggregates", g.Ordered, g.Label("QL-P2"), func() {
		var fixtureBase time.Time
		var calls, completed int
		g.BeforeAll(func() {
			fixtureBase = base.Truncate(6 * time.Second)
			readiness := append([]qlgen.AggregateFixture(nil), suite.CapacityReadiness...)
			readiness[0].Cases = append([]qlgen.Case(nil), readiness[0].Cases...)
			readiness[0].Cases = append(readiness[0].Cases, suite.ShardAggregateReadiness[0].Cases...)
			prepareGeneratedCapacityFixtures(fixtureBase, readiness)
		})
		g.AfterAll(func() {
			g.GinkgoWriter.Printf("P2 generated: completed_specs=%d registered_specs=%d verification_rpcs=%d fixture=%s\n",
				completed, len(suite.ShardAggregateScenarios), calls, suite.CapacityFixtureDigest)
		})
		for _, scenario := range suite.ShardAggregateScenarios {
			g.It(scenario.Name, func() {
				var capacityRejections int
				verify := func(c qlgen.Case) {
					calls++
					before := vectorized.QueryMemoryStats()
					batchBefore := vmeasure.BatchExecutionStats()
					defer func() {
						gm.Eventually(func() int64 { return vectorized.QueryMemoryStats().LiveReservedBytes }, 5*time.Second).Should(gm.Equal(before.LiveReservedBytes))
						gm.Expect(vmeasure.BatchExecutionStats().LiveResults).To(gm.Equal(batchBefore.LiveResults))
					}()
					query, expected := renderGeneratedQLCase(fixtureBase, c)
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()
					_, queryErr := measuredata.VerifyQL(ctx, SharedContext.Connection, query, expected, c.Ordered)
					if status.Code(queryErr) == codes.ResourceExhausted {
						capacityRejections++
						g.GinkgoWriter.Printf("P2 capacity rejection: case=%s error=%v\n", c.Name, queryErr)
						return
					}
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
				gm.Expect(capacityRejections).To(gm.BeZero(), "P2 full-cohort correctness remains unqualified when native admission rejects")
				completed++
			})
		}
	})
}

func prepareGeneratedCapacityFixtures(base time.Time, fixtures []qlgen.AggregateFixture) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	_, groupErr := databasev1.NewGroupRegistryServiceClient(SharedContext.Connection).Create(ctx,
		&databasev1.GroupRegistryServiceCreateRequest{Group: qlgen.P1FixtureGroup()})
	gm.Expect(groupErr == nil || status.Code(groupErr) == codes.AlreadyExists).To(gm.BeTrue(), "group registration: %v", groupErr)
	registry := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection)
	var readinessCalls int
	for _, fixture := range fixtures {
		for _, rule := range qlgen.P1IndexRules() {
			_, indexErr := databasev1.NewIndexRuleRegistryServiceClient(SharedContext.Connection).Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{IndexRule: rule})
			gm.Expect(indexErr == nil || status.Code(indexErr) == codes.AlreadyExists).To(gm.BeTrue(), "index registration: %v", indexErr)
		}
		bindingRegistry := databasev1.NewIndexRuleBindingRegistryServiceClient(SharedContext.Connection)
		_, bindingErr := bindingRegistry.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
			IndexRuleBinding: qlgen.P1IndexBinding(),
		})
		gm.Expect(bindingErr == nil || status.Code(bindingErr) == codes.AlreadyExists).To(gm.BeTrue(), "binding registration: %v", bindingErr)
		schema := qlgen.P1FixtureSchema()
		registered, getErr := registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: schema.GetMetadata()})
		if status.Code(getErr) == codes.NotFound {
			_, createErr := registry.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: schema})
			gm.Expect(createErr).NotTo(gm.HaveOccurred())
			registered, getErr = registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: schema.GetMetadata()})
		}
		gm.Expect(getErr).NotTo(gm.HaveOccurred())
		writes := qlgen.P1FixtureWrites(base)
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
				tracedQuery := strings.Replace(query, "\n#!params:", " WITH QUERY_TRACE\n#!params:", 1)
				response, queryErr := measuredata.VerifyQL(writeCtx, SharedContext.Connection, tracedQuery, expected, c.Ordered)
				inner.Expect(queryErr).NotTo(gm.HaveOccurred())
				if queryErr == nil && os.Getenv("P1_EXPECT_WORKERS") == "2" {
					workers := p1WorkerRows(response.GetTrace())
					inner.Expect(workers).To(gm.HaveLen(2), "both workers must return fixture data")
					var total int64
					for _, rows := range workers {
						total += rows
					}
					wantRows := int64(320)
					if c.Name == "p2_worker_contribution" {
						// Raw frames include two zero-value idle rows; positive filtering
						// occurs at the coordinator. More than two rows per worker proves
						// both workers contribute positive identities as well.
						wantRows = 10
						for _, rows := range workers {
							inner.Expect(rows).To(gm.BeNumerically(">", 2), "each worker must contribute positive rows")
						}
					}
					inner.Expect(total).To(gm.Equal(wantRows), "naturally partitioned shards must return exactly one copy of each page row")

					g.GinkgoWriter.Printf("P1 worker contributions: %v\n", workers)
				}
			}
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(gm.Succeed())
	}
	g.GinkgoWriter.Printf("P1 visibility: pages=%d actual_readiness_rpcs=%d (retry-inclusive)\n", generatedSortingReadinessCount(fixtures), readinessCalls)
}

func p1WorkerRows(trace *commonv1.Trace) map[string]int64 {
	workers := map[string]int64{}
	var frames func([]*commonv1.Span) int64
	frames = func(spans []*commonv1.Span) int64 {
		var rows int64
		for _, span := range spans {
			if span.Message == "frame-encode" {
				for _, tag := range span.Tags {
					if tag.Key == "rows_out" {
						value, err := strconv.ParseInt(tag.Value, 10, 64)
						gm.Expect(err).NotTo(gm.HaveOccurred())
						rows += value
					}
				}
			}
			rows += frames(span.Children)
		}
		return rows
	}
	var visit func([]*commonv1.Span)
	visit = func(spans []*commonv1.Span) {
		for _, span := range spans {
			if strings.HasPrefix(span.Message, "data-") {
				if rows := frames(span.Children); rows > 0 {
					workers[span.Message] += rows
				}
			} else {
				visit(span.Children)
			}
		}
	}
	visit(trace.GetSpans())
	return workers
}
