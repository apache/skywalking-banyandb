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
	"math/bits"
	"sync/atomic"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	ql "github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	measuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func registerGeneratedQLSortingScenarios(suite qlgen.Suite, base *time.Time) {
	g.Describe("generated typed ordering and bounded pagination", g.Ordered, g.Label("QLB-T17"), func() {
		var fixtureBase time.Time
		var calls, completed int
		verify := func(c qlgen.Case, fullSort bool) { calls++; verifyGeneratedSortingCall(fixtureBase, c, fullSort) }
		g.BeforeAll(func() {
			fixtureBase = base.Truncate(6 * time.Second)
			gm.Expect(vectorized.QueryMetricsEnabled).To(gm.BeTrue(), "QLB-T17 resource/work checks require -tags=querymetrics")
			prepareGeneratedSortingFixtures(fixtureBase, suite.SortingReadiness)
		})
		g.AfterAll(func() {
			g.GinkgoWriter.Printf("T17 generated: completed_specs=%d registered_specs=%d verification_rpcs=%d readiness_pages=%d fixture=%s\n",
				completed, len(suite.SortingScenarios)+len(suite.SortingRejections)+len(suite.SortingResources), calls,
				generatedSortingReadinessCount(suite.SortingReadiness), suite.FixtureDigest)
		})
		for _, scenario := range suite.SortingScenarios {
			g.It(scenario.Name, func() {
				liaisongrpc.ClearBydbQLPlanCachesForTest()
				before := liaisongrpc.BydbQLPlanCacheStats()
				for _, c := range scenario.Cases {
					verify(c, false)
				}
				warm := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(warm.Compiles-before.Compiles).To(gm.Equal(uint64(1)), "cold and changed bindings compile one immutable statement")
				gm.Expect(warm.Hits - before.Hits).To(gm.Equal(uint64(2)))
				func() {
					restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{ForceBypass: true})
					defer restore()
					for _, c := range scenario.Cases {
						verify(c, false)
					}
				}()
				uncached := liaisongrpc.BydbQLPlanCacheStats()
				gm.Expect(uncached.Hits).To(gm.Equal(warm.Hits))
				gm.Expect(uncached.Bypass - warm.Bypass).To(gm.Equal(uint64(3)))
				if !scenario.Cases[0].Sorting.FullSort {
					restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{Context: ql.WithFullSort})
					defer restore()
					for _, c := range scenario.Cases {
						verify(c, true)
					}
				}
				completed++
			})
		}
		for _, c := range suite.SortingRejections {
			g.It(c.Name, func() {
				verify(c, false)
				verify(suite.SortingScenarios[0].Cases[0], false)
				completed++
			})
		}
		for _, resource := range suite.SortingResources {
			g.It(resource.Case.Name, func() {
				calls++
				if resource.Control != nil {
					calls++
				}
				verifyGeneratedSortingResource(fixtureBase, resource)
				verify(suite.SortingScenarios[0].Cases[0], false)
				completed++
			})
		}
	})
}

func generatedSortingReadinessCount(fixtures []qlgen.AggregateFixture) int {
	var total int
	for _, fixture := range fixtures {
		total += len(fixture.Cases)
	}
	return total
}

func prepareGeneratedSortingFixtures(base time.Time, fixtures []qlgen.AggregateFixture) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	registry := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection)
	var readinessCalls int
	for _, fixture := range fixtures {
		schema := qlgen.T17FixtureSchema(fixture.Name)
		registered, getErr := registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: schema.GetMetadata()})
		if status.Code(getErr) == codes.NotFound {
			_, createErr := registry.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: schema})
			gm.Expect(createErr).NotTo(gm.HaveOccurred())
			registered, getErr = registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: schema.GetMetadata()})
		}
		gm.Expect(getErr).NotTo(gm.HaveOccurred())
		writes := qlgen.T17FixtureWrites(fixture.Name, base)
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
				_, queryErr := measuredata.VerifyQL(writeCtx, SharedContext.Connection, query, expected, true)
				inner.Expect(queryErr).NotTo(gm.HaveOccurred())
			}
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(gm.Succeed())
	}
	g.GinkgoWriter.Printf("T17 visibility: pages=%d actual_readiness_rpcs=%d (retry-inclusive)\n", generatedSortingReadinessCount(fixtures), readinessCalls)
}

func verifyGeneratedSortingCall(base time.Time, c qlgen.Case, forceFullSort bool) {
	before := vmeasure.BatchExecutionStats()
	memoryBefore := vectorized.QueryMemoryStats()
	sortBefore := ql.SortExecutionStats()
	defer assertGeneratedSortingCleanup(before, memoryBefore.LiveReservedBytes, sortBefore)
	var observed atomic.Pointer[ql.SortStats]
	restore := ql.SetSortExecutionHookForTest(func(_ context.Context, stats ql.SortStats) error { observed.Store(&stats); return nil })
	defer restore()
	query, expected := renderGeneratedQLCase(base, c)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if c.WantCode != codes.OK {
		gm.Expect(measuredata.VerifyQLError(ctx, SharedContext.Connection, query, c.WantCode)).To(gm.Succeed(), c.Name)
	} else {
		response, queryErr := measuredata.VerifyQL(ctx, SharedContext.Connection, query, expected, true)
		gm.Expect(queryErr).NotTo(gm.HaveOccurred(), c.Name)
		if c.Sorting.Grouped || c.Sorting.Distinct {
			for _, point := range response.GetDataPoints() {
				gm.Expect(point.GetSid()).To(gm.BeZero())
				gm.Expect(point.GetVersion()).To(gm.BeZero())
				if !c.Sorting.BucketMetadata {
					gm.Expect(point.GetTimestamp()).To(gm.BeNil())
				}
			}
		}
	}
	if c.NoScan {
		gm.Expect(vmeasure.BatchExecutionStats().NativeLeafCalls).To(gm.Equal(before.NativeLeafCalls), "invalid keys, Empty and LIMIT0 skip native scanning")
		gm.Expect(ql.SortExecutionStats().InputRows).To(gm.Equal(sortBefore.InputRows))
		return
	}
	if c.WantCode != codes.OK {
		return
	}
	after := ql.SortExecutionStats()
	gm.Expect(after.Executions - sortBefore.Executions).To(gm.Equal(uint64(1)))
	gm.Expect(after.InputRows-sortBefore.InputRows).To(gm.Equal(c.Sorting.InputRows), "ordering consumes all post-filter/group/distinct rows")
	gm.Expect(after.OutputRows - sortBefore.OutputRows).To(gm.Equal(c.Sorting.OutputRows))
	gm.Expect(after.KeyEvaluations - sortBefore.KeyEvaluations).To(gm.Equal(c.Sorting.KeyEvaluations))
	gm.Expect(after.KeyKernelRows - sortBefore.KeyKernelRows).To(gm.Equal(c.Sorting.KeyKernelRows))
	gm.Expect(after.KeyScalarEvaluations - sortBefore.KeyScalarEvaluations).To(gm.Equal(c.Sorting.KeyScalarEvaluations))
	if c.Sorting.InputRows > 0 {
		snapshot := observed.Load()
		gm.Expect(snapshot).NotTo(gm.BeNil())
		gm.Expect(snapshot.KeyRootCalls).To(gm.Equal(snapshot.Batches*c.Sorting.KeyExpressions), "each modeled key root is called once per input batch")
		gm.Expect(snapshot.SourcePins).To(gm.BeZero(), "owned candidates retain no source batch pins")
		gm.Expect(snapshot.PayloadCapacity).To(gm.BeNumerically("<=", 2*snapshot.PayloadBytes))
		candidates := c.Sorting.InputRows
		if !c.Sorting.FullSort && !forceFullSort {
			candidates = min(candidates, c.Sorting.K)
		}
		gm.Expect(snapshot.Candidates).To(gm.Equal(candidates))
		gm.Expect(snapshot.PeakCandidates).To(gm.BeNumerically("<=", candidates))
		gm.Expect(snapshot.CandidateCapacity).To(gm.BeNumerically("<=", max(uint64(16), 2*candidates)))
		gm.Expect(snapshot.FullSort).To(gm.Equal(c.Sorting.FullSort || forceFullSort))
	}
	if c.Sorting.InputRows > 1 {
		gm.Expect(after.Comparisons - sortBefore.Comparisons).To(gm.BeNumerically(">", 0))
	}
	if (c.Sorting.FullSort || forceFullSort) && c.Sorting.InputRows >= 1024 {
		bound := 4 * c.Sorting.InputRows * uint64(bits.Len64(c.Sorting.InputRows-1))
		gm.Expect(after.Comparisons - sortBefore.Comparisons).To(gm.BeNumerically("<=", bound))
	}
}

func assertGeneratedSortingCleanup(before vmeasure.BatchExecutionCounters, memoryBefore int64, sortBefore ql.SortExecutionCounters) {
	after := vmeasure.BatchExecutionStats()
	sorted := ql.SortExecutionStats()
	gm.Expect(after.IntermediateDataPoints).To(gm.Equal(before.IntermediateDataPoints))
	gm.Expect(after.LiveResults).To(gm.Equal(before.LiveResults))
	gm.Expect(after.NativeBatchExecutions).To(gm.Equal(before.NativeBatchExecutions), "readiness and correctness use only BydbQL Query")
	gm.Eventually(func() int64 { return vectorized.QueryMemoryStats().LiveReservedBytes }, flags.EventuallyTimeout, 10*time.Millisecond).Should(gm.Equal(memoryBefore))
	gm.Expect(sorted.LiveBytes).To(gm.Equal(sortBefore.LiveBytes))
	gm.Expect(sorted.ReservedBytes - sortBefore.ReservedBytes).To(gm.Equal(sorted.ReleasedBytes - sortBefore.ReleasedBytes))
	gm.Expect(sorted.SourcePins).To(gm.Equal(sortBefore.SourcePins))
}

func verifyGeneratedSortingResource(base time.Time, resource qlgen.SortingResource) {
	if resource.Control != nil {
		func() {
			restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{Context: func(ctx context.Context) context.Context {
				return vectorized.WithQueryMemoryTracker(ctx, resource.BudgetBytes)
			}})
			defer restore()
			verifyGeneratedSortingCall(base, *resource.Control, false)
		}()
	}
	before := vmeasure.BatchExecutionStats()
	memoryBefore := vectorized.QueryMemoryStats()
	sortBefore := ql.SortExecutionStats()
	defer assertGeneratedSortingCleanup(before, memoryBefore.LiveReservedBytes, sortBefore)
	var observed atomic.Bool
	restoreSort := ql.SetSortExecutionHookForTest(func(_ context.Context, stats ql.SortStats) error {
		if stats.Candidates > 0 && stats.SourcePins == 0 {
			observed.Store(true)
		}
		if resource.Cancel {
			return context.Canceled
		}
		return nil
	})
	defer restoreSort()
	restoreCache := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{Context: func(ctx context.Context) context.Context {
		if resource.FullSort {
			ctx = ql.WithFullSort(ctx)
		}
		if resource.BudgetBytes > 0 {
			ctx = vectorized.WithQueryMemoryTracker(ctx, resource.BudgetBytes)
		}
		return ctx
	}})
	defer restoreCache()
	query, _ := renderGeneratedQLCase(base, resource.Case)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	gm.Expect(measuredata.VerifyQLError(ctx, SharedContext.Connection, query, resource.Case.WantCode)).To(gm.Succeed(), resource.Case.Name)
	after := ql.SortExecutionStats()
	gm.Expect(after.AcceptedRows-sortBefore.AcceptedRows).To(gm.BeNumerically(">", 0), "resource failure follows real candidate ownership")
	if resource.Cancel {
		gm.Expect(observed.Load()).To(gm.BeTrue(), "cancellation occurs after a copied source batch")
	}
}
