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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"

	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	ql "github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	measuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func registerGeneratedQLReuseScenarios(suite qlgen.Suite, base *time.Time) {
	g.Describe("generated compiled-plan reuse processes", g.Ordered, g.Label("QLB-T05"), func() {
		g.BeforeAll(func() {
			gm.Expect(vectorized.QueryMetricsEnabled).To(gm.BeTrue(), "QLB-T05 lifecycle verification requires -tags=querymetrics")
			prepareGeneratedReuseFixture(*base)
		})
		for _, scenario := range suite.ReuseScenarios {
			g.It(scenario.Name, func() {
				before := vmeasure.BatchExecutionStats()
				memoryBefore := vectorized.QueryMemoryStats()
				defer func() {
					gm.Eventually(func(inner gm.Gomega) {
						after := vmeasure.BatchExecutionStats()
						inner.Expect(after.IntermediateDataPoints).To(gm.Equal(before.IntermediateDataPoints))
						inner.Expect(after.LiveResults).To(gm.Equal(before.LiveResults))
						inner.Expect(vectorized.QueryMemoryStats().LiveReservedBytes).To(gm.Equal(memoryBefore.LiveReservedBytes))
						stats := liaisongrpc.BydbQLPlanCacheStats()
						inner.Expect(stats.InFlight).To(gm.BeZero())
						inner.Expect(stats.Waiters).To(gm.BeZero())
						inner.Expect(stats.CompileReservedBytes).To(gm.BeZero())
						inner.Expect(stats.RetiredBytes).To(gm.BeZero())
					}, 5*time.Second, 10*time.Millisecond).Should(gm.Succeed())
				}()
				liaisongrpc.ClearBydbQLPlanCachesForTest()
				switch scenario.Kind {
				case "rebind":
					verifyReuseBindings(*base, scenario)
				case "cold-burst", "first-waiter-cancel", "all-waiters-cancel", "schema-change-during-compile":
					verifyReuseColdProcess(*base, scenario)
				case "literal-bypass", "oversized-fallback":
					verifyReuseBoundsAndBypass(*base, scenario)
				case "evict-in-flight":
					verifyReuseEviction(*base, scenario)
				default:
					verifyReuseDependencies(*base, scenario)
				}
			})
		}
	})
}

func verifyReuseBindings(base time.Time, scenario qlgen.ReuseScenario) {
	before := liaisongrpc.BydbQLPlanCacheStats()
	for _, c := range scenario.Cases {
		gm.Expect(executeGeneratedReuseCall(context.Background(), base, c)).To(gm.Succeed(), c.Name)
	}
	after := liaisongrpc.BydbQLPlanCacheStats()
	gm.Expect(after.Compiles-before.Compiles).To(gm.Equal(uint64(1)), "same text with new parameters must compile once")
	gm.Expect(after.Hits - before.Hits).To(gm.BeNumerically(">=", 1))
	gm.Expect(after.Entries).To(gm.BeNumerically(">", 0))
	restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{ForceBypass: true})
	defer restore()
	for _, c := range scenario.Cases {
		gm.Expect(executeGeneratedReuseCall(context.Background(), base, c)).To(gm.Succeed(), "uncached "+c.Name)
	}
	uncached := liaisongrpc.BydbQLPlanCacheStats()
	gm.Expect(uncached.Hits).To(gm.Equal(after.Hits))
	gm.Expect(uncached.Bypass).To(gm.BeNumerically(">", after.Bypass))
}

func executeGeneratedReuseCall(parent context.Context, base time.Time, c qlgen.Case) error {
	query, renderErr := qlgen.Render(c.Query, base)
	if renderErr != nil {
		return renderErr
	}
	expected, expectedErr := qlgen.Render(string(c.WantJSON), base)
	if expectedErr != nil {
		return expectedErr
	}
	var clockCalls atomic.Uint64
	if c.ClockMillis != nil {
		restore := ql.SetExecutionClockForTest(func() time.Time {
			clockCalls.Add(1)
			return base.Add(time.Duration(*c.ClockMillis) * time.Millisecond)
		})
		defer restore()
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	before := vmeasure.BatchExecutionStats()
	if c.WantCode != codes.OK {
		queryErr := measuredata.VerifyQLError(ctx, SharedContext.Connection, query, c.WantCode)
		if queryErr != nil {
			return queryErr
		}
		if c.NoScan && vmeasure.BatchExecutionStats().NativeLeafCalls != before.NativeLeafCalls {
			return fmt.Errorf("rejected generated QL unexpectedly issued a native scan")
		}
		return nil
	}
	_, queryErr := measuredata.VerifyQL(ctx, SharedContext.Connection, query, []byte(expected), c.Ordered)
	if queryErr != nil {
		return queryErr
	}
	if c.ClockMillis != nil && clockCalls.Load() != 1 {
		return fmt.Errorf("execution captured now %d times, want one", clockCalls.Load())
	}
	if c.NoScan && vmeasure.BatchExecutionStats().NativeLeafCalls != before.NativeLeafCalls {
		return fmt.Errorf("empty generated QL unexpectedly issued a native scan")
	}
	return nil
}

func generatedReuseQuery(base time.Time, c qlgen.Case) string {
	rendered, renderErr := qlgen.Render(c.Query, base)
	gm.Expect(renderErr).NotTo(gm.HaveOccurred())
	query, _, extractErr := helpers.ExtractQL(rendered)
	gm.Expect(extractErr).NotTo(gm.HaveOccurred())
	return query
}

func verifyReuseColdProcess(base time.Time, scenario qlgen.ReuseScenario) {
	query := generatedReuseQuery(base, scenario.Cases[0])
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	nativeSlots := make(chan struct{}, 4)
	restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{
		AfterAcquire: func(ctx context.Context, text string) {
			if scenario.Kind != "cold-burst" || text != query {
				return
			}
			// All 32 callers join one compilation and acquire separate handles.
			// Limit downstream scans to four so the default finite storage
			// query pool does not turn this cache correctness case into a
			// storage admission stress test. RPC completion releases each slot.
			select {
			case nativeSlots <- struct{}{}:
				run.Go(ctx, "ql-t05-native-slot", nil, func(taskCtx context.Context) {
					<-taskCtx.Done()
					<-nativeSlots
				})
			case <-ctx.Done():
			}
		},
		BeforeCompile: func(ctx context.Context, text string) {
			if text != query {
				return
			}
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
			}
		},
	})
	defer restore()
	before := liaisongrpc.BydbQLPlanCacheStats()
	callers := scenario.Callers
	if callers == 0 {
		callers = 2
	}
	results := make(chan error, callers)
	contexts := make([]context.Context, callers)
	cancels := make([]context.CancelFunc, callers)
	for index := range callers {
		contexts[index], cancels[index] = context.WithCancel(context.Background())
		defer cancels[index]()
	}
	launch := func(index int) {
		run.Go(contexts[index], "ql-t05-query", nil, func(taskCtx context.Context) {
			c := scenario.Cases[index%len(scenario.Cases)]
			results <- executeGeneratedReuseCall(taskCtx, base, c)
		})
	}
	launch(0)
	gm.Eventually(started, 5*time.Second).Should(gm.Receive())
	for index := 1; index < callers; index++ {
		launch(index)
	}
	gm.Eventually(func() int64 { return liaisongrpc.BydbQLPlanCacheStats().Waiters }, 5*time.Second, time.Millisecond).
		Should(gm.Equal(int64(callers)), "the cold barrier must admit every caller before release")
	switch scenario.Kind {
	case "first-waiter-cancel":
		cancels[0]()
		gm.Eventually(func() int64 { return liaisongrpc.BydbQLPlanCacheStats().Waiters }, 5*time.Second, time.Millisecond).Should(gm.Equal(int64(callers - 1)))
	case "all-waiters-cancel":
		for _, cancel := range cancels {
			cancel()
		}
		gm.Eventually(func() int64 { return liaisongrpc.BydbQLPlanCacheStats().InFlight }, 5*time.Second, time.Millisecond).Should(gm.BeZero())
	case "schema-change-during-compile":
		restoreGeneration := liaisongrpc.BumpBydbQLMeasureGenerationForTest(qlgen.FixtureGroup, qlgen.T05FixtureName, nil)
		defer restoreGeneration()
	}
	unblock.Do(func() { close(release) })
	for range callers {
		var resultErr error
		gm.Eventually(results, 20*time.Second).Should(gm.Receive(&resultErr))
		gm.Expect(resultErr).NotTo(gm.HaveOccurred())
	}
	gm.Eventually(func() int { return len(nativeSlots) }, 5*time.Second).Should(gm.BeZero())
	after := liaisongrpc.BydbQLPlanCacheStats()
	if scenario.Kind == "schema-change-during-compile" {
		gm.Expect(after.Compiles-before.Compiles).To(gm.Equal(uint64(2)), "a generation change must discard and retry the candidate once")
	} else {
		gm.Expect(after.Compiles-before.Compiles).To(gm.Equal(uint64(1)), "one shared compile per cold key")
	}
	if scenario.Kind == "cold-burst" {
		gm.Expect(after.Coalesced - before.Coalesced).To(gm.Equal(uint64(callers - 1)))
	}
	if scenario.Kind == "all-waiters-cancel" {
		gm.Eventually(func() int64 { return liaisongrpc.BydbQLPlanCacheStats().InFlight }, 5*time.Second).Should(gm.BeZero())
		gm.Expect(liaisongrpc.BydbQLPlanCacheStats().Entries).To(gm.BeZero(), "canceled compilation must never publish")
		gm.Expect(executeGeneratedReuseCall(context.Background(), base, scenario.Cases[len(scenario.Cases)-1])).To(gm.Succeed())
	}
}
