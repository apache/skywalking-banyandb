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
	"sync"
	"time"

	gm "github.com/onsi/gomega"

	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func verifyReuseEviction(base time.Time, scenario qlgen.ReuseScenario) {
	query := generatedReuseQuery(base, scenario.Cases[0])
	acquired := make(chan struct{}, 1)
	release := make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{
		MaxEntries: 1,
		AfterAcquire: func(ctx context.Context, text string) {
			if text != query {
				return
			}
			select {
			case acquired <- struct{}{}:
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
	result := make(chan error, 1)
	run.Go(context.Background(), "ql-t05-eviction-query", nil, func(taskCtx context.Context) {
		result <- executeGeneratedReuseCall(taskCtx, base, scenario.Cases[0])
	})
	gm.Eventually(acquired, 5*time.Second).Should(gm.Receive())
	gm.Expect(executeGeneratedReuseCall(context.Background(), base, scenario.Cases[1])).To(gm.Succeed())
	during := liaisongrpc.BydbQLPlanCacheStats()
	gm.Expect(during.Evictions).To(gm.BeNumerically(">", before.Evictions))
	gm.Expect(during.Entries).To(gm.BeZero(), "the retired in-use template consumes the one-plan cap, so a second plan executes uncached")
	gm.Expect(during.Oversize).To(gm.BeNumerically(">", before.Oversize))
	gm.Expect(during.RetiredBytes).To(gm.BeNumerically(">", 0), "evicted in-use template must remain accounted until its RPC returns")
	gm.Expect(during.RetainedBytes).To(gm.BeNumerically(">", during.EntryBytes))
	unblock.Do(func() { close(release) })
	var resultErr error
	gm.Eventually(result, 20*time.Second).Should(gm.Receive(&resultErr))
	gm.Expect(resultErr).NotTo(gm.HaveOccurred())
	gm.Eventually(func() int64 { return liaisongrpc.BydbQLPlanCacheStats().RetiredBytes }, 5*time.Second).Should(gm.BeZero())
	gm.Expect(executeGeneratedReuseCall(context.Background(), base, scenario.Cases[2])).To(gm.Succeed())
	gm.Expect(liaisongrpc.BydbQLPlanCacheStats().Compiles - before.Compiles).To(gm.Equal(uint64(3)))
}
