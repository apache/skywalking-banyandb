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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	ql "github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	measuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func registerGeneratedHavingReviewScenarios(suite qlgen.Suite, base *time.Time) {
	for _, scenario := range suite.HavingReviewScenarios {
		g.It(scenario.Name, g.Label("QLB-T14-review"), func() {
			liaisongrpc.ClearBydbQLPlanCachesForTest()
			before := liaisongrpc.BydbQLPlanCacheStats()
			for _, c := range scenario.Cases {
				verifyGeneratedHavingReviewCall(*base, c)
			}
			warm := liaisongrpc.BydbQLPlanCacheStats()
			gm.Expect(warm.Compiles - before.Compiles).To(gm.Equal(uint64(1)))
			gm.Expect(warm.Hits - before.Hits).To(gm.Equal(uint64(2)))
			func() {
				restore := liaisongrpc.SetBydbQLPlanCacheHooksForTest(liaisongrpc.PlanCacheTestHooks{ForceBypass: true})
				defer restore()
				for _, c := range scenario.Cases {
					verifyGeneratedHavingReviewCall(*base, c)
				}
			}()
			uncached := liaisongrpc.BydbQLPlanCacheStats()
			gm.Expect(uncached.Hits).To(gm.Equal(warm.Hits))
			gm.Expect(uncached.Bypass - warm.Bypass).To(gm.Equal(uint64(3)))
			verifyGeneratedHavingCall(*base, suite.HavingScenarios[0].Cases[0], 0, false)
		})
	}
}

func prepareGeneratedHavingReviewIndexes(ctx context.Context, name string) {
	if name == qlgen.T14ContainsFixtureName {
		groups := databasev1.NewGroupRegistryServiceClient(SharedContext.Connection)
		_, groupErr := groups.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{Group: &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: name}, Catalog: commonv1.Catalog_CATALOG_MEASURE,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 1, SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
				Ttl: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
			},
		}})
		gm.Expect(groupErr).NotTo(gm.HaveOccurred())
	}
	indexRegistry := databasev1.NewIndexRuleRegistryServiceClient(SharedContext.Connection)
	for _, rule := range qlgen.T14ReviewIndexRules(name) {
		_, indexErr := indexRegistry.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{IndexRule: rule})
		gm.Expect(indexErr).NotTo(gm.HaveOccurred())
	}
	bindingRegistry := databasev1.NewIndexRuleBindingRegistryServiceClient(SharedContext.Connection)
	binding := qlgen.T14ReviewIndexBinding(name)
	_, bindingErr := bindingRegistry.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{IndexRuleBinding: binding})
	gm.Expect(bindingErr).NotTo(gm.HaveOccurred())
	registered, getErr := bindingRegistry.Get(ctx, &databasev1.IndexRuleBindingRegistryServiceGetRequest{Metadata: binding.Metadata})
	gm.Expect(getErr).NotTo(gm.HaveOccurred())
	gm.Expect(registered.GetIndexRuleBinding().GetRules()).To(gm.Equal(binding.Rules))
}

func verifyGeneratedHavingReviewCall(base time.Time, c qlgen.Case) {
	before := vmeasure.BatchExecutionStats()
	memoryBefore := vectorized.QueryMemoryStats()
	filterBefore, havingBefore := ql.FilterExecutionStats(), ql.HavingExecutionStats()
	expressionBefore := ql.ExpressionExecutionStats()
	aggregateBefore, nativeBefore := ql.AggregateExecutionStats(), ql.NativeAggregateExecutionStats()
	query, expected := renderGeneratedQLCase(base, c)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if c.WantCode == codes.OK {
		_, queryErr := measuredata.VerifyQL(ctx, SharedContext.Connection, query, expected, c.Ordered)
		gm.Expect(queryErr).NotTo(gm.HaveOccurred(), c.Name)
	} else {
		gm.Expect(measuredata.VerifyQLError(ctx, SharedContext.Connection, query, c.WantCode, c.WantMessage)).To(gm.Succeed(), c.Name)
	}
	after := vmeasure.BatchExecutionStats()
	gm.Expect(after.IntermediateDataPoints).To(gm.Equal(before.IntermediateDataPoints))
	gm.Expect(after.NativeBatchExecutions).To(gm.Equal(before.NativeBatchExecutions), "only QL query RPCs are used")
	gm.Expect(after.LiveResults).To(gm.Equal(before.LiveResults))
	gm.Expect(vectorized.QueryMemoryStats().LiveReservedBytes).To(gm.Equal(memoryBefore.LiveReservedBytes))
	for _, pair := range [][2]ql.FilterStats{{filterBefore, ql.FilterExecutionStats()}, {havingBefore, ql.HavingExecutionStats()}} {
		gm.Expect(pair[1].LiveBytes).To(gm.Equal(pair[0].LiveBytes))
		gm.Expect(pair[1].ReservedBytes - pair[0].ReservedBytes).To(gm.Equal(pair[1].ReleasedBytes - pair[0].ReleasedBytes))
	}
	gm.Expect(ql.ExpressionExecutionStats().LiveBytes).To(gm.Equal(expressionBefore.LiveBytes))
	gm.Expect(ql.AggregateExecutionStats().LiveBytes).To(gm.Equal(aggregateBefore.LiveBytes))
	gm.Expect(ql.NativeAggregateExecutionStats().LiveBytes).To(gm.Equal(nativeBefore.LiveBytes))
	if c.NoScan {
		gm.Expect(after.NativeLeafCalls).To(gm.Equal(before.NativeLeafCalls), "indexed arrays reject before data dispatch")
		gm.Expect(after.QLBatchExecutions).To(gm.Equal(before.QLBatchExecutions))
		gm.Expect(ql.FilterExecutionStats().Executions).To(gm.Equal(filterBefore.Executions))
	}
}
