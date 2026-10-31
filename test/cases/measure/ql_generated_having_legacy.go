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

	gm "github.com/onsi/gomega"

	ql "github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	measuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func verifyGeneratedLegacyHavingCall(base time.Time, c qlgen.Case) {
	before := vmeasure.BatchExecutionStats()
	memoryBefore := vectorized.QueryMemoryStats()
	filterBefore, havingBefore := ql.FilterExecutionStats(), ql.HavingExecutionStats()
	expressionBefore := ql.ExpressionExecutionStats()
	aggregateBefore, nativeBefore := ql.AggregateExecutionStats(), ql.NativeAggregateExecutionStats()
	defer func() {
		after := vmeasure.BatchExecutionStats()
		gm.Expect(after.LiveResults).To(gm.Equal(before.LiveResults))
		gm.Expect(after.QLBatchExecutions-before.QLBatchExecutions).To(gm.Equal(uint64(1)),
			"legacy QueryRPC owns one native QL batch boundary without typed residual operators")
		gm.Expect(after.IntermediateDataPoints).To(gm.Equal(before.IntermediateDataPoints))
		gm.Eventually(func() int64 { return vectorized.QueryMemoryStats().LiveReservedBytes }, flags.EventuallyTimeout, 10*time.Millisecond).Should(gm.Equal(memoryBefore.LiveReservedBytes))
		gm.Expect(ql.FilterExecutionStats().LiveBytes).To(gm.Equal(filterBefore.LiveBytes))
		gm.Expect(ql.HavingExecutionStats().LiveBytes).To(gm.Equal(havingBefore.LiveBytes))
		gm.Expect(ql.FilterExecutionStats().Executions).To(gm.Equal(filterBefore.Executions))
		gm.Expect(ql.HavingExecutionStats().Executions).To(gm.Equal(havingBefore.Executions))
		gm.Expect(ql.ExpressionExecutionStats().LiveBytes).To(gm.Equal(expressionBefore.LiveBytes))
		gm.Expect(ql.ExpressionExecutionStats().Executions).To(gm.Equal(expressionBefore.Executions))
		gm.Expect(ql.AggregateExecutionStats().Executions).To(gm.Equal(aggregateBefore.Executions))
		gm.Expect(ql.NativeAggregateExecutionStats().Executions).To(gm.Equal(nativeBefore.Executions))
	}()
	query, expected := renderGeneratedQLCase(base, c)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, queryErr := measuredata.VerifyQL(ctx, SharedContext.Connection, query, expected, true)
	gm.Expect(queryErr).NotTo(gm.HaveOccurred(), c.Name)
}
