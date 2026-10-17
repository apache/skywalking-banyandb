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
	_ "embed"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	ql "github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	measuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

//go:embed qlgen/generated/suite.json
var generatedQLSuite []byte

// RegisterGeneratedQLTable registers generated QL cases using only the QL query RPC.
func RegisterGeneratedQLTable(description string) bool {
	suite, loadErr := qlgen.Load(generatedQLSuite)
	if loadErr != nil {
		panic(fmt.Errorf("load generated QL suite: %w", loadErr))
	}
	readiness, readinessErr := generatedQLReadiness(suite, "generated_scan_all")
	if readinessErr != nil {
		panic(readinessErr)
	}
	indexedReadiness, indexedReadinessErr := generatedQLReadiness(suite, "generated_scan_indexed")
	if indexedReadinessErr != nil {
		panic(indexedReadinessErr)
	}
	t03Readiness, t03ReadinessErr := generatedQLReadiness(suite, "t03_scan_all")
	if t03ReadinessErr != nil {
		panic(t03ReadinessErr)
	}
	return g.Describe(description, g.Ordered, g.Label("measure-ql", "QL-generated"), func() {
		var base time.Time
		var executed int
		g.BeforeAll(func() {
			base = SharedContext.BaseTime.Add(-5 * time.Minute).Truncate(2 * time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
			defer cancel()
			indexRegistry := databasev1.NewIndexRuleRegistryServiceClient(SharedContext.Connection)
			for _, rule := range qlgen.FixtureIndexRules() {
				_, indexErr := indexRegistry.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{IndexRule: rule})
				gm.Expect(indexErr).NotTo(gm.HaveOccurred())
			}
			bindingRegistry := databasev1.NewIndexRuleBindingRegistryServiceClient(SharedContext.Connection)
			_, bindingErr := bindingRegistry.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
				IndexRuleBinding: qlgen.FixtureIndexBinding(),
			})
			gm.Expect(bindingErr).NotTo(gm.HaveOccurred())
			registry := databasev1.NewMeasureRegistryServiceClient(SharedContext.Connection)
			_, createErr := registry.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: qlgen.FixtureSchema()})
			gm.Expect(createErr).NotTo(gm.HaveOccurred())
			registered, getErr := registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: qlgen.FixtureSchema().GetMetadata()})
			gm.Expect(getErr).NotTo(gm.HaveOccurred())
			writes := qlgen.FixtureWrites(base)
			gm.Expect(writes).NotTo(gm.BeEmpty())
			// Registry visibility precedes the liaison's write-cache visibility. Retry
			// complete streams with identical values until every row is acknowledged.
			for index, request := range writes {
				request.Metadata = nil
				if index == 0 {
					request.Metadata = registered.GetMeasure().GetMetadata()
				}
			}
			query, expected := renderGeneratedQLCase(base, readiness)
			indexedQuery, indexedExpected := renderGeneratedQLCase(base, indexedReadiness)
			// Index bindings and measure schemas reach caches independently. Retry
			// identical writes until both scan and indexed QL see every fixture row.
			// Neither readiness nor cases issue a native measure query.
			gm.Eventually(func(inner gm.Gomega) {
				writeCtx, writeCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer writeCancel()
				inner.Expect(writeGeneratedQLFixture(writeCtx, SharedContext.Connection, writes)).To(gm.Succeed())
				queryCtx, queryCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer queryCancel()
				_, queryErr := measuredata.VerifyQL(queryCtx, SharedContext.Connection, query, expected, readiness.Ordered)
				inner.Expect(queryErr).NotTo(gm.HaveOccurred())
				_, indexedQueryErr := measuredata.VerifyQL(queryCtx, SharedContext.Connection, indexedQuery, indexedExpected, indexedReadiness.Ordered)
				inner.Expect(indexedQueryErr).NotTo(gm.HaveOccurred())
			}, flags.EventuallyTimeout, 100*time.Millisecond).Should(gm.Succeed())
			t03Schema := qlgen.T03FixtureSchema()
			_, t03CreateErr := registry.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: t03Schema})
			gm.Expect(t03CreateErr).NotTo(gm.HaveOccurred())
			t03Registered, t03GetErr := registry.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: t03Schema.GetMetadata()})
			gm.Expect(t03GetErr).NotTo(gm.HaveOccurred())
			t03Writes := qlgen.T03FixtureWrites(base)
			for index, request := range t03Writes {
				request.Metadata = nil
				if index == 0 {
					request.Metadata = t03Registered.GetMeasure().GetMetadata()
				}
			}
			t03Query, t03Expected := renderGeneratedQLCase(base, t03Readiness)
			gm.Eventually(func(inner gm.Gomega) {
				writeCtx, writeCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer writeCancel()
				inner.Expect(writeGeneratedQLFixture(writeCtx, SharedContext.Connection, t03Writes)).To(gm.Succeed())
				_, t03QueryErr := measuredata.VerifyQL(writeCtx, SharedContext.Connection, t03Query, t03Expected, true)
				inner.Expect(t03QueryErr).NotTo(gm.HaveOccurred())
			}, flags.EventuallyTimeout, 100*time.Millisecond).Should(gm.Succeed())
			g.GinkgoWriter.Printf("generated QL suite: seed=%d cases=%d fixture=%s\n", suite.Seed, len(suite.Cases), suite.FixtureDigest)
		})
		g.AfterAll(func() {
			g.GinkgoWriter.Printf("generated QL cases executed=%d (registered=%d)\n", executed, len(suite.Cases))
		})
		for _, testCase := range suite.Cases {
			labels := []string{}
			if testCase.Ticket != "" {
				labels = append(labels, "QLB-"+testCase.Ticket)
			}
			g.It(testCase.Name, g.Label(labels...), func() {
				executed++
				if testCase.Ticket == "T03" {
					gm.Expect(vectorized.QueryMetricsEnabled).To(gm.BeTrue(), "QLB-T03 clock/counter verification requires -tags=querymetrics")
					before := vmeasure.BatchExecutionStats()
					memoryBefore := vectorized.QueryMemoryStats()
					defer func() {
						after := vmeasure.BatchExecutionStats()
						gm.Expect(after.IntermediateDataPoints).To(gm.Equal(before.IntermediateDataPoints))
						gm.Expect(after.LiveResults).To(gm.Equal(before.LiveResults))
						gm.Expect(vectorized.QueryMemoryStats().LiveReservedBytes).To(gm.Equal(memoryBefore.LiveReservedBytes))
						if testCase.NoScan {
							gm.Expect(after.NativeLeafCalls).To(gm.Equal(before.NativeLeafCalls), "Empty, LIMIT 0 and binding errors must skip native scans")
						}
					}()
				}
				if testCase.ClockMillis != nil {
					var clockCalls atomic.Uint64
					restore := ql.SetExecutionClockForTest(func() time.Time {
						clockCalls.Add(1)
						return base.Add(time.Duration(*testCase.ClockMillis) * time.Millisecond)
					})
					defer restore()
					defer func() { gm.Expect(clockCalls.Load()).To(gm.Equal(uint64(1)), "one execution clock capture per RPC") }()
				}
				query, expected := renderGeneratedQLCase(base, testCase)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				g.GinkgoWriter.Printf("generated QL features=%v\n", testCase.Features)
				if testCase.WantCode != codes.OK {
					gm.Expect(measuredata.VerifyQLError(ctx, SharedContext.Connection, query, testCase.WantCode)).To(gm.Succeed())
					return
				}
				response, queryErr := measuredata.VerifyQL(ctx, SharedContext.Connection, query, expected, testCase.Ordered)
				gm.Expect(queryErr).NotTo(gm.HaveOccurred())
				if testCase.Trace {
					gm.Expect(response.GetTrace().GetSpans()).NotTo(gm.BeEmpty(), "QL trace requested but no spans returned")
				}
			})
		}
		registerGeneratedQLReuseScenarios(suite, &base)
		registerGeneratedQLAggregateScenarios(suite, &base)
		registerGeneratedQLNativeAggregateScenarios(suite, &base)
		registerGeneratedQLExpressionScenarios(suite, &base)
		registerGeneratedQLFilterScenarios(suite, &base)
		registerGeneratedQLLegacyCompatibility(suite, &base)
	})
}

func generatedQLReadiness(suite qlgen.Suite, name string) (qlgen.Case, error) {
	for _, testCase := range suite.Cases {
		if testCase.Name == name && testCase.WantCode == codes.OK {
			return testCase, nil
		}
	}
	return qlgen.Case{}, fmt.Errorf("generated QL suite lacks its successful full-fixture readiness case %q", name)
}

func renderGeneratedQLCase(base time.Time, testCase qlgen.Case) (string, []byte) {
	query, queryErr := qlgen.Render(testCase.Query, base)
	gm.Expect(queryErr).NotTo(gm.HaveOccurred())
	expected, expectedErr := qlgen.Render(string(testCase.WantJSON), base)
	gm.Expect(expectedErr).NotTo(gm.HaveOccurred())
	return query, []byte(expected)
}

func writeGeneratedQLFixture(ctx context.Context, conn grpc.ClientConnInterface, writes []*measurev1.WriteRequest) error {
	stream, streamErr := measurev1.NewMeasureServiceClient(conn).Write(ctx)
	if streamErr != nil {
		return fmt.Errorf("open generated QL fixture writes: %w", streamErr)
	}
	for _, request := range writes {
		if sendErr := stream.Send(proto.Clone(request).(*measurev1.WriteRequest)); sendErr != nil {
			return fmt.Errorf("send generated QL fixture row: %w", sendErr)
		}
	}
	if closeErr := stream.CloseSend(); closeErr != nil {
		return fmt.Errorf("close generated QL fixture writes: %w", closeErr)
	}
	acknowledged := 0
	for {
		response, receiveErr := stream.Recv()
		if errors.Is(receiveErr, io.EOF) {
			break
		}
		if receiveErr != nil {
			return fmt.Errorf("receive generated QL fixture acknowledgement: %w", receiveErr)
		}
		if response.GetStatus() != modelv1.Status_STATUS_SUCCEED.String() {
			return fmt.Errorf("generated QL fixture write rejected: %s", response.GetStatus())
		}
		acknowledged++
	}
	if acknowledged != len(writes) {
		return fmt.Errorf("generated QL fixture acknowledgements: got %d, want %d", acknowledged, len(writes))
	}
	return nil
}
