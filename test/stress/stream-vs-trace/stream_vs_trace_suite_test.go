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
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package streamvstrace provides performance comparison test between Stream and Trace models.
package streamvstrace

import (
	"context"
	"fmt"
	"testing"
	"time"

	g "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

func TestStreamVsTrace(t *testing.T) {
	gomega.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "Stream vs Trace Performance Suite", g.Label("integration", "performance", "slow"))
}

var _ = g.Describe("Stream vs Trace Performance", func() {
	g.BeforeEach(func() {
		gomega.Expect(logger.Init(logger.Logging{
			Env:   "dev",
			Level: flags.LogLevel,
		})).To(gomega.Succeed())
	})

	g.It("should setup schemas and run performance comparison", func() {
		path, _, err := test.NewSpace()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// g.DeferCleanup(deferFn)

		var ports []int
		ports, err = test.AllocateFreePorts(4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Setup BanyanDB with schema loaders
		addr, _, closerServerFunc := setup.ClosableStandaloneWithSchemaLoaders(
			path, ports,
			[]setup.SchemaLoader{NewSchemaLoader("performance-test")},
			"--logging-level", "info")

		g.DeferCleanup(func() {
			closerServerFunc()
			helpers.PrintDiskUsage(path, 5, 0)
		})

		// Wait for server to be ready
		gomega.Eventually(helpers.HealthCheck(addr, 10*time.Second, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials())),
			flags.EventuallyTimeout).Should(gomega.Succeed())

		// Create gRPC connection
		conn, err := grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer conn.Close()

		// Create clients
		streamClient := NewStreamClient(conn)
		traceClient := NewTraceClient(conn)
		// Create context for operations
		ctx := context.Background()

		// Verify schemas are created
		// Test basic connectivity

		// Verify stream group
		streamGroupExists, err := streamClient.VerifyGroup(ctx, "stream_performance_test")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(streamGroupExists).To(gomega.BeTrue())

		// Verify trace group
		traceGroupExists, err := traceClient.VerifyGroup(ctx, "trace_performance_test")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(traceGroupExists).To(gomega.BeTrue())

		// Verify stream index rules
		streamIndexRules := []string{
			"latency_index",
			"trace_id_index",
			"span_id_index",
			"parent_span_id_index",
			"operation_name_index",
			"component_index",
			"is_error_index",
		}
		var ruleExists bool
		for _, ruleName := range streamIndexRules {
			ruleExists, err = streamClient.VerifyIndexRule(ctx, "stream_performance_test", ruleName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(ruleExists).To(gomega.BeTrue())
		}

		// Verify trace index rules
		traceIndexRules := []string{
			"time_based_index",
			"latency_based_index",
		}
		for _, ruleName := range traceIndexRules {
			ruleExists, err = traceClient.VerifyIndexRule(ctx, "trace_performance_test", ruleName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(ruleExists).To(gomega.BeTrue())
		}

		// Verify stream index rule binding
		streamBindingExists, err := streamClient.VerifyIndexRuleBinding(ctx, "stream_performance_test", "segment_stream_binding")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(streamBindingExists).To(gomega.BeTrue())

		// Verify trace index rule binding
		traceBindingExists, err := traceClient.VerifyIndexRuleBinding(ctx, "trace_performance_test", "segment_trace_binding")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(traceBindingExists).To(gomega.BeTrue())

		// Verify stream schema
		streamExists, err := streamClient.VerifySchema(ctx, "stream_performance_test", "segment_stream")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(streamExists).To(gomega.BeTrue())

		// Verify trace schema
		traceExists, err := traceClient.VerifySchema(ctx, "trace_performance_test", "segment_trace")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(traceExists).To(gomega.BeTrue())

		fmt.Println("All schemas, groups, index rules, and index rule bindings verified successfully!")

		// Run performance tests
		g.By("Running Stream vs Trace performance comparison")

		// Create benchmark runner with small scale for testing
		config := DefaultBenchmarkConfig(SmallScale)
		config.TestDuration = 2 * time.Minute // Shorter duration for testing
		config.Concurrency = 5                // Lower concurrency for testing

		benchmarkRunner := NewBenchmarkRunner(config, streamClient, traceClient)

		// Run write benchmark
		err = benchmarkRunner.RunWriteBenchmark(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Compare results
		benchmarkRunner.CompareResults()
	})
})
