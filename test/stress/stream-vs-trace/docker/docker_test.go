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

// Package docker provides performance comparison test between Stream and Trace models using Docker.
package docker

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	g "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	streamvstrace "github.com/apache/skywalking-banyandb/test/stress/stream-vs-trace"
)

func TestStreamVsTraceDocker(t *testing.T) {
	if os.Getenv("DOCKER_TEST") != "true" {
		t.Skip("Skipping Docker test. Set DOCKER_TEST=true to run.")
	}
	gomega.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "Stream vs Trace Performance Docker Suite", g.Label("docker", "performance", "slow"))
}

var _ = g.Describe("Stream vs Trace Performance Docker", func() {
	g.BeforeEach(func() {
		gomega.Expect(logger.Init(logger.Logging{
			Env:   "dev",
			Level: flags.LogLevel,
		})).To(gomega.Succeed())
	})

	g.It("should run performance comparison using Docker containers", func() {
		// Define connection addresses for the two containers
		streamAddr := "localhost:17912"  // Stream container gRPC port
		traceAddr := "localhost:27912"   // Trace container gRPC port

		// Wait for both containers to be ready
		g.By("Waiting for Stream container to be ready")
		gomega.Eventually(helpers.HealthCheck(streamAddr, 10*time.Second, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials())),
			2*time.Minute).Should(gomega.Succeed())

		g.By("Waiting for Trace container to be ready")
		gomega.Eventually(helpers.HealthCheck(traceAddr, 10*time.Second, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials())),
			2*time.Minute).Should(gomega.Succeed())

		// Create gRPC connections
		streamConn, err := grpchelper.Conn(streamAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer streamConn.Close()

		traceConn, err := grpchelper.Conn(traceAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer traceConn.Close()

		// Create Docker clients with exposed connections
		streamClient := NewDockerStreamClient(streamConn)
		traceClient := NewDockerTraceClient(traceConn)

		// Create context for operations
		ctx := context.Background()

		// Load schemas for both containers
		g.By("Loading schemas into containers")
		err = loadSchemasToContainer(ctx, streamClient, traceClient)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Verify schemas are loaded
		g.By("Verifying schemas in Stream container")
		streamGroupExists, err := streamClient.VerifyGroup(ctx, "stream_performance_test")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(streamGroupExists).To(gomega.BeTrue())

		streamExists, err := streamClient.VerifySchema(ctx, "stream_performance_test", "segment_stream")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(streamExists).To(gomega.BeTrue())

		g.By("Verifying schemas in Trace container")
		traceGroupExists, err := traceClient.VerifyGroup(ctx, "trace_performance_test")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(traceGroupExists).To(gomega.BeTrue())

		traceExists, err := traceClient.VerifySchema(ctx, "trace_performance_test", "segment_trace")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(traceExists).To(gomega.BeTrue())

		fmt.Println("All schemas, groups, index rules, and index rule bindings verified successfully!")

		// Run performance tests
		g.By("Running Stream vs Trace performance comparison")

		// Create benchmark runner with configuration
		config := streamvstrace.DefaultBenchmarkConfig(streamvstrace.SmallScale)
		config.TestDuration = 2 * time.Minute // Shorter duration for Docker testing
		config.Concurrency = 5                // Lower concurrency for Docker

		benchmarkRunner := streamvstrace.NewBenchmarkRunner(config, streamClient.StreamClient, traceClient.TraceClient)

		// Run write benchmark
		err = benchmarkRunner.RunWriteBenchmark(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Compare results
		benchmarkRunner.CompareResults()
	})
})

// loadSchemasToContainer loads the required schemas into both containers
func loadSchemasToContainer(ctx context.Context, streamClient *DockerStreamClient, traceClient *DockerTraceClient) error {
	// Create schema clients for both connections
	streamSchemaClient := NewSchemaClient(streamClient.conn)
	traceSchemaClient := NewSchemaClient(traceClient.conn)

	// Load stream-related schemas
	if err := streamSchemaClient.LoadStreamSchemas(ctx); err != nil {
		return fmt.Errorf("failed to load stream schemas: %w", err)
	}

	// Load trace-related schemas
	if err := traceSchemaClient.LoadTraceSchemas(ctx); err != nil {
		return fmt.Errorf("failed to load trace schemas: %w", err)
	}

	return nil
}