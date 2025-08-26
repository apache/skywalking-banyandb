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

package samedata

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	propertyrepair "github.com/apache/skywalking-banyandb/test/property_repair"
)

var (
	composeFile           string
	conn                  *grpc.ClientConn
	groupClient           databasev1.GroupRegistryServiceClient
	propertyClient        databasev1.PropertyRegistryServiceClient
	propertyServiceClient propertyv1.PropertyServiceClient
)

func TestPropertyRepairSameData(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Property Repair Same Data Test Suite", ginkgo.Label("integration", "slow", "property_repair", "same_data"))
}

var _ = ginkgo.BeforeSuite(func() {
	fmt.Println("Starting Property Repair Same Data Integration Test Suite...")

	// Disable Ryuk reaper to avoid container creation issues
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	// Set Docker host if needed (for local development)
	if os.Getenv("DOCKER_HOST") == "" {
		os.Setenv("DOCKER_HOST", "unix:///var/run/docker.sock")
	}
})

var _ = ginkgo.AfterSuite(func() {
	if conn != nil {
		_ = conn.Close()
	}
	if composeFile != "" {
		fmt.Println("Stopping compose stack...")
		propertyrepair.ExecuteComposeCommand(composeFile, "down")
	}
})

var _ = ginkgo.Describe("Property Repair Same Data Test", ginkgo.Ordered, func() {
	ginkgo.Describe("Step 1: Initial Data Load", func() {
		ginkgo.It("Should start 3 data node cluster", func() {
			// Initialize compose stack with 3-node configuration
			var err error
			composeFile, err = filepath.Abs("docker-compose-3nodes.yml")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			fmt.Printf("Using compose file: %s\n", composeFile)

			// Start the docker compose stack without waiting first
			fmt.Println("Starting services...")
			err = propertyrepair.ExecuteComposeCommand("-f", composeFile, "up", "-d")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Simple wait for services to be ready
			time.Sleep(10 * time.Second)
		})

		ginkgo.It("Should connect to liaison and setup clients", func() {
			var err error
			fmt.Println("Connecting to Liaison server...")

			conn, err = grpchelper.Conn(propertyrepair.LiaisonAddr, 30*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			fmt.Println("Connected to Liaison server successfully")

			groupClient = databasev1.NewGroupRegistryServiceClient(conn)
			propertyClient = databasev1.NewPropertyRegistryServiceClient(conn)
			propertyServiceClient = propertyv1.NewPropertyServiceClient(conn)
		})

		ginkgo.It("Should create group with 2 replicas and write 100k properties", func() {
			ctx := context.Background()

			fmt.Println("=== Step 1: Creating group with 2 replicas and loading data ===")

			// Create group with 2 replicas
			propertyrepair.CreateGroup(ctx, groupClient, 2)

			// Create property schema
			propertyrepair.CreatePropertySchema(ctx, propertyClient)

			// Write 100,000 properties (same amount across all replicas)
			fmt.Println("Starting to write 100,000 properties...")
			startTime := time.Now()

			err := propertyrepair.WriteProperties(ctx, propertyServiceClient, 0, 100000)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			duration := time.Since(startTime)
			fmt.Printf("=== Step 1 completed: wrote 100,000 properties in %v ===\n", duration)
		})
	})

	ginkgo.Describe("Verification", func() {
		ginkgo.It("Should verify the property repair completed and prometheus metrics", func() {
			fmt.Println("=== Verification: Property repair process and prometheus metrics ===")

			// Get initial metrics from all data nodes
			fmt.Println("Reading initial prometheus metrics from all data nodes...")
			beforeMetrics := propertyrepair.GetAllNodeMetrics()

			// Print initial metrics state
			fmt.Println("Initial metrics state:")
			for _, metrics := range beforeMetrics {
				gomega.Expect(metrics.IsHealthy).To(gomega.BeTrue(),
					fmt.Sprintf("Node %s should be healthy before verification: %s",
						metrics.NodeName, metrics.ErrorMessage))
				fmt.Printf("- %s: total_propagation_count=%d, repair_success_count=%d\n",
					metrics.NodeName, metrics.TotalPropagationCount, metrics.RepairSuccessCount)
			}

			fmt.Println("\n=== Triggering property repair by waiting for scheduled repair cycle ===")
			fmt.Println("Waiting for property repair to trigger (@every 10 minutes)...")

			gomega.Eventually(func() bool {
				time.Sleep(time.Second * 30)
				// Get metrics after repair
				fmt.Println("Trying to reading prometheus metrics to check repair status...")
				afterMetrics := propertyrepair.GetAllNodeMetrics()
				propertyrepair.PrintMetricsComparison(beforeMetrics, afterMetrics)

				// Check all node health, no crash
				for _, metrics := range afterMetrics {
					gomega.Expect(metrics.IsHealthy).To(gomega.BeTrue(),
						fmt.Sprintf("Node %s should be healthy after repair: %s",
							metrics.NodeName, metrics.ErrorMessage))
				}

				// For same data scenario, only verify propagation count increased (not repair count)
				// Since data is consistent, repairs may not be needed but propagation should still occur
				isPropagationActive := false
				for i, before := range beforeMetrics {
					after := afterMetrics[i]
					if before.TotalPropagationCount < after.TotalPropagationCount {
						isPropagationActive = true
						break
					}
				}
				return isPropagationActive
			}, time.Hour*2).Should(gomega.BeTrue(), "Property propagation should be active even with consistent data")
		})
	})
})
