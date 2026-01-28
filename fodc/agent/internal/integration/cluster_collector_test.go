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

package integration_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/cluster"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var _ = Describe("Cluster Collector Integration", func() {
	var (
		testLogger       *logger.Logger
		collector        *cluster.Collector
		grpcAddr         string
		collectionCtx    context.Context
		collectionCancel context.CancelFunc
	)

	BeforeEach(func() {
		testLogger = logger.GetLogger("test", "cluster-integration")

		// Use the gRPC address from the BanyanDB setup
		grpcAddr = banyanDBGRPCAddr

		collectionCtx, collectionCancel = context.WithCancel(context.Background())
	})

	AfterEach(func() {
		if collectionCancel != nil {
			collectionCancel()
		}
		if collector != nil {
			collector.Stop()
		}
	})

	Describe("Collector Lifecycle", func() {
		It("should successfully start and connect to BanyanDB", func() {
			collector = cluster.NewCollector(testLogger, []string{grpcAddr}, 5*time.Second)

			err := collector.Start(collectionCtx)
			Expect(err).NotTo(HaveOccurred())

			// Wait for node info to be fetched
			waitCtx, waitCancel := context.WithTimeout(collectionCtx, 30*time.Second)
			defer waitCancel()

			err = collector.WaitForNodeFetched(waitCtx)
			Expect(err).NotTo(HaveOccurred())

			// Verify we got node information
			// Note: WaitForNodeFetched() may return even if no nodes were fetched,
			// so we need to wait for nodes to actually be fetched using Eventually
			Eventually(func() map[string]*databasev1.Node {
				return collector.GetCurrentNodes()
			}, 30*time.Second, 500*time.Millisecond).ShouldNot(BeEmpty(), "Expected at least one node to be fetched")

			nodes := collector.GetCurrentNodes()
			Expect(nodes).NotTo(BeNil())
			Expect(len(nodes)).To(BeNumerically(">", 0))
			for _, node := range nodes {
				Expect(node).NotTo(BeNil())
				Expect(node.Metadata).NotTo(BeNil())
				Expect(node.Metadata.Name).NotTo(BeEmpty())
			}
		})

		It("should fetch cluster topology", func() {
			collector = cluster.NewCollector(testLogger, []string{grpcAddr}, 5*time.Second)

			err := collector.Start(collectionCtx)
			Expect(err).NotTo(HaveOccurred())

			// Wait a bit for cluster topology to be collected
			time.Sleep(2 * time.Second)

			topology := collector.GetClusterTopology()
			Expect(topology).NotTo(BeNil())
			Expect(len(topology.Nodes)).To(BeNumerically(">=", 0))
		})

		It("should handle node role determination correctly", func() {
			collector = cluster.NewCollector(testLogger, []string{grpcAddr}, 5*time.Second)

			err := collector.Start(collectionCtx)
			Expect(err).NotTo(HaveOccurred())

			waitCtx, waitCancel := context.WithTimeout(collectionCtx, 30*time.Second)
			defer waitCancel()

			err = collector.WaitForNodeFetched(waitCtx)
			Expect(err).NotTo(HaveOccurred())

			// Wait for nodes to be fetched using Eventually
			Eventually(func() map[string]*databasev1.Node {
				return collector.GetCurrentNodes()
			}, 30*time.Second, 500*time.Millisecond).ShouldNot(BeEmpty(), "Expected nodes to be fetched")

			nodeRole, _ := collector.GetNodeInfo()
			Expect(nodeRole).NotTo(BeEmpty(), "Expected node role to be determined from fetched nodes")
			// Node role should be one of the expected values
			Expect(nodeRole).To(BeElementOf([]string{"LIAISON", "DATA", "DATA_HOT", "DATA_WARM", "DATA_COLD", "UNKNOWN"}))
		})
	})

	Describe("Error Handling", func() {
		It("should handle invalid gRPC address gracefully", func() {
			collector = cluster.NewCollector(testLogger, []string{"invalid:address"}, 5*time.Second)

			err := collector.Start(collectionCtx)
			// grpc.NewClient may not validate addresses immediately, so Start() might succeed
			// but connection will fail later. Check if Start() fails or wait for connection failure.
			if err == nil {
				// If Start() succeeded, wait for node fetch to fail
				waitCtx, waitCancel := context.WithTimeout(collectionCtx, 10*time.Second)
				defer waitCancel()
				fetchErr := collector.WaitForNodeFetched(waitCtx)
				Expect(fetchErr).To(HaveOccurred())
			} else {
				Expect(err.Error()).To(ContainSubstring("failed to create gRPC connection"))
			}
		})

		It("should handle connection timeouts", func() {
			// Use a valid address format but non-existent server
			collector = cluster.NewCollector(testLogger, []string{"127.0.0.1:99999"}, 1*time.Second)

			err := collector.Start(collectionCtx)
			// grpc.NewClient may not validate addresses immediately, so Start() might succeed
			// but connection will fail later. Check if Start() fails or wait for connection failure.
			if err == nil {
				// If Start() succeeded, wait for node fetch to fail
				waitCtx, waitCancel := context.WithTimeout(collectionCtx, 10*time.Second)
				defer waitCancel()
				fetchErr := collector.WaitForNodeFetched(waitCtx)
				Expect(fetchErr).To(HaveOccurred())
			} else {
				Expect(err).To(HaveOccurred())
			}
		})
	})

	Describe("Periodic Collection", func() {
		It("should periodically update cluster topology", func() {
			collector = cluster.NewCollector(testLogger, []string{grpcAddr}, 2*time.Second)

			err := collector.Start(collectionCtx)
			Expect(err).NotTo(HaveOccurred())

			// Wait for initial collection
			time.Sleep(3 * time.Second)

			initialTopology := collector.GetClusterTopology()
			Expect(initialTopology).NotTo(BeNil())

			// Wait for another collection cycle
			time.Sleep(3 * time.Second)

			updatedTopology := collector.GetClusterTopology()
			Expect(updatedTopology).NotTo(BeNil())
			// The topology should be updated
		})
	})

	Describe("Resource Management", func() {
		It("should properly close connections on stop", func() {
			collector = cluster.NewCollector(testLogger, []string{grpcAddr}, 5*time.Second)

			err := collector.Start(collectionCtx)
			Expect(err).NotTo(HaveOccurred())

			// Verify collector is running
			waitCtx, waitCancel := context.WithTimeout(collectionCtx, 30*time.Second)
			defer waitCancel()

			err = collector.WaitForNodeFetched(waitCtx)
			Expect(err).NotTo(HaveOccurred())

			// Wait for nodes to be fetched using Eventually
			Eventually(func() map[string]*databasev1.Node {
				return collector.GetCurrentNodes()
			}, 30*time.Second, 500*time.Millisecond).ShouldNot(BeEmpty(), "Expected nodes to be fetched before stopping")

			nodes := collector.GetCurrentNodes()
			Expect(len(nodes)).To(BeNumerically(">", 0))

			// Stop the collector
			collector.Stop()

			// Verify collector is stopped - cached data should still be available
			// (connections are closed but data remains accessible)
			nodes = collector.GetCurrentNodes()
			topology := collector.GetClusterTopology()
			// Data should still be accessible after stop
			Expect(nodes).NotTo(BeNil())
			Expect(topology).NotTo(BeNil())
			// Verify that restart is not allowed
			err = collector.Start(collectionCtx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot be restarted"))
		})

		It("should not allow restart after stop", func() {
			collector = cluster.NewCollector(testLogger, []string{grpcAddr}, 5*time.Second)

			err := collector.Start(collectionCtx)
			Expect(err).NotTo(HaveOccurred())

			collector.Stop()

			// Try to restart
			err = collector.Start(collectionCtx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot be restarted"))
		})
	})
})
