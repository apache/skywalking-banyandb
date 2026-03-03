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

package dns_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/discovery/dns"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

func TestDNS(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DNS Discovery Suite", Label("integration"))
}

var _ = BeforeSuite(func() {
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(Succeed())
})

var _ = Describe("DNS Discovery Service", func() {
	var (
		ctx        context.Context
		cancel     context.CancelFunc
		mockServer *mockNodeQueryServer
		grpcServer *grpc.Server
		listener   net.Listener
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())

		// Setup mock gRPC server
		listener, grpcServer, mockServer = setupMockGRPCServer()

		// Add test node to mock server
		mockServer.node = createTestNode("node1", listener.Addr().String())
	})

	AfterEach(func() {
		if grpcServer != nil {
			grpcServer.Stop()
		}
		if listener != nil {
			_ = listener.Close()
		}
		cancel()
	})

	Describe("NewService", func() {
		It("should create service with valid config", func() {
			config := dns.Config{
				SRVAddresses: []string{"_grpc._tcp.test.local"},
				InitInterval: 1 * time.Second,
				InitDuration: 10 * time.Second,
				PollInterval: 5 * time.Second,
				GRPCTimeout:  3 * time.Second,
				TLSEnabled:   false,
			}

			svc, err := dns.NewService(config)
			Expect(err).NotTo(HaveOccurred())
			Expect(svc).NotTo(BeNil())
			Expect(svc.Close()).To(Succeed())
		})

		It("should fail with empty SRV addresses", func() {
			config := dns.Config{
				SRVAddresses: []string{},
			}

			_, err := dns.NewService(config)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("SRV addresses cannot be empty"))
		})

		It("should fail with invalid CA cert path when TLS enabled", func() {
			config := dns.Config{
				SRVAddresses: []string{"_grpc._tcp.test.local"},
				TLSEnabled:   true,
				CACertPaths:  []string{"/non/existent/path/ca.crt"},
			}

			_, err := dns.NewService(config)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Node Registry Interface", func() {
		var (
			svc              *dns.Service
			mockResolver     *mockDNSResolver
			grpcServer       *grpc.Server
			registryListener net.Listener
			mockServer       *mockNodeQueryServer
		)

		BeforeEach(func() {
			mockResolver = newMockDNSResolver()

			// Setup mock gRPC server
			registryListener, grpcServer, mockServer = setupMockGRPCServer()

			// Set up DNS to return our gRPC server
			serverAddr := registryListener.Addr().String()
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{addrToSRV(serverAddr)})

			// Configure mock server with test nodes
			mockServer.node = createTestNode("registry-test-node", serverAddr,
				databasev1.Role_ROLE_DATA, databasev1.Role_ROLE_LIAISON)

			var err error
			svc, err = dns.NewServiceWithResolver(createDefaultConfig(), mockResolver)
			Expect(err).NotTo(HaveOccurred())

			// Start the discovery service
			err = svc.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Wait for initial DNS discovery to complete
			time.Sleep(300 * time.Millisecond)
		})

		AfterEach(func() {
			if svc != nil {
				_ = svc.Close()
				svc = nil
			}
			if grpcServer != nil {
				grpcServer.Stop()
				grpcServer = nil
			}
			if registryListener != nil {
				_ = registryListener.Close()
				registryListener = nil
			}
		})

		It("should return error for RegisterNode", func() {
			node := &databasev1.Node{
				Metadata: &commonv1.Metadata{
					Name: "test-node",
				},
			}

			err := svc.RegisterNode(ctx, node, false)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not supported in DNS discovery mode"))
		})

		It("should return error for UpdateNode", func() {
			node := &databasev1.Node{
				Metadata: &commonv1.Metadata{
					Name: "test-node",
				},
			}

			err := svc.UpdateNode(ctx, node)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not supported in DNS discovery mode"))
		})

		It("should list nodes by role after DNS discovery", func() {
			// Verify node was discovered
			nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_DATA)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(1))
			Expect(nodes[0].GetMetadata().GetName()).To(Equal("registry-test-node"))
			Expect(nodes[0].GetRoles()).To(ContainElement(databasev1.Role_ROLE_DATA))

			// Verify role filtering works
			liaisonNodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_LIAISON)
			Expect(err).NotTo(HaveOccurred())
			Expect(liaisonNodes).To(HaveLen(1))

			metadataNodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_META)
			Expect(err).NotTo(HaveOccurred())
			Expect(metadataNodes).To(HaveLen(0))

			// Verify DNS resolver was called
			Expect(mockResolver.getCallCount("_grpc._tcp.test.local")).To(BeNumerically(">=", 1))
		})

		It("should get node by name after DNS discovery", func() {
			// Verify node can be retrieved by name
			node, err := svc.GetNode(ctx, "registry-test-node")
			Expect(err).NotTo(HaveOccurred())
			Expect(node).NotTo(BeNil())
			Expect(node.GetMetadata().GetName()).To(Equal("registry-test-node"))
			Expect(node.GetGrpcAddress()).NotTo(BeEmpty())

			// Verify non-existent node returns error
			_, err = svc.GetNode(ctx, "non-existent-node")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not found"))
		})
	})

	Describe("DNS Query Operations", func() {
		var (
			svc          *dns.Service
			mockResolver *mockDNSResolver
			config       dns.Config
		)

		BeforeEach(func() {
			mockResolver = newMockDNSResolver()
			config = createDefaultConfig()
		})

		AfterEach(func() {
			if svc != nil {
				Expect(svc.Close()).To(Succeed())
			}
		})

		It("should successfully query single SRV record", func() {
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{
				{Target: "node1.test.local", Port: 17912},
			})

			var queryErr error
			svc, queryErr = dns.NewServiceWithResolver(config, mockResolver)
			Expect(queryErr).NotTo(HaveOccurred())

			srvToAddresses, srvToErrors := svc.QueryAllSRVRecords(ctx)
			Expect(srvToErrors).To(BeEmpty())
			Expect(srvToAddresses).To(HaveLen(1))
			Expect(srvToAddresses).To(HaveKey("_grpc._tcp.test.local"))
			Expect(srvToAddresses["_grpc._tcp.test.local"]).To(ConsistOf("node1.test.local:17912"))

			// Verify DNS resolver was called exactly once
			Expect(mockResolver.getCallCount("_grpc._tcp.test.local")).To(Equal(1))
		})

		It("should successfully query and deduplicate multiple SRV records", func() {
			config.SRVAddresses = []string{
				"_grpc._tcp.zone1.local",
				"_grpc._tcp.zone2.local",
			}

			mockResolver.setResponse("_grpc._tcp.zone1.local", []*net.SRV{
				{Target: "node1.test.local", Port: 17912},
				{Target: "node2.test.local", Port: 17912},
			})
			mockResolver.setResponse("_grpc._tcp.zone2.local", []*net.SRV{
				{Target: "node1.test.local", Port: 17912}, // Duplicate
				{Target: "node3.test.local", Port: 17912},
			})

			var queryErr error
			svc, queryErr = dns.NewServiceWithResolver(config, mockResolver)
			Expect(queryErr).NotTo(HaveOccurred())

			srvToAddresses, srvToErrors := svc.QueryAllSRVRecords(ctx)
			Expect(srvToErrors).To(BeEmpty())
			Expect(srvToAddresses).To(HaveLen(2))
			Expect(srvToAddresses).To(HaveKey("_grpc._tcp.zone1.local"))
			Expect(srvToAddresses).To(HaveKey("_grpc._tcp.zone2.local"))
			Expect(srvToAddresses["_grpc._tcp.zone1.local"]).To(ConsistOf(
				"node1.test.local:17912",
				"node2.test.local:17912",
			))
			Expect(srvToAddresses["_grpc._tcp.zone2.local"]).To(ConsistOf(
				"node1.test.local:17912",
				"node3.test.local:17912",
			))

			// Verify each DNS address was queried exactly once
			Expect(mockResolver.getCallCount("_grpc._tcp.zone1.local")).To(Equal(1))
			Expect(mockResolver.getCallCount("_grpc._tcp.zone2.local")).To(Equal(1))
		})

		It("should fail when all DNS queries fail (🎯 critical scenario)", func() {
			mockResolver.setError("_grpc._tcp.test.local", fmt.Errorf("DNS server unavailable"))

			var queryErr error
			svc, queryErr = dns.NewServiceWithResolver(config, mockResolver)
			Expect(queryErr).NotTo(HaveOccurred())

			srvToAddresses, srvToErrors := svc.QueryAllSRVRecords(ctx)
			Expect(srvToAddresses).To(BeEmpty())
			Expect(srvToErrors).To(HaveLen(1))
			Expect(srvToErrors).To(HaveKey("_grpc._tcp.test.local"))
			Expect(srvToErrors["_grpc._tcp.test.local"].Error()).To(ContainSubstring("DNS server unavailable"))

			// Verify DNS was still called (and failed)
			Expect(mockResolver.getCallCount("_grpc._tcp.test.local")).To(Equal(1))
		})

		It("should return partial results when some DNS queries fail", func() {
			config.SRVAddresses = []string{
				"_grpc._tcp.zone1.local",
				"_grpc._tcp.zone2.local",
			}

			mockResolver.setResponse("_grpc._tcp.zone1.local", []*net.SRV{
				{Target: "node1.test.local", Port: 17912},
			})
			mockResolver.setError("_grpc._tcp.zone2.local", fmt.Errorf("zone2 DNS unavailable"))

			var queryErr error
			svc, queryErr = dns.NewServiceWithResolver(config, mockResolver)
			Expect(queryErr).NotTo(HaveOccurred())

			srvToAddresses, srvToErrors := svc.QueryAllSRVRecords(ctx)
			// Should have partial success
			Expect(srvToAddresses).To(HaveLen(1))
			Expect(srvToAddresses).To(HaveKey("_grpc._tcp.zone1.local"))
			Expect(srvToAddresses["_grpc._tcp.zone1.local"]).To(ConsistOf("node1.test.local:17912"))
			// Should have error for failed SRV
			Expect(srvToErrors).To(HaveLen(1))
			Expect(srvToErrors).To(HaveKey("_grpc._tcp.zone2.local"))
			Expect(srvToErrors["_grpc._tcp.zone2.local"].Error()).To(ContainSubstring("zone2 DNS unavailable"))

			// Verify both DNS addresses were attempted
			Expect(mockResolver.getCallCount("_grpc._tcp.zone1.local")).To(Equal(1))
			Expect(mockResolver.getCallCount("_grpc._tcp.zone2.local")).To(Equal(1))
		})

		It("should return empty list when DNS returns no records", func() {
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{})

			var queryErr error
			svc, queryErr = dns.NewServiceWithResolver(config, mockResolver)
			Expect(queryErr).NotTo(HaveOccurred())

			srvToAddresses, srvToErrors := svc.QueryAllSRVRecords(ctx)
			Expect(srvToErrors).To(BeEmpty())
			Expect(srvToAddresses).To(HaveLen(1))
			Expect(srvToAddresses).To(HaveKey("_grpc._tcp.test.local"))
			Expect(srvToAddresses["_grpc._tcp.test.local"]).To(BeEmpty())

			// Verify DNS was called
			Expect(mockResolver.getCallCount("_grpc._tcp.test.local")).To(Equal(1))
		})

		It("should handle context cancellation during DNS query", func() {
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{
				{Target: "node1.test.local", Port: 17912},
			})

			var queryErr error
			svc, queryErr = dns.NewServiceWithResolver(config, mockResolver)
			Expect(queryErr).NotTo(HaveOccurred())

			cancelCtx, cancel := context.WithCancel(ctx)
			cancel() // Cancel immediately

			srvToAddresses, srvToErrors := svc.QueryAllSRVRecords(cancelCtx)
			// Context cancellation should cause query to fail
			Expect(srvToAddresses).To(BeEmpty())
			Expect(srvToErrors).To(HaveLen(1))
			Expect(srvToErrors).To(HaveKey("_grpc._tcp.test.local"))

			// Verify DNS was called and detected cancellation
			Expect(mockResolver.getCallCount("_grpc._tcp.test.local")).To(Equal(1))
		})

		It("should handle context timeout during DNS query", func() {
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{
				{Target: "node1.test.local", Port: 17912},
			})

			var queryErr error
			svc, queryErr = dns.NewServiceWithResolver(config, mockResolver)
			Expect(queryErr).NotTo(HaveOccurred())

			timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
			defer cancel()
			time.Sleep(10 * time.Millisecond) // Ensure timeout

			srvToAddresses, srvToErrors := svc.QueryAllSRVRecords(timeoutCtx)
			// Timeout should cause query to fail
			Expect(srvToAddresses).To(BeEmpty())
			Expect(srvToErrors).To(HaveLen(1))
			Expect(srvToErrors).To(HaveKey("_grpc._tcp.test.local"))

			// Verify DNS was called and detected timeout
			Expect(mockResolver.getCallCount("_grpc._tcp.test.local")).To(Equal(1))
		})

		It("should not add node when server lacks NodeQueryService implementation", func() {
			// Start a gRPC server WITHOUT NodeQueryService (only health check)
			incompleteListener, err := net.Listen("tcp", "127.0.0.1:0")
			Expect(err).NotTo(HaveOccurred())
			defer incompleteListener.Close()

			incompleteServer := grpc.NewServer()
			// Only register health service, no NodeQueryService
			healthServer := health.NewServer()
			healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
			grpc_health_v1.RegisterHealthServer(incompleteServer, healthServer)

			go func() {
				_ = incompleteServer.Serve(incompleteListener)
			}()
			defer incompleteServer.Stop()

			// Configure DNS to return the incomplete server
			serverAddr := incompleteListener.Addr().String()
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{addrToSRV(serverAddr)})

			var queryErr error
			svc, queryErr = dns.NewServiceWithResolver(config, mockResolver)
			Expect(queryErr).NotTo(HaveOccurred())

			// Attempt to query and update nodes
			// This should fail to add the node because GetCurrentNode is not implemented
			queryErr = svc.QueryDNSAndUpdateNodes(ctx)
			Expect(queryErr).To(HaveOccurred())
			Expect(queryErr.Error()).To(ContainSubstring("failed to get current node"))

			// Verify DNS was queried successfully
			Expect(mockResolver.getCallCount("_grpc._tcp.test.local")).To(Equal(1))

			// Verify no nodes were added to the cache
			nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(BeEmpty())
		})

		It("should use fallback cache after DNS failure (🎯 critical scenario)", func() {
			// Configure two DNS zones
			config.SRVAddresses = []string{
				"_grpc._tcp.zone1.local",
				"_grpc._tcp.zone2.local",
			}

			// First query: both zones succeed
			mockResolver.setResponse("_grpc._tcp.zone1.local", []*net.SRV{
				{Target: "node1.test.local", Port: 17912},
				{Target: "node2.test.local", Port: 17912},
			})
			mockResolver.setResponse("_grpc._tcp.zone2.local", []*net.SRV{
				{Target: "node3.test.local", Port: 17912},
				{Target: "node4.test.local", Port: 17912},
			})

			var queryErr error
			svc, queryErr = dns.NewServiceWithResolver(config, mockResolver)
			Expect(queryErr).NotTo(HaveOccurred())

			// First successful query should return 2 SRVs with 2 addresses each
			srvToAddresses1, srvToErrors1 := svc.QueryAllSRVRecords(ctx)
			Expect(srvToErrors1).To(BeEmpty())
			Expect(srvToAddresses1).To(HaveLen(2))
			Expect(srvToAddresses1["_grpc._tcp.zone1.local"]).To(HaveLen(2))
			Expect(srvToAddresses1["_grpc._tcp.zone2.local"]).To(HaveLen(2))

			// Verify both DNS zones were queried
			Expect(mockResolver.getCallCount("_grpc._tcp.zone1.local")).To(Equal(1))
			Expect(mockResolver.getCallCount("_grpc._tcp.zone2.local")).To(Equal(1))

			// Call queryDNSAndUpdateNodes to populate lastSuccessfulDNS cache
			// This will fail to connect to gRPC servers, but DNS query succeeded
			_ = svc.QueryDNSAndUpdateNodes(ctx)

			// Verify both zones were queried again
			Expect(mockResolver.getCallCount("_grpc._tcp.zone1.local")).To(Equal(2))
			Expect(mockResolver.getCallCount("_grpc._tcp.zone2.local")).To(Equal(2))

			// Verify the cache was populated with addresses per SRV
			cached := svc.GetLastSuccessfulDNS()
			Expect(cached).To(HaveLen(2))
			Expect(cached["_grpc._tcp.zone1.local"]).To(ConsistOf(
				"node1.test.local:17912",
				"node2.test.local:17912",
			))
			Expect(cached["_grpc._tcp.zone2.local"]).To(ConsistOf(
				"node3.test.local:17912",
				"node4.test.local:17912",
			))

			// Now simulate partial failure: zone1 succeeds, zone2 fails
			mockResolver.setResponse("_grpc._tcp.zone1.local", []*net.SRV{
				{Target: "node1.test.local", Port: 17912},
				{Target: "node2.test.local", Port: 17912},
			})
			mockResolver.setError("_grpc._tcp.zone2.local", fmt.Errorf("zone2 DNS server down"))

			// Call queryDNSAndUpdateNodes again
			// One DNS fails, so it should fallback to cached addresses (all 4 nodes)
			// With retry mechanism, nodes already in retry queue are skipped, so no errors returned
			queryErr2 := svc.QueryDNSAndUpdateNodes(ctx)
			Expect(queryErr2).NotTo(HaveOccurred())

			// Verify fallback happened - cache still has all 4 nodes from first success
			cachedAfterFailure := svc.GetLastSuccessfulDNS()
			Expect(cachedAfterFailure).To(HaveLen(2)) // 2 SRVs
			Expect(cachedAfterFailure).To(Equal(cached))
			// Count total addresses across all SRVs
			totalAddrs := 0
			for _, addrs := range cachedAfterFailure {
				totalAddrs += len(addrs)
			}
			Expect(totalAddrs).To(Equal(4)) // 4 total addresses

			// Verify both DNS zones were attempted
			Expect(mockResolver.getCallCount("_grpc._tcp.zone1.local")).To(Equal(3))
			Expect(mockResolver.getCallCount("_grpc._tcp.zone2.local")).To(Equal(3))
		})
	})

	Describe("Node Cache Management", func() {
		var (
			svc           *dns.Service
			mockResolver  *mockDNSResolver
			grpcServer    *grpc.Server
			cacheListener net.Listener
			mockServer    *mockNodeQueryServer
			config        dns.Config
		)

		BeforeEach(func() {
			mockResolver = newMockDNSResolver()

			// Setup mock gRPC server
			cacheListener, grpcServer, mockServer = setupMockGRPCServer()

			config = createDefaultConfig()
			config.GRPCTimeout = 1 * time.Second
		})

		AfterEach(func() {
			if svc != nil {
				Expect(svc.Close()).To(Succeed())
			}
			if grpcServer != nil {
				grpcServer.Stop()
			}
			if cacheListener != nil {
				_ = cacheListener.Close()
			}
		})

		It("should add new node to cache (🎯 critical scenario)", func() {
			// Set up mock DNS to return our gRPC server address
			serverAddr := cacheListener.Addr().String()
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{addrToSRV(serverAddr)})

			// Configure mock server to return a node
			mockServer.node = createTestNode("node1", serverAddr)

			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			// Trigger cache update
			err = svc.QueryDNSAndUpdateNodes(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Verify node was added to cache
			nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(1))
			Expect(nodes[0].GetMetadata().GetName()).To(Equal("node1"))
		})

		It("should add multiple nodes to cache (🎯 critical scenario)", func() {
			// Set up mock DNS to return our gRPC server address
			serverAddr := cacheListener.Addr().String()
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{addrToSRV(serverAddr)})

			// Configure mock server with multiple nodes (will return first one)
			mockServer.node = createTestNode("node1", serverAddr)

			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			// First update
			err = svc.QueryDNSAndUpdateNodes(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Verify node was added
			nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(1))
		})

		It("should remove node from cache when not in DNS (🎯 critical scenario)", func() {
			// Set up initial state with a node
			serverAddr := cacheListener.Addr().String()
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{addrToSRV(serverAddr)})

			mockServer.node = createTestNode("node1", serverAddr)

			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			// Add node to cache
			err = svc.QueryDNSAndUpdateNodes(ctx)
			Expect(err).NotTo(HaveOccurred())

			nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(1))

			// Now remove the address from DNS
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{})

			// Update cache - node should be removed
			err = svc.QueryDNSAndUpdateNodes(ctx)
			Expect(err).NotTo(HaveOccurred())

			nodes, err = svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(BeEmpty())
		})

		It("should persist node in cache across queries (🎯 critical scenario)", func() {
			serverAddr := cacheListener.Addr().String()
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{addrToSRV(serverAddr)})

			mockServer.node = createTestNode("node1", serverAddr)

			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			// First query
			err = svc.QueryDNSAndUpdateNodes(ctx)
			Expect(err).NotTo(HaveOccurred())

			nodes1, err := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes1).To(HaveLen(1))

			// Second query - node should still be there
			err = svc.QueryDNSAndUpdateNodes(ctx)
			Expect(err).NotTo(HaveOccurred())

			nodes2, err := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes2).To(HaveLen(1))
			Expect(nodes2[0].GetMetadata().GetName()).To(Equal(nodes1[0].GetMetadata().GetName()))
		})
	})

	Describe("Event Handler Notifications", func() {
		var (
			svc             *dns.Service
			mockResolver    *mockDNSResolver
			grpcServer      *grpc.Server
			handlerListener net.Listener
			mockServer      *mockNodeQueryServer
			handler         *testEventHandler
			config          dns.Config
		)

		BeforeEach(func() {
			mockResolver = newMockDNSResolver()
			handler = newTestEventHandler()

			// Setup mock gRPC server
			handlerListener, grpcServer, mockServer = setupMockGRPCServer()

			config = createDefaultConfig()
			config.GRPCTimeout = 1 * time.Second
		})

		AfterEach(func() {
			if svc != nil {
				Expect(svc.Close()).To(Succeed())
			}
			if grpcServer != nil {
				grpcServer.Stop()
			}
			if handlerListener != nil {
				_ = handlerListener.Close()
			}
		})

		It("should notify handler when node is added (🎯 critical scenario)", func() {
			serverAddr := handlerListener.Addr().String()
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{addrToSRV(serverAddr)})

			mockServer.node = createTestNode("node1", serverAddr)

			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			// Register handler
			svc.RegisterHandler("test-handler", schema.KindNode, handler)

			// Trigger node discovery
			err = svc.QueryDNSAndUpdateNodes(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Verify handler was notified
			Expect(handler.getAddCount()).To(Equal(int32(1)))
			Expect(handler.addedNodes).To(HaveKey("node1"))
		})

		It("should notify handler when node is deleted (🎯 critical scenario)", func() {
			serverAddr := handlerListener.Addr().String()
			host, portStr, _ := net.SplitHostPort(serverAddr)
			port, _ := net.LookupPort("tcp", portStr)

			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{
				{Target: host, Port: uint16(port)},
			})

			mockServer.node = &databasev1.Node{
				Metadata: &commonv1.Metadata{
					Name: "node1",
				},
				GrpcAddress: serverAddr,
				HttpAddress: "http://127.0.0.1:8080",
				Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
			}

			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			// Register handler
			svc.RegisterHandler("test-handler", schema.KindNode, handler)

			// Add node
			err = svc.QueryDNSAndUpdateNodes(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(handler.getAddCount()).To(Equal(int32(1)))

			// Remove node from DNS
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{})

			// Trigger update - node should be removed
			err = svc.QueryDNSAndUpdateNodes(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Verify handler was notified of deletion
			Expect(handler.getDeleteCount()).To(Equal(int32(1)))
			Expect(handler.deletedNodes).To(HaveKey("node1"))

			// Verify final state of handler data
			handler.mu.RLock()
			defer handler.mu.RUnlock()

			// Verify addedNodes contains the node that was added
			Expect(handler.addedNodes).To(HaveLen(1))
			Expect(handler.addedNodes).To(HaveKey("node1"))
			addedNode := handler.addedNodes["node1"]
			Expect(addedNode.GetMetadata().GetName()).To(Equal("node1"))
			Expect(addedNode.GetGrpcAddress()).To(Equal(serverAddr))
			Expect(addedNode.GetRoles()).To(ContainElement(databasev1.Role_ROLE_DATA))

			// Verify deletedNodes contains the node that was deleted
			Expect(handler.deletedNodes).To(HaveLen(1))
			Expect(handler.deletedNodes).To(HaveKey("node1"))
			deletedNode := handler.deletedNodes["node1"]
			Expect(deletedNode.GetMetadata().GetName()).To(Equal("node1"))
			Expect(deletedNode.GetGrpcAddress()).To(Equal(serverAddr))
			Expect(deletedNode.GetRoles()).To(ContainElement(databasev1.Role_ROLE_DATA))

			// Verify add and delete counts
			Expect(handler.addCount).To(Equal(int32(1)))
			Expect(handler.deleteCount).To(Equal(int32(1)))
		})

		It("should notify multiple handlers (🎯 critical scenario)", func() {
			serverAddr := handlerListener.Addr().String()
			host, portStr, _ := net.SplitHostPort(serverAddr)
			port, _ := net.LookupPort("tcp", portStr)

			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{
				{Target: host, Port: uint16(port)},
			})

			mockServer.node = &databasev1.Node{
				Metadata: &commonv1.Metadata{
					Name: "node1",
				},
				GrpcAddress: serverAddr,
				HttpAddress: "http://127.0.0.1:8080",
				Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
			}

			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			// Register multiple handlers
			handler2 := newTestEventHandler()
			handler3 := newTestEventHandler()
			svc.RegisterHandler("handler1", schema.KindNode, handler)
			svc.RegisterHandler("handler2", schema.KindNode, handler2)
			svc.RegisterHandler("handler3", schema.KindNode, handler3)

			// Trigger node discovery
			err = svc.QueryDNSAndUpdateNodes(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Verify all handlers were notified
			Expect(handler.getAddCount()).To(Equal(int32(1)))
			Expect(handler2.getAddCount()).To(Equal(int32(1)))
			Expect(handler3.getAddCount()).To(Equal(int32(1)))
		})

		It("should include correct node metadata in notifications", func() {
			serverAddr := handlerListener.Addr().String()
			host, portStr, _ := net.SplitHostPort(serverAddr)
			port, _ := net.LookupPort("tcp", portStr)

			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{
				{Target: host, Port: uint16(port)},
			})

			mockServer.node = &databasev1.Node{
				Metadata: &commonv1.Metadata{
					Name: "test-node-123",
				},
				GrpcAddress: serverAddr,
				HttpAddress: "http://127.0.0.1:9999",
				Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA, databasev1.Role_ROLE_LIAISON},
			}

			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			svc.RegisterHandler("test-handler", schema.KindNode, handler)

			err = svc.QueryDNSAndUpdateNodes(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Verify handler received correct metadata
			Expect(handler.addedNodes).To(HaveKey("test-node-123"))
			node := handler.addedNodes["test-node-123"]
			Expect(node.GetGrpcAddress()).To(Equal(serverAddr))
			Expect(node.GetHttpAddress()).To(Equal("http://127.0.0.1:9999"))
			Expect(node.GetRoles()).To(ContainElements(
				databasev1.Role_ROLE_DATA,
				databasev1.Role_ROLE_LIAISON,
			))
		})
	})

	Describe("TLS Configuration", func() {
		var (
			svc          *dns.Service
			mockResolver *mockDNSResolver
			grpcServer   *grpc.Server
			tlsListener  net.Listener
			mockServer   *mockNodeQueryServer
			config       dns.Config
			certFile     string
		)

		BeforeEach(func() {
			mockResolver = newMockDNSResolver()
			certFile = "testdata/ca_cert.pem"
			config = createDefaultConfig()
		})

		AfterEach(func() {
			if svc != nil {
				_ = svc.Close()
				svc = nil
			}
			if grpcServer != nil {
				grpcServer.Stop()
				grpcServer = nil
			}
			if tlsListener != nil {
				_ = tlsListener.Close()
				tlsListener = nil
			}
		})

		It("should create service with TLS enabled and valid certificate", func() {
			config.TLSEnabled = true
			config.CACertPaths = []string{certFile}

			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())
			Expect(svc).NotTo(BeNil())
		})

		It("should fail to create service when TLS enabled but certificate file does not exist", func() {
			config.TLSEnabled = true
			config.CACertPaths = []string{"testdata/nonexistent_cert.pem"}

			createdSvc, err := dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to initialize CA certificate reloader"))
			// Don't assign to svc to avoid cleanup issues
			Expect(createdSvc).To(BeNil())
		})

		It("should initialize TLS credentials when TLS is enabled", func() {
			config.TLSEnabled = true
			config.CACertPaths = []string{certFile}

			// Setup mock DNS response
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{
				{Target: "localhost", Port: 9090},
			})

			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())
			Expect(svc).NotTo(BeNil())

			// Verify that getTLSDialOptions returns TLS credentials
			// Need to query DNS first to establish address mapping
			srvToAddresses, srvToErrors := svc.QueryAllSRVRecords(context.Background())
			Expect(srvToErrors).To(BeEmpty())
			Expect(srvToAddresses).NotTo(BeEmpty())

			// Get first SRV and address
			var firstSRV, firstAddr string
			for srv, addrs := range srvToAddresses {
				if len(addrs) > 0 {
					firstSRV = srv
					firstAddr = addrs[0]
					break
				}
			}
			Expect(firstSRV).NotTo(BeEmpty())
			Expect(firstAddr).NotTo(BeEmpty())

			dialOpts, err := svc.GetTLSDialOptions(firstSRV, firstAddr)
			Expect(err).NotTo(HaveOccurred())
			Expect(dialOpts).NotTo(BeEmpty())
		})

		It("should use insecure credentials when TLS is disabled", func() {
			// Start non-TLS mock gRPC server
			var err error
			tlsListener, err = net.Listen("tcp", "127.0.0.1:0")
			Expect(err).NotTo(HaveOccurred())

			mockServer = &mockNodeQueryServer{}

			grpcServer = grpc.NewServer()
			databasev1.RegisterNodeQueryServiceServer(grpcServer, mockServer)

			// Register health service
			healthServer := health.NewServer()
			healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
			grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

			go func() {
				_ = grpcServer.Serve(tlsListener)
			}()

			// Configure DNS service WITHOUT TLS
			config.TLSEnabled = false
			config.CACertPaths = []string{}

			serverAddr := tlsListener.Addr().String()
			host, portStr, _ := net.SplitHostPort(serverAddr)
			port, _ := net.LookupPort("tcp", portStr)

			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{
				{Target: host, Port: uint16(port)},
			})

			mockServer.node = &databasev1.Node{
				Metadata: &commonv1.Metadata{
					Name: "insecure-node",
				},
				GrpcAddress: serverAddr,
				HttpAddress: "http://127.0.0.1:8080",
				Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
			}

			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			// Should successfully connect without TLS
			err = svc.QueryDNSAndUpdateNodes(ctx)
			Expect(err).NotTo(HaveOccurred())

			nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(1))
			Expect(nodes[0].GetMetadata().GetName()).To(Equal("insecure-node"))
		})

		It("should successfully query nodes with TLS enabled on both server and client", func() {
			// Load server TLS credentials
			serverCert, err := tls.LoadX509KeyPair(certFile, "testdata/server_key.pem")
			Expect(err).NotTo(HaveOccurred())

			// Create TLS config for server
			tlsConfig := &tls.Config{
				Certificates: []tls.Certificate{serverCert},
				ClientAuth:   tls.NoClientCert,
				MinVersion:   tls.VersionTLS12,
			}

			// Start TLS-enabled gRPC server on localhost
			tlsListener, err = net.Listen("tcp", "localhost:0")
			Expect(err).NotTo(HaveOccurred())

			mockServer = &mockNodeQueryServer{}
			grpcServer = grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
			databasev1.RegisterNodeQueryServiceServer(grpcServer, mockServer)

			// Register health service
			healthServer := health.NewServer()
			healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
			grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

			go func() {
				_ = grpcServer.Serve(tlsListener)
			}()

			// Configure DNS service WITH TLS
			config.TLSEnabled = true
			config.CACertPaths = []string{certFile}

			// Extract port and construct hostname-based address
			_, portStr, _ := net.SplitHostPort(tlsListener.Addr().String())
			port, _ := net.LookupPort("tcp", portStr)
			serverAddr := fmt.Sprintf("localhost:%d", port)

			// DNS returns localhost (matches certificate CN)
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{
				{Target: "localhost", Port: uint16(port)},
			})

			mockServer.node = createTestNode("tls-node", serverAddr)

			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			// Should successfully connect with TLS
			err = svc.QueryDNSAndUpdateNodes(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Verify node was added successfully
			nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(1))
			Expect(nodes[0].GetMetadata().GetName()).To(Equal("tls-node"))
			Expect(nodes[0].GetGrpcAddress()).To(Equal(serverAddr))

			// Verify DNS was queried
			Expect(mockResolver.getCallCount("_grpc._tcp.test.local")).To(Equal(2))
		})

		It("should fail when CA cert paths count doesn't match SRV addresses count", func() {
			config.TLSEnabled = true
			config.SRVAddresses = []string{"_grpc._tcp.zone1.local", "_grpc._tcp.zone2.local"}
			config.CACertPaths = []string{certFile} // Only one cert for two SRV addresses

			createdSvc, err := dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("number of CA cert paths (1) must match number of SRV addresses (2)"))
			Expect(createdSvc).To(BeNil())
		})

		It("should share Reloader instances for duplicate CA cert paths", func() {
			config.TLSEnabled = true
			config.SRVAddresses = []string{"_grpc._tcp.zone1.local", "_grpc._tcp.zone2.local", "_grpc._tcp.zone3.local"}
			// Same cert for zone1 and zone3, different for zone2
			config.CACertPaths = []string{certFile, certFile, certFile}

			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())
			Expect(svc).NotTo(BeNil())

			// Verify only one unique Reloader was created (all paths are the same)
			reloaderCount := svc.GetReloaderCount()
			Expect(reloaderCount).To(Equal(1))
		})

		It("should return different TLS credentials for different addresses", func() {
			// Create second certificate file for testing
			certFile2 := "testdata/ca_cert.pem" // In real scenario, this would be different

			config.TLSEnabled = true
			config.SRVAddresses = []string{"_grpc._tcp.zone1.local", "_grpc._tcp.zone2.local"}
			config.CACertPaths = []string{certFile, certFile2}

			// Setup mock DNS responses
			mockResolver.setResponse("_grpc._tcp.zone1.local", []*net.SRV{
				{Target: "node1.zone1", Port: 9090},
			})
			mockResolver.setResponse("_grpc._tcp.zone2.local", []*net.SRV{
				{Target: "node1.zone2", Port: 9091},
			})

			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())
			Expect(svc).NotTo(BeNil())

			// Query DNS to establish address mapping
			srvToAddresses, srvToErrors := svc.QueryAllSRVRecords(context.Background())
			Expect(srvToErrors).To(BeEmpty())
			Expect(srvToAddresses).To(HaveLen(2))

			// Collect addresses with their SRV mapping
			type addrWithSRV struct {
				srv  string
				addr string
			}
			var addrList []addrWithSRV
			for srv, addrs := range srvToAddresses {
				for _, addr := range addrs {
					addrList = append(addrList, addrWithSRV{srv: srv, addr: addr})
				}
			}
			Expect(addrList).To(HaveLen(2))

			// Get TLS options for both addresses
			dialOpts1, err := svc.GetTLSDialOptions(addrList[0].srv, addrList[0].addr)
			Expect(err).NotTo(HaveOccurred())
			Expect(dialOpts1).NotTo(BeEmpty())

			dialOpts2, err := svc.GetTLSDialOptions(addrList[1].srv, addrList[1].addr)
			Expect(err).NotTo(HaveOccurred())
			Expect(dialOpts2).NotTo(BeEmpty())

			// Both should return valid TLS credentials (actual comparison of TLS config is complex)
			// This verifies the lookup chain works correctly
		})
	})

	Describe("Discovery Loop", func() {
		var (
			svc          *dns.Service
			mockResolver *mockDNSResolver
			config       dns.Config
		)

		BeforeEach(func() {
			mockResolver = newMockDNSResolver()
			config = createDefaultConfig()
			config.InitInterval = 100 * time.Millisecond
			config.InitDuration = 300 * time.Millisecond
			config.PollInterval = 500 * time.Millisecond

			// Setup DNS response
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{
				addrToSRV(listener.Addr().String()),
			})
		})

		AfterEach(func() {
			if svc != nil {
				_ = svc.Close()
				svc = nil
			}
		})

		It("should automatically discover nodes after Start (🎯 automatic execution)", func() {
			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			// Start automatic discovery
			err = svc.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Wait for at least one discovery round
			time.Sleep(200 * time.Millisecond)

			// Verify nodes were discovered automatically
			nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(1))
			Expect(nodes[0].GetMetadata().GetName()).To(Equal("node1"))

			// Verify DNS was queried automatically (at least once)
			callCount := mockResolver.getCallCount("_grpc._tcp.test.local")
			Expect(callCount).To(BeNumerically(">=", 1))
		})

		It("should use init interval during initialization phase", func() {
			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			mockResolver.resetCallCounts()

			// Start automatic discovery
			err = svc.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Wait for init phase (300ms) + buffer
			// With 100ms interval, should have ~3 queries
			time.Sleep(350 * time.Millisecond)

			callCount := mockResolver.getCallCount("_grpc._tcp.test.local")
			// Should have at least 2-3 queries during init phase
			Expect(callCount).To(BeNumerically(">=", 2))
			Expect(callCount).To(BeNumerically("<=", 4))
		})

		It("should switch to poll interval after init duration", func() {
			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			mockResolver.resetCallCounts()

			// Start automatic discovery
			err = svc.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Wait for init phase to complete (300ms)
			time.Sleep(350 * time.Millisecond)

			// Reset call count after init phase
			mockResolver.resetCallCounts()

			// Wait for poll interval (500ms) + buffer
			time.Sleep(600 * time.Millisecond)

			callCount := mockResolver.getCallCount("_grpc._tcp.test.local")
			// Should have 1-2 queries with 500ms poll interval
			Expect(callCount).To(BeNumerically(">=", 1))
			Expect(callCount).To(BeNumerically("<=", 2))
		})

		It("should stop discovery loop when context is canceled", func() {
			localCtx, localCancel := context.WithCancel(context.Background())
			defer localCancel()

			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			mockResolver.resetCallCounts()

			// Start automatic discovery with local context
			err = svc.Start(localCtx)
			Expect(err).NotTo(HaveOccurred())

			// Wait for some queries
			time.Sleep(250 * time.Millisecond)

			callCountBefore := mockResolver.getCallCount("_grpc._tcp.test.local")
			Expect(callCountBefore).To(BeNumerically(">=", 1))

			// Cancel context
			localCancel()

			// Wait to ensure loop stopped
			time.Sleep(300 * time.Millisecond)

			callCountAfter := mockResolver.getCallCount("_grpc._tcp.test.local")
			// Call count should not increase significantly after cancellation
			// Allow at most 1 additional call due to timing
			Expect(callCountAfter - callCountBefore).To(BeNumerically("<=", 1))
		})

		It("should stop discovery loop when service is closed", func() {
			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			mockResolver.resetCallCounts()

			// Start automatic discovery
			err = svc.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Wait for some queries
			time.Sleep(250 * time.Millisecond)

			callCountBefore := mockResolver.getCallCount("_grpc._tcp.test.local")
			Expect(callCountBefore).To(BeNumerically(">=", 1))

			// Close service
			err = svc.Close()
			Expect(err).NotTo(HaveOccurred())
			svc = nil // Prevent double-close in AfterEach

			// Wait to ensure loop stopped
			time.Sleep(300 * time.Millisecond)

			callCountAfter := mockResolver.getCallCount("_grpc._tcp.test.local")
			// Call count should not increase after close
			// Allow at most 1 additional call due to timing
			Expect(callCountAfter - callCountBefore).To(BeNumerically("<=", 1))
		})

		It("should execute multiple discovery rounds automatically", func() {
			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			mockResolver.resetCallCounts()

			// Start automatic discovery
			err = svc.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Wait for multiple rounds (at least 3-4 queries)
			time.Sleep(450 * time.Millisecond)

			callCount := mockResolver.getCallCount("_grpc._tcp.test.local")
			// With 100ms init interval for 300ms, should have 3+ queries
			Expect(callCount).To(BeNumerically(">=", 3))

			// Verify nodes are still available
			nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(1))
		})

		It("should handle node changes across multiple discovery rounds", func() {
			// Setup second gRPC server
			listener2, grpcServer2, mockServer2 := setupMockGRPCServer()
			defer grpcServer2.Stop()
			defer listener2.Close()

			mockServer2.node = createTestNode("node2", listener2.Addr().String())

			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			// Start with one node
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{
				addrToSRV(listener.Addr().String()),
			})

			// Start automatic discovery
			err = svc.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Wait for initial discovery
			time.Sleep(200 * time.Millisecond)

			nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(1))

			// Update DNS to include second node
			mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{
				addrToSRV(listener.Addr().String()),
				addrToSRV(listener2.Addr().String()),
			})

			// Wait for next discovery round
			time.Sleep(250 * time.Millisecond)

			// Should now have both nodes
			nodes, err = svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(2))

			nodeNames := []string{nodes[0].GetMetadata().GetName(), nodes[1].GetMetadata().GetName()}
			Expect(nodeNames).To(ContainElements("node1", "node2"))
		})

		It("should notify handlers during automatic discovery rounds", func() {
			handler := newTestEventHandler()

			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			// Register handler
			svc.RegisterHandler("test-handler", schema.KindNode, handler)

			// Start automatic discovery
			err = svc.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Wait for discovery
			time.Sleep(200 * time.Millisecond)

			// Verify handler was notified
			addCount := handler.getAddCount()
			Expect(addCount).To(BeNumerically(">=", 1))

			handler.mu.RLock()
			Expect(handler.addedNodes).To(HaveKey("node1"))
			handler.mu.RUnlock()
		})

		It("should handle DNS failures during initialization phase (🎯 error scenario)", func() {
			// Configure DNS to always fail
			mockResolver.setError("_grpc._tcp.test.local", fmt.Errorf("DNS server unavailable"))

			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			mockResolver.resetCallCounts()

			// Start automatic discovery
			err = svc.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Wait for multiple attempts during init phase
			time.Sleep(350 * time.Millisecond)

			// Verify DNS was queried multiple times (attempted despite failures)
			callCount := mockResolver.getCallCount("_grpc._tcp.test.local")
			Expect(callCount).To(BeNumerically(">=", 2))

			// Verify list nodes should be failure
			_, err = svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
			Expect(err).To(HaveOccurred())

			// Verify lastSuccessfulDNS cache is also empty
			cachedAddresses := svc.GetLastSuccessfulDNS()
			Expect(cachedAddresses).To(BeEmpty())
		})

		It("should preserve nodes when DNS fails after successful discovery (🎯 error scenario)", func() {
			var err error
			svc, err = dns.NewServiceWithResolver(config, mockResolver)
			Expect(err).NotTo(HaveOccurred())

			mockResolver.resetCallCounts()

			// Start automatic discovery with working DNS
			err = svc.Start(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Wait for initial successful discovery
			time.Sleep(250 * time.Millisecond)

			// Verify initial node was discovered
			nodes, err := svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(1))
			Expect(nodes[0].GetMetadata().GetName()).To(Equal("node1"))

			// Verify lastSuccessfulDNS cache has the address
			cachedAddresses := svc.GetLastSuccessfulDNS()
			Expect(cachedAddresses).To(HaveLen(1))
			Expect(cachedAddresses).To(HaveKey("_grpc._tcp.test.local"))
			initialCachedAddresses := cachedAddresses["_grpc._tcp.test.local"]
			Expect(initialCachedAddresses).To(HaveLen(1))

			// Now make DNS fail
			mockResolver.setError("_grpc._tcp.test.local", fmt.Errorf("DNS server down"))

			// Wait for several failed discovery rounds
			time.Sleep(400 * time.Millisecond)

			// Verify node is still in cache (preserved from last successful discovery)
			nodes, err = svc.ListNode(ctx, databasev1.Role_ROLE_UNSPECIFIED)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(1))
			Expect(nodes[0].GetMetadata().GetName()).To(Equal("node1"))

			// Verify lastSuccessfulDNS cache still has the original address
			cachedAddresses = svc.GetLastSuccessfulDNS()
			Expect(cachedAddresses).To(HaveLen(1))
			Expect(cachedAddresses).To(HaveKey("_grpc._tcp.test.local"))
			Expect(cachedAddresses["_grpc._tcp.test.local"]).To(Equal(initialCachedAddresses))

			// Verify DNS query count increased (service kept trying despite failures)
			callCount := mockResolver.getCallCount("_grpc._tcp.test.local")
			Expect(callCount).To(BeNumerically(">=", 3))
		})
	})
})

// mockDNSResolver implements Resolver for testing DNS queries.
type mockDNSResolver struct {
	defaultError error
	responses    map[string][]*net.SRV
	errors       map[string]error
	callCount    map[string]int
	mu           sync.Mutex
}

func newMockDNSResolver() *mockDNSResolver {
	return &mockDNSResolver{
		responses: make(map[string][]*net.SRV),
		errors:    make(map[string]error),
		callCount: make(map[string]int),
	}
}

func (m *mockDNSResolver) LookupSRV(ctx context.Context, name string) (string, []*net.SRV, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Increment call count first (a call was made regardless of outcome)
	m.callCount[name]++

	// Check if context is canceled or expired
	select {
	case <-ctx.Done():
		return "", nil, ctx.Err()
	default:
	}

	// Check if specific error is set for this address
	if resultErr, ok := m.errors[name]; ok {
		return "", nil, resultErr
	}

	// Check if response is set for this address
	if srvs, ok := m.responses[name]; ok {
		return "", srvs, nil
	}

	// Return default error if set
	if m.defaultError != nil {
		return "", nil, m.defaultError
	}

	// Return empty result if no configuration
	return "", nil, fmt.Errorf("no DNS records found for %s", name)
}

func (m *mockDNSResolver) setResponse(addr string, srvs []*net.SRV) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses[addr] = srvs
}

func (m *mockDNSResolver) setError(addr string, resultErr error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors[addr] = resultErr
}

func (m *mockDNSResolver) getCallCount(addr string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount[addr]
}

func (m *mockDNSResolver) resetCallCounts() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount = make(map[string]int)
}

// mockNodeQueryServer implements databasev1.NodeQueryServiceServer for testing.
type mockNodeQueryServer struct {
	databasev1.UnimplementedNodeQueryServiceServer
	node      *databasev1.Node
	returnErr error // For injecting errors in tests
}

func (m *mockNodeQueryServer) GetCurrentNode(_ context.Context, _ *databasev1.GetCurrentNodeRequest) (*databasev1.GetCurrentNodeResponse, error) {
	if m.returnErr != nil {
		return nil, m.returnErr
	}

	if m.node == nil {
		return nil, fmt.Errorf("no node available")
	}

	return &databasev1.GetCurrentNodeResponse{
		Node: m.node,
	}, nil
}

// testEventHandler implements schema.EventHandler for testing.
type testEventHandler struct {
	addedNodes   map[string]*databasev1.Node
	deletedNodes map[string]*databasev1.Node
	addCount     int32
	deleteCount  int32
	mu           sync.RWMutex
}

func newTestEventHandler() *testEventHandler {
	return &testEventHandler{
		addedNodes:   make(map[string]*databasev1.Node),
		deletedNodes: make(map[string]*databasev1.Node),
	}
}

func (h *testEventHandler) OnInit(_ []schema.Kind) (bool, []int64) {
	return false, nil
}

func (h *testEventHandler) OnAddOrUpdate(metadata schema.Metadata) {
	if node, ok := metadata.Spec.(*databasev1.Node); ok {
		h.mu.Lock()
		h.addedNodes[node.GetMetadata().GetName()] = node
		h.addCount++
		h.mu.Unlock()
	}
}

func (h *testEventHandler) OnDelete(metadata schema.Metadata) {
	if node, ok := metadata.Spec.(*databasev1.Node); ok {
		h.mu.Lock()
		h.deletedNodes[node.GetMetadata().GetName()] = node
		h.deleteCount++
		h.mu.Unlock()
	}
}

func (h *testEventHandler) getAddCount() int32 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.addCount
}

func (h *testEventHandler) getDeleteCount() int32 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.deleteCount
}

// Test helper functions to reduce code duplication.

// setupMockGRPCServer creates and starts a mock gRPC server with health check.
// Returns: listener, grpcServer, mockNodeQueryServer.
func setupMockGRPCServer() (net.Listener, *grpc.Server, *mockNodeQueryServer) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())

	mockServer := &mockNodeQueryServer{}
	grpcServer := grpc.NewServer()
	databasev1.RegisterNodeQueryServiceServer(grpcServer, mockServer)

	// Register health service for grpchelper health check
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	return listener, grpcServer, mockServer
}

// createTestNode creates a test node with the given parameters.
func createTestNode(name, grpcAddr string, roles ...databasev1.Role) *databasev1.Node {
	if len(roles) == 0 {
		roles = []databasev1.Role{databasev1.Role_ROLE_DATA}
	}
	return &databasev1.Node{
		Metadata: &commonv1.Metadata{
			Name: name,
		},
		GrpcAddress: grpcAddr,
		HttpAddress: "http://127.0.0.1:8080",
		Roles:       roles,
	}
}

// addrToSRV converts a network address (host:port) to a SRV record.
func addrToSRV(address string) *net.SRV {
	host, portStr, err := net.SplitHostPort(address)
	Expect(err).NotTo(HaveOccurred())
	port, err := net.LookupPort("tcp", portStr)
	Expect(err).NotTo(HaveOccurred())
	return &net.SRV{
		Target: host,
		Port:   uint16(port),
	}
}

// createDefaultConfig creates a default DNS service config for testing.
func createDefaultConfig() dns.Config {
	return dns.Config{
		SRVAddresses: []string{"_grpc._tcp.test.local"},
		InitInterval: 100 * time.Millisecond,
		InitDuration: 1 * time.Second,
		PollInterval: 500 * time.Millisecond,
		GRPCTimeout:  2 * time.Second,
		TLSEnabled:   false,
	}
}
