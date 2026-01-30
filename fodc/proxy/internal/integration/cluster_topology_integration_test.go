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
	"fmt"
	"net"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/agent/testhelper"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/api"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/cluster"
	grpcproxy "github.com/apache/skywalking-banyandb/fodc/proxy/internal/grpc"
	metricsproxy "github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

var _ = Describe("Cluster Topology Integration", func() {
	var (
		proxyGRPCAddr     string
		proxyHTTPAddr     string
		grpcServer        *grpcproxy.Server
		httpServer        *api.Server
		agentRegistry     *registry.AgentRegistry
		metricsAggregator *metricsproxy.Aggregator
		grpcService       *grpcproxy.FODCService
		clusterManager    *cluster.Manager
		flightRecorder1   interface{}
		flightRecorder2   interface{}
		proxyClient1      *testhelper.ProxyClientWrapper
		proxyClient2      *testhelper.ProxyClientWrapper
		agentCtx1         context.Context
		agentCancel1      context.CancelFunc
		agentCtx2         context.Context
		agentCancel2      context.CancelFunc
		testLogger        *logger.Logger
	)

	BeforeEach(func() {
		testLogger = logger.GetLogger("test", "integration")

		heartbeatTimeout := 5 * time.Second
		cleanupTimeout := 10 * time.Second
		heartbeatInterval := 2 * time.Second
		agentRegistry = registry.NewAgentRegistry(testLogger, heartbeatTimeout, cleanupTimeout, 100)

		// Create cluster manager for topology aggregation
		clusterManager = cluster.NewManager(agentRegistry, nil, testLogger)

		metricsAggregator = metricsproxy.NewAggregator(agentRegistry, nil, testLogger)
		grpcService = grpcproxy.NewFODCService(agentRegistry, metricsAggregator, clusterManager, testLogger, heartbeatInterval)
		metricsAggregator.SetGRPCService(grpcService)
		clusterManager.SetGRPCService(grpcService)

		grpcListener, listenErr := net.Listen("tcp", "localhost:0")
		Expect(listenErr).NotTo(HaveOccurred())
		proxyGRPCAddr = grpcListener.Addr().String()
		_ = grpcListener.Close()
		grpcServer = grpcproxy.NewServer(grpcService, proxyGRPCAddr, 4194304, testLogger)
		Expect(grpcServer.Start()).To(Succeed())

		time.Sleep(100 * time.Millisecond)

		httpListener, httpListenErr := net.Listen("tcp", "localhost:0")
		Expect(httpListenErr).NotTo(HaveOccurred())
		proxyHTTPAddr = httpListener.Addr().String()
		_ = httpListener.Close()
		httpServer = api.NewServer(metricsAggregator, nil, agentRegistry, testLogger)
		Expect(httpServer.Start(proxyHTTPAddr, 10*time.Second, 10*time.Second)).To(Succeed())

		Eventually(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s/health", proxyHTTPAddr))
			if err != nil {
				return err
			}
			resp.Body.Close()
			return nil
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Succeed())
	})

	AfterEach(func() {
		if agentCancel1 != nil {
			agentCancel1()
		}
		if agentCancel2 != nil {
			agentCancel2()
		}
		if proxyClient1 != nil {
			_ = proxyClient1.Disconnect()
		}
		if proxyClient2 != nil {
			_ = proxyClient2.Disconnect()
		}
		if httpServer != nil {
			_ = httpServer.Stop()
		}
		if grpcServer != nil {
			grpcServer.Stop()
		}
		if agentRegistry != nil {
			agentRegistry.Stop()
		}
	})

	It("should aggregate topology from multiple agents", func() {
		// Create contexts for agents
		agentCtx1, agentCancel1 = context.WithCancel(context.Background())
		agentCtx2, agentCancel2 = context.WithCancel(context.Background())

		// Create Flight Recorders for agents
		capacitySize := int64(10 * 1024 * 1024)
		flightRecorder1 = testhelper.NewFlightRecorder(capacitySize)
		flightRecorder2 = testhelper.NewFlightRecorder(capacitySize)

		// Create proxy clients for two agents
		heartbeatInterval := 2 * time.Second
		reconnectInterval := 1 * time.Second
		proxyClient1 = testhelper.NewProxyClientWrapper(
			proxyGRPCAddr,
			"node-1",
			"pod-1",
			[]string{"container-1"},
			map[string]string{"role": "role-1"},
			heartbeatInterval,
			reconnectInterval,
			flightRecorder1,
			testLogger,
		)
		Expect(proxyClient1).NotTo(BeNil())
		proxyClient2 = testhelper.NewProxyClientWrapper(
			proxyGRPCAddr,
			"node-2",
			"pod-2",
			[]string{"container-2"},
			map[string]string{"role": "role-2"},
			heartbeatInterval,
			reconnectInterval,
			flightRecorder2,
			testLogger,
		)
		Expect(proxyClient2).NotTo(BeNil())

		// Start agent 1
		Expect(proxyClient1.Connect(agentCtx1)).To(Succeed())
		Expect(proxyClient1.StartRegistrationStream(agentCtx1)).To(Succeed())
		Expect(proxyClient1.StartClusterStateStream(agentCtx1)).To(Succeed())

		// Start agent 2
		Expect(proxyClient2.Connect(agentCtx2)).To(Succeed())
		Expect(proxyClient2.StartRegistrationStream(agentCtx2)).To(Succeed())
		Expect(proxyClient2.StartClusterStateStream(agentCtx2)).To(Succeed())

		// Wait for agents to register
		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, "10s", "100ms").Should(Equal(2))

		// Simulate topology data from agents
		agents := agentRegistry.ListAgents()
		Expect(len(agents)).To(Equal(2))

		// Prepare topology data
		testTopology1 := &fodcv1.Topology{
			Nodes: []*databasev1.Node{
				{
					Metadata: &commonv1.Metadata{
						Name: "node-1",
					},
				},
			},
		}

		testTopology2 := &fodcv1.Topology{
			Nodes: []*databasev1.Node{
				{
					Metadata: &commonv1.Metadata{
						Name: "node-2",
					},
				},
			},
		}

		// Start collection in a goroutine - this sets up channels
		ctx := context.Background()
		done := make(chan *cluster.TopologyMap)
		go func() {
			done <- clusterManager.CollectClusterTopology(ctx)
		}()

		// Give it a moment to set up channels and request data
		time.Sleep(100 * time.Millisecond)

		// Update topology for both agents - this sends to collection channels
		clusterManager.UpdateClusterTopology(agents[0].AgentID, testTopology1)
		clusterManager.UpdateClusterTopology(agents[1].AgentID, testTopology2)

		// Wait for collection to complete
		var topology *cluster.TopologyMap
		select {
		case topology = <-done:
		case <-time.After(5 * time.Second):
			Fail("Collection timed out")
		}

		Expect(topology).NotTo(BeNil())
		Expect(len(topology.Nodes)).To(BeNumerically(">=", 2))

		// Check that both agents are present
		nodeNames := make(map[string]bool)
		for _, node := range topology.Nodes {
			if node.Metadata != nil {
				nodeNames[node.Metadata.Name] = true
			}
		}
		Expect(nodeNames["node-1"]).To(BeTrue())
		Expect(nodeNames["node-2"]).To(BeTrue())

		// Clean up
		agentCancel1()
		agentCancel2()
		proxyClient1.Disconnect()
		proxyClient2.Disconnect()
	})
})
