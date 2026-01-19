// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/fodc/agent/testhelper"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/api"
	grpcproxy "github.com/apache/skywalking-banyandb/fodc/proxy/internal/grpc"
	metricsproxy "github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

const (
	highAvailabilityAgentCount = 110
	reconnectSubsetSize        = 8
)

type agentFixture struct {
	client         *testhelper.ProxyClientWrapper
	flightRecorder interface{}
	ctx            context.Context
	cancel         context.CancelFunc
	nodeIP         string
	nodeRole       string
	nodePort       int
	startWg        sync.WaitGroup
	mu             sync.Mutex
}

var _ = Describe("High Availability and Scalability", func() {
	var (
		proxyGRPCAddr     string
		proxyHTTPAddr     string
		grpcServer        *grpcproxy.Server
		httpServer        *api.Server
		agentRegistry     *registry.AgentRegistry
		metricsAggregator *metricsproxy.Aggregator
		grpcService       *grpcproxy.FODCService
		agents            []*agentFixture
	)

	setupProxy := func() {
		testLogger := logger.GetLogger("test", "integration")
		heartbeatTimeout := 5 * time.Second
		cleanupTimeout := 10 * time.Second
		heartbeatInterval := 2 * time.Second

		agentRegistry = registry.NewAgentRegistry(testLogger, heartbeatTimeout, cleanupTimeout, 1000)
		metricsAggregator = metricsproxy.NewAggregator(agentRegistry, nil, testLogger)
		grpcService = grpcproxy.NewFODCService(agentRegistry, metricsAggregator, testLogger, heartbeatInterval)
		metricsAggregator.SetGRPCService(grpcService)

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
		httpServer = api.NewServer(metricsAggregator, agentRegistry, testLogger)
		Expect(httpServer.Start(proxyHTTPAddr, 30*time.Second, 30*time.Second)).To(Succeed())

		Eventually(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s/health", proxyHTTPAddr))
			if err != nil {
				return err
			}
			resp.Body.Close()
			return nil
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Succeed())
	}

	setupAgents := func() {
		agents = make([]*agentFixture, highAvailabilityAgentCount)
		roles := []string{"liaison", "datanode-hot", "datanode-warm", "datanode-cold"}

		for idx := 0; idx < highAvailabilityAgentCount; idx++ {
			ip := fmt.Sprintf("10.0.%d.%d", idx/256, (idx%256)+1)
			port := 8080 + idx
			role := roles[idx%len(roles)]
			ctx, cancel := context.WithCancel(context.Background())
			capacitySize := int64(2 * 1024 * 1024)
			fr := testhelper.NewFlightRecorder(capacitySize)
			client := testhelper.NewProxyClientWrapper(
				proxyGRPCAddr,
				role,
				ip+":"+strconv.Itoa(port),
				[]string{"data"},
				map[string]string{"zone": "zone-" + strconv.Itoa(idx%3)},
				2*time.Second,
				1*time.Second,
				fr,
				logger.GetLogger("test", "agent"),
			)

			Expect(client).NotTo(BeNil())

			agents[idx] = &agentFixture{
				client:         client,
				flightRecorder: fr,
				ctx:            ctx,
				cancel:         cancel,
				nodeIP:         ip,
				nodePort:       port,
				nodeRole:       role,
			}
		}
	}

	BeforeEach(func() {
		setupProxy()
		setupAgents()
	})

	AfterEach(func() {
		for _, agent := range agents {
			if agent != nil {
				agent.mu.Lock()
				if agent.cancel != nil {
					agent.cancel()
				}
				agent.mu.Unlock()
				agent.startWg.Wait()
				_ = agent.client.Disconnect()
			}
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

	It("supports 100+ concurrent agents and reconnection under load", func() {
		By("starting all agents with automatic connection and stream setup")
		for idx := range agents {
			agent := agents[idx]
			agent.mu.Lock()
			ctx := agent.ctx
			agent.mu.Unlock()
			agent.startWg.Add(1)
			go func(a *agentFixture, agentCtx context.Context) {
				defer a.startWg.Done()
				_ = a.client.Start(agentCtx)
			}(agent, ctx)
		}

		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, 15*time.Second, 200*time.Millisecond).Should(BeNumerically(">=", highAvailabilityAgentCount))

		time.Sleep(500 * time.Millisecond)

		By("populating each Flight Recorder with sample metrics")
		agentRoles := []string{"liaison", "datanode-hot", "datanode-warm", "datanode-cold"}
		for idx, agent := range agents {
			role := agentRoles[idx%len(agentRoles)]
			metric := testhelper.RawMetric{
				Name:  "ha_metric",
				Value: float64(idx + 1),
				Desc:  "High availability metric",
				Labels: []testhelper.Label{
					{Name: "agent_idx", Value: strconv.Itoa(idx)},
					{Name: "node_role", Value: role},
					{Name: "pod_name", Value: "test"},
					{Name: "container_name", Value: "data"},
				},
			}
			Expect(testhelper.UpdateMetrics(agent.flightRecorder, []testhelper.RawMetric{metric})).To(Succeed())
		}

		time.Sleep(500 * time.Millisecond)

		By("verifying on-demand collection returns metrics quickly")
		var metricsListMu sync.Mutex
		var metricsList []map[string]interface{}
		start := time.Now()
		Eventually(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s/metrics-windows", proxyHTTPAddr))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}
			var decodedMetrics []map[string]interface{}
			if decodeErr := json.NewDecoder(resp.Body).Decode(&decodedMetrics); decodeErr != nil {
				return decodeErr
			}
			if len(decodedMetrics) < highAvailabilityAgentCount {
				return fmt.Errorf("expected at least %d metrics, got %d", highAvailabilityAgentCount, len(decodedMetrics))
			}
			metricsListMu.Lock()
			metricsList = decodedMetrics
			metricsListMu.Unlock()
			return nil
		}, 30*time.Second, 500*time.Millisecond).Should(Succeed())
		duration := time.Since(start)
		Expect(duration).To(BeNumerically("<", 15*time.Second))

		Eventually(func() int {
			return metricsAggregator.ActiveCollections()
		}, 30*time.Second, 500*time.Millisecond).Should(Equal(0))

		metricsListMu.Lock()
		currentMetricsList := metricsList
		metricsListMu.Unlock()

		agentIDs := make(map[string]bool)
		for _, metric := range currentMetricsList {
			agentIDs[metric["agent_id"].(string)] = true
			labels := metric["labels"].(map[string]interface{})
			Expect(labels["node_role"]).NotTo(BeNil())
			Expect(labels["pod_name"]).NotTo(BeEmpty())
		}
		Expect(len(agentIDs)).To(Equal(highAvailabilityAgentCount))

		By("reconnecting a subset of agents under load")
		for idx := 0; idx < reconnectSubsetSize; idx++ {
			agent := agents[idx]
			agent.mu.Lock()
			oldCancel := agent.cancel
			agent.mu.Unlock()

			oldCancel()
			Expect(agent.client.Disconnect()).To(Succeed())

			agent.startWg.Wait()

			// Create a new client instance (connection manager cannot be restarted after disconnect)
			newClient := testhelper.NewProxyClientWrapper(
				proxyGRPCAddr,
				agent.nodeRole,
				"test",
				[]string{"data"},
				map[string]string{"zone": "zone-" + strconv.Itoa(idx%3)},
				2*time.Second,
				1*time.Second,
				agent.flightRecorder,
				logger.GetLogger("test", "agent"),
			)
			Expect(newClient).NotTo(BeNil())

			agent.mu.Lock()
			agent.client = newClient
			agent.ctx, agent.cancel = context.WithCancel(context.Background())
			newCtx := agent.ctx
			agent.mu.Unlock()

			agent.startWg.Add(1)
			go func(a *agentFixture, ctx context.Context) {
				defer a.startWg.Done()
				_ = a.client.Start(ctx)
			}(agent, newCtx)
		}

		By("waiting for reconnected agents to register")
		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, 10*time.Second, 200*time.Millisecond).Should(BeNumerically(">=", highAvailabilityAgentCount))

		By("adding new metrics to reconnected agents after they're fully connected")
		time.Sleep(2 * time.Second) // Wait for new client instances to establish metrics streams
		for idx := 0; idx < reconnectSubsetSize; idx++ {
			agent := agents[idx]
			role := agentRoles[idx%len(agentRoles)]
			Expect(testhelper.UpdateMetrics(agent.flightRecorder, []testhelper.RawMetric{{
				Name:  fmt.Sprintf("reconnect_metric_%d", idx),
				Value: float64(idx + 1000),
				Desc:  "reconnect metric",
				Labels: []testhelper.Label{
					{Name: "agent_idx", Value: strconv.Itoa(idx)},
					{Name: "node_role", Value: role},
					{Name: "pod_name", Value: "test"},
					{Name: "container_name", Value: "data"},
				},
			}})).To(Succeed())
		}

		By("collecting metrics again to ensure reconnection succeeded")
		Eventually(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s/metrics-windows", proxyHTTPAddr))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}
			var decodedMetrics []map[string]interface{}
			if decodeErr := json.NewDecoder(resp.Body).Decode(&decodedMetrics); decodeErr != nil {
				return decodeErr
			}
			if len(decodedMetrics) < reconnectSubsetSize {
				return fmt.Errorf("expected at least %d metrics after reconnection, got %d", reconnectSubsetSize, len(decodedMetrics))
			}

			metricsListMu.Lock()
			metricsList = decodedMetrics
			metricsListMu.Unlock()

			// Check if reconnect metrics are present
			for idx := 0; idx < reconnectSubsetSize; idx++ {
				name := fmt.Sprintf("reconnect_metric_%d", idx)
				foundReconnectMetric := false
				for _, metric := range decodedMetrics {
					if metric["name"] == name {
						foundReconnectMetric = true
						break
					}
				}
				if !foundReconnectMetric {
					return fmt.Errorf("reconnect metric %s not found", name)
				}
			}
			return nil
		}, 30*time.Second, 500*time.Millisecond).Should(Succeed())

		time.Sleep(2 * time.Second)

		Eventually(func() int {
			return metricsAggregator.ActiveCollections()
		}, 30*time.Second, 500*time.Millisecond).Should(Equal(0))
	})
})
