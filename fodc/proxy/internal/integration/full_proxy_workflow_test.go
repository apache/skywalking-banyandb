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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
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

var _ = Describe("Full FODC Proxy Workflow", func() {
	var (
		proxyGRPCAddr     string
		proxyHTTPAddr     string
		grpcServer        *grpcproxy.Server
		httpServer        *api.Server
		agentRegistry     *registry.AgentRegistry
		metricsAggregator *metricsproxy.Aggregator
		grpcService       *grpcproxy.FODCService
		flightRecorder1   interface{}
		flightRecorder2   interface{}
		flightRecorder3   interface{}
		proxyClient1      *testhelper.ProxyClientWrapper
		proxyClient2      *testhelper.ProxyClientWrapper
		proxyClient3      *testhelper.ProxyClientWrapper
		agentCtx1         context.Context
		agentCancel1      context.CancelFunc
		agentCtx2         context.Context
		agentCancel2      context.CancelFunc
		agentCtx3         context.Context
		agentCancel3      context.CancelFunc
	)

	BeforeEach(func() {
		testLogger := logger.GetLogger("test", "integration")

		heartbeatTimeout := 5 * time.Second
		cleanupTimeout := 10 * time.Second
		heartbeatInterval := 2 * time.Second
		agentRegistry = registry.NewAgentRegistry(testLogger, heartbeatTimeout, cleanupTimeout, 100)
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
		Expect(httpServer.Start(proxyHTTPAddr, 10*time.Second, 10*time.Second)).To(Succeed())

		Eventually(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s/health", proxyHTTPAddr))
			if err != nil {
				return err
			}
			resp.Body.Close()
			return nil
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Succeed())

		capacitySize := int64(10 * 1024 * 1024)
		flightRecorder1 = testhelper.NewFlightRecorder(capacitySize)
		flightRecorder2 = testhelper.NewFlightRecorder(capacitySize)
		flightRecorder3 = testhelper.NewFlightRecorder(capacitySize)

		proxyClient1 = testhelper.NewProxyClientWrapper(
			proxyGRPCAddr,
			"127.0.0.1",
			8080,
			"liaison",
			"demo",
			[]string{"liaison"},
			map[string]string{"zone": "us-west-1", "env": "production"},
			heartbeatInterval,
			1*time.Second,
			flightRecorder1,
			testLogger,
		)
		Expect(proxyClient1).NotTo(BeNil())

		proxyClient2 = testhelper.NewProxyClientWrapper(
			proxyGRPCAddr,
			"127.0.0.2",
			8081,
			"datanode-hot",
			"demo",
			[]string{"data"},
			map[string]string{"zone": "us-west-1", "env": "production"},
			heartbeatInterval,
			1*time.Second,
			flightRecorder2,
			testLogger,
		)
		Expect(proxyClient2).NotTo(BeNil())

		proxyClient3 = testhelper.NewProxyClientWrapper(
			proxyGRPCAddr,
			"127.0.0.3",
			8082,
			"datanode-warm",
			"demo",
			[]string{"data"},
			map[string]string{"zone": "us-east-1", "env": "staging"},
			heartbeatInterval,
			1*time.Second,
			flightRecorder3,
			testLogger,
		)
		Expect(proxyClient3).NotTo(BeNil())

		agentCtx1, agentCancel1 = context.WithCancel(context.Background())
		agentCtx2, agentCancel2 = context.WithCancel(context.Background())
		agentCtx3, agentCancel3 = context.WithCancel(context.Background())
	})

	AfterEach(func() {
		if agentCancel1 != nil {
			agentCancel1()
		}
		if agentCancel2 != nil {
			agentCancel2()
		}
		if agentCancel3 != nil {
			agentCancel3()
		}
		if proxyClient1 != nil {
			_ = proxyClient1.Disconnect()
		}
		if proxyClient2 != nil {
			_ = proxyClient2.Disconnect()
		}
		if proxyClient3 != nil {
			_ = proxyClient3.Disconnect()
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

	It("validates metrics collection and metadata for all agents", func() {
		By("connecting agents and registering with the proxy")
		Expect(proxyClient1.Connect(agentCtx1)).To(Succeed())
		Expect(proxyClient2.Connect(agentCtx2)).To(Succeed())
		Expect(proxyClient3.Connect(agentCtx3)).To(Succeed())

		Expect(proxyClient1.StartRegistrationStream(agentCtx1)).To(Succeed())
		Expect(proxyClient2.StartRegistrationStream(agentCtx2)).To(Succeed())
		Expect(proxyClient3.StartRegistrationStream(agentCtx3)).To(Succeed())

		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Equal(3))

		By("opening metrics streams toward the proxy")
		Expect(proxyClient1.StartMetricsStream(agentCtx1)).To(Succeed())
		Expect(proxyClient2.StartMetricsStream(agentCtx2)).To(Succeed())
		Expect(proxyClient3.StartMetricsStream(agentCtx3)).To(Succeed())

		time.Sleep(200 * time.Millisecond)

		By("populating flight recorder metrics for each agent")
		first := testhelper.RawMetric{
			Name:   "liaison_full_metric",
			Value:  10.0,
			Desc:   "liaison metric for workflow",
			Labels: []testhelper.Label{{Name: "zone", Value: "us-west-1"}},
		}
		Expect(testhelper.UpdateMetrics(flightRecorder1, []testhelper.RawMetric{first})).To(Succeed())

		second := testhelper.RawMetric{
			Name:   "datanode_hot_full_metric",
			Value:  20.0,
			Desc:   "datanode hot metric for workflow",
			Labels: []testhelper.Label{{Name: "zone", Value: "us-west-1"}},
		}
		Expect(testhelper.UpdateMetrics(flightRecorder2, []testhelper.RawMetric{second})).To(Succeed())

		third := testhelper.RawMetric{
			Name:   "datanode_warm_full_metric",
			Value:  30.0,
			Desc:   "datanode warm metric for workflow",
			Labels: []testhelper.Label{{Name: "zone", Value: "us-east-1"}},
		}
		Expect(testhelper.UpdateMetrics(flightRecorder3, []testhelper.RawMetric{third})).To(Succeed())

		time.Sleep(200 * time.Millisecond)

		By("querying /metrics-windows to trigger on-demand collection")
		var metricsList []map[string]interface{}
		Eventually(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s/metrics-windows", proxyHTTPAddr))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}
			if decodeErr := json.NewDecoder(resp.Body).Decode(&metricsList); decodeErr != nil {
				return decodeErr
			}
			if len(metricsList) < 3 {
				return fmt.Errorf("expected metrics from 3 agents, got %d", len(metricsList))
			}
			return nil
		}, 15*time.Second, 500*time.Millisecond).Should(Succeed())

		foundRoles := map[string]bool{}
		agentIDs := make(map[string]bool)

		for _, metric := range metricsList {
			agentID := metric["agent_id"].(string)
			agentIDs[agentID] = true

			labels := metric["labels"].(map[string]interface{})
			nodeRole := labels["node_role"].(string)
			Expect(metric["ip"]).NotTo(BeEmpty())
			Expect(metric["port"]).NotTo(BeZero())

			switch metric["name"] {
			case "liaison_full_metric":
				foundRoles["liaison"] = true
				Expect(nodeRole).To(Equal("liaison"))
				Expect(metric["ip"]).To(Equal("127.0.0.1"))
			case "datanode_hot_full_metric":
				foundRoles["datanode-hot"] = true
				Expect(nodeRole).To(Equal("datanode-hot"))
				Expect(metric["ip"]).To(Equal("127.0.0.2"))
			case "datanode_warm_full_metric":
				foundRoles["datanode-warm"] = true
				Expect(nodeRole).To(Equal("datanode-warm"))
				Expect(metric["ip"]).To(Equal("127.0.0.3"))
			}
		}

		Expect(foundRoles["liaison"]).To(BeTrue())
		Expect(foundRoles["datanode-hot"]).To(BeTrue())
		Expect(foundRoles["datanode-warm"]).To(BeTrue())
		Expect(len(agentIDs)).To(Equal(3))
	})
})
