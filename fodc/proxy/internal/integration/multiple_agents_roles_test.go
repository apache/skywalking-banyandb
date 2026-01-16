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

var _ = Describe("Test Case 4: Multiple Agents and Roles", func() {
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

	It("should register multiple agents with different roles", func() {
		By("Starting connection managers for all agents")
		proxyClient1.StartConnManager(agentCtx1)
		proxyClient2.StartConnManager(agentCtx2)
		proxyClient3.StartConnManager(agentCtx3)

		By("Connecting all agents to proxy")
		connectErr1 := proxyClient1.Connect(agentCtx1)
		Expect(connectErr1).NotTo(HaveOccurred())
		connectErr2 := proxyClient2.Connect(agentCtx2)
		Expect(connectErr2).NotTo(HaveOccurred())
		connectErr3 := proxyClient3.Connect(agentCtx3)
		Expect(connectErr3).NotTo(HaveOccurred())

		By("Starting registration streams")
		regErr1 := proxyClient1.StartRegistrationStream(agentCtx1)
		Expect(regErr1).NotTo(HaveOccurred())
		regErr2 := proxyClient2.StartRegistrationStream(agentCtx2)
		Expect(regErr2).NotTo(HaveOccurred())
		regErr3 := proxyClient3.StartRegistrationStream(agentCtx3)
		Expect(regErr3).NotTo(HaveOccurred())

		By("Waiting for all agents to register")
		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Equal(3))

		By("Verifying all agents are registered")
		agents := agentRegistry.ListAgents()
		Expect(len(agents)).To(Equal(3))

		rolesFound := make(map[string]bool)
		addressesFound := make(map[string]bool)
		for _, agentInfo := range agents {
			rolesFound[agentInfo.NodeRole] = true
			addressKey := fmt.Sprintf("%s:%d", agentInfo.PrimaryAddress.IP, agentInfo.PrimaryAddress.Port)
			addressesFound[addressKey] = true
			Expect(agentInfo.Status).To(Equal(registry.AgentStatusOnline))
		}

		Expect(rolesFound["liaison"]).To(BeTrue(), "liaison role should be found")
		Expect(rolesFound["datanode-hot"]).To(BeTrue(), "datanode-hot role should be found")
		Expect(rolesFound["datanode-warm"]).To(BeTrue(), "datanode-warm role should be found")
		Expect(addressesFound["127.0.0.1:8080"]).To(BeTrue(), "127.0.0.1:8080 should be found")
		Expect(addressesFound["127.0.0.2:8081"]).To(BeTrue(), "127.0.0.2:8081 should be found")
		Expect(addressesFound["127.0.0.3:8082"]).To(BeTrue(), "127.0.0.3:8082 should be found")
	})

	It("should aggregate metrics from all agents", func() {
		By("Starting connection managers for all agents")
		proxyClient1.StartConnManager(agentCtx1)
		proxyClient2.StartConnManager(agentCtx2)
		proxyClient3.StartConnManager(agentCtx3)

		By("Connecting all agents to proxy")
		connectErr1 := proxyClient1.Connect(agentCtx1)
		Expect(connectErr1).NotTo(HaveOccurred())
		connectErr2 := proxyClient2.Connect(agentCtx2)
		Expect(connectErr2).NotTo(HaveOccurred())
		connectErr3 := proxyClient3.Connect(agentCtx3)
		Expect(connectErr3).NotTo(HaveOccurred())

		By("Starting registration streams")
		regErr1 := proxyClient1.StartRegistrationStream(agentCtx1)
		Expect(regErr1).NotTo(HaveOccurred())
		regErr2 := proxyClient2.StartRegistrationStream(agentCtx2)
		Expect(regErr2).NotTo(HaveOccurred())
		regErr3 := proxyClient3.StartRegistrationStream(agentCtx3)
		Expect(regErr3).NotTo(HaveOccurred())

		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Equal(3))

		By("Starting metrics streams")
		metricsErr1 := proxyClient1.StartMetricsStream(agentCtx1)
		Expect(metricsErr1).NotTo(HaveOccurred())
		metricsErr2 := proxyClient2.StartMetricsStream(agentCtx2)
		Expect(metricsErr2).NotTo(HaveOccurred())
		metricsErr3 := proxyClient3.StartMetricsStream(agentCtx3)
		Expect(metricsErr3).NotTo(HaveOccurred())

		time.Sleep(200 * time.Millisecond)

		By("Adding metrics to each Flight Recorder")
		rawMetrics1 := []testhelper.RawMetric{
			{
				Name:   "liaison_metric",
				Value:  10.0,
				Desc:   "Liaison metric",
				Labels: []testhelper.Label{{Name: "label1", Value: "value1"}},
			},
		}
		updateErr1 := testhelper.UpdateMetrics(flightRecorder1, rawMetrics1)
		Expect(updateErr1).NotTo(HaveOccurred())

		rawMetrics2 := []testhelper.RawMetric{
			{
				Name:   "datanode_hot_metric",
				Value:  20.0,
				Desc:   "DataNode hot metric",
				Labels: []testhelper.Label{{Name: "label2", Value: "value2"}},
			},
		}
		updateErr2 := testhelper.UpdateMetrics(flightRecorder2, rawMetrics2)
		Expect(updateErr2).NotTo(HaveOccurred())

		rawMetrics3 := []testhelper.RawMetric{
			{
				Name:   "datanode_warm_metric",
				Value:  30.0,
				Desc:   "DataNode warm metric",
				Labels: []testhelper.Label{{Name: "label3", Value: "value3"}},
			},
		}
		updateErr3 := testhelper.UpdateMetrics(flightRecorder3, rawMetrics3)
		Expect(updateErr3).NotTo(HaveOccurred())

		time.Sleep(200 * time.Millisecond)

		By("Querying /metrics-windows endpoint")
		var metricsList []map[string]interface{}
		Eventually(func() error {
			metricsResp, metricsHTTPErr := http.Get(fmt.Sprintf("http://%s/metrics-windows", proxyHTTPAddr))
			if metricsHTTPErr != nil {
				return metricsHTTPErr
			}
			defer metricsResp.Body.Close()
			if metricsResp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", metricsResp.StatusCode)
			}
			metricsDecodeErr := json.NewDecoder(metricsResp.Body).Decode(&metricsList)
			if metricsDecodeErr != nil {
				return metricsDecodeErr
			}
			if len(metricsList) < 3 {
				return fmt.Errorf("expected at least 3 metrics, got %d", len(metricsList))
			}
			return nil
		}, 15*time.Second, 500*time.Millisecond).Should(Succeed())

		By("Verifying metrics aggregation includes all nodes")
		foundLiaisonMetric := false
		foundHotMetric := false
		foundWarmMetric := false
		agentIDsFound := make(map[string]bool)

		for _, metricItem := range metricsList {
			metric := metricItem
			name := metric["name"].(string)
			agentID := metric["agent_id"].(string)
			agentIDsFound[agentID] = true

			switch name {
			case "liaison_metric":
				foundLiaisonMetric = true
				labels := metric["labels"].(map[string]interface{})
				Expect(labels["node_role"]).To(Equal("liaison"))
				Expect(metric["ip"]).To(Equal("127.0.0.1"))
			case "datanode_hot_metric":
				foundHotMetric = true
				labels := metric["labels"].(map[string]interface{})
				Expect(labels["node_role"]).To(Equal("datanode-hot"))
				Expect(metric["ip"]).To(Equal("127.0.0.2"))
			case "datanode_warm_metric":
				foundWarmMetric = true
				labels := metric["labels"].(map[string]interface{})
				Expect(labels["node_role"]).To(Equal("datanode-warm"))
				Expect(metric["ip"]).To(Equal("127.0.0.3"))
			}
		}

		Expect(foundLiaisonMetric).To(BeTrue(), "liaison_metric should be found")
		Expect(foundHotMetric).To(BeTrue(), "datanode_hot_metric should be found")
		Expect(foundWarmMetric).To(BeTrue(), "datanode_warm_metric should be found")
		Expect(len(agentIDsFound)).To(Equal(3), "should have metrics from 3 different agents")
	})

	It("should filter metrics by role", func() {
		By("Starting connection managers for all agents")
		proxyClient1.StartConnManager(agentCtx1)
		proxyClient2.StartConnManager(agentCtx2)
		proxyClient3.StartConnManager(agentCtx3)

		By("Connecting all agents to proxy")
		connectErr1 := proxyClient1.Connect(agentCtx1)
		Expect(connectErr1).NotTo(HaveOccurred())
		connectErr2 := proxyClient2.Connect(agentCtx2)
		Expect(connectErr2).NotTo(HaveOccurred())
		connectErr3 := proxyClient3.Connect(agentCtx3)
		Expect(connectErr3).NotTo(HaveOccurred())

		By("Starting registration streams")
		regErr1 := proxyClient1.StartRegistrationStream(agentCtx1)
		Expect(regErr1).NotTo(HaveOccurred())
		regErr2 := proxyClient2.StartRegistrationStream(agentCtx2)
		Expect(regErr2).NotTo(HaveOccurred())
		regErr3 := proxyClient3.StartRegistrationStream(agentCtx3)
		Expect(regErr3).NotTo(HaveOccurred())

		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Equal(3))

		By("Starting metrics streams")
		metricsErr1 := proxyClient1.StartMetricsStream(agentCtx1)
		Expect(metricsErr1).NotTo(HaveOccurred())
		metricsErr2 := proxyClient2.StartMetricsStream(agentCtx2)
		Expect(metricsErr2).NotTo(HaveOccurred())
		metricsErr3 := proxyClient3.StartMetricsStream(agentCtx3)
		Expect(metricsErr3).NotTo(HaveOccurred())

		time.Sleep(200 * time.Millisecond)

		By("Adding metrics to each Flight Recorder")
		rawMetrics1 := []testhelper.RawMetric{
			{
				Name:   "liaison_metric",
				Value:  10.0,
				Desc:   "Liaison metric",
				Labels: []testhelper.Label{{Name: "label1", Value: "value1"}},
			},
		}
		updateErr1 := testhelper.UpdateMetrics(flightRecorder1, rawMetrics1)
		Expect(updateErr1).NotTo(HaveOccurred())

		rawMetrics2 := []testhelper.RawMetric{
			{
				Name:   "datanode_hot_metric",
				Value:  20.0,
				Desc:   "DataNode hot metric",
				Labels: []testhelper.Label{{Name: "label2", Value: "value2"}},
			},
		}
		updateErr2 := testhelper.UpdateMetrics(flightRecorder2, rawMetrics2)
		Expect(updateErr2).NotTo(HaveOccurred())

		rawMetrics3 := []testhelper.RawMetric{
			{
				Name:   "datanode_warm_metric",
				Value:  30.0,
				Desc:   "DataNode warm metric",
				Labels: []testhelper.Label{{Name: "label3", Value: "value3"}},
			},
		}
		updateErr3 := testhelper.UpdateMetrics(flightRecorder3, rawMetrics3)
		Expect(updateErr3).NotTo(HaveOccurred())

		time.Sleep(200 * time.Millisecond)

		By("Querying /metrics-windows with role filter")
		var metricsList []map[string]interface{}
		Eventually(func() error {
			metricsResp, metricsHTTPErr := http.Get(fmt.Sprintf("http://%s/metrics-windows?role=liaison", proxyHTTPAddr))
			if metricsHTTPErr != nil {
				return metricsHTTPErr
			}
			defer metricsResp.Body.Close()
			if metricsResp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", metricsResp.StatusCode)
			}
			metricsDecodeErr := json.NewDecoder(metricsResp.Body).Decode(&metricsList)
			if metricsDecodeErr != nil {
				return metricsDecodeErr
			}
			if len(metricsList) < 1 {
				return fmt.Errorf("expected at least 1 metric, got %d", len(metricsList))
			}
			return nil
		}, 15*time.Second, 500*time.Millisecond).Should(Succeed())

		By("Verifying only liaison role metrics are returned")
		foundLiaisonMetric := false
		foundHotMetric := false
		foundWarmMetric := false

		for _, metricItem := range metricsList {
			metric := metricItem
			name := metric["name"].(string)
			labels := metric["labels"].(map[string]interface{})
			nodeRole := labels["node_role"].(string)

			Expect(nodeRole).To(Equal("liaison"), "all metrics should be from liaison role")

			switch name {
			case "liaison_metric":
				foundLiaisonMetric = true
			case "datanode_hot_metric":
				foundHotMetric = true
			case "datanode_warm_metric":
				foundWarmMetric = true
			}
		}

		Expect(foundLiaisonMetric).To(BeTrue(), "liaison_metric should be found")
		Expect(foundHotMetric).To(BeFalse(), "datanode_hot_metric should not be found")
		Expect(foundWarmMetric).To(BeFalse(), "datanode_warm_metric should not be found")
	})

	It("should filter metrics by address", func() {
		By("Starting connection managers for all agents")
		proxyClient1.StartConnManager(agentCtx1)
		proxyClient2.StartConnManager(agentCtx2)
		proxyClient3.StartConnManager(agentCtx3)

		By("Connecting all agents to proxy")
		connectErr1 := proxyClient1.Connect(agentCtx1)
		Expect(connectErr1).NotTo(HaveOccurred())
		connectErr2 := proxyClient2.Connect(agentCtx2)
		Expect(connectErr2).NotTo(HaveOccurred())
		connectErr3 := proxyClient3.Connect(agentCtx3)
		Expect(connectErr3).NotTo(HaveOccurred())

		By("Starting registration streams")
		regErr1 := proxyClient1.StartRegistrationStream(agentCtx1)
		Expect(regErr1).NotTo(HaveOccurred())
		regErr2 := proxyClient2.StartRegistrationStream(agentCtx2)
		Expect(regErr2).NotTo(HaveOccurred())
		regErr3 := proxyClient3.StartRegistrationStream(agentCtx3)
		Expect(regErr3).NotTo(HaveOccurred())

		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Equal(3))

		By("Starting metrics streams")
		metricsErr1 := proxyClient1.StartMetricsStream(agentCtx1)
		Expect(metricsErr1).NotTo(HaveOccurred())
		metricsErr2 := proxyClient2.StartMetricsStream(agentCtx2)
		Expect(metricsErr2).NotTo(HaveOccurred())
		metricsErr3 := proxyClient3.StartMetricsStream(agentCtx3)
		Expect(metricsErr3).NotTo(HaveOccurred())

		time.Sleep(200 * time.Millisecond)

		By("Adding metrics to each Flight Recorder")
		rawMetrics1 := []testhelper.RawMetric{
			{
				Name:   "liaison_metric",
				Value:  10.0,
				Desc:   "Liaison metric",
				Labels: []testhelper.Label{{Name: "label1", Value: "value1"}},
			},
		}
		updateErr1 := testhelper.UpdateMetrics(flightRecorder1, rawMetrics1)
		Expect(updateErr1).NotTo(HaveOccurred())

		rawMetrics2 := []testhelper.RawMetric{
			{
				Name:   "datanode_hot_metric",
				Value:  20.0,
				Desc:   "DataNode hot metric",
				Labels: []testhelper.Label{{Name: "label2", Value: "value2"}},
			},
		}
		updateErr2 := testhelper.UpdateMetrics(flightRecorder2, rawMetrics2)
		Expect(updateErr2).NotTo(HaveOccurred())

		rawMetrics3 := []testhelper.RawMetric{
			{
				Name:   "datanode_warm_metric",
				Value:  30.0,
				Desc:   "DataNode warm metric",
				Labels: []testhelper.Label{{Name: "label3", Value: "value3"}},
			},
		}
		updateErr3 := testhelper.UpdateMetrics(flightRecorder3, rawMetrics3)
		Expect(updateErr3).NotTo(HaveOccurred())

		time.Sleep(200 * time.Millisecond)

		By("Querying /metrics-windows with address filter")
		var metricsList []map[string]interface{}
		Eventually(func() error {
			metricsResp, metricsHTTPErr := http.Get(fmt.Sprintf("http://%s/metrics-windows?address=127.0.0.2", proxyHTTPAddr))
			if metricsHTTPErr != nil {
				return metricsHTTPErr
			}
			defer metricsResp.Body.Close()
			if metricsResp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", metricsResp.StatusCode)
			}
			metricsDecodeErr := json.NewDecoder(metricsResp.Body).Decode(&metricsList)
			if metricsDecodeErr != nil {
				return metricsDecodeErr
			}
			if len(metricsList) < 1 {
				return fmt.Errorf("expected at least 1 metric, got %d", len(metricsList))
			}
			return nil
		}, 15*time.Second, 500*time.Millisecond).Should(Succeed())

		By("Verifying only metrics from filtered address are returned")
		foundHotMetric := false
		foundLiaisonMetric := false
		foundWarmMetric := false

		for _, metricItem := range metricsList {
			metric := metricItem
			name := metric["name"].(string)
			ip := metric["ip"].(string)

			Expect(ip).To(Equal("127.0.0.2"), "all metrics should be from 127.0.0.2")

			switch name {
			case "datanode_hot_metric":
				foundHotMetric = true
			case "liaison_metric":
				foundLiaisonMetric = true
			case "datanode_warm_metric":
				foundWarmMetric = true
			}
		}

		Expect(foundHotMetric).To(BeTrue(), "datanode_hot_metric should be found")
		Expect(foundLiaisonMetric).To(BeFalse(), "liaison_metric should not be found")
		Expect(foundWarmMetric).To(BeFalse(), "datanode_warm_metric should not be found")
	})
})
