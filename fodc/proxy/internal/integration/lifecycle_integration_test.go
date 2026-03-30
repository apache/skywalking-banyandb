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
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/fodc/agent/testhelper"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/api"
	grpcproxy "github.com/apache/skywalking-banyandb/fodc/proxy/internal/grpc"
	proxylifecycle "github.com/apache/skywalking-banyandb/fodc/proxy/internal/lifecycle"
	metricsproxy "github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

var _ = Describe("Lifecycle Integration", func() {
	var (
		proxyGRPCAddr     string
		proxyHTTPAddr     string
		grpcServer        *grpcproxy.Server
		httpServer        *api.Server
		agentRegistry     *registry.AgentRegistry
		metricsAggregator *metricsproxy.Aggregator
		grpcService       *grpcproxy.FODCService
		lifecycleMgr      *proxylifecycle.Manager
		flightRecorder1   interface{}
		flightRecorder2   interface{}
		proxyClient1      *testhelper.ProxyClientWrapper
		proxyClient2      *testhelper.ProxyClientWrapper
		agentCtx1         context.Context
		agentCancel1      context.CancelFunc
		agentCtx2         context.Context
		agentCancel2      context.CancelFunc
		httpClient        *http.Client
		testLogger        *logger.Logger
		reportDir         string
	)

	BeforeEach(func() {
		testLogger = logger.GetLogger("test", "lifecycle-integration")
		httpClient = &http.Client{Timeout: 2 * time.Second}
		reportDir = GinkgoT().TempDir()

		heartbeatTimeout := 5 * time.Second
		cleanupTimeout := 10 * time.Second
		heartbeatInterval := 2 * time.Second
		agentRegistry = registry.NewAgentRegistry(testLogger, heartbeatTimeout, cleanupTimeout, 100)

		lifecycleMgr = proxylifecycle.NewManager(agentRegistry, nil, testLogger)
		metricsAggregator = metricsproxy.NewAggregator(agentRegistry, nil, testLogger)
		grpcService = grpcproxy.NewFODCService(agentRegistry, metricsAggregator, nil, lifecycleMgr, testLogger, heartbeatInterval)
		metricsAggregator.SetGRPCService(grpcService)
		lifecycleMgr.SetGRPCService(grpcService)

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
		httpServer = api.NewServer(metricsAggregator, nil, lifecycleMgr, agentRegistry, testLogger)
		Expect(httpServer.Start(proxyHTTPAddr, 10*time.Second, 10*time.Second)).To(Succeed())

		Eventually(func() error {
			resp, err := httpClient.Get(fmt.Sprintf("http://%s/health", proxyHTTPAddr))
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

	setupTwoAgents := func() {
		agentCtx1, agentCancel1 = context.WithCancel(context.Background())
		agentCtx2, agentCancel2 = context.WithCancel(context.Background())

		capacitySize := int64(10 * 1024 * 1024)
		flightRecorder1 = testhelper.NewFlightRecorder(capacitySize)
		flightRecorder2 = testhelper.NewFlightRecorder(capacitySize)

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
			reportDir,
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
			reportDir,
		)
		Expect(proxyClient2).NotTo(BeNil())

		Expect(proxyClient1.Connect(agentCtx1)).To(Succeed())
		Expect(proxyClient1.StartRegistrationStream(agentCtx1)).To(Succeed())
		Expect(proxyClient1.StartLifecycleStream(agentCtx1)).To(Succeed())

		Expect(proxyClient2.Connect(agentCtx2)).To(Succeed())
		Expect(proxyClient2.StartRegistrationStream(agentCtx2)).To(Succeed())
		Expect(proxyClient2.StartLifecycleStream(agentCtx2)).To(Succeed())

		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Equal(2))
	}

	waitForLifecycleStreams := func() {
		agents := agentRegistry.ListAgents()
		Expect(len(agents)).To(Equal(2))
		Eventually(func(g Gomega) {
			g.Expect(grpcService.HasLifecycleStream(agents[0].AgentID)).To(BeTrue())
			g.Expect(grpcService.HasLifecycleStream(agents[1].AgentID)).To(BeTrue())
		}, "5s", "100ms").Should(Succeed())
	}

	getLifecycleData := func() *proxylifecycle.InspectionResult {
		resp, err := httpClient.Get(fmt.Sprintf("http://%s/cluster/lifecycle", proxyHTTPAddr))
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		var result proxylifecycle.InspectionResult
		Expect(json.NewDecoder(resp.Body).Decode(&result)).To(Succeed())
		return &result
	}

	writeReport := func(name, content string) {
		Expect(os.WriteFile(filepath.Join(reportDir, name), []byte(content), 0o600)).To(Succeed())
	}

	It("should return empty lifecycle data when no report files exist", func() {
		setupTwoAgents()
		waitForLifecycleStreams()

		Eventually(func(g Gomega) {
			data := getLifecycleData()
			g.Expect(data.Groups).To(BeEmpty())
			g.Expect(data.LifecycleStatuses).To(BeEmpty())
		}, "5s", "100ms").Should(Succeed())
	})

	It("should aggregate lifecycle statuses from multiple agents via report files", func() {
		setupTwoAgents()
		waitForLifecycleStreams()

		writeReport("2026-03-26.json", `{"status":"ok"}`)

		Eventually(func(g Gomega) {
			result := getLifecycleData()
			g.Expect(result.Groups).To(BeEmpty())
			g.Expect(len(result.LifecycleStatuses)).To(Equal(2))
			podNames := []string{result.LifecycleStatuses[0].PodName, result.LifecycleStatuses[1].PodName}
			g.Expect(podNames).To(ContainElements("pod-1", "pod-2"))
		}, "5s", "100ms").Should(Succeed())
	})

	It("should include report filenames collected from disk", func() {
		setupTwoAgents()
		waitForLifecycleStreams()

		writeReport("2026-03-25.json", `{"check":"pass"}`)
		writeReport("2026-03-26.json", `{"check":"fail"}`)

		Eventually(func(g Gomega) {
			result := getLifecycleData()
			g.Expect(len(result.LifecycleStatuses)).To(Equal(2))
			for _, status := range result.LifecycleStatuses {
				g.Expect(len(status.Reports)).To(Equal(2))
				filenames := []string{status.Reports[0].Filename, status.Reports[1].Filename}
				g.Expect(filenames).To(ContainElements("2026-03-25.json", "2026-03-26.json"))
			}
		}, "5s", "100ms").Should(Succeed())
	})
})
