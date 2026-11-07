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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/fodc/agent/testhelper"
	grpcproxy "github.com/apache/skywalking-banyandb/fodc/proxy/internal/grpc"
	metricsproxy "github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

// A liaison with thousands of histogram series answers a time-window request with far more
// than the 32MB gRPC message limit. The reply must arrive whole, and the agent must keep its
// connection and identity.
var _ = Describe("Metrics time window larger than one gRPC message", func() {
	const (
		seriesCount     = 1200
		samplesPerSerie = 100
		proxyMaxMsgSize = 32 << 20 // the proxy's --grpc-max-msg-size default
	)

	var (
		grpcServer        *grpcproxy.Server
		agentRegistry     *registry.AgentRegistry
		metricsAggregator *metricsproxy.Aggregator
		proxyClient       *testhelper.ProxyClientWrapper
		flightRecorder    interface{}
		agentCtx          context.Context
		agentCancel       context.CancelFunc
	)

	BeforeEach(func() {
		testLogger := logger.GetLogger("test", "integration")
		heartbeatInterval := 2 * time.Second
		agentRegistry = registry.NewAgentRegistry(testLogger, 60*time.Second, 120*time.Second, 100)
		metricsAggregator = metricsproxy.NewAggregator(agentRegistry, nil, testLogger)
		grpcService := grpcproxy.NewFODCService(agentRegistry, metricsAggregator, nil, nil, nil, nil, testLogger, heartbeatInterval)
		metricsAggregator.SetGRPCService(grpcService)

		grpcListener, listenErr := net.Listen("tcp", "localhost:0")
		Expect(listenErr).NotTo(HaveOccurred())
		proxyGRPCAddr := grpcListener.Addr().String()
		_ = grpcListener.Close()
		grpcServer = grpcproxy.NewServer(grpcService, proxyGRPCAddr, proxyMaxMsgSize, testLogger)
		Expect(grpcServer.Start()).To(Succeed())

		flightRecorder = testhelper.NewFlightRecorder(1 << 30)
		proxyClient = testhelper.NewProxyClientWrapper(proxyGRPCAddr, "liaison", "127.0.0.1", []string{"liaison"},
			map[string]string{"env": "test"}, heartbeatInterval, time.Second, flightRecorder, testLogger, "")
		Expect(proxyClient).NotTo(BeNil())
		agentCtx, agentCancel = context.WithCancel(context.Background())
	})

	AfterEach(func() {
		agentCancel()
		_ = proxyClient.Disconnect()
		grpcServer.Stop()
		agentRegistry.Stop()
	})

	It("delivers the whole reply in chunks without reconnecting the agent", func() {
		Expect(proxyClient.Connect(agentCtx)).To(Succeed())
		Expect(proxyClient.StartRegistrationStream(agentCtx)).To(Succeed())
		Eventually(func() int { return len(agentRegistry.ListAgents()) }, flags.EventuallyTimeout, 100*time.Millisecond).Should(Equal(1))
		agentID := agentRegistry.ListAgents()[0].AgentID
		Expect(proxyClient.StartMetricsStream(agentCtx)).To(Succeed())
		time.Sleep(500 * time.Millisecond)

		By("Recording a time window whose reply exceeds the gRPC message limit")
		description := strings.Repeat("Latency of publishing a batch to the data nodes. ", 4)
		rawMetrics := make([]testhelper.RawMetric, 0, seriesCount)
		for idx := 0; idx < seriesCount; idx++ {
			rawMetrics = append(rawMetrics, testhelper.RawMetric{
				Name: "banyandb_queue_pub_total_latency_bucket",
				Desc: description,
				Type: "histogram",
				Labels: []testhelper.Label{
					{Name: "le", Value: fmt.Sprintf("%d", idx)},
					{Name: "topic", Value: "measure-write-with-a-long-topic-name"},
				},
				Value: float64(idx),
			})
		}
		start := time.Now().Add(-5 * time.Second)
		for sample := 0; sample < samplesPerSerie; sample++ {
			Expect(testhelper.UpdateMetrics(flightRecorder, rawMetrics)).To(Succeed())
		}
		end := time.Now().Add(5 * time.Second)

		By("Collecting the window through the proxy")
		collectCtx, collectCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer collectCancel()
		collected, collectErr := metricsAggregator.GetMetricsWindow(collectCtx, start, end, &metricsproxy.Filter{})
		Expect(collectErr).NotTo(HaveOccurred())
		Expect(collected).To(HaveLen(seriesCount * samplesPerSerie))

		By("Checking that the agent kept its connection and identity")
		agents := agentRegistry.ListAgents()
		Expect(agents).To(HaveLen(1))
		Expect(agents[0].AgentID).To(Equal(agentID))
	})
})
