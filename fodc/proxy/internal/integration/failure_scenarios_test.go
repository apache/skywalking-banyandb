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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/agent/testhelper"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/api"
	grpcproxy "github.com/apache/skywalking-banyandb/fodc/proxy/internal/grpc"
	metricsproxy "github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

type failureFixture struct {
	grpcServer    *grpcproxy.Server
	httpServer    *api.Server
	registry      *registry.AgentRegistry
	aggregator    *metricsproxy.Aggregator
	service       *grpcproxy.FODCService
	logger        *logger.Logger
	proxyGRPCAddr string
	proxyHTTPAddr string
}

type agentHandle struct {
	client   *testhelper.ProxyClientWrapper
	flight   interface{}
	ctx      context.Context
	cancel   context.CancelFunc
	nodeIP   string
	role     string
	nodePort int
}

func setupFailureFixture() *failureFixture {
	testLogger := logger.GetLogger("test", "failure")
	heartbeatTimeout := 2 * time.Second
	cleanupTimeout := 4 * time.Second
	heartbeatInterval := 400 * time.Millisecond

	reg := registry.NewAgentRegistry(testLogger, heartbeatTimeout, cleanupTimeout, 100)
	agg := metricsproxy.NewAggregator(reg, nil, testLogger)
	svc := grpcproxy.NewFODCService(reg, agg, testLogger, heartbeatInterval)
	agg.SetGRPCService(svc)

	grpcListener, listenErr := net.Listen("tcp", "localhost:0")
	Expect(listenErr).NotTo(HaveOccurred())
	grpcAddr := grpcListener.Addr().String()
	_ = grpcListener.Close()
	grpcSrv := grpcproxy.NewServer(svc, grpcAddr, 4194304, testLogger)
	Expect(grpcSrv.Start()).To(Succeed())

	httpListener, httpErr := net.Listen("tcp", "localhost:0")
	Expect(httpErr).NotTo(HaveOccurred())
	httpAddr := httpListener.Addr().String()
	_ = httpListener.Close()
	httpSrv := api.NewServer(agg, reg, testLogger)
	Expect(httpSrv.Start(httpAddr, 10*time.Second, 10*time.Second)).To(Succeed())

	Eventually(func() error {
		resp, err := http.Get(fmt.Sprintf("http://%s/health", httpAddr))
		if err != nil {
			return err
		}
		resp.Body.Close()
		return nil
	}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Succeed())

	return &failureFixture{
		proxyGRPCAddr: grpcAddr,
		proxyHTTPAddr: httpAddr,
		grpcServer:    grpcSrv,
		httpServer:    httpSrv,
		registry:      reg,
		aggregator:    agg,
		service:       svc,
		logger:        testLogger,
	}
}

func (f *failureFixture) teardown() {
	if f.httpServer != nil {
		_ = f.httpServer.Stop()
	}
	if f.grpcServer != nil {
		f.grpcServer.Stop()
	}
	if f.registry != nil {
		f.registry.Stop()
	}
}

func (f *failureFixture) unregisterByAddress(ip string, port int) {
	for _, agent := range f.registry.ListAgents() {
		if agent.PrimaryAddress.IP == ip && agent.PrimaryAddress.Port == port {
			_ = f.registry.UnregisterAgent(agent.AgentID)
			return
		}
	}
}

func startAgentForFixture(f *failureFixture, ip string, port int, role string) *agentHandle {
	ctx, cancel := context.WithCancel(context.Background())
	fr := testhelper.NewFlightRecorder(int64(4 * 1024 * 1024))
	client := testhelper.NewProxyClientWrapper(
		f.proxyGRPCAddr,
		ip,
		port,
		role,
		map[string]string{"zone": "failure"},
		2*time.Second,
		1*time.Second,
		fr,
		logger.GetLogger("test", fmt.Sprintf("agent-%s", ip)),
	)

	Expect(client).NotTo(BeNil())

	return &agentHandle{
		client:   client,
		flight:   fr,
		ctx:      ctx,
		cancel:   cancel,
		nodeIP:   ip,
		nodePort: port,
		role:     role,
	}
}

func registerAgent(agent *agentHandle) {
	agent.client.StartConnManager(agent.ctx)
	Expect(agent.client.Connect(agent.ctx)).To(Succeed())
	Expect(agent.client.StartRegistrationStream(agent.ctx)).To(Succeed())
	Expect(agent.client.StartMetricsStream(agent.ctx)).To(Succeed())
}

func stopAgent(agent *agentHandle) {
	if agent == nil {
		return
	}
	agent.cancel()
	Expect(agent.client.Disconnect()).To(Succeed())
}

var _ = Describe("Failure Scenarios", func() {
	var fixture *failureFixture

	BeforeEach(func() {
		fixture = setupFailureFixture()
	})

	AfterEach(func() {
		fixture.teardown()
	})

	It("handles agent disconnection gracefully", func() {
		agent := startAgentForFixture(fixture, "127.0.0.10", 9000, "liaison")
		registerAgent(agent)

		Expect(testhelper.UpdateMetrics(agent.flight, []testhelper.RawMetric{{
			Name:  "failure_disconnect_metric",
			Value: 1.0,
			Desc:  "initial metric",
			Labels: []testhelper.Label{
				{Name: "phase", Value: "initial"},
			},
		}})).To(Succeed())

		var metricsList []map[string]interface{}
		Eventually(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s/metrics-windows", fixture.proxyHTTPAddr))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}
			return json.NewDecoder(resp.Body).Decode(&metricsList)
		}, 10*time.Second, 500*time.Millisecond).Should(Succeed())

		Expect(len(metricsList)).To(BeNumerically(">=", 1))
		Expect(fixture.aggregator.ActiveCollections()).To(Equal(0))

		stopAgent(agent)
		fixture.unregisterByAddress(agent.nodeIP, agent.nodePort)

		Eventually(func() int {
			return len(fixture.registry.ListAgents())
		}, 15*time.Second, 500*time.Millisecond).Should(Equal(0))

		metricsList = nil
		Eventually(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s/metrics-windows", fixture.proxyHTTPAddr))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}
			return json.NewDecoder(resp.Body).Decode(&metricsList)
		}, 15*time.Second, 500*time.Millisecond).Should(Succeed())

		Expect(len(metricsList)).To(Equal(0))
	})

	It("rejects invalid agent registration data", func() {
		conn, dialErr := grpc.NewClient(fixture.proxyGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(dialErr).NotTo(HaveOccurred())
		defer conn.Close()

		client := fodcv1.NewFODCServiceClient(conn)
		stream, streamErr := client.RegisterAgent(context.Background())
		Expect(streamErr).NotTo(HaveOccurred())

		req := &fodcv1.RegisterAgentRequest{
			NodeRole: "",
			Labels:   map[string]string{},
			PrimaryAddress: &fodcv1.Address{
				Ip:   "127.0.0.1",
				Port: 9000,
			},
		}
		Expect(stream.Send(req)).To(Succeed())

		resp, err := stream.Recv()
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Success).To(BeFalse())
		Expect(resp.Message).To(ContainSubstring("node role cannot be empty"))
		Expect(len(fixture.registry.ListAgents())).To(Equal(0))
	})

	It("continues metrics collection when part of the cluster is partitioned", func() {
		first := startAgentForFixture(fixture, "127.0.0.21", 9010, "datanode-hot")
		second := startAgentForFixture(fixture, "127.0.0.22", 9011, "datanode-warm")

		registerAgent(first)
		registerAgent(second)

		Expect(testhelper.UpdateMetrics(first.flight, []testhelper.RawMetric{{
			Name:  "partition_metric",
			Value: 110.0,
			Desc:  "agent one metric",
			Labels: []testhelper.Label{
				{Name: "group", Value: "primary"},
			},
		}})).To(Succeed())
		Expect(testhelper.UpdateMetrics(second.flight, []testhelper.RawMetric{{
			Name:  "partition_metric",
			Value: 220.0,
			Desc:  "agent two metric",
			Labels: []testhelper.Label{
				{Name: "group", Value: "secondary"},
			},
		}})).To(Succeed())

		stopAgent(second)
		fixture.unregisterByAddress(second.nodeIP, second.nodePort)

		var metricsList []map[string]interface{}
		Eventually(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s/metrics-windows", fixture.proxyHTTPAddr))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}
			return json.NewDecoder(resp.Body).Decode(&metricsList)
		}, 15*time.Second, 500*time.Millisecond).Should(Succeed())

		Expect(len(metricsList)).To(BeNumerically(">=", 1))
		for _, metric := range metricsList {
			Expect(metric["labels"].(map[string]interface{})["node_role"]).To(Equal("datanode-hot"))
		}
		Expect(fixture.aggregator.ActiveCollections()).To(Equal(0))

		stopAgent(first)
	})

	It("recovers after the proxy's gRPC service restarts", func() {
		agent := startAgentForFixture(fixture, "127.0.0.30", 9020, "datanode-cold")
		registerAgent(agent)

		Expect(testhelper.UpdateMetrics(agent.flight, []testhelper.RawMetric{{
			Name:  "recovery_metric",
			Value: 5.0,
			Desc:  "initial value",
			Labels: []testhelper.Label{
				{Name: "phase", Value: "pre-failure"},
			},
		}})).To(Succeed())

		var metricsList []map[string]interface{}
		Eventually(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s/metrics-windows", fixture.proxyHTTPAddr))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}
			return json.NewDecoder(resp.Body).Decode(&metricsList)
		}, 10*time.Second, 500*time.Millisecond).Should(Succeed())

		Expect(len(metricsList)).To(BeNumerically(">=", 1))

		agent.cancel()
		Expect(agent.client.Disconnect()).To(Succeed())

		fixture.grpcServer.Stop()
		fixture.service = grpcproxy.NewFODCService(fixture.registry, fixture.aggregator, fixture.logger, 2*time.Second)
		fixture.aggregator.SetGRPCService(fixture.service)
		fixture.grpcServer = grpcproxy.NewServer(fixture.service, fixture.proxyGRPCAddr, 4194304, fixture.logger)
		Expect(fixture.grpcServer.Start()).To(Succeed())
		
		// Wait for gRPC server to be ready
		time.Sleep(100 * time.Millisecond)
		
		// Create a new agent client for reconnection after server restart
		agent = startAgentForFixture(fixture, "127.0.0.30", 9020, "datanode-cold")
		registerAgent(agent)

		Expect(testhelper.UpdateMetrics(agent.flight, []testhelper.RawMetric{{
			Name:  "recovery_metric",
			Value: 500.0,
			Desc:  "post restart value",
			Labels: []testhelper.Label{
				{Name: "phase", Value: "post-failure"},
			},
		}})).To(Succeed())

		Eventually(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s/metrics-windows", fixture.proxyHTTPAddr))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}
			metricsList = nil
			return json.NewDecoder(resp.Body).Decode(&metricsList)
		}, 20*time.Second, 500*time.Millisecond).Should(Succeed())

		found := false
		for _, metric := range metricsList {
			if metric["name"].(string) == "recovery_metric" {
				Expect(metric["data"]).NotTo(BeNil())
				for _, data := range metric["data"].([]interface{}) {
					dataPoint := data.(map[string]interface{})
					if dataPoint["value"].(float64) == 500.0 {
						found = true
					}
				}
			}
		}
		Expect(found).To(BeTrue())

		stopAgent(agent)
	})
})
