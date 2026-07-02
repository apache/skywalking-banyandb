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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/fodc/agent/testhelper"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/api"
	grpcproxy "github.com/apache/skywalking-banyandb/fodc/proxy/internal/grpc"
	proxylifecycle "github.com/apache/skywalking-banyandb/fodc/proxy/internal/lifecycle"
	metricsproxy "github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/pressure"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// This is the in-process ("code way") counterpart of the Kind real-process e2e: it wires the
// real watchdog -> flight recorder -> pressure profiler -> agent proxy client against a real
// proxy (gRPC + HTTP), with only the BanyanDB node replaced by an httptest stub that serves
// /metrics and the pprof endpoints. A low trigger percentage forces a capture deterministically.
var _ = Describe("Pressure Profile Integration", func() {
	var (
		proxyGRPCAddr string
		proxyHTTPAddr string
		grpcServer    *grpcproxy.Server
		httpServer    *api.Server
		agentRegistry *registry.AgentRegistry
		metricsAgg    *metricsproxy.Aggregator
		pressureAgg   *pressure.Aggregator
		grpcService   *grpcproxy.FODCService
		lifecycleMgr  *proxylifecycle.Manager
		httpClient    *http.Client
		testLogger    *logger.Logger
		banyandStub   *httptest.Server
		heapBytes     []byte
		gorBytes      []byte
	)

	BeforeEach(func() {
		testLogger = logger.GetLogger("test", "pressure-integration")
		httpClient = &http.Client{Timeout: 10 * time.Second}

		heartbeatTimeout := 5 * time.Second
		cleanupTimeout := 10 * time.Second
		heartbeatInterval := 2 * time.Second
		agentRegistry = registry.NewAgentRegistry(testLogger, heartbeatTimeout, cleanupTimeout, 100)
		lifecycleMgr = proxylifecycle.NewManager(agentRegistry, nil, testLogger)
		metricsAgg = metricsproxy.NewAggregator(agentRegistry, nil, testLogger)
		pressureAgg = pressure.NewAggregator(agentRegistry, nil, testLogger)
		grpcService = grpcproxy.NewFODCService(agentRegistry, metricsAgg, nil, lifecycleMgr, nil, pressureAgg, testLogger, heartbeatInterval)
		metricsAgg.SetGRPCService(grpcService)
		lifecycleMgr.SetGRPCService(grpcService)
		pressureAgg.SetGRPCService(grpcService)

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
		httpServer = api.NewServer(metricsAgg, nil, lifecycleMgr, agentRegistry, nil, pressureAgg, grpcService, testLogger)
		Expect(httpServer.Start(proxyHTTPAddr, 10*time.Second, 10*time.Second)).To(Succeed())
		Eventually(func() error {
			resp, healthErr := httpClient.Get(fmt.Sprintf("http://%s/health", proxyHTTPAddr))
			if healthErr != nil {
				return healthErr
			}
			resp.Body.Close()
			return nil
		}, "10s", "100ms").Should(Succeed())

		// Stub BanyanDB node: real /metrics text plus real pprof bytes.
		heapBytes = []byte("HEAP-PPROF-" + strings.Repeat("h", 4096))
		gorBytes = []byte("GOROUTINE-PPROF-" + strings.Repeat("g", 1024))
		mux := http.NewServeMux()
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "# HELP process_resident_memory_bytes Resident memory size in bytes.\n"+
				"# TYPE process_resident_memory_bytes gauge\n"+
				"process_resident_memory_bytes 500\n"+
				"# HELP banyandb_memory_protector_cgroup_limit_bytes Cgroup memory limit in bytes.\n"+
				"# TYPE banyandb_memory_protector_cgroup_limit_bytes gauge\n"+
				"banyandb_memory_protector_cgroup_limit_bytes 1000\n")
		})
		mux.HandleFunc("/debug/pprof/heap", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(heapBytes) })
		mux.HandleFunc("/debug/pprof/goroutine", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(gorBytes) })
		banyandStub = httptest.NewServer(mux)
	})

	AfterEach(func() {
		if banyandStub != nil {
			banyandStub.Close()
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

	stubPort := func() string {
		u, parseErr := url.Parse(banyandStub.URL)
		Expect(parseErr).NotTo(HaveOccurred())
		return u.Port()
	}

	getProfiles := func() []pressure.AggregatedPressureProfile {
		resp, getErr := httpClient.Get(fmt.Sprintf("http://%s/pressure-profiles", proxyHTTPAddr))
		Expect(getErr).NotTo(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		var records []pressure.AggregatedPressureProfile
		Expect(json.NewDecoder(resp.Body).Decode(&records)).To(Succeed())
		return records
	}

	download := func(pod, profileID, profType string) (int, []byte, string) {
		resp, getErr := httpClient.Get(fmt.Sprintf("http://%s/pressure-profiles/%s/%s/%s", proxyHTTPAddr, pod, profileID, profType))
		Expect(getErr).NotTo(HaveOccurred())
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, body, resp.Header.Get("Content-Disposition")
	}

	startAgent := func(podName, dir string) func() {
		agentCtx, agentCancel := context.WithCancel(context.Background())
		agent, newErr := testhelper.NewPressureAgent(agentCtx, testhelper.PressureAgentConfig{
			Logger:         testLogger,
			ProxyAddr:      proxyGRPCAddr,
			PodName:        podName,
			Role:           "standalone",
			MetricsURL:     banyandStub.URL + "/metrics",
			PprofPort:      stubPort(),
			Dir:            dir,
			Cooldown:       time.Hour, // capture exactly once for a deterministic assertion
			TriggerPercent: 1,         // 500/1000 = 50% >= 1% -> always triggers
		})
		Expect(newErr).NotTo(HaveOccurred())
		Expect(agent.Connect(agentCtx)).To(Succeed())
		Expect(agent.StartRegistrationStream(agentCtx)).To(Succeed())
		Expect(agent.StartPressureProfilesStream(agentCtx)).To(Succeed())

		Eventually(func(g Gomega) {
			agents := agentRegistry.ListAgents()
			found := false
			for _, agentInfo := range agents {
				if agentInfo.AgentIdentity.PodName == podName {
					found = true
					g.Expect(grpcService.HasPressureProfilesStream(agentInfo.AgentID)).To(BeTrue())
				}
			}
			g.Expect(found).To(BeTrue())
		}, "10s", "100ms").Should(Succeed())

		return func() {
			agent.Stop()
			agentCancel()
		}
	}

	It("captures pprof under memory pressure and serves it end-to-end via the proxy", func() {
		dir := GinkgoT().TempDir()
		podName := "banyand-standalone-0"
		stop := startAgent(podName, dir)
		defer stop()

		By("listing the captured event via GET /pressure-profiles")
		var records []pressure.AggregatedPressureProfile
		Eventually(func(g Gomega) {
			records = getProfiles()
			g.Expect(records).To(HaveLen(1))
			g.Expect(records[0].Profiles).To(HaveLen(2))
		}, "20s", "500ms").Should(Succeed())

		rec := records[0]
		Expect(rec.PodName).To(Equal(podName))
		Expect(rec.Role).To(Equal("standalone"))
		Expect(rec.RSSBytes).To(Equal(uint64(500)))
		Expect(rec.CgroupLimitBytes).To(Equal(uint64(1000)))
		Expect(rec.ThresholdBytes).To(Equal(uint64(10))) // 1000 * 1 / 100

		By("downloading each profile via GET /pressure-profiles/{pod}/{id}/{type}")
		heapStatus, heapDL, heapCD := download(podName, rec.ProfileID, "heap")
		Expect(heapStatus).To(Equal(http.StatusOK))
		Expect(heapDL).To(Equal(heapBytes))
		Expect(heapCD).To(ContainSubstring(fmt.Sprintf("%s-standalone-%s-heap.pprof", podName, rec.ProfileID)))

		gorStatus, gorDL, _ := download(podName, rec.ProfileID, "goroutine")
		Expect(gorStatus).To(Equal(http.StatusOK))
		Expect(gorDL).To(Equal(gorBytes))

		By("verifying the captured files exist on the agent's disk")
		entries, readErr := os.ReadDir(filepath.Join(dir, rec.ProfileID))
		Expect(readErr).NotTo(HaveOccurred())
		names := make(map[string]bool)
		for _, e := range entries {
			names[e.Name()] = true
		}
		Expect(names["heap.pprof"]).To(BeTrue())
		Expect(names["goroutine.pprof"]).To(BeTrue())
		Expect(names["meta.json"]).To(BeTrue())

		By("returning 404 for an unknown profile id")
		missingStatus, _, _ := download(podName, "20000101T000000.000000000Z", "heap")
		Expect(missingStatus).To(Equal(http.StatusNotFound))
	})
})
