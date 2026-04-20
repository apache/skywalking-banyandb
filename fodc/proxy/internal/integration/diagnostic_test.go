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
	"strings"
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
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

// writePanicReport serializes a PanicRecord as a lifecycle report JSON file in dir.
func writePanicReport(dir, filename string, rec *panicdiag.PanicRecord) {
	data, marshalErr := json.MarshalIndent(rec, "", "  ")
	Expect(marshalErr).NotTo(HaveOccurred())
	Expect(os.WriteFile(filepath.Join(dir, filename), append(data, '\n'), 0o600)).To(Succeed())
}

var _ = Describe("Diagnostic Integration", func() {
	var (
		proxyGRPCAddr string
		proxyHTTPAddr string
		grpcServer    *grpcproxy.Server
		httpServer    *api.Server
		agentRegistry *registry.AgentRegistry
		metricsAgg    *metricsproxy.Aggregator
		grpcService   *grpcproxy.FODCService
		lifecycleMgr  *proxylifecycle.Manager
		httpClient    *http.Client
		testLogger    *logger.Logger
	)

	BeforeEach(func() {
		testLogger = logger.GetLogger("test", "diagnostic-integration")
		httpClient = &http.Client{Timeout: 5 * time.Second}

		heartbeatTimeout := 5 * time.Second
		cleanupTimeout := 10 * time.Second
		heartbeatInterval := 2 * time.Second
		agentRegistry = registry.NewAgentRegistry(testLogger, heartbeatTimeout, cleanupTimeout, 100)

		lifecycleMgr = proxylifecycle.NewManager(agentRegistry, nil, testLogger)
		metricsAgg = metricsproxy.NewAggregator(agentRegistry, nil, testLogger)
		grpcService = grpcproxy.NewFODCService(agentRegistry, metricsAgg, nil, lifecycleMgr, testLogger, heartbeatInterval)
		metricsAgg.SetGRPCService(grpcService)
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
		httpServer = api.NewServer(metricsAgg, nil, lifecycleMgr, agentRegistry, testLogger)
		Expect(httpServer.Start(proxyHTTPAddr, 10*time.Second, 10*time.Second)).To(Succeed())

		Eventually(func() error {
			resp, healthErr := httpClient.Get(fmt.Sprintf("http://%s/health", proxyHTTPAddr))
			if healthErr != nil {
				return healthErr
			}
			resp.Body.Close()
			return nil
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Succeed())
	})

	AfterEach(func() {
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

	// getLifecycleResult queries /cluster/lifecycle and decodes the result.
	getLifecycleResult := func() *proxylifecycle.InspectionResult {
		resp, getErr := httpClient.Get(fmt.Sprintf("http://%s/cluster/lifecycle", proxyHTTPAddr))
		Expect(getErr).NotTo(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		var result proxylifecycle.InspectionResult
		Expect(json.NewDecoder(resp.Body).Decode(&result)).To(Succeed())
		return &result
	}

	// connectAgentWithLifecycleStream registers an agent, establishes its lifecycle stream,
	// and waits until the stream is visible in the gRPC service. Returns cancel for cleanup.
	connectAgentWithLifecycleStream := func(
		podName, role, reportDir string,
	) (*testhelper.ProxyClientWrapper, context.CancelFunc) {
		agentCtx, agentCancel := context.WithCancel(context.Background())
		fr := testhelper.NewFlightRecorder(int64(10 * 1024 * 1024))
		client := testhelper.NewProxyClientWrapper(
			proxyGRPCAddr,
			role,
			podName,
			[]string{role},
			map[string]string{"role": role},
			2*time.Second,
			1*time.Second,
			fr,
			testLogger,
			reportDir,
		)
		Expect(client).NotTo(BeNil())
		Expect(client.Connect(agentCtx)).To(Succeed())
		Expect(client.StartRegistrationStream(agentCtx)).To(Succeed())
		Expect(client.StartLifecycleStream(agentCtx)).To(Succeed())

		// Wait until the registry sees this agent.
		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(BeNumerically(">=", 1))

		// Wait until the lifecycle stream is active for this agent.
		Eventually(func(g Gomega) {
			agents := agentRegistry.ListAgents()
			found := false
			for _, a := range agents {
				if a.AgentIdentity.PodName == podName {
					found = true
					g.Expect(grpcService.HasLifecycleStream(a.AgentID)).To(BeTrue())
				}
			}
			g.Expect(found).To(BeTrue())
		}, "5s", "100ms").Should(Succeed())

		return client, agentCancel
	}

	// TestPanicRecordSurvivedThroughProxyLifecycle verifies that a PanicRecord written as a
	// lifecycle report JSON file is surfaced unmodified via /cluster/lifecycle, and that the
	// proxy preserves the component, panicValue, and recovered fields in the embedded ReportJson.
	It("surfaces a single-agent panic record via /cluster/lifecycle", func() {
		reportDir := GinkgoT().TempDir()
		client, cancel := connectAgentWithLifecycleStream("pod-diag-single", "storage-engine", reportDir)
		defer func() {
			cancel()
			_ = client.Disconnect()
		}()

		By("writing a panic record as a lifecycle report file")
		rec := &panicdiag.PanicRecord{
			OccurredAt: time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC),
			Component:  "storage-engine",
			PanicValue: "nil pointer dereference in compaction",
			Recovered:  true,
		}
		writePanicReport(reportDir, "2026-04-20T100000Z-panic.json", rec)

		By("verifying the proxy surfaces the panic record in the lifecycle response")
		Eventually(func(g Gomega) {
			result := getLifecycleResult()
			g.Expect(len(result.LifecycleStatuses)).To(Equal(1))
			status := result.LifecycleStatuses[0]
			g.Expect(status.PodName).To(Equal("pod-diag-single"))
			g.Expect(len(status.Reports)).To(Equal(1))
			reportJSON := status.Reports[0].ReportJson
			g.Expect(reportJSON).To(ContainSubstring("storage-engine"))
			g.Expect(reportJSON).To(ContainSubstring("nil pointer dereference in compaction"))
		}, "5s", "100ms").Should(Succeed())
	})

	// TestMultipleAgentCrashesAggregatedByProxy verifies that when two agents — a liaison
	// and a data-node — both have panic records in their report directories, the proxy
	// aggregates lifecycle statuses from both, preserving the pod names and component fields.
	It("aggregates panic reports from two agents with distinct components", func() {
		reportDirLiaison := GinkgoT().TempDir()
		reportDirData := GinkgoT().TempDir()

		clientLiaison, cancelLiaison := connectAgentWithLifecycleStream("pod-liaison", "liaison", reportDirLiaison)
		defer func() {
			cancelLiaison()
			_ = clientLiaison.Disconnect()
		}()

		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Equal(1))

		clientData, cancelData := connectAgentWithLifecycleStream("pod-data", "datanode", reportDirData)
		defer func() {
			cancelData()
			_ = clientData.Disconnect()
		}()

		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Equal(2))

		By("writing distinct panic records for each agent")
		writePanicReport(reportDirLiaison, "2026-04-20T100000Z-panic.json", &panicdiag.PanicRecord{
			OccurredAt: time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC),
			Component:  "liaison",
			PanicValue: "concurrent map write in routing table",
			Recovered:  true,
		})
		writePanicReport(reportDirData, "2026-04-20T100001Z-panic.json", &panicdiag.PanicRecord{
			OccurredAt: time.Date(2026, time.April, 20, 10, 0, 1, 0, time.UTC),
			Component:  "datanode",
			PanicValue: "index out of range in flush buffer",
			Recovered:  false,
		})

		By("verifying the proxy aggregates both pods")
		Eventually(func(g Gomega) {
			result := getLifecycleResult()
			g.Expect(len(result.LifecycleStatuses)).To(Equal(2))

			byPod := make(map[string]*proxylifecycle.PodLifecycleStatus, 2)
			for _, podStatus := range result.LifecycleStatuses {
				byPod[podStatus.PodName] = podStatus
			}

			liaisonStatus, exists := byPod["pod-liaison"]
			g.Expect(exists).To(BeTrue(), "liaison pod must appear in lifecycle statuses")
			g.Expect(len(liaisonStatus.Reports)).To(Equal(1))
			g.Expect(liaisonStatus.Reports[0].ReportJson).To(ContainSubstring("concurrent map write in routing table"))

			dataStatus, exists := byPod["pod-data"]
			g.Expect(exists).To(BeTrue(), "data pod must appear in lifecycle statuses")
			g.Expect(len(dataStatus.Reports)).To(Equal(1))
			g.Expect(dataStatus.Reports[0].ReportJson).To(ContainSubstring("index out of range in flush buffer"))
		}, "5s", "100ms").Should(Succeed())
	})

	// TestBreadcrumbsPreservedThroughProxyLifecycle verifies that a PanicRecord containing
	// breadcrumbs written as a lifecycle report JSON is relayed unmodified to the caller of
	// /cluster/lifecycle, so operators can trace the execution path that led to the crash.
	It("preserves panic record breadcrumbs through the proxy lifecycle pipeline", func() {
		reportDir := GinkgoT().TempDir()
		client, cancel := connectAgentWithLifecycleStream("pod-breadcrumb", "query-engine", reportDir)
		defer func() {
			cancel()
			_ = client.Disconnect()
		}()

		By("writing a panic record with three breadcrumbs to the report directory")
		rec := &panicdiag.PanicRecord{
			OccurredAt: time.Date(2026, time.April, 20, 11, 0, 0, 0, time.UTC),
			Component:  "query-engine",
			PanicValue: "invalid memory address or nil pointer dereference",
			Recovered:  true,
			Breadcrumbs: []panicdiag.Breadcrumb{
				{
					Stage:     "parse-query",
					Component: "query-parser",
					Fields:    map[string]string{"query_id": "q-100", "table": "trace_segments"},
				},
				{
					Stage:     "build-plan",
					Component: "planner",
					Fields:    nil,
				},
				{
					Stage:     "execute-scan",
					Component: "executor",
					Fields:    map[string]string{"shard": "shard-2"},
				},
			},
		}
		writePanicReport(reportDir, "2026-04-20T110000Z-panic.json", rec)

		By("verifying breadcrumbs are present in the JSON surfaced by the proxy")
		Eventually(func(g Gomega) {
			result := getLifecycleResult()
			g.Expect(len(result.LifecycleStatuses)).To(Equal(1))
			g.Expect(len(result.LifecycleStatuses[0].Reports)).To(Equal(1))
			reportJSON := result.LifecycleStatuses[0].Reports[0].ReportJson

			g.Expect(reportJSON).To(ContainSubstring("parse-query"))
			g.Expect(reportJSON).To(ContainSubstring("build-plan"))
			g.Expect(reportJSON).To(ContainSubstring("execute-scan"))
			g.Expect(reportJSON).To(ContainSubstring("q-100"))
			g.Expect(reportJSON).To(ContainSubstring("shard-2"))
		}, "5s", "100ms").Should(Succeed())
	})

	// TestProxyLifecycleReportsCapAt5Files verifies that the agent's lifecycle collector
	// enforces the maxReportFiles=5 limit before sending to the proxy.  When six crash
	// report files are present, only the five lexicographically newest filenames must
	// appear in the proxy's /cluster/lifecycle response; the oldest must be absent.
	It("limits lifecycle reports to the five newest files per agent", func() {
		reportDir := GinkgoT().TempDir()
		client, cancel := connectAgentWithLifecycleStream("pod-cap", "storage-compactor", reportDir)
		defer func() {
			cancel()
			_ = client.Disconnect()
		}()

		By("writing six crash report files in chronological name order")
		timestamps := []string{
			"2026-04-20T060000Z",
			"2026-04-20T070000Z",
			"2026-04-20T080000Z",
			"2026-04-20T090000Z",
			"2026-04-20T100000Z",
			"2026-04-20T110000Z",
		}
		for idx, ts := range timestamps {
			writePanicReport(reportDir, ts+"-panic.json", &panicdiag.PanicRecord{
				OccurredAt: time.Date(2026, time.April, 20, 6+idx, 0, 0, 0, time.UTC),
				Component:  "storage-compactor",
				PanicValue: fmt.Sprintf("crash %d", idx),
				Recovered:  true,
			})
		}

		By("verifying the proxy surfaces exactly five reports and the oldest is absent")
		Eventually(func(g Gomega) {
			result := getLifecycleResult()
			g.Expect(len(result.LifecycleStatuses)).To(Equal(1))
			reports := result.LifecycleStatuses[0].Reports
			g.Expect(len(reports)).To(Equal(5))

			filenames := make([]string, 0, len(reports))
			for _, r := range reports {
				filenames = append(filenames, r.Filename)
			}
			g.Expect(filenames).NotTo(ContainElement("2026-04-20T060000Z-panic.json"),
				"oldest report must be evicted by the 5-file cap")
			for _, ts := range timestamps[1:] {
				g.Expect(filenames).To(ContainElement(ts+"-panic.json"),
					"newer reports must appear in the lifecycle response")
			}
		}, "5s", "100ms").Should(Succeed())
	})

	// TestProxyLifecycleFreshDataAfterNewCrash verifies that a second call to
	// /cluster/lifecycle made after a new panic report file is written returns the new
	// file — i.e., the proxy does not cache stale lifecycle data across requests.
	It("returns updated crash reports after a new panic is written", func() {
		reportDir := GinkgoT().TempDir()
		client, cancel := connectAgentWithLifecycleStream("pod-refresh", "compactor", reportDir)
		defer func() {
			cancel()
			_ = client.Disconnect()
		}()

		By("writing the first crash report and waiting for the proxy to surface it")
		writePanicReport(reportDir, "2026-04-20T080000Z-panic.json", &panicdiag.PanicRecord{
			OccurredAt: time.Date(2026, time.April, 20, 8, 0, 0, 0, time.UTC),
			Component:  "compactor",
			PanicValue: "first crash: out of memory",
			Recovered:  true,
		})

		Eventually(func(g Gomega) {
			result := getLifecycleResult()
			g.Expect(len(result.LifecycleStatuses)).To(Equal(1))
			g.Expect(result.LifecycleStatuses[0].Reports[0].ReportJson).
				To(ContainSubstring("first crash: out of memory"))
		}, "5s", "100ms").Should(Succeed())

		By("writing a second crash report and verifying the proxy now surfaces both")
		writePanicReport(reportDir, "2026-04-20T090000Z-panic.json", &panicdiag.PanicRecord{
			OccurredAt: time.Date(2026, time.April, 20, 9, 0, 0, 0, time.UTC),
			Component:  "compactor",
			PanicValue: "second crash: index out of range",
			Recovered:  false,
		})

		Eventually(func(g Gomega) {
			result := getLifecycleResult()
			g.Expect(len(result.LifecycleStatuses)).To(Equal(1))
			filenames := make([]string, 0)
			for _, r := range result.LifecycleStatuses[0].Reports {
				filenames = append(filenames, r.Filename)
			}
			g.Expect(filenames).To(ContainElements(
				"2026-04-20T080000Z-panic.json",
				"2026-04-20T090000Z-panic.json",
			))
			reportJSONs := make([]string, 0)
			for _, r := range result.LifecycleStatuses[0].Reports {
				reportJSONs = append(reportJSONs, r.ReportJson)
			}
			combined := strings.Join(reportJSONs, " ")
			g.Expect(combined).To(ContainSubstring("first crash: out of memory"))
			g.Expect(combined).To(ContainSubstring("second crash: index out of range"))
		}, "5s", "100ms").Should(Succeed())
	})
})
