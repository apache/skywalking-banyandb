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
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/fodc/agent/testhelper"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/api"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/diagnostics"
	grpcproxy "github.com/apache/skywalking-banyandb/fodc/proxy/internal/grpc"
	proxylifecycle "github.com/apache/skywalking-banyandb/fodc/proxy/internal/lifecycle"
	metricsproxy "github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

type crashArtifact struct {
	occurredAt time.Time
	component  string
	panicValue string
	recovered  bool
	stateDump  map[string]string
}

var _ = Describe("Diagnostic Integration", func() {
	var (
		proxyGRPCAddr       string
		proxyHTTPAddr       string
		grpcServer          *grpcproxy.Server
		httpServer          *api.Server
		agentRegistry       *registry.AgentRegistry
		metricsAgg          *metricsproxy.Aggregator
		diagnosticsAgg      *diagnostics.Aggregator
		grpcService         *grpcproxy.FODCService
		lifecycleMgr        *proxylifecycle.Manager
		httpClient          *http.Client
		testLogger          *logger.Logger
		defaultMaxArtifacts int
	)

	BeforeEach(func() {
		testLogger = logger.GetLogger("test", "diagnostic-integration")
		httpClient = &http.Client{Timeout: 5 * time.Second}
		defaultMaxArtifacts = panicdiag.DefaultMaxArtifacts()
		panicdiag.SetDefaultMaxArtifacts(0)

		heartbeatTimeout := 5 * time.Second
		cleanupTimeout := 10 * time.Second
		heartbeatInterval := 2 * time.Second
		agentRegistry = registry.NewAgentRegistry(testLogger, heartbeatTimeout, cleanupTimeout, 100)

		lifecycleMgr = proxylifecycle.NewManager(agentRegistry, nil, testLogger)
		metricsAgg = metricsproxy.NewAggregator(agentRegistry, nil, testLogger)
		diagnosticsAgg = diagnostics.NewAggregator(agentRegistry, nil, testLogger)
		grpcService = grpcproxy.NewFODCService(agentRegistry, metricsAgg, nil, lifecycleMgr, diagnosticsAgg, testLogger, heartbeatInterval)
		metricsAgg.SetGRPCService(grpcService)
		lifecycleMgr.SetGRPCService(grpcService)
		diagnosticsAgg.SetGRPCService(grpcService)

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
		httpServer = api.NewServer(metricsAgg, nil, lifecycleMgr, agentRegistry, diagnosticsAgg, testLogger)
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
		panicdiag.SetDefaultMaxArtifacts(defaultMaxArtifacts)
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

	getDiagnostics := func(query string) []diagnostics.AggregatedCrashRecord {
		url := fmt.Sprintf("http://%s/diagnostics", proxyHTTPAddr)
		if query != "" {
			url += "?" + query
		}
		resp, getErr := httpClient.Get(url)
		Expect(getErr).NotTo(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		var records []diagnostics.AggregatedCrashRecord
		Expect(json.NewDecoder(resp.Body).Decode(&records)).To(Succeed())
		return records
	}

	connectAgentWithCrashStream := func(podName, role, crashDir string) (*testhelper.ProxyClientWrapper, context.CancelFunc) {
		agentCtx, agentCancel := context.WithCancel(context.Background())
		fr := testhelper.NewFlightRecorder(int64(10 * 1024 * 1024))
		client := testhelper.NewProxyClientWrapperWithCrashDir(
			proxyGRPCAddr,
			role,
			podName,
			[]string{role},
			map[string]string{"role": role},
			2*time.Second,
			1*time.Second,
			fr,
			testLogger,
			crashDir,
		)
		Expect(client).NotTo(BeNil())
		Expect(client.Connect(agentCtx)).To(Succeed())
		Expect(client.StartRegistrationStream(agentCtx)).To(Succeed())
		Expect(client.StartCrashStream(agentCtx)).To(Succeed())

		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(BeNumerically(">=", 1))

		Eventually(func(g Gomega) {
			agents := agentRegistry.ListAgents()
			found := false
			for _, agentInfo := range agents {
				if agentInfo.AgentIdentity.PodName == podName {
					found = true
					g.Expect(grpcService.HasCrashDiagnosticsStream(agentInfo.AgentID)).To(BeTrue())
				}
			}
			g.Expect(found).To(BeTrue())
		}, "5s", "100ms").Should(Succeed())

		return client, agentCancel
	}

	writeCrashArtifact := func(rootDir string, artifact crashArtifact) string {
		writer := panicdiag.NewArtifactWriter(rootDir)
		artifactDir, writeErr := writer.Write(&panicdiag.PanicRecord{
			OccurredAt:     artifact.occurredAt,
			Component:      artifact.component,
			PanicValue:     artifact.panicValue,
			Recovered:      artifact.recovered,
			GoroutineStack: "goroutine 42 [running]:\nmain.foo(...)\n\t/src/main.go:17\n",
		})
		Expect(writeErr).NotTo(HaveOccurred())
		if artifact.stateDump != nil {
			_, _, dumpErr := writer.WriteStateDump(artifactDir, artifact.stateDump, 1024)
			Expect(dumpErr).NotTo(HaveOccurred())
		}
		return filepath.Base(artifactDir)
	}

	It("surfaces a single-agent crash artifact via /diagnostics", func() {
		crashDir := GinkgoT().TempDir()
		client, cancel := connectAgentWithCrashStream("pod-diag-single", "storage-engine", crashDir)
		defer func() {
			cancel()
			_ = client.Disconnect()
		}()

		By("writing a crash artifact directory with crash.txt")
		artifactDir := writeCrashArtifact(crashDir, crashArtifact{
			occurredAt: time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC),
			component:  "storage-engine",
			panicValue: "nil pointer dereference in compaction",
			recovered:  true,
			stateDump:  map[string]string{"shard": "shard-1"},
		})
		client.ScanCrashDirectory()

		By("verifying the proxy surfaces the crash record in /diagnostics")
		Eventually(func(g Gomega) {
			records := getDiagnostics("")
			g.Expect(records).To(HaveLen(1))
			record := records[0]
			g.Expect(record.PodName).To(Equal("pod-diag-single"))
			g.Expect(record.SourceEndpoint).To(Equal("file://" + crashDir))
			g.Expect(record.ArtifactDir).To(Equal(artifactDir))
			g.Expect(record.Files).To(ContainElements("crash.txt", "deep-dump.json"))
			g.Expect(record.PanicRecord).NotTo(BeNil())
			g.Expect(record.PanicRecord.Component).To(Equal("storage-engine"))
			g.Expect(record.PanicRecord.PanicValue).To(Equal("nil pointer dereference in compaction"))
			g.Expect(record.PanicRecord.Recovered).To(BeTrue())
		}, "8s", "100ms").Should(Succeed())
	})

	It("aggregates crash artifacts from two agents with distinct components", func() {
		crashDirLiaison := GinkgoT().TempDir()
		crashDirData := GinkgoT().TempDir()

		clientLiaison, cancelLiaison := connectAgentWithCrashStream("pod-liaison", "liaison", crashDirLiaison)
		defer func() {
			cancelLiaison()
			_ = clientLiaison.Disconnect()
		}()

		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Equal(1))

		clientData, cancelData := connectAgentWithCrashStream("pod-data", "datanode", crashDirData)
		defer func() {
			cancelData()
			_ = clientData.Disconnect()
		}()

		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Equal(2))

		By("writing distinct crash artifacts for each agent")
		writeCrashArtifact(crashDirLiaison, crashArtifact{
			occurredAt: time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC),
			component:  "liaison",
			panicValue: "concurrent map write in routing table",
			recovered:  true,
		})
		writeCrashArtifact(crashDirData, crashArtifact{
			occurredAt: time.Date(2026, time.April, 20, 10, 0, 1, 0, time.UTC),
			component:  "datanode",
			panicValue: "index out of range in flush buffer",
			recovered:  false,
		})
		clientLiaison.ScanCrashDirectory()
		clientData.ScanCrashDirectory()

		By("verifying the proxy aggregates both pods")
		Eventually(func(g Gomega) {
			records := getDiagnostics("")
			g.Expect(records).To(HaveLen(2))

			byPod := make(map[string]diagnostics.AggregatedCrashRecord, 2)
			for _, record := range records {
				byPod[record.PodName] = record
			}

			liaisonRecord, exists := byPod["pod-liaison"]
			g.Expect(exists).To(BeTrue(), "liaison pod must appear in diagnostics")
			g.Expect(liaisonRecord.PanicRecord).NotTo(BeNil())
			g.Expect(liaisonRecord.PanicRecord.PanicValue).To(Equal("concurrent map write in routing table"))

			dataRecord, exists := byPod["pod-data"]
			g.Expect(exists).To(BeTrue(), "data pod must appear in diagnostics")
			g.Expect(dataRecord.PanicRecord).NotTo(BeNil())
			g.Expect(dataRecord.PanicRecord.PanicValue).To(Equal("index out of range in flush buffer"))
			g.Expect(dataRecord.PanicRecord.Recovered).To(BeFalse())
		}, "8s", "100ms").Should(Succeed())
	})

	It("limits crash artifacts to the five newest directories per agent", func() {
		panicdiag.SetDefaultMaxArtifacts(5)
		crashDir := GinkgoT().TempDir()
		client, cancel := connectAgentWithCrashStream("pod-cap", "storage-compactor", crashDir)
		defer func() {
			cancel()
			_ = client.Disconnect()
		}()

		By("writing six crash artifact directories in chronological name order")
		timestamps := []time.Time{
			time.Date(2026, time.April, 20, 6, 0, 0, 0, time.UTC),
			time.Date(2026, time.April, 20, 7, 0, 0, 0, time.UTC),
			time.Date(2026, time.April, 20, 8, 0, 0, 0, time.UTC),
			time.Date(2026, time.April, 20, 9, 0, 0, 0, time.UTC),
			time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC),
			time.Date(2026, time.April, 20, 11, 0, 0, 0, time.UTC),
		}
		artifactDirs := make([]string, 0, len(timestamps))
		for idx, occurredAt := range timestamps {
			artifactDirs = append(artifactDirs, writeCrashArtifact(crashDir, crashArtifact{
				occurredAt: occurredAt,
				component:  "storage-compactor",
				panicValue: fmt.Sprintf("crash %d", idx),
				recovered:  true,
			}))
		}
		client.ScanCrashDirectory()

		By("verifying the proxy surfaces exactly five artifacts and the oldest is absent")
		Eventually(func(g Gomega) {
			records := getDiagnostics("")
			g.Expect(records).To(HaveLen(5))

			seenDirs := make([]string, 0, len(records))
			for _, record := range records {
				seenDirs = append(seenDirs, record.ArtifactDir)
			}
			g.Expect(seenDirs).NotTo(ContainElement(artifactDirs[0]), "oldest artifact must be pruned by the 5-directory cap")
			for _, artifactDir := range artifactDirs[1:] {
				g.Expect(seenDirs).To(ContainElement(artifactDir), "newer artifacts must appear in /diagnostics")
			}
		}, "8s", "100ms").Should(Succeed())
	})

	It("returns updated crash diagnostics after a new artifact is written", func() {
		crashDir := GinkgoT().TempDir()
		client, cancel := connectAgentWithCrashStream("pod-refresh", "compactor", crashDir)
		defer func() {
			cancel()
			_ = client.Disconnect()
		}()

		By("writing the first crash artifact and waiting for the proxy to surface it")
		firstArtifactDir := writeCrashArtifact(crashDir, crashArtifact{
			occurredAt: time.Date(2026, time.April, 20, 8, 0, 0, 0, time.UTC),
			component:  "compactor",
			panicValue: "first crash: out of memory",
			recovered:  true,
		})
		client.ScanCrashDirectory()

		Eventually(func(g Gomega) {
			records := getDiagnostics("")
			g.Expect(records).To(HaveLen(1))
			g.Expect(records[0].ArtifactDir).To(Equal(firstArtifactDir))
			g.Expect(records[0].PanicRecord).NotTo(BeNil())
			g.Expect(records[0].PanicRecord.PanicValue).To(Equal("first crash: out of memory"))
		}, "8s", "100ms").Should(Succeed())

		By("writing a second crash artifact and verifying the proxy now surfaces both")
		secondArtifactDir := writeCrashArtifact(crashDir, crashArtifact{
			occurredAt: time.Date(2026, time.April, 20, 9, 0, 0, 0, time.UTC),
			component:  "compactor",
			panicValue: "second crash: index out of range",
			recovered:  false,
		})
		client.ScanCrashDirectory()

		Eventually(func(g Gomega) {
			records := getDiagnostics("")
			g.Expect(records).To(HaveLen(2))
			byDir := make(map[string]diagnostics.AggregatedCrashRecord, len(records))
			for _, record := range records {
				byDir[record.ArtifactDir] = record
			}
			g.Expect(byDir).To(HaveKey(firstArtifactDir))
			g.Expect(byDir).To(HaveKey(secondArtifactDir))
			g.Expect(byDir[secondArtifactDir].PanicRecord).NotTo(BeNil())
			g.Expect(byDir[secondArtifactDir].PanicRecord.PanicValue).To(Equal("second crash: index out of range"))
			g.Expect(byDir[secondArtifactDir].PanicRecord.Recovered).To(BeFalse())
		}, "8s", "100ms").Should(Succeed())
	})
})
