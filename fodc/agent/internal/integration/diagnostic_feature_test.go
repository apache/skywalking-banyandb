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
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/crashcollector"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/exporter"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/server"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
	testenv "github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

func TestDiagnosticFeatureE2E(t *testing.T) {
	t.Helper()
	gomega.RegisterTestingT(t)

	initErr := logger.Init(logger.Logging{Env: "dev", Level: "debug"})
	if initErr != nil {
		require.NoError(t, initErr)
	}

	artifactRoot := t.TempDir()
	previousRoot := panicdiag.DefaultArtifactRoot()
	panicdiag.SetDefaultArtifactRoot(artifactRoot)
	t.Cleanup(func() {
		panicdiag.SetDefaultArtifactRoot(previousRoot)
	})

	ports, err := testenv.AllocateFreePorts(3)
	require.NoError(t, err)

	observabilityAddr := fmt.Sprintf(":%d", ports[0])
	agentMetricsAddr := fmt.Sprintf("127.0.0.1:%d", ports[1])
	agentDiagnosisAddr := fmt.Sprintf("127.0.0.1:%d", ports[2])

	_, httpAddr, closeStandalone := setup.EmptyStandalone(
		nil,
		"--observability-modes=prometheus",
		"--observability-listener-addr="+observabilityAddr,
	)
	t.Cleanup(closeStandalone)

	observabilityHost := testHost(t, httpAddr)
	sourceMetricsEndpoint := fmt.Sprintf("http://%s%s/metrics", observabilityHost, observabilityAddr)
	sourceCollectionsURL := fmt.Sprintf("http://%s%s/diagnostics/collections", observabilityHost, observabilityAddr)
	agentCollectionsURL := "http://" + agentDiagnosisAddr + "/collections"

	require.Eventually(t, func() bool {
		response, getErr := http.Get(sourceMetricsEndpoint)
		if getErr != nil {
			return false
		}
		defer func() {
			_ = response.Body.Close()
		}()
		return response.StatusCode == http.StatusOK
	}, 10*time.Second, 200*time.Millisecond)

	artifactDir := writeDiagnosticArtifact(t, artifactRoot, "diagnostic-e2e")

	require.Eventually(t, func() bool {
		collections, fetchErr := fetchCollections(sourceCollectionsURL)
		if fetchErr != nil {
			return false
		}
		return containsCollection(collections, filepath.Base(artifactDir), "diagnostic-e2e")
	}, 10*time.Second, 200*time.Millisecond)

	collector := crashcollector.New(logger.GetLogger("test", "diagnostic-e2e"), crashcollector.Config{
		SourceEndpoints: []string{sourceMetricsEndpoint},
		PollInterval:    100 * time.Millisecond,
		BufferSize:      8,
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stopCollector := collector.Start(ctx)
	if stopCollector != nil {
		t.Cleanup(stopCollector)
	}

	fr := flightrecorder.NewFlightRecorder(1024 * 1024)
	metricsServer, serverErr := server.NewServer(server.Config{
		ListenAddr:          agentMetricsAddr,
		DiagnosisListenAddr: agentDiagnosisAddr,
		ReadHeaderTimeout:   time.Second,
		ShutdownTimeout:     time.Second,
	})
	require.NoError(t, serverErr)

	serverErrCh, startErr := metricsServer.Start(
		prometheus.NewRegistry(),
		exporter.NewDatasourceCollector(fr),
		collector,
	)
	require.NoError(t, startErr)
	t.Cleanup(func() {
		stopErr := metricsServer.Stop(context.Background())
		require.NoError(t, stopErr)
		select {
		case asyncErr := <-serverErrCh:
			require.NoError(t, asyncErr)
		default:
		}
	})

	require.Eventually(t, func() bool {
		records, fetchErr := fetchCollectionRecords(agentCollectionsURL)
		if fetchErr != nil {
			return false
		}
		return containsCollectedRecord(records, sourceMetricsEndpoint, filepath.Base(artifactDir), "diagnostic-e2e")
	}, 10*time.Second, 200*time.Millisecond)

	sourceCollections, err := fetchCollections(sourceCollectionsURL)
	require.NoError(t, err)
	collectedRecords, err := fetchCollectionRecords(agentCollectionsURL)
	require.NoError(t, err)

	assert.True(t, containsCollection(sourceCollections, filepath.Base(artifactDir), "diagnostic-e2e"))
	assert.True(t, containsCollectedRecord(collectedRecords, sourceMetricsEndpoint, filepath.Base(artifactDir), "diagnostic-e2e"))
}

func writeDiagnosticArtifact(t *testing.T, root string, component string) string {
	t.Helper()

	writer := panicdiag.NewArtifactWriter(root)
	artifactDir, err := writer.Write(&panicdiag.PanicRecord{
		OccurredAt:     time.Date(2026, time.April, 15, 8, 0, 0, 0, time.UTC),
		Component:      component,
		PanicValue:     "boom",
		Recovered:      true,
		GoroutineStack: "goroutine 1 [running]:\nexample stack\n",
	})
	require.NoError(t, err)
	return artifactDir
}

func fetchCollections(url string) ([]panicdiag.Collection, error) {
	response, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d", response.StatusCode)
	}

	var collections []panicdiag.Collection
	if decodeErr := json.NewDecoder(response.Body).Decode(&collections); decodeErr != nil {
		return nil, decodeErr
	}
	return collections, nil
}

func fetchCollectionRecords(url string) ([]crashcollector.CollectionRecord, error) {
	response, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d", response.StatusCode)
	}

	var records []crashcollector.CollectionRecord
	if decodeErr := json.NewDecoder(response.Body).Decode(&records); decodeErr != nil {
		return nil, decodeErr
	}
	return records, nil
}

func containsCollection(collections []panicdiag.Collection, artifactDir string, component string) bool {
	for _, collection := range collections {
		if collection.ArtifactDir != artifactDir {
			continue
		}
		if collection.Record == nil {
			continue
		}
		if collection.Record.Component != component {
			continue
		}
		if !hasFile(collection.Files, "panic.json") || !hasFile(collection.Files, "crash.txt") {
			continue
		}
		return true
	}
	return false
}

func containsCollectedRecord(records []crashcollector.CollectionRecord, sourceEndpoint string, artifactDir string, component string) bool {
	for _, record := range records {
		if record.SourceEndpoint != sourceEndpoint {
			continue
		}
		if record.Collection.ArtifactDir != artifactDir {
			continue
		}
		if record.Collection.Record == nil {
			continue
		}
		if record.Collection.Record.Component != component {
			continue
		}
		return true
	}
	return false
}

func hasFile(files []string, target string) bool {
	for _, file := range files {
		if file == target {
			return true
		}
	}
	return false
}

func testHost(t *testing.T, httpAddr string) string {
	t.Helper()

	host, _, err := net.SplitHostPort(httpAddr)
	require.NoError(t, err)
	if host == "" {
		return defaultLocalhost
	}
	return host
}
