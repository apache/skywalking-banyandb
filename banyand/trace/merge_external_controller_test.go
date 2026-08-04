// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package trace

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark"
	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark/controller"
	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const externalControllerEnv = "BANYANDB_TRACE_EXTERNAL_CONTROLLER"

type externalControllerConfig struct {
	Parts                      []externalControllerPart    `json:"parts"`
	CPUs                       []int                       `json:"cpus"`
	Endpoint                   string                      `json:"endpoint"`
	ReportPath                 string                      `json:"reportPath"`
	DataNode                   controller.ResourceIdentity `json:"dataNode"`
	AllowUnisolatedDevelopment bool                        `json:"allowUnisolatedDevelopment"`
}

type externalControllerPart struct {
	StageMoves   []controller.Move `json:"stageMoves"`
	PublishMoves []controller.Move `json:"publishMoves"`
	PartID       uint64            `json:"partID"`
	LogicalNow   int64             `json:"logicalNow"`
}

type externalControllerReport struct {
	Identity         controller.ResourceIdentity `json:"identity"`
	ResourceIsolated bool                        `json:"resourceIsolated"`
	Moves            int                         `json:"moves"`
	Parts            int                         `json:"parts"`
	Bytes            uint64                      `json:"bytes"`
}

type externalPublishRequest struct {
	Checksums  map[string]string `json:"checksums"`
	LogicalNow int64             `json:"logicalNow"`
	PartID     uint64            `json:"partID"`
}

func TestMergeExternalControllerSmoke(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	root := t.TempDir()
	tableRoot := filepath.Join(root, "data")
	preparedRoot := filepath.Join(root, "prepared")
	stagingRoot := filepath.Join(root, "staging")
	require.NoError(t, os.MkdirAll(tableRoot, 0o755))
	for _, base := range []string{preparedRoot, stagingRoot} {
		for _, relative := range []string{"core", filepath.Join("sidx", "latency"), filepath.Join("sidx", "start_time")} {
			require.NoError(t, os.MkdirAll(filepath.Join(base, relative), 0o755))
		}
	}
	for _, indexName := range []string{"latency", "start_time"} {
		require.NoError(t, os.MkdirAll(filepath.Join(tableRoot, sidxDirName, indexName), 0o755))
	}

	partIDs := []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	logicalBase := time.Date(2026, time.July, 30, 12, 0, 0, 0, time.UTC)
	buildExternalCoreParts(t, fileSystem, filepath.Join(preparedRoot, "core"), partIDs, logicalBase)
	for _, indexName := range []string{"latency", "start_time"} {
		buildExternalIndexParts(t, fileSystem, filepath.Join(preparedRoot, "sidx", indexName), partIDs, logicalBase)
	}

	var logicalNow atomic.Int64
	logicalNow.Store(logicalBase.Add(-time.Minute).UnixNano())
	tableOptions := option{
		flushTimeout: 0,
		mergePolicy:  newDefaultMergePolicy(),
		protector:    protector.Nop{},
	}
	table, tableErr := newTSTable(
		fileSystem,
		tableRoot,
		common.Position{Database: "external-controller"},
		logger.GetLogger("external-controller"),
		timestamp.TimeRange{},
		tableOptions,
		nil,
	)
	require.NoError(t, tableErr)
	table.setMergeNow(time.Unix(0, logicalNow.Load()))
	tableClosed := false
	t.Cleanup(func() {
		if !tableClosed {
			require.NoError(t, table.Close())
		}
	})
	for _, indexName := range []string{"latency", "start_time"} {
		table.mustGetOrCreateSidx(indexName)
	}
	table.observePartID(partIDs[len(partIDs)-1])

	expectedChecksums := make(map[uint64]map[string]string, len(partIDs))
	controllerParts := make([]externalControllerPart, 0, len(partIDs))
	for partIdx, partID := range partIDs {
		var stageMoves, publishMoves []controller.Move
		expectedChecksums[partID] = make(map[string]string, 3)
		locations := []struct {
			prepared  string
			staged    string
			published string
		}{
			{
				prepared:  filepath.Join(preparedRoot, "core", formatExternalPartID(partID)),
				staged:    filepath.Join(stagingRoot, "core", formatExternalPartID(partID)),
				published: filepath.Join(tableRoot, formatExternalPartID(partID)),
			},
		}
		for _, indexName := range []string{"latency", "start_time"} {
			locations = append(locations, struct {
				prepared  string
				staged    string
				published string
			}{
				prepared:  filepath.Join(preparedRoot, "sidx", indexName, formatExternalPartID(partID)),
				staged:    filepath.Join(stagingRoot, "sidx", indexName, formatExternalPartID(partID)),
				published: filepath.Join(tableRoot, sidxDirName, indexName, formatExternalPartID(partID)),
			})
		}
		for _, location := range locations {
			manifest, manifestErr := benchmark.TreeManifest(location.prepared)
			require.NoError(t, manifestErr)
			stageMoves = append(stageMoves, controller.Move{Source: location.prepared, Destination: location.staged, SHA256: manifest.SHA256})
			publishMoves = append(publishMoves, controller.Move{Source: location.staged, Destination: location.published, SHA256: manifest.SHA256})
			expectedChecksums[partID][location.published] = manifest.SHA256
		}
		controllerParts = append(controllerParts, externalControllerPart{
			StageMoves:   stageMoves,
			PublishMoves: publishMoves,
			PartID:       partID,
			LogicalNow:   logicalBase.Add(time.Duration(partIdx) * time.Minute).UnixNano(),
		})
	}

	handler := http.NewServeMux()
	handler.HandleFunc("/publish", func(responseWriter http.ResponseWriter, request *http.Request) {
		var publish externalPublishRequest
		if decodeErr := json.NewDecoder(request.Body).Decode(&publish); decodeErr != nil {
			http.Error(responseWriter, decodeErr.Error(), http.StatusBadRequest)
			return
		}
		expected, found := expectedChecksums[publish.PartID]
		if !found || len(expected) != len(publish.Checksums) {
			http.Error(responseWriter, "unexpected publication", http.StatusBadRequest)
			return
		}
		for path, checksum := range expected {
			manifest, manifestErr := benchmark.TreeManifest(path)
			if manifestErr != nil || manifest.SHA256 != checksum || publish.Checksums[path] != checksum {
				http.Error(responseWriter, "published checksum mismatch", http.StatusBadRequest)
				return
			}
		}
		logicalNow.Store(publish.LogicalNow)
		table.setMergeNow(time.Unix(0, publish.LogicalNow))
		table.mustAddFilePart(publish.PartID, map[string]string{
			"latency":    filepath.Join(tableRoot, sidxDirName, "latency", formatExternalPartID(publish.PartID)),
			"start_time": filepath.Join(tableRoot, sidxDirName, "start_time", formatExternalPartID(publish.PartID)),
		})
		if triggerErr := table.triggerMerge(); triggerErr != nil {
			http.Error(responseWriter, triggerErr.Error(), http.StatusInternalServerError)
			return
		}
		responseWriter.WriteHeader(http.StatusNoContent)
	})
	handler.HandleFunc("/idle", func(responseWriter http.ResponseWriter, request *http.Request) {
		waitContext, cancel := context.WithTimeout(request.Context(), 20*time.Second)
		defer cancel()
		if triggerErr := table.triggerMerge(); triggerErr != nil {
			http.Error(responseWriter, triggerErr.Error(), http.StatusInternalServerError)
			return
		}
		if waitErr := table.waitForMergeIdle(waitContext); waitErr != nil {
			http.Error(responseWriter, waitErr.Error(), http.StatusInternalServerError)
			return
		}
		responseWriter.WriteHeader(http.StatusNoContent)
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	dataIdentity, identityErr := controller.CurrentResourceIdentity(os.Getpid())
	require.NoError(t, identityErr)
	controllerCPUs := append([]int(nil), dataIdentity.CPUs...)
	if len(controllerCPUs) > 1 {
		controllerCPUs = controllerCPUs[len(controllerCPUs)-1:]
	}
	reportPath := filepath.Join(root, "controller-report.json")
	configPath := filepath.Join(root, "controller-config.json")
	config := externalControllerConfig{
		DataNode:                   dataIdentity,
		Parts:                      controllerParts,
		CPUs:                       controllerCPUs,
		Endpoint:                   server.URL,
		ReportPath:                 reportPath,
		AllowUnisolatedDevelopment: true,
	}
	configData, marshalErr := json.Marshal(config)
	require.NoError(t, marshalErr)
	require.NoError(t, os.WriteFile(configPath, configData, 0o600))

	executable, executableErr := os.Executable()
	require.NoError(t, executableErr)
	commandContext, cancelCommand := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelCommand()
	command := exec.CommandContext(commandContext, executable, "-test.run=^TestTraceMergeExternalControllerHelper$", "-test.count=1")
	command.Env = append(os.Environ(), externalControllerEnv+"="+configPath)
	output, commandErr := command.CombinedOutput()
	require.NoError(t, commandErr, "external controller failed: %s", output)
	reportData, reportReadErr := os.ReadFile(reportPath)
	require.NoError(t, reportReadErr)
	var report externalControllerReport
	require.NoError(t, json.Unmarshal(reportData, &report))
	assert.NotEqual(t, os.Getpid(), report.Identity.PID)
	assert.False(t, report.ResourceIsolated, "a host smoke run is development-only and cannot be reported as a resource-isolated measurement")
	assert.Equal(t, len(partIDs), report.Parts)
	assert.Equal(t, len(partIDs)*6, report.Moves)
	assert.Positive(t, report.Bytes)
	if len(controllerCPUs) > 0 {
		assert.Equal(t, controllerCPUs, report.Identity.CPUs)
	}

	current := table.currentSnapshot()
	require.NotNil(t, current)
	require.Len(t, current.parts, 1)
	mergedPartID := current.parts[0].ID()
	current.decRef()
	require.Greater(t, mergedPartID, partIDs[len(partIDs)-1])
	require.NoError(t, table.Close())
	tableClosed = true

	reopened, reopenErr := newTSTable(
		fileSystem,
		tableRoot,
		common.Position{Database: "external-controller"},
		logger.GetLogger("external-controller-reopen"),
		timestamp.TimeRange{},
		tableOptions,
		nil,
	)
	require.NoError(t, reopenErr)
	reopened.setMergeNow(time.Unix(0, logicalNow.Load()))
	defer func() { require.NoError(t, reopened.Close()) }()
	reopenedSnapshot := reopened.currentSnapshot()
	require.NotNil(t, reopenedSnapshot)
	require.Len(t, reopenedSnapshot.parts, 1)
	assert.Equal(t, mergedPartID, reopenedSnapshot.parts[0].ID())
	reopenedSnapshot.decRef()

	coreReader, coreOpenErr := dumptrace.OpenPart(mergedPartID, tableRoot, fileSystem)
	require.NoError(t, coreOpenErr)
	coreIterator := coreReader.Iterator()
	var coreRows int
	for coreIterator.Next() {
		coreRows++
	}
	require.NoError(t, coreIterator.Err())
	require.NoError(t, coreIterator.Close())
	require.NoError(t, coreReader.Close())
	assert.Equal(t, len(partIDs), coreRows)
	for _, indexName := range []string{"latency", "start_time"} {
		index, found := reopened.getSidx(indexName)
		require.True(t, found)
		stats, statsErr := index.Stats(context.Background())
		require.NoError(t, statsErr)
		assert.Equal(t, int64(1), stats.PartCount)
		var indexRows int
		require.NoError(t, sidx.ScanRaw(context.Background(), index, func(row sidx.RawRow) error {
			indexRows++
			return nil
		}))
		assert.Equal(t, len(partIDs), indexRows)
	}
}

func TestTraceMergeExternalControllerHelper(t *testing.T) {
	configPath := os.Getenv(externalControllerEnv)
	if configPath == "" {
		t.Skip("external controller helper")
	}
	configData, readErr := os.ReadFile(configPath)
	require.NoError(t, readErr)
	var config externalControllerConfig
	require.NoError(t, json.Unmarshal(configData, &config))
	if len(config.CPUs) > 0 {
		require.NoError(t, controller.PinToCPUs(config.CPUs))
	}
	identity, identityErr := controller.CurrentResourceIdentity(os.Getpid())
	require.NoError(t, identityErr)
	isolated, isolationErr := validateExternalControllerIsolation(config, identity)
	require.NoError(t, isolationErr)
	report := externalControllerReport{Identity: identity, ResourceIsolated: isolated}
	client := &http.Client{Timeout: 20 * time.Second}
	for _, part := range config.Parts {
		stageReport, stageErr := controller.AtomicPublish(part.StageMoves)
		require.NoError(t, stageErr)
		publishReport, publishErr := controller.AtomicPublish(part.PublishMoves)
		require.NoError(t, publishErr)
		request := externalPublishRequest{
			Checksums:  make(map[string]string, len(publishReport.Moves)),
			LogicalNow: part.LogicalNow,
			PartID:     part.PartID,
		}
		for _, move := range publishReport.Moves {
			request.Checksums[move.Destination] = move.Manifest.SHA256
			report.Bytes += move.Manifest.Bytes
		}
		report.Moves += len(stageReport.Moves) + len(publishReport.Moves)
		requestData, marshalErr := json.Marshal(request)
		require.NoError(t, marshalErr)
		response, requestErr := client.Post(config.Endpoint+"/publish", "application/json", bytes.NewReader(requestData))
		require.NoError(t, requestErr)
		require.NoError(t, response.Body.Close())
		require.Equal(t, http.StatusNoContent, response.StatusCode)
		report.Parts++
	}
	response, requestErr := client.Post(config.Endpoint+"/idle", "application/json", nil)
	require.NoError(t, requestErr)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	reportData, marshalErr := json.Marshal(report)
	require.NoError(t, marshalErr)
	require.NoError(t, os.WriteFile(config.ReportPath, reportData, 0o600))
}

func TestExternalControllerIsolationIsStrictByDefault(t *testing.T) {
	dataNode := controller.ResourceIdentity{PID: 10, Cgroup: "/benchmark/data", CPUs: []int{0, 1}}
	sameResources := controller.ResourceIdentity{PID: 20, Cgroup: dataNode.Cgroup, CPUs: []int{1}}
	isolated, isolationErr := validateExternalControllerIsolation(externalControllerConfig{DataNode: dataNode}, sameResources)
	assert.False(t, isolated)
	require.ErrorContains(t, isolationErr, "resource-isolated")

	isolated, isolationErr = validateExternalControllerIsolation(
		externalControllerConfig{DataNode: dataNode, AllowUnisolatedDevelopment: true}, sameResources,
	)
	require.NoError(t, isolationErr)
	assert.False(t, isolated)

	distinctResources := controller.ResourceIdentity{PID: 20, Cgroup: "/benchmark/controller", CPUs: []int{2}}
	isolated, isolationErr = validateExternalControllerIsolation(externalControllerConfig{DataNode: dataNode}, distinctResources)
	require.NoError(t, isolationErr)
	assert.True(t, isolated)
}

func validateExternalControllerIsolation(config externalControllerConfig, identity controller.ResourceIdentity) (bool, error) {
	isolationErr := controller.ValidateResourceIsolation(config.DataNode, identity)
	if isolationErr == nil {
		return true, nil
	}
	if config.AllowUnisolatedDevelopment {
		return false, nil
	}
	return false, fmt.Errorf("external controller is not resource-isolated: %w", isolationErr)
}

func buildExternalCoreParts(t *testing.T, fileSystem fs.FileSystem, root string, partIDs []uint64, logicalBase time.Time) {
	t.Helper()
	for partIdx, partID := range partIDs {
		timestampNanos := logicalBase.Add(time.Duration(partIdx) * time.Minute).UnixNano()
		_, _, cleanup := BuildPartForDump(root, fileSystem, partID, []DumpRow{{
			TraceID:   fmt.Sprintf("trace-%d", partID),
			SpanID:    fmt.Sprintf("span-%d", partID),
			Span:      []byte(fmt.Sprintf("payload-%d", partID)),
			Timestamp: timestampNanos,
			Tags: []DumpTag{
				{Name: "start_time", Value: Timestamp(timestampNanos)},
				{Name: "latency", Value: int64(partID * 10)},
			},
		}})
		t.Cleanup(cleanup)
	}
}

func buildExternalIndexParts(t *testing.T, fileSystem fs.FileSystem, root string, partIDs []uint64, logicalBase time.Time) {
	t.Helper()
	options, optionsErr := sidx.NewOptions(root, protector.Nop{})
	require.NoError(t, optionsErr)
	index, indexErr := sidx.NewSIDX(fileSystem, options)
	require.NoError(t, indexErr)
	for partIdx, partID := range partIDs {
		timestampNanos := logicalBase.Add(time.Duration(partIdx) * time.Minute).UnixNano()
		memPart, convertErr := index.ConvertToMemPart([]sidx.WriteRequest{{
			SeriesID: common.SeriesID(partID),
			Key:      timestampNanos,
			Data:     append([]byte{1}, fmt.Sprintf("trace-%d", partID)...),
			Tags: []sidx.Tag{{
				Name:      "source",
				Value:     []byte("external-controller"),
				ValueType: pbv1.ValueTypeStr,
			}},
		}}, 0, nil, nil)
		require.NoError(t, convertErr)
		memPart.MustFlush(fileSystem, filepath.Join(root, formatExternalPartID(partID)))
		sidx.ReleaseMemPart(memPart)
	}
	require.NoError(t, index.Close())
}

func formatExternalPartID(partID uint64) string {
	return fmt.Sprintf("%016x", partID)
}
