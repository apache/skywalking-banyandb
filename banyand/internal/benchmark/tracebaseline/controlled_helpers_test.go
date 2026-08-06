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

package tracebaseline

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark"
	storagetrace "github.com/apache/skywalking-banyandb/banyand/trace"
)

func TestValidateControlledMergeOptionsRejectsUnknownMode(t *testing.T) {
	_, err := validateControlledMergeOptions(ControlledMergeRunOptions{Mode: "bogus"})
	require.ErrorContains(t, err, "unsupported controlled merge pipeline mode")
}

func TestValidateControlledMergeOptionsRequiresRetainAllPlugin(t *testing.T) {
	_, err := validateControlledMergeOptions(ControlledMergeRunOptions{
		Mode:             string(ControlledMergePipelineRetainAll),
		DataRoot:         "/data",
		SeedManifestPath: "/seed.json",
	})
	require.ErrorContains(t, err, "retain-all plugin path is required")
}

func TestValidateControlledMergeOptionsRequiresManifest(t *testing.T) {
	_, err := validateControlledMergeOptions(ControlledMergeRunOptions{DataRoot: "/data"})
	require.ErrorContains(t, err, "controlled seed manifest path is required")
}

func TestValidateControlledMergeOptionsRequiresDataRoot(t *testing.T) {
	_, err := validateControlledMergeOptions(ControlledMergeRunOptions{SeedManifestPath: "/seed.json"})
	require.ErrorContains(t, err, "controlled seed data root is required")
}

func TestValidateControlledMergeOptionsAcceptsDisabledDefaults(t *testing.T) {
	mode, err := validateControlledMergeOptions(ControlledMergeRunOptions{
		SeedManifestPath: "/seed.json", DataRoot: "/data",
	})
	require.NoError(t, err)
	require.Equal(t, ControlledMergePipelineDisabled, mode)
}

func TestLoadControlledMergePluginSkipsPluginWhenDisabled(t *testing.T) {
	sampler, sha, tr, err := loadControlledMergePlugin(ControlledMergePipelineDisabled,
		ControlledMergeRunOptions{}, ControlledMergeSeedManifest{})
	require.NoError(t, err)
	require.Nil(t, sampler)
	require.Equal(t, "", sha)
	require.True(t, tr.Start.IsZero() && tr.End.IsZero())
}

func TestLoadControlledMergePluginRejectsRetainAllWithoutPath(t *testing.T) {
	sampler, pluginSHA256, segmentTimeRange, err := loadControlledMergePlugin(ControlledMergePipelineRetainAll,
		ControlledMergeRunOptions{}, ControlledMergeSeedManifest{})
	require.ErrorContains(t, err, "retain-all plugin path is required")
	require.Nil(t, sampler)
	require.Empty(t, pluginSHA256)
	require.True(t, segmentTimeRange.Start.IsZero() && segmentTimeRange.End.IsZero())
}

func TestBuildControlledMergeReportFailsOnEventMismatch(t *testing.T) {
	options := ControlledMergeRunOptions{
		RunID: "r1", DataRoot: "/data", Mode: string(ControlledMergePipelineDisabled),
		ExecutionIdentity: ExecutionIdentity{ImageDigest: "img", CloneMethod: "os.CopyFS", BinarySHA256: "bin"},
	}
	manifest := ControlledMergeSeedManifest{
		Snapshot:         benchmark.Manifest{SHA256: "snap"},
		Selection:        ControlledMergeSelection{SHA256: "expected-sha"},
		MatureLogicalNow: time.Unix(0, 0),
	}
	_, err := buildControlledMergeReport(options, ControlledMergePipelineDisabled, manifest,
		storagetrace.BenchmarkMergeEvent{}, storagetrace.BenchmarkMergeInventory{}, storagetrace.BenchmarkMergeStagingLimits{}, "",
		map[string]string{}, map[string]string{}, false)
	require.ErrorContains(t, err, "controlled merge correctness gate failed")
}

func TestBuildControlledMergeReportRecordsInventoryAndPluginIdentity(t *testing.T) {
	options := ControlledMergeRunOptions{
		RunID: "r1", DataRoot: t.TempDir(), Mode: string(ControlledMergePipelineRetainAll), Commit: "abc123",
		ExecutionIdentity: ExecutionIdentity{ImageDigest: "img", CloneMethod: "os.CopyFS", BinarySHA256: "bin"},
	}
	manifest := ControlledMergeSeedManifest{
		Snapshot: benchmark.Manifest{SHA256: "snap"}, Selection: ControlledMergeSelection{SHA256: "selection"},
		MatureLogicalNow: time.Unix(0, 0),
	}
	event := storagetrace.BenchmarkMergeEvent{
		SelectionSHA256: "selection", InputPartIDs: []uint64{1, 2}, MatureInputParts: 2, InputRows: 10, OutputRows: 10,
		Sampling: storagetrace.BenchmarkMergeSamplingExecuted, PluginCalls: 1, TracesEvaluated: 4, TracesRetained: 4,
		Children: []storagetrace.BenchmarkMergeChild{{Name: LedgerLatency}, {Name: LedgerStartTime}},
	}
	inventory := storagetrace.BenchmarkMergeInventory{CoreRows: 10}
	stagingLimits := storagetrace.BenchmarkMergeStagingLimits{MemoryLimit: 8 << 30, StageBytes: 64 << 20, TraceBytes: 64 << 20, MaxTraceCount: 65536}
	ledger := map[string]string{LedgerCore: "core", LedgerLatency: "latency", LedgerStartTime: "start"}

	report, reportErr := buildControlledMergeReport(options, ControlledMergePipelineRetainAll, manifest, event, inventory, stagingLimits,
		"plugin-sha", ledger, ledger, false)

	require.NoError(t, reportErr)
	require.Equal(t, inventory, report.Inventory)
	require.Equal(t, stagingLimits, report.StagingLimits)
	require.Equal(t, "plugin-sha", report.Environment.PluginSHA256)
	require.Equal(t, "abc123", report.Environment.Commit)
}

func TestWriteControlledMergeReportWritesJSON(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/report.json"
	report := ControlledMergeRunReport{Version: 3, RunID: "r1", PipelineMode: ControlledMergePipelineDisabled}
	require.NoError(t, writeControlledMergeReport(path, report))
}

func TestCaptureControlledMergePreDispatchProfileNoopWhenEmpty(t *testing.T) {
	require.NoError(t, captureControlledMergePreDispatchProfile(""))
}

func TestPrepareControlledMergeProfileDirReturnsEmptyForEmpty(t *testing.T) {
	resolved, err := prepareControlledMergeProfileDir("")
	require.NoError(t, err)
	require.Equal(t, "", resolved)
}

func TestStartControlledMergeProfilerReturnsIdleWhenEmpty(t *testing.T) {
	profiler, err := startControlledMergeProfiler("")
	require.NoError(t, err)
	require.NotNil(t, profiler)
	require.False(t, profiler.active)
}
