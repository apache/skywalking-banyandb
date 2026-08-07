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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type retainAllBenchmarkSampler struct{}

func (retainAllBenchmarkSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (retainAllBenchmarkSampler) Project() sdk.Projection { return sdk.Projection{} }
func (retainAllBenchmarkSampler) Close() error            { return nil }
func (retainAllBenchmarkSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(batch.Traces))
	for traceIdx := range keep {
		keep[traceIdx] = true
	}
	return sdk.Verdict{Keep: keep}, nil
}

func TestBenchmarkMergeReceiverUsesFiniteStagingMemoryLimit(t *testing.T) {
	const memoryLimit = uint64(8 << 30)
	receiver, receiverErr := NewBenchmarkMergeReceiver(t.TempDir(), BenchmarkMergeReceiverOptions{
		MergeGrace:  2 * time.Hour,
		MemoryLimit: memoryLimit,
	})
	require.NoError(t, receiverErr)
	t.Cleanup(func() { require.NoError(t, receiver.Close()) })

	limits, limitsErr := receiver.MergeStagingLimits()
	require.NoError(t, limitsErr)
	require.Equal(t, memoryLimit, limits.MemoryLimit)
	require.Equal(t, stageBudgetFromLimit(memoryLimit), limits.StageBytes)
	require.Equal(t, stageBudgetFromLimit(memoryLimit), limits.TraceBytes)
	require.Equal(t, maxStagedTraceCountFromBudget(limits.StageBytes), limits.MaxTraceCount)
}

func TestBenchmarkMergeReceiverPublishesAndDrainsProductionMerges(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	workspace := t.TempDir()
	sourceRoot := filepath.Join(workspace, "source")
	tableRoot := filepath.Join(workspace, "table")
	for _, root := range []string{sourceRoot, tableRoot} {
		for _, indexName := range []string{"latency", "start_time"} {
			require.NoError(t, os.MkdirAll(filepath.Join(root, sidxDirName, indexName), 0o755))
		}
	}
	partIDs := []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	logicalBase := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	buildExternalCoreParts(t, fileSystem, sourceRoot, partIDs, logicalBase)
	for _, indexName := range []string{"latency", "start_time"} {
		buildExternalIndexParts(t, fileSystem, filepath.Join(sourceRoot, sidxDirName, indexName), partIDs, logicalBase)
	}
	var eventOutput bytes.Buffer
	receiver, receiverErr := NewBenchmarkMergeReceiver(tableRoot, BenchmarkMergeReceiverOptions{
		LogicalNow:     logicalBase,
		MergeGrace:     2 * time.Hour,
		EventWriter:    &eventOutput,
		MaxInputPartID: partIDs[len(partIDs)-1],
		Attribution:    true,
	})
	require.NoError(t, receiverErr)
	t.Cleanup(func() { require.NoError(t, receiver.Close()) })
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelWait()
	publicationTimes := make(map[int64]struct{}, len(partIDs))
	for _, partID := range partIDs {
		partName := formatExternalPartID(partID)
		corePath := filepath.Join(tableRoot, partName)
		require.NoError(t, os.Rename(filepath.Join(sourceRoot, partName), corePath))
		indexPaths := map[string]string{
			"latency":    filepath.Join(tableRoot, sidxDirName, "latency", partName),
			"start_time": filepath.Join(tableRoot, sidxDirName, "start_time", partName),
		}
		for indexName, indexPath := range indexPaths {
			require.NoError(t, os.Rename(filepath.Join(sourceRoot, sidxDirName, indexName, partName), indexPath))
		}
		publication := logicalBase.Add(time.Duration(partID) * time.Minute)
		publicationTimes[publication.UnixNano()] = struct{}{}
		require.NoError(t, receiver.PublishExistingPart(partID, corePath, indexPaths, publication))
		require.NoError(t, receiver.WaitForMergeIdle(waitCtx))
	}
	status, statusErr := receiver.MergeStatus()
	require.NoError(t, statusErr)
	require.Zero(t, status.QueuedMerges)
	require.Zero(t, status.RunningMerges)
	require.Zero(t, status.InFlightParts)
	require.NoError(t, receiver.AdvanceMergeTime(waitCtx, logicalBase.Add(26*time.Hour), BenchmarkMergePhaseCooldown))

	inventory, inventoryErr := receiver.MergeInventory()
	require.NoError(t, inventoryErr)
	require.Equal(t, uint64(8), inventory.CoreRows)
	require.Equal(t, uint64(8), inventory.IndexRows["latency"])
	require.Equal(t, uint64(8), inventory.IndexRows["start_time"])
	require.Equal(t, 1, inventory.CoreParts)
	require.Equal(t, 1, inventory.IndexParts["latency"])
	require.Equal(t, 1, inventory.IndexParts["start_time"])
	report, reportErr := receiver.MergeRecordingReport()
	require.NoError(t, reportErr)
	require.NotEmpty(t, report.Events)
	for eventIdx := range report.Events {
		_, publicationFound := publicationTimes[report.Events[eventIdx].LogicalNow]
		require.True(t, publicationFound)
		require.Equal(t, BenchmarkMergeSamplingNotExecuted, report.Events[eventIdx].Sampling)
		require.Equal(t, BenchmarkMergeReasonPipelineDisabled, report.Events[eventIdx].Reason)
		require.Zero(t, report.Events[eventIdx].PluginCalls)
		require.Greater(t, report.Events[eventIdx].OutputPartID, partIDs[len(partIDs)-1])
	}
	require.NotEmpty(t, eventOutput.String())
}

func TestBenchmarkMergeReceiverRunsExactlyOneMatureProductionSelection(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	workspace := t.TempDir()
	sourceRoot := filepath.Join(workspace, "source")
	tableRoot := filepath.Join(workspace, "table")
	for _, root := range []string{sourceRoot, tableRoot} {
		for _, indexName := range []string{"latency", "start_time"} {
			require.NoError(t, os.MkdirAll(filepath.Join(root, sidxDirName, indexName), 0o755))
		}
	}
	partIDs := make([]uint64, 30)
	for partIdx := range partIDs {
		partIDs[partIdx] = uint64(partIdx + 1)
	}
	logicalBase := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	buildExternalCoreParts(t, fileSystem, sourceRoot, partIDs, logicalBase)
	for _, indexName := range []string{"latency", "start_time"} {
		buildExternalIndexParts(t, fileSystem, filepath.Join(sourceRoot, sidxDirName, indexName), partIDs, logicalBase)
	}
	receiver, receiverErr := NewBenchmarkMergeReceiver(tableRoot, BenchmarkMergeReceiverOptions{
		LogicalNow: logicalBase, MergeGrace: 2 * time.Hour, MaxInputPartID: partIDs[len(partIDs)-1], BlockMerges: true,
	})
	require.NoError(t, receiverErr)
	t.Cleanup(func() { require.NoError(t, receiver.Close()) })
	for _, partID := range partIDs {
		partName := formatExternalPartID(partID)
		corePath := filepath.Join(tableRoot, partName)
		require.NoError(t, os.Rename(filepath.Join(sourceRoot, partName), corePath))
		indexPaths := map[string]string{
			"latency":    filepath.Join(tableRoot, sidxDirName, "latency", partName),
			"start_time": filepath.Join(tableRoot, sidxDirName, "start_time", partName),
		}
		for indexName, indexPath := range indexPaths {
			require.NoError(t, os.Rename(filepath.Join(sourceRoot, sidxDirName, indexName, partName), indexPath))
		}
		require.NoError(t, receiver.PublishExistingPart(partID, corePath, indexPaths, logicalBase.Add(time.Duration(partID)*time.Minute)))
	}

	preview, previewErr := receiver.PreviewMergeSelection()
	require.NoError(t, previewErr)
	require.NotEmpty(t, preview.SelectionSHA256)
	require.Len(t, preview.InputPartIDs, 15)
	beforeReport, beforeReportErr := receiver.MergeRecordingReport()
	require.NoError(t, beforeReportErr)
	require.Empty(t, beforeReport.Events)

	mergeCtx, cancelMerge := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelMerge()
	event, mergeErr := receiver.RunOneMerge(mergeCtx, BenchmarkOneMergeOptions{
		LogicalNow: logicalBase.Add(3 * time.Hour), ExpectedSelectionSHA256: preview.SelectionSHA256, RequireAllMature: true,
	})
	require.NoError(t, mergeErr)
	require.Equal(t, preview.InputPartIDs, event.InputPartIDs)
	require.Equal(t, uint32(15), event.MatureInputParts)
	require.Zero(t, event.HotInputParts)
	require.Equal(t, event.InputRows, event.MatureInputRows)
	require.Len(t, event.Children, 2)

	report, reportErr := receiver.MergeRecordingReport()
	require.NoError(t, reportErr)
	require.Len(t, report.Events, 1)
	_, nextPreviewErr := receiver.PreviewMergeSelection()
	require.NoError(t, nextPreviewErr, "another eligible merge proves the one-round runner stopped recursive dispatch")
}

func TestBenchmarkMergeReceiverRunsRetainAllSamplerForControlledMatureMerge(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	workspace := t.TempDir()
	sourceRoot := filepath.Join(workspace, "source")
	tableRoot := filepath.Join(workspace, "table")
	for _, root := range []string{sourceRoot, tableRoot} {
		for _, indexName := range []string{"latency", "start_time"} {
			require.NoError(t, os.MkdirAll(filepath.Join(root, sidxDirName, indexName), 0o755))
		}
	}
	partIDs := make([]uint64, 30)
	for partIdx := range partIDs {
		partIDs[partIdx] = uint64(partIdx + 1)
	}
	logicalBase := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	buildExternalCoreParts(t, fileSystem, sourceRoot, partIDs, logicalBase)
	for _, indexName := range []string{"latency", "start_time"} {
		buildExternalIndexParts(t, fileSystem, filepath.Join(sourceRoot, sidxDirName, indexName), partIDs, logicalBase)
	}
	receiver, receiverErr := NewBenchmarkMergeReceiver(tableRoot, BenchmarkMergeReceiverOptions{
		LogicalNow: logicalBase, MergeGrace: 2 * time.Hour, MaxInputPartID: partIDs[len(partIDs)-1], BlockMerges: true,
		MemoryLimit: 8 << 30,
		Sampler:     retainAllBenchmarkSampler{}, SegmentTimeRange: timestamp.NewInclusiveTimeRange(logicalBase.Add(-24*time.Hour), logicalBase.Add(24*time.Hour)),
	})
	require.NoError(t, receiverErr)
	t.Cleanup(func() { require.NoError(t, receiver.Close()) })
	for _, partID := range partIDs {
		partName := formatExternalPartID(partID)
		corePath := filepath.Join(tableRoot, partName)
		require.NoError(t, os.Rename(filepath.Join(sourceRoot, partName), corePath))
		indexPaths := map[string]string{
			"latency":    filepath.Join(tableRoot, sidxDirName, "latency", partName),
			"start_time": filepath.Join(tableRoot, sidxDirName, "start_time", partName),
		}
		for indexName, indexPath := range indexPaths {
			require.NoError(t, os.Rename(filepath.Join(sourceRoot, sidxDirName, indexName, partName), indexPath))
		}
		require.NoError(t, receiver.PublishExistingPart(partID, corePath, indexPaths, logicalBase.Add(time.Duration(partID)*time.Minute)))
	}

	preview, previewErr := receiver.PreviewMergeSelection()
	require.NoError(t, previewErr)
	mergeCtx, cancelMerge := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelMerge()
	event, mergeErr := receiver.RunOneMerge(mergeCtx, BenchmarkOneMergeOptions{
		LogicalNow: logicalBase.Add(3 * time.Hour), ExpectedSelectionSHA256: preview.SelectionSHA256, RequireAllMature: true,
	})
	require.NoError(t, mergeErr)
	require.Equal(t, BenchmarkMergeSamplingExecuted, event.Sampling)
	require.Greater(t, event.PluginCalls, uint64(0))
	require.Greater(t, event.TracesEvaluated, uint64(0))
	require.Equal(t, event.TracesEvaluated, event.TracesRetained)
	require.Zero(t, event.TracesDropped)
	require.Equal(t, event.InputRows, event.OutputRows)
}
