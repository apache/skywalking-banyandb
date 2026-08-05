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
)

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
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelWait()
	require.NoError(t, receiver.WaitForMergeIdle(waitCtx))
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
		require.Equal(t, BenchmarkMergeSamplingNotExecuted, report.Events[eventIdx].Sampling)
		require.Equal(t, BenchmarkMergeReasonPipelineDisabled, report.Events[eventIdx].Reason)
		require.Zero(t, report.Events[eventIdx].PluginCalls)
		require.Greater(t, report.Events[eventIdx].OutputPartID, partIDs[len(partIDs)-1])
	}
	require.NotEmpty(t, eventOutput.String())
}
