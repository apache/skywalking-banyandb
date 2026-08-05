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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// BenchmarkMergeReceiverOptions configures a pipeline-disabled production merge benchmark receiver.
type BenchmarkMergeReceiverOptions struct {
	EventWriter    io.Writer
	LogicalNow     time.Time
	MergeGrace     time.Duration
	IndexNames     []string
	MaxInputPartID uint64
	Attribution    bool
}

// BenchmarkMergeInventory summarizes durable core and secondary-index output.
type BenchmarkMergeInventory struct {
	IndexRows  map[string]uint64 `json:"indexRows"`
	IndexBytes map[string]uint64 `json:"indexBytes"`
	IndexParts map[string]int    `json:"indexParts"`
	CoreRows   uint64            `json:"coreRows"`
	CoreBytes  uint64            `json:"coreBytes"`
	CoreParts  int               `json:"coreParts"`
}

// BenchmarkMergeStatus summarizes current picker and part-backlog state without scanning part contents.
type BenchmarkMergeStatus struct {
	OldestTimestamp int64  `json:"oldestTimestamp"`
	CoreBytes       uint64 `json:"coreBytes"`
	CoreParts       int    `json:"coreParts"`
	QueuedMerges    int    `json:"queuedMerges"`
	RunningMerges   int    `json:"runningMerges"`
	InFlightParts   int    `json:"inFlightParts"`
}

// NewBenchmarkMergeReceiver opens the production merge loop with sampling disabled and benchmark recording enabled.
func NewBenchmarkMergeReceiver(root string, options BenchmarkMergeReceiverOptions) (*BenchmarkPartReceiver, error) {
	absoluteRoot, absoluteErr := filepath.Abs(root)
	if absoluteErr != nil {
		return nil, fmt.Errorf("cannot resolve merge benchmark root %q: %w", root, absoluteErr)
	}
	if options.MergeGrace <= 0 {
		return nil, fmt.Errorf("merge grace must be positive")
	}
	if mkdirErr := os.MkdirAll(absoluteRoot, 0o755); mkdirErr != nil {
		return nil, fmt.Errorf("cannot create merge benchmark root %q: %w", absoluteRoot, mkdirErr)
	}
	fileSystem := fs.NewLocalFileSystem()
	table, tableErr := newTSTable(fileSystem, absoluteRoot, common.Position{Database: "trace-merge-baseline"},
		logger.GetLogger("trace-merge-baseline"), timestamp.TimeRange{}, option{
			flushTimeout: 0, mergePolicy: newDefaultMergePolicy(), protector: protector.Nop{},
			nativePipelineEnabled: false, maxTraceFragmentGap: time.Minute, mergeGraceDefault: options.MergeGrace,
		}, nil)
	if tableErr != nil {
		return nil, fmt.Errorf("cannot open merge benchmark table %q: %w", absoluteRoot, tableErr)
	}
	receiver := &BenchmarkPartReceiver{table: table, fileSystem: fileSystem, root: absoluteRoot}
	if options.MaxInputPartID > 0 {
		table.observePartID(options.MaxInputPartID)
	}
	indexNames := options.IndexNames
	if len(indexNames) == 0 {
		indexNames = []string{"latency", "start_time"}
	}
	for _, indexName := range indexNames {
		table.mustGetOrCreateSidx(indexName)
	}
	if !options.LogicalNow.IsZero() {
		table.setMergeNow(options.LogicalNow)
	}
	if recordingErr := receiver.EnableMergeRecording(BenchmarkMergeRecordingOptions{
		Writer: options.EventWriter, Phase: BenchmarkMergePhasePrimary, Attribution: options.Attribution,
	}); recordingErr != nil {
		return nil, errors.Join(fmt.Errorf("cannot enable merge benchmark recording: %w", recordingErr), table.Close())
	}
	return receiver, nil
}

// PublishExistingPart introduces an atomically published core part and its matching secondary-index parts.
func (bpr *BenchmarkPartReceiver) PublishExistingPart(partID uint64, corePath string, indexPaths map[string]string, logicalNow time.Time) error {
	if bpr == nil || bpr.table == nil {
		return fmt.Errorf("benchmark receiver is not open")
	}
	expectedCorePath := partPath(bpr.root, partID)
	if filepath.Clean(corePath) != filepath.Clean(expectedCorePath) {
		return fmt.Errorf("core part %016x must be published at %q", partID, expectedCorePath)
	}
	metadata, metadataErr := ParsePartMetadata(bpr.fileSystem, corePath)
	if metadataErr != nil {
		return fmt.Errorf("cannot validate published core part %016x: %w", partID, metadataErr)
	}
	if metadata.ID != 0 && metadata.ID != partID {
		return fmt.Errorf("published core metadata ID %016x does not match %016x", metadata.ID, partID)
	}
	for indexName, indexPath := range indexPaths {
		expectedIndexPath := sidxPartPath(bpr.root, indexName, partID)
		if filepath.Clean(indexPath) != filepath.Clean(expectedIndexPath) {
			return fmt.Errorf("secondary-index part %s/%016x must be published at %q", indexName, partID, expectedIndexPath)
		}
		indexMetadata, indexErr := sidx.ParsePartMetadata(bpr.fileSystem, indexPath)
		if indexErr != nil {
			return fmt.Errorf("cannot validate published secondary-index part %s/%016x: %w", indexName, partID, indexErr)
		}
		if indexMetadata.ID != 0 && indexMetadata.ID != partID {
			return fmt.Errorf("published secondary-index metadata ID %016x does not match %016x", indexMetadata.ID, partID)
		}
		if _, found := bpr.table.getSidx(indexName); !found {
			return fmt.Errorf("secondary index %q was not configured before publication", indexName)
		}
	}
	bpr.table.setMergeNow(logicalNow)
	bpr.table.mustAddFilePart(partID, indexPaths)
	if triggerErr := bpr.table.triggerMerge(); triggerErr != nil {
		return fmt.Errorf("cannot trigger merge after publishing part %016x: %w", partID, triggerErr)
	}
	return nil
}

// WaitForMergeIdle waits until the production picker and both merge lanes have no remaining work.
func (bpr *BenchmarkPartReceiver) WaitForMergeIdle(ctx context.Context) error {
	if bpr == nil || bpr.table == nil {
		return fmt.Errorf("benchmark receiver is not open")
	}
	return bpr.table.waitForMergeIdle(ctx)
}

// AdvanceMergeTime advances logical time, changes the report phase, triggers the picker, and drains all eligible work.
func (bpr *BenchmarkPartReceiver) AdvanceMergeTime(ctx context.Context, logicalNow time.Time, phase BenchmarkMergePhase) error {
	if phaseErr := bpr.SetMergeRecordingPhase(phase); phaseErr != nil {
		return fmt.Errorf("cannot set merge recording phase %q: %w", phase, phaseErr)
	}
	bpr.table.setMergeNow(logicalNow)
	if triggerErr := bpr.table.triggerMerge(); triggerErr != nil {
		return fmt.Errorf("cannot trigger merge after advancing logical time: %w", triggerErr)
	}
	return bpr.WaitForMergeIdle(ctx)
}

// MergeInventory returns durable core and secondary-index row, byte, and part totals.
func (bpr *BenchmarkPartReceiver) MergeInventory() (BenchmarkMergeInventory, error) {
	if bpr == nil || bpr.table == nil {
		return BenchmarkMergeInventory{}, fmt.Errorf("benchmark receiver is not open")
	}
	inventory := BenchmarkMergeInventory{
		IndexRows: make(map[string]uint64), IndexBytes: make(map[string]uint64), IndexParts: make(map[string]int),
	}
	snapshot := bpr.table.currentSnapshot()
	if snapshot == nil {
		return inventory, nil
	}
	partIDs := make(map[uint64]struct{}, len(snapshot.parts))
	for _, partData := range snapshot.parts {
		metadata := &partData.p.partMetadata
		inventory.CoreRows += metadata.TotalCount
		inventory.CoreBytes += metadata.CompressedSizeBytes
		inventory.CoreParts++
		partIDs[partData.ID()] = struct{}{}
	}
	snapshot.decRef()
	for indexName, index := range bpr.table.getAllSidx() {
		paths := index.PartPaths(partIDs)
		indexBytes, indexRows, totalsErr := benchmarkSidxPartTotals(bpr.fileSystem, paths)
		if totalsErr != nil {
			return BenchmarkMergeInventory{}, fmt.Errorf("cannot inspect secondary index %q inventory: %w", indexName, totalsErr)
		}
		inventory.IndexBytes[indexName] = indexBytes
		inventory.IndexRows[indexName] = indexRows
		inventory.IndexParts[indexName] = len(paths)
	}
	return inventory, nil
}

// MergeStatus returns a low-cost snapshot of current merge and core-part backlog.
func (bpr *BenchmarkPartReceiver) MergeStatus() (BenchmarkMergeStatus, error) {
	if bpr == nil || bpr.table == nil {
		return BenchmarkMergeStatus{}, fmt.Errorf("benchmark receiver is not open")
	}
	status := BenchmarkMergeStatus{}
	snapshot := bpr.table.currentSnapshot()
	if snapshot != nil {
		for partIdx, partData := range snapshot.parts {
			metadata := &partData.p.partMetadata
			status.CoreBytes += metadata.CompressedSizeBytes
			status.CoreParts++
			if partIdx == 0 || metadata.MinTimestamp < status.OldestTimestamp {
				status.OldestTimestamp = metadata.MinTimestamp
			}
		}
		snapshot.decRef()
	}
	if bpr.table.mergeControl != nil {
		loopState := bpr.table.mergeControl.state()
		status.QueuedMerges = loopState.queued
		status.RunningMerges = loopState.running
	}
	bpr.table.inFlightMu.RLock()
	status.InFlightParts = len(bpr.table.inFlight)
	bpr.table.inFlightMu.RUnlock()
	return status, nil
}
