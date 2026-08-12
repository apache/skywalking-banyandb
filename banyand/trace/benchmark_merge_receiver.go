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
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// ErrBenchmarkNoMergeSelection means the production policy has no eligible merge in the current snapshot.
var ErrBenchmarkNoMergeSelection = errNoPartToMerge

// BenchmarkMergeReceiverOptions configures a production merge benchmark receiver.
type BenchmarkMergeReceiverOptions struct {
	LogicalNow       time.Time
	EventWriter      io.Writer
	Sampler          sdk.Sampler
	PartMergeDepths  map[uint64]uint32
	SegmentTimeRange timestamp.TimeRange
	IndexNames       []string
	MergeGrace       time.Duration
	MemoryLimit      uint64
	MaxInputPartID   uint64
	Attribution      bool
	BlockMerges      bool
}

// BenchmarkMergeStagingLimits reports the effective limits used by the trace sampler staging path.
type BenchmarkMergeStagingLimits struct {
	MemoryLimit   uint64 `json:"memoryLimit"`
	StageBytes    uint64 `json:"stageBytes"`
	TraceBytes    uint64 `json:"traceBytes"`
	MaxTraceCount int    `json:"maxTraceCount"`
}

type benchmarkMemoryProtector struct {
	protector.Nop
	limit uint64
}

func (bmp *benchmarkMemoryProtector) GetLimit() uint64 {
	return bmp.limit
}

func (bmp *benchmarkMemoryProtector) AvailableBytes() int64 {
	return int64(min(bmp.limit, uint64(math.MaxInt64)))
}

// BenchmarkOneMergeOptions defines the clock and expected picker result for one controlled mature merge.
type BenchmarkOneMergeOptions struct {
	LogicalNow              time.Time
	ExpectedSelectionSHA256 string
	RequireAllMature        bool
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

// NewBenchmarkMergeReceiver opens the production merge loop with benchmark recording enabled.
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
	if options.Sampler != nil && options.Sampler.Kind() != sdk.KindSampler {
		return nil, fmt.Errorf("merge benchmark plugin kind %d is not a sampler", options.Sampler.Kind())
	}
	memoryLimit := options.MemoryLimit
	if memoryLimit == 0 {
		cgroupLimit, memoryLimitErr := cgroups.MemoryLimit()
		if memoryLimitErr == nil && cgroupLimit > 0 {
			memoryLimit = uint64(cgroupLimit)
		} else if options.Sampler != nil {
			if memoryLimitErr != nil {
				return nil, fmt.Errorf("cannot resolve merge benchmark cgroup memory limit: %w", memoryLimitErr)
			}
			return nil, fmt.Errorf("merge benchmark sampler requires a finite cgroup memory limit")
		}
	}
	memoryProtector := protector.Memory(protector.Nop{})
	if memoryLimit > 0 {
		memoryProtector = &benchmarkMemoryProtector{limit: memoryLimit}
	}
	groupDigest := sha256.Sum256([]byte(absoluteRoot))
	group := fmt.Sprintf("trace-merge-benchmark-%x", groupDigest[:8])
	fileSystem := fs.NewLocalFileSystem()
	table, tableErr := newTSTable(fileSystem, absoluteRoot, common.Position{Database: group},
		logger.GetLogger("trace-merge-benchmark"), options.SegmentTimeRange, option{
			flushTimeout: 0, mergePolicy: newDefaultMergePolicy(), protector: memoryProtector,
			nativePipelineEnabled: options.Sampler != nil, maxTraceFragmentGap: time.Minute, mergeGraceDefault: options.MergeGrace,
			decideTimeout: 5 * time.Second, decideTimeoutCircuitBreak: 3, benchmarkMergeBlocked: options.BlockMerges,
		}, nil)
	if tableErr != nil {
		return nil, fmt.Errorf("cannot open merge benchmark table %q: %w", absoluteRoot, tableErr)
	}
	receiver := &BenchmarkPartReceiver{table: table, fileSystem: fileSystem, root: absoluteRoot}
	if depthErr := receiver.restoreMergeDepths(options.PartMergeDepths); depthErr != nil {
		return nil, errors.Join(fmt.Errorf("cannot restore controlled merge depths: %w", depthErr), table.Close())
	}
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
	if options.Sampler != nil {
		deregister := registerSampler(group, options.Sampler)
		receiver.closeCallbacks = append(receiver.closeCallbacks, func() error {
			deregister()
			return options.Sampler.Close()
		})
	}
	if !options.LogicalNow.IsZero() {
		table.setMergeNow(options.LogicalNow)
	}
	if recordingErr := receiver.EnableMergeRecording(BenchmarkMergeRecordingOptions{
		Writer: options.EventWriter, Phase: BenchmarkMergePhasePrimary, Attribution: options.Attribution,
	}); recordingErr != nil {
		return nil, errors.Join(fmt.Errorf("cannot enable merge benchmark recording: %w", recordingErr), receiver.Close())
	}
	return receiver, nil
}

// MergeStagingLimits returns the memory-derived byte and trace-count limits used by sampler evaluation.
func (bpr *BenchmarkPartReceiver) MergeStagingLimits() (BenchmarkMergeStagingLimits, error) {
	if bpr == nil || bpr.table == nil {
		return BenchmarkMergeStagingLimits{}, fmt.Errorf("benchmark receiver is not open")
	}
	memoryLimit := bpr.table.option.protector.GetLimit()
	stageBytes := resolveStageBudget(bpr.table.option)
	return BenchmarkMergeStagingLimits{
		MemoryLimit: memoryLimit, StageBytes: stageBytes, TraceBytes: resolveTraceBudget(bpr.table.option),
		MaxTraceCount: maxStagedTraceCountFromBudget(stageBytes),
	}, nil
}

// MergePartDepths returns the benchmark-only merge generation of every active core part.
func (bpr *BenchmarkPartReceiver) MergePartDepths() (map[uint64]uint32, error) {
	if bpr == nil || bpr.table == nil {
		return nil, fmt.Errorf("benchmark receiver is not open")
	}
	snapshot := bpr.table.currentSnapshot()
	if snapshot == nil {
		return map[uint64]uint32{}, nil
	}
	defer snapshot.decRef()
	depths := make(map[uint64]uint32, len(snapshot.parts))
	for _, partData := range snapshot.parts {
		depths[partData.ID()] = partData.mergeDepth
	}
	return depths, nil
}

// ActivePartIDs returns the core part IDs referenced by the current durable snapshot.
func (bpr *BenchmarkPartReceiver) ActivePartIDs() ([]uint64, error) {
	if bpr == nil || bpr.table == nil {
		return nil, fmt.Errorf("benchmark receiver is not open")
	}
	snapshot := bpr.table.currentSnapshot()
	if snapshot == nil {
		return nil, nil
	}
	defer snapshot.decRef()
	partIDs := make([]uint64, 0, len(snapshot.parts))
	for _, partData := range snapshot.parts {
		partIDs = append(partIDs, partData.ID())
	}
	sort.Slice(partIDs, func(leftIdx, rightIdx int) bool { return partIDs[leftIdx] < partIDs[rightIdx] })
	return partIDs, nil
}

// TraceFragmentMaybeOutsideSelection reports whether the persisted fragment guard would defer dropping a trace because an outside candidate part may contain it.
func (bpr *BenchmarkPartReceiver) TraceFragmentMaybeOutsideSelection(selectedPartIDs []uint64, traceID string,
	minTimestamp, maxTimestamp int64, grace time.Duration,
) (bool, error) {
	if bpr == nil || bpr.table == nil {
		return false, fmt.Errorf("benchmark receiver is not open")
	}
	if traceID == "" || minTimestamp > maxTimestamp || grace < 0 {
		return false, fmt.Errorf("invalid benchmark trace fragment query")
	}
	selected := make(map[uint64]struct{}, len(selectedPartIDs))
	for _, partID := range selectedPartIDs {
		selected[partID] = struct{}{}
	}
	guardMin := traceFragmentSaturatingSub(minTimestamp, int64(grace))
	guardMax := traceFragmentSaturatingAdd(maxTimestamp, int64(grace))
	snapshot := bpr.table.currentSnapshot()
	if snapshot == nil {
		return false, fmt.Errorf("benchmark trace fragment snapshot is unavailable")
	}
	defer snapshot.decRef()
	for _, partData := range snapshot.parts {
		if partData == nil || partData.p == nil {
			return true, nil
		}
		if _, isSelected := selected[partData.ID()]; isSelected {
			continue
		}
		metadata := &partData.p.partMetadata
		if metadata.TotalCount == 0 || metadata.MaxTimestamp < guardMin || metadata.MinTimestamp > guardMax {
			continue
		}
		if partData.p.traceIDFilter.filter == nil || partData.p.traceIDFilter.filter.MightContain(convert.StringToBytes(traceID)) {
			return true, nil
		}
	}
	return false, nil
}

func (bpr *BenchmarkPartReceiver) restoreMergeDepths(depths map[uint64]uint32) error {
	if len(depths) == 0 {
		return nil
	}
	snapshot := bpr.table.currentSnapshot()
	if snapshot == nil {
		return fmt.Errorf("cannot restore %d merge depths into an empty snapshot", len(depths))
	}
	defer snapshot.decRef()
	if len(depths) != len(snapshot.parts) {
		return fmt.Errorf("merge depth count %d does not match active part count %d", len(depths), len(snapshot.parts))
	}
	for _, partData := range snapshot.parts {
		depth, found := depths[partData.ID()]
		if !found {
			return fmt.Errorf("merge depth for active part %016x is missing", partData.ID())
		}
		partData.mergeDepth = depth
	}
	return nil
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

// RunFinalizeRound runs one production finalization round against all parts cooled at logicalNow.
func (bpr *BenchmarkPartReceiver) RunFinalizeRound(ctx context.Context, logicalNow time.Time, grace time.Duration) (bool, error) {
	if bpr == nil || bpr.table == nil {
		return false, fmt.Errorf("benchmark receiver is not open")
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return false, fmt.Errorf("benchmark finalize round canceled before dispatch: %w", contextErr)
	}
	if logicalNow.IsZero() {
		return false, fmt.Errorf("finalize logical time is required")
	}
	if grace <= 0 {
		return false, fmt.Errorf("finalize grace must be positive")
	}
	if phaseErr := bpr.SetMergeRecordingPhase(BenchmarkMergePhaseCooldown); phaseErr != nil {
		return false, fmt.Errorf("cannot set merge recording phase %q: %w", BenchmarkMergePhaseCooldown, phaseErr)
	}
	bpr.table.setMergeNow(logicalNow)
	// A dispatched finalization is owned by the table lifecycle so its durable introduction is not interrupted by an HTTP disconnect.
	finalized, finalizeErr := bpr.table.runFinalizeRound(lookupSamplers(bpr.table.group), int64(grace)) //nolint:contextcheck
	if finalizeErr != nil {
		return false, fmt.Errorf("cannot run benchmark finalize round: %w", finalizeErr)
	}
	return finalized, nil
}

// PreviewMergeSelection reports the next selection from the production merge policy without dispatching it.
func (bpr *BenchmarkPartReceiver) PreviewMergeSelection() (BenchmarkMergeEvent, error) {
	if bpr == nil || bpr.table == nil {
		return BenchmarkMergeEvent{}, fmt.Errorf("benchmark receiver is not open")
	}
	snapshot := bpr.table.currentSnapshot()
	if snapshot == nil {
		return BenchmarkMergeEvent{}, fmt.Errorf("cannot preview merge selection: %w", errNoPartToMerge)
	}
	defer snapshot.decRef()
	parts, _ := bpr.table.getPartsToMerge(snapshot, bpr.table.freeDiskSpace(bpr.table.root))
	if len(parts) < 2 {
		return BenchmarkMergeEvent{}, fmt.Errorf("cannot preview merge selection: %w", errNoPartToMerge)
	}
	lane := mergeLaneSlow
	if sumCompressedSize(parts) < computeSmallMergeThreshold() {
		lane = mergeLaneFast
	}
	return buildMergeBenchmarkEvent(bpr.table, parts, mergeTypeFile, lane, mergePhasePrimary), nil
}

// RunOneMerge executes exactly one production-picker merge and leaves recursive work blocked.
func (bpr *BenchmarkPartReceiver) RunOneMerge(ctx context.Context, options BenchmarkOneMergeOptions) (BenchmarkMergeEvent, error) {
	if bpr == nil || bpr.table == nil {
		return BenchmarkMergeEvent{}, fmt.Errorf("benchmark receiver is not open")
	}
	if options.LogicalNow.IsZero() {
		return BenchmarkMergeEvent{}, fmt.Errorf("controlled merge logical time is required")
	}
	if options.ExpectedSelectionSHA256 == "" {
		return BenchmarkMergeEvent{}, fmt.Errorf("controlled merge selection checksum is required")
	}
	if blockErr := bpr.table.blockMergeUntilWave(); blockErr != nil {
		return BenchmarkMergeEvent{}, fmt.Errorf("cannot block recursive merge dispatch: %w", blockErr)
	}
	bpr.table.setMergeNow(options.LogicalNow)
	preview, previewErr := bpr.PreviewMergeSelection()
	if previewErr != nil {
		return BenchmarkMergeEvent{}, previewErr
	}
	if preview.SelectionSHA256 != options.ExpectedSelectionSHA256 {
		return BenchmarkMergeEvent{}, fmt.Errorf("controlled merge selection checksum %s does not match expected %s",
			preview.SelectionSHA256, options.ExpectedSelectionSHA256)
	}
	if options.RequireAllMature && preview.HotInputParts > 0 {
		return BenchmarkMergeEvent{}, fmt.Errorf("controlled merge selection contains %d hot input parts", preview.HotInputParts)
	}
	beforeReport, beforeReportErr := bpr.MergeRecordingReport()
	if beforeReportErr != nil {
		return BenchmarkMergeEvent{}, beforeReportErr
	}
	merged, mergeErr := bpr.table.triggerSingleMergeWave(ctx)
	if mergeErr != nil {
		return BenchmarkMergeEvent{}, fmt.Errorf("cannot execute controlled merge: %w", mergeErr)
	}
	if !merged {
		return BenchmarkMergeEvent{}, fmt.Errorf("controlled merge did not dispatch")
	}
	afterReport, afterReportErr := bpr.MergeRecordingReport()
	if afterReportErr != nil {
		return BenchmarkMergeEvent{}, afterReportErr
	}
	if len(afterReport.Events) != len(beforeReport.Events)+1 {
		return BenchmarkMergeEvent{}, fmt.Errorf("controlled merge recorded %d new events instead of one",
			len(afterReport.Events)-len(beforeReport.Events))
	}
	return afterReport.Events[len(afterReport.Events)-1], nil
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
