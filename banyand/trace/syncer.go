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

package trace

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

// PartTypeCore is the type of the core part.
const PartTypeCore = "core"

func (tst *tsTable) syncLoop(syncCh chan *syncIntroduction, flusherNotifier watcher.Channel) {
	defer tst.loopCloser.Done()

	var epoch uint64
	var lastTriggerTime time.Time

	ew := flusherNotifier.Add(0, tst.loopCloser.CloseNotify())
	if ew == nil {
		return
	}

	syncTimer := time.NewTimer(tst.option.syncInterval)
	defer syncTimer.Stop()

	trySync := func(triggerTime time.Time) bool {
		defer syncTimer.Reset(tst.option.syncInterval)
		curSnapshot := tst.currentSnapshot()
		if curSnapshot == nil {
			return false
		}
		defer curSnapshot.decRef()
		if curSnapshot.epoch != epoch {
			tst.incTotalSyncLoopStarted(1)
			defer tst.incTotalSyncLoopFinished(1)
			var err error
			if err = tst.syncSnapshot(curSnapshot, syncCh); err != nil {
				if tst.loopCloser.Closed() {
					return true
				}
				tst.l.Logger.Warn().Err(err).Msgf("cannot sync snapshot: %d", curSnapshot.epoch)
				tst.incTotalSyncLoopErr(1)
				time.Sleep(2 * time.Second)
				return false
			}
			epoch = curSnapshot.epoch
			lastTriggerTime = triggerTime
			if tst.currentEpoch() != epoch {
				return false
			}
		}
		ew = flusherNotifier.Add(epoch, tst.loopCloser.CloseNotify())
		return ew == nil
	}

	for {
		select {
		case <-tst.loopCloser.CloseNotify():
			return
		case <-ew.Watch():
			if trySync(time.Now()) {
				return
			}
		case <-syncTimer.C:
			now := time.Now()
			if now.Sub(lastTriggerTime) < tst.option.syncInterval {
				syncTimer.Reset(tst.option.syncInterval - now.Sub(lastTriggerTime))
				continue
			}
			if trySync(now) {
				return
			}
		}
	}
}

func createPartFileReaders(part *part) ([]queue.FileInfo, func()) {
	var files []queue.FileInfo
	var buffersToRelease []*bytes.Buffer

	buf := bigValuePool.Generate()
	// Trace metadata
	for i := range part.primaryBlockMetadata {
		buf.Buf = part.primaryBlockMetadata[i].marshal(buf.Buf)
	}
	bb := bigValuePool.Generate()
	bb.Buf = zstd.Compress(bb.Buf[:0], buf.Buf, 1)
	bigValuePool.Release(buf)
	buffersToRelease = append(buffersToRelease, bb)
	files = append(files,
		queue.FileInfo{
			Name:   traceMetaName,
			Reader: bb.SequentialRead(),
		},
		queue.FileInfo{
			Name:   tracePrimaryName,
			Reader: part.primary.SequentialRead(),
		},
		queue.FileInfo{
			Name:   traceSpansName,
			Reader: part.spans.SequentialRead(),
		},
	)

	// Trace tags data
	if part.tags != nil {
		for name, reader := range part.tags {
			files = append(files, queue.FileInfo{
				Name:   fmt.Sprintf("%s%s", traceTagsPrefix, name),
				Reader: reader.SequentialRead(),
			})
		}
		for name, reader := range part.tagMetadata {
			files = append(files, queue.FileInfo{
				Name:   fmt.Sprintf("%s%s", traceTagMetadataPrefix, name),
				Reader: reader.SequentialRead(),
			})
		}
	}

	// TraceID filter data
	if part.fileSystem != nil {
		traceIDFilterPath := filepath.Join(part.path, traceIDFilterFilename)
		if filterData, err := part.fileSystem.Read(traceIDFilterPath); err == nil && len(filterData) > 0 {
			filterBuf := bigValuePool.Generate()
			filterBuf.Buf = append(filterBuf.Buf[:0], filterData...)
			buffersToRelease = append(buffersToRelease, filterBuf)
			files = append(files, queue.FileInfo{
				Name:   traceIDFilterFilename,
				Reader: filterBuf.SequentialRead(),
			})
		}
	}

	// Tag type data
	if part.fileSystem != nil {
		tagTypePath := filepath.Join(part.path, tagTypeFilename)
		if tagTypeData, err := part.fileSystem.Read(tagTypePath); err == nil && len(tagTypeData) > 0 {
			tagTypeBuf := bigValuePool.Generate()
			tagTypeBuf.Buf = append(tagTypeBuf.Buf[:0], tagTypeData...)
			buffersToRelease = append(buffersToRelease, tagTypeBuf)
			files = append(files, queue.FileInfo{
				Name:   tagTypeFilename,
				Reader: tagTypeBuf.SequentialRead(),
			})
		}
	}

	return files, func() {
		for _, buffer := range buffersToRelease {
			bigValuePool.Release(buffer)
		}
	}
}

func (tst *tsTable) syncSnapshot(curSnapshot *snapshot, syncCh chan *syncIntroduction) error {
	startTime := time.Now()
	defer func() {
		tst.incTotalSyncLoopLatency(time.Since(startTime).Seconds())
	}()

	if tst.loopCloser != nil && tst.loopCloser.Closed() {
		return errClosed
	}

	// Collect parts to sync
	partsToSync, partIDsToSync := tst.collectPartsToSync(curSnapshot)

	// Validate sync preconditions
	if !tst.needToSync(partsToSync) {
		return nil
	}

	// Initialize failed parts handler
	failedPartsHandler := storage.NewFailedPartsHandler(tst.fileSystem, tst.root, tst.l)

	// Execute sync operation
	if err := tst.executeSyncOperation(partsToSync, partIDsToSync, failedPartsHandler); err != nil {
		return err
	}

	// Handle sync introductions (includes both successful and permanently failed parts)
	return tst.handleSyncIntroductions(partsToSync, syncCh)
}

// collectPartsToSync collects both core and sidx parts that need to be synchronized.
func (tst *tsTable) collectPartsToSync(curSnapshot *snapshot) ([]*part, map[uint64]struct{}) {
	// Get all parts from the current snapshot
	var partsToSync []*part
	sidxPartsToSync := make(map[uint64]struct{})
	for _, pw := range curSnapshot.parts {
		if pw.mp == nil && pw.p.partMetadata.TotalCount > 0 {
			partsToSync = append(partsToSync, pw.p)
			sidxPartsToSync[pw.p.partMetadata.ID] = struct{}{}
		}
	}
	return partsToSync, sidxPartsToSync
}

// needToSync validates that there are parts to sync and nodes available.
func (tst *tsTable) needToSync(partsToSync []*part) bool {
	nodes := tst.getNodes()
	return len(partsToSync) > 0 && len(nodes) > 0
}

// syncPartsToNodesHelper syncs given parts to all nodes and returns failed parts.
// This helper is used for both initial sync and retry attempts.
func (tst *tsTable) syncPartsToNodesHelper(
	ctx context.Context, parts []*part, partIDsMap map[uint64]struct{},
	nodes []string, sidxMap map[string]sidx.SIDX, releaseFuncs *[]func(),
) ([]queue.FailedPart, error) {
	var allFailedParts []queue.FailedPart

	for _, node := range nodes {
		if tst.loopCloser != nil && tst.loopCloser.Closed() {
			return allFailedParts, errClosed
		}

		// Prepare streaming parts data
		streamingParts := make([]queue.StreamingPartData, 0)

		// Add sidx streaming parts
		for name, sidx := range sidxMap {
			sidxStreamingParts, sidxReleaseFuncs := sidx.StreamingParts(partIDsMap, tst.group, uint32(tst.shardID), name)
			streamingParts = append(streamingParts, sidxStreamingParts...)
			*releaseFuncs = append(*releaseFuncs, sidxReleaseFuncs...)
		}

		// Create streaming parts from core trace parts
		for _, part := range parts {
			files, release := createPartFileReaders(part)
			*releaseFuncs = append(*releaseFuncs, release)
			streamingParts = append(streamingParts, queue.StreamingPartData{
				ID:                    part.partMetadata.ID,
				Group:                 tst.group,
				ShardID:               uint32(tst.shardID),
				Topic:                 data.TopicTracePartSync.String(),
				Files:                 files,
				CompressedSizeBytes:   part.partMetadata.CompressedSizeBytes,
				UncompressedSizeBytes: part.partMetadata.UncompressedSpanSizeBytes,
				TotalCount:            part.partMetadata.TotalCount,
				BlocksCount:           part.partMetadata.BlocksCount,
				MinTimestamp:          part.partMetadata.MinTimestamp,
				MaxTimestamp:          part.partMetadata.MaxTimestamp,
				PartType:              PartTypeCore,
			})
		}

		sort.Slice(streamingParts, func(i, j int) bool {
			if streamingParts[i].ID == streamingParts[j].ID {
				return streamingParts[i].PartType < streamingParts[j].PartType
			}
			return streamingParts[i].ID < streamingParts[j].ID
		})

		failedParts, err := tst.syncStreamingPartsToNode(ctx, node, streamingParts)
		if err != nil {
			return allFailedParts, err
		}
		allFailedParts = append(allFailedParts, failedParts...)
	}

	return allFailedParts, nil
}

// executeSyncOperation performs the actual synchronization of parts to nodes.
func (tst *tsTable) executeSyncOperation(partsToSync []*part, partIDsToSync map[uint64]struct{}, failedPartsHandler *storage.FailedPartsHandler) error {
	sort.Slice(partsToSync, func(i, j int) bool {
		return partsToSync[i].partMetadata.ID < partsToSync[j].partMetadata.ID
	})

	ctx := context.Background()
	releaseFuncs := make([]func(), 0, len(partsToSync))
	defer func() {
		for _, release := range releaseFuncs {
			release()
		}
	}()

	nodes := tst.getNodes()
	if tst.loopCloser != nil && tst.loopCloser.Closed() {
		return errClosed
	}

	sidxMap := tst.getAllSidx()

	// Build part info map for failed parts handler
	// Each part ID can have multiple entries (core + SIDX parts)
	partsInfo := make(map[uint64][]*storage.PartInfo)
	// Add core parts
	for _, part := range partsToSync {
		partsInfo[part.partMetadata.ID] = []*storage.PartInfo{
			{
				PartID:     part.partMetadata.ID,
				SourcePath: part.path,
				PartType:   PartTypeCore,
			},
		}
	}
	// Add SIDX parts
	for sidxName, sidxInstance := range sidxMap {
		partPaths := sidxInstance.PartPaths(partIDsToSync)
		for partID, partPath := range partPaths {
			partsInfo[partID] = append(partsInfo[partID], &storage.PartInfo{
				PartID:     partID,
				SourcePath: partPath,
				PartType:   sidxName,
			})
		}
	}

	// Initial sync attempt - track failures per node
	perNodeFailures := make(map[string][]queue.FailedPart) // node -> failed parts
	for _, node := range nodes {
		if tst.loopCloser != nil && tst.loopCloser.Closed() {
			return errClosed
		}
		failedParts, err := tst.syncPartsToNodesHelper(ctx, partsToSync, partIDsToSync, []string{node}, sidxMap, &releaseFuncs)
		if err != nil {
			tst.l.Error().Err(err).Str("node", node).Msg("sync error")
			// Mark all parts as failed for this node
			var allPartsFailed []queue.FailedPart
			for _, part := range partsToSync {
				allPartsFailed = append(allPartsFailed, queue.FailedPart{
					PartID: strconv.FormatUint(part.partMetadata.ID, 10),
					Error:  fmt.Sprintf("node %s: %v", node, err),
				})
			}
			perNodeFailures[node] = allPartsFailed
			continue
		}
		if len(failedParts) > 0 {
			perNodeFailures[node] = failedParts
		}
	}

	// After sync attempts, enqueue parts for offline nodes
	tst.enqueueForOfflineNodes(nodes, partsToSync, partIDsToSync)

	// If there are failed parts, use the retry handler
	if len(perNodeFailures) > 0 {
		// Collect all unique failed parts for retry handler
		allFailedParts := make([]queue.FailedPart, 0)
		for _, failedParts := range perNodeFailures {
			allFailedParts = append(allFailedParts, failedParts...)
		}

		// Create a sync function for retries - only retry on nodes where parts failed
		syncFunc := func(partIDs []uint64) ([]queue.FailedPart, error) {
			// Build map of partIDs to retry
			partIDsSet := make(map[uint64]struct{})
			for _, partID := range partIDs {
				partIDsSet[partID] = struct{}{}
			}

			// Filter parts to retry
			partsToRetry := make([]*part, 0)
			partIDsMap := make(map[uint64]struct{})
			for _, partID := range partIDs {
				partIDsMap[partID] = struct{}{}
				for _, part := range partsToSync {
					if part.partMetadata.ID == partID {
						partsToRetry = append(partsToRetry, part)
						break
					}
				}
			}

			if len(partsToRetry) == 0 {
				return nil, nil
			}

			retryReleaseFuncs := make([]func(), 0)
			defer func() {
				for _, release := range retryReleaseFuncs {
					release()
				}
			}()

			// Only retry on nodes where these specific parts failed
			var retryFailedParts []queue.FailedPart
			for node, nodeFailedParts := range perNodeFailures {
				// Check if any of the parts to retry failed on this node
				shouldRetryOnNode := false
				for _, failedPart := range nodeFailedParts {
					failedPartID, _ := strconv.ParseUint(failedPart.PartID, 10, 64)
					if _, exists := partIDsSet[failedPartID]; exists {
						shouldRetryOnNode = true
						break
					}
				}

				if !shouldRetryOnNode {
					continue
				}

				// Retry only on this specific node
				failedParts, err := tst.syncPartsToNodesHelper(ctx, partsToRetry, partIDsMap, []string{node}, sidxMap, &retryReleaseFuncs)
				if err != nil {
					// On error, mark all parts as failed for this node
					for _, partID := range partIDs {
						retryFailedParts = append(retryFailedParts, queue.FailedPart{
							PartID: strconv.FormatUint(partID, 10),
							Error:  fmt.Sprintf("node %s: %v", node, err),
						})
					}
					continue
				}
				retryFailedParts = append(retryFailedParts, failedParts...)
			}

			return retryFailedParts, nil
		}

		permanentlyFailedParts, err := failedPartsHandler.RetryFailedParts(ctx, allFailedParts, partsInfo, syncFunc)
		if err != nil {
			tst.l.Warn().Err(err).Msg("error during retry process")
		}
		if len(permanentlyFailedParts) > 0 {
			tst.l.Error().
				Uints64("partIDs", permanentlyFailedParts).
				Int("count", len(permanentlyFailedParts)).
				Msg("parts permanently failed after all retries and have been copied to failed-parts directory")
		}
	}

	return nil
}

// handleSyncIntroductions creates and processes sync introductions for both core and sidx parts.
// Includes both successful and permanently failed parts to ensure snapshot cleanup.
func (tst *tsTable) handleSyncIntroductions(partsToSync []*part, syncCh chan *syncIntroduction) error {
	// Create core sync introduction
	si := generateSyncIntroduction()
	defer releaseSyncIntroduction(si)
	si.applied = make(chan struct{})

	// Add all parts (both successful and permanently failed)
	for _, part := range partsToSync {
		si.synced[part.partMetadata.ID] = struct{}{}
	}
	// Permanently failed parts are already included since they're in partsToSync

	select {
	case syncCh <- si:
	case <-tst.loopCloser.CloseNotify():
		return errClosed
	}

	select {
	case <-si.applied:
	case <-tst.loopCloser.CloseNotify():
		return errClosed
	}
	return nil
}

// syncStreamingPartsToNode synchronizes streaming parts to a node.
// Returns the list of failed parts.
func (tst *tsTable) syncStreamingPartsToNode(ctx context.Context, node string, streamingParts []queue.StreamingPartData) ([]queue.FailedPart, error) {
	// Get chunked sync client for this node
	chunkedClient, err := tst.option.tire2Client.NewChunkedSyncClient(node, 1024*1024)
	if err != nil {
		return nil, fmt.Errorf("failed to create chunked sync client for node %s: %w", node, err)
	}
	defer chunkedClient.Close()

	// Sync parts using chunked transfer with streaming
	result, err := chunkedClient.SyncStreamingParts(ctx, streamingParts)
	if err != nil {
		return nil, fmt.Errorf("failed to sync streaming parts to node %s: %w", node, err)
	}

	tst.incTotalSyncLoopBytes(result.TotalBytes)
	if dl := tst.l.Debug(); dl.Enabled() {
		dl.
			Str("node", node).
			Str("session", result.SessionID).
			Uint64("bytes", result.TotalBytes).
			Int64("duration_ms", result.DurationMs).
			Uint32("chunks", result.ChunksCount).
			Uint32("parts", result.PartsCount).
			Int("failed_parts", len(result.FailedParts)).
			Msg("chunked sync completed")
	}

	return result.FailedParts, nil
}
