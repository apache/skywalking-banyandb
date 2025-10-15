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
	"time"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/logger"
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

	// Execute sync operation
	if err := tst.executeSyncOperation(partsToSync, partIDsToSync); err != nil {
		return err
	}

	// Handle sync introductions
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

// executeSyncOperation performs the actual synchronization of parts to nodes.
func (tst *tsTable) executeSyncOperation(partsToSync []*part, partIDsToSync map[uint64]struct{}) error {
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
	for _, node := range nodes {
		if tst.loopCloser != nil && tst.loopCloser.Closed() {
			return errClosed
		}
		// Prepare all streaming parts data
		streamingParts := make([]queue.StreamingPartData, 0)
		// Add sidx streaming parts
		for name, sidx := range sidxMap {
			sidxStreamingParts, sidxReleaseFuncs := sidx.StreamingParts(partIDsToSync, tst.group, uint32(tst.shardID), name)
			if len(sidxStreamingParts) != len(partIDsToSync) {
				logger.Panicf("sidx streaming parts count mismatch: %d != %d", len(sidxStreamingParts), len(partIDsToSync))
				return nil
			}
			streamingParts = append(streamingParts, sidxStreamingParts...)
			releaseFuncs = append(releaseFuncs, sidxReleaseFuncs...)
		}
		if len(streamingParts) != len(partIDsToSync)*len(sidxMap) {
			logger.Panicf("streaming parts count mismatch: %d != %d", len(streamingParts), len(partIDsToSync)*len(sidxMap))
			return nil
		}
		// Create streaming parts from core trace parts.
		for _, part := range partsToSync {
			files, release := createPartFileReaders(part)
			releaseFuncs = append(releaseFuncs, release)
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
		if err := tst.syncStreamingPartsToNode(ctx, node, streamingParts); err != nil {
			return err
		}
	}
	return nil
}

// handleSyncIntroductions creates and processes sync introductions for both core and sidx parts.
func (tst *tsTable) handleSyncIntroductions(partsToSync []*part, syncCh chan *syncIntroduction) error {
	// Create core sync introduction
	si := generateSyncIntroduction()
	defer releaseSyncIntroduction(si)
	si.applied = make(chan struct{})
	for _, part := range partsToSync {
		si.synced[part.partMetadata.ID] = struct{}{}
	}

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
func (tst *tsTable) syncStreamingPartsToNode(ctx context.Context, node string, streamingParts []queue.StreamingPartData) error {
	// Get chunked sync client for this node
	chunkedClient, err := tst.option.tire2Client.NewChunkedSyncClient(node, 1024*1024)
	if err != nil {
		return fmt.Errorf("failed to create chunked sync client for node %s: %w", node, err)
	}
	defer chunkedClient.Close()

	// Sync parts using chunked transfer with streaming
	result, err := chunkedClient.SyncStreamingParts(ctx, streamingParts)
	if err != nil {
		return fmt.Errorf("failed to sync streaming parts to node %s: %w", node, err)
	}

	if !result.Success {
		return fmt.Errorf("chunked sync partially failed: %v", result.ErrorMessage)
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
			Msg("chunked sync completed successfully")
	}
	return nil
}
