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
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
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
	if tst.loopCloser != nil && tst.loopCloser.Closed() {
		return errClosed
	}

	// Collect parts to sync
	partsToSync, sidxPartsToSync, err := tst.collectPartsToSync(curSnapshot)
	if err != nil {
		return err
	}
	if len(partsToSync) == 0 && len(sidxPartsToSync) == 0 {
		return nil
	}
	hasSidxParts := false
	for _, sidxParts := range sidxPartsToSync {
		if len(sidxParts) == 0 {
			continue
		}
		hasSidxParts = true
		break
	}
	if len(partsToSync) == 0 && !hasSidxParts {
		return nil
	}

	// Validate sync preconditions
	if err := tst.validateSyncPreconditions(partsToSync, sidxPartsToSync); err != nil {
		return err
	}

	// Execute sync operation
	if err := tst.executeSyncOperation(partsToSync, sidxPartsToSync); err != nil {
		return err
	}

	// Handle sync introductions
	return tst.handleSyncIntroductions(partsToSync, sidxPartsToSync, syncCh)
}

// collectPartsToSync collects both core and sidx parts that need to be synchronized.
func (tst *tsTable) collectPartsToSync(curSnapshot *snapshot) ([]*part, map[string][]*sidx.Part, error) {
	// Get all parts from the current snapshot
	var partsToSync []*part
	for _, pw := range curSnapshot.parts {
		if pw.mp == nil && pw.p.partMetadata.TotalCount > 0 {
			partsToSync = append(partsToSync, pw.p)
		}
	}

	sidxPartsToSync := sidx.NewPartsToSync()
	for name, sidx := range tst.getAllSidx() {
		if tst.loopCloser != nil && tst.loopCloser.Closed() {
			return nil, nil, errClosed
		}
		sidxPartsToSync[name] = sidx.PartsToSync()
	}

	return partsToSync, sidxPartsToSync, nil
}

// validateSyncPreconditions validates that there are parts to sync and nodes available.
func (tst *tsTable) validateSyncPreconditions(partsToSync []*part, sidxPartsToSync map[string][]*sidx.Part) error {
	hasCoreParts := len(partsToSync) > 0
	hasSidxParts := false
	for _, parts := range sidxPartsToSync {
		if len(parts) > 0 {
			hasSidxParts = true
			break
		}
	}
	if !hasCoreParts && !hasSidxParts {
		return nil
	}

	nodes := tst.getNodes()
	if len(nodes) == 0 {
		return fmt.Errorf("no nodes to sync parts")
	}

	return nil
}

// executeSyncOperation performs the actual synchronization of parts to nodes.
func (tst *tsTable) executeSyncOperation(partsToSync []*part, sidxPartsToSync map[string][]*sidx.Part) error {
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
	return tst.syncStreamingPartsToNodes(ctx, nodes, partsToSync, sidxPartsToSync, &releaseFuncs)
}

// handleSyncIntroductions creates and processes sync introductions for both core and sidx parts.
func (tst *tsTable) handleSyncIntroductions(partsToSync []*part, sidxPartsToSync map[string][]*sidx.Part, syncCh chan *syncIntroduction) error {
	// Create core sync introduction
	si := generateSyncIntroduction()
	defer releaseSyncIntroduction(si)
	si.applied = make(chan struct{})
	for _, part := range partsToSync {
		ph := partHandle{
			partID:   part.partMetadata.ID,
			partType: PartTypeCore,
		}
		si.synced[ph] = struct{}{}
	}

	// Create sidx sync introductions
	sidxSyncIntroductions := tst.createSidxSyncIntroductions(sidxPartsToSync)
	defer tst.releaseSidxSyncIntroductions(sidxSyncIntroductions)

	// Send sync introductions
	if err := tst.sendSyncIntroductions(si, sidxSyncIntroductions, syncCh); err != nil {
		return err
	}

	// Wait for sync introductions to be applied
	return tst.waitForSyncIntroductions(si, sidxSyncIntroductions)
}

// createSidxSyncIntroductions creates sync introductions for sidx parts.
func (tst *tsTable) createSidxSyncIntroductions(sidxPartsToSync map[string][]*sidx.Part) map[string]*sidx.SyncIntroduction {
	sidxSyncIntroductions := make(map[string]*sidx.SyncIntroduction)
	for name, sidxParts := range sidxPartsToSync {
		if len(sidxParts) > 0 {
			ssi := sidx.GenerateSyncIntroduction()
			ssi.Applied = make(chan struct{})
			for _, part := range sidxParts {
				ssi.Synced[part.ID()] = struct{}{}
			}
			sidxSyncIntroductions[name] = ssi
		}
	}
	return sidxSyncIntroductions
}

// releaseSidxSyncIntroductions releases sidx sync introductions.
func (tst *tsTable) releaseSidxSyncIntroductions(sidxSyncIntroductions map[string]*sidx.SyncIntroduction) {
	for _, ssi := range sidxSyncIntroductions {
		sidx.ReleaseSyncIntroduction(ssi)
	}
}

// sendSyncIntroductions sends sync introductions to their respective channels.
func (tst *tsTable) sendSyncIntroductions(si *syncIntroduction, sidxSyncIntroductions map[string]*sidx.SyncIntroduction, syncCh chan *syncIntroduction) error {
	select {
	case syncCh <- si:
	case <-tst.loopCloser.CloseNotify():
		return errClosed
	}

	for name, ssi := range sidxSyncIntroductions {
		sidx, ok := tst.getSidx(name)
		if !ok {
			return fmt.Errorf("sidx %s not found", name)
		}
		select {
		case sidx.SyncCh() <- ssi:
		case <-tst.loopCloser.CloseNotify():
			return errClosed
		}
	}
	return nil
}

// waitForSyncIntroductions waits for all sync introductions to be applied.
func (tst *tsTable) waitForSyncIntroductions(si *syncIntroduction, sidxSyncIntroductions map[string]*sidx.SyncIntroduction) error {
	select {
	case <-si.applied:
	case <-tst.loopCloser.CloseNotify():
		return errClosed
	}

	for _, ssi := range sidxSyncIntroductions {
		select {
		case <-ssi.Applied:
		case <-tst.loopCloser.CloseNotify():
			return errClosed
		}
	}
	return nil
}

// syncStreamingPartsToNodes synchronizes streamingparts to multiple nodes.
func (tst *tsTable) syncStreamingPartsToNodes(ctx context.Context, nodes []string,
	partsToSync []*part, sidxPartsToSync map[string][]*sidx.Part, releaseFuncs *[]func(),
) error {
	if tst.loopCloser != nil && tst.loopCloser.Closed() {
		return errClosed
	}
	for _, node := range nodes {
		if tst.loopCloser != nil && tst.loopCloser.Closed() {
			return errClosed
		}
		// Prepare all streaming parts data
		streamingParts := make([]queue.StreamingPartData, 0)
		// Add sidx streaming parts
		for name, sidxParts := range sidxPartsToSync {
			sidx, ok := tst.getSidx(name)
			if !ok {
				return fmt.Errorf("sidx %s not found", name)
			}
			sidxStreamingParts, sidxReleaseFuncs := sidx.StreamingParts(sidxParts, tst.group, uint32(tst.shardID), name)
			streamingParts = append(streamingParts, sidxStreamingParts...)
			*releaseFuncs = append(*releaseFuncs, sidxReleaseFuncs...)
		}
		// Create streaming parts from core trace parts.
		for _, part := range partsToSync {
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
		if err := tst.syncStreamingPartsToNode(ctx, node, streamingParts); err != nil {
			return err
		}
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
	tst.l.Info().
		Str("node", node).
		Str("session", result.SessionID).
		Uint64("bytes", result.TotalBytes).
		Int64("duration_ms", result.DurationMs).
		Uint32("chunks", result.ChunksCount).
		Uint32("parts", result.PartsCount).
		Msg("chunked sync completed successfully")
	return nil
}
