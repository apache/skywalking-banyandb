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
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type syncPartContext struct {
	tsTable *tsTable
	// segment is held until Close. On stages with Close=true, closeIdleSegments
	// can drive refCount to 0; releasing earlier lets each sync call bottom out
	// there, so the next session's incRef re-runs initTSTable and deletes the
	// in-flight part.
	segment             storage.Segment[*tsTable, *commonv1.ResourceOpts]
	fileSystem          fs.FileSystem
	l                   *logger.Logger
	writers             *writers
	partPath            string
	sidxPartContexts    map[string]*sidx.SyncPartContext
	traceIDFilterBuffer []byte
	tagTypeBuffer       []byte
	partMeta            partMetadata
	partID              uint64
}

func (s *syncPartContext) NewPartType(ctx *queue.ChunkedSyncPartContext) error {
	if ctx.PartType != PartTypeCore {
		// Allocate partID on first call so SIDX parts use the same ID as the core part.
		if s.partID == 0 {
			s.partID = atomic.AddUint64(&s.tsTable.curPartID, 1)
			s.fileSystem = s.tsTable.fileSystem
		}
		// Pre-register the SIDX instance before writing files so its init/loadSnapshot
		// runs against an empty directory. Otherwise mustGetOrCreateSidx in introducePart
		// would lazily create the SIDX, scan the directory, and delete the freshly-written
		// part because it isn't in the (nil) availablePartIDs list.
		if _, err := s.tsTable.getOrCreateSidx(ctx.PartType); err != nil {
			return fmt.Errorf("cannot pre-create sidx %q: %w", ctx.PartType, err)
		}
		sidxPartContext := sidx.NewSyncPartContext()
		sidxPartPath := filepath.Join(s.tsTable.root, sidxDirName, ctx.PartType, fmt.Sprintf("%016x", s.partID))
		sidxPartContext.SetForFile(ctx.PartType, s.fileSystem, sidxPartPath, ctx, s.tsTable.pm.ShouldCache(int64(ctx.CompressedSizeBytes)))
		if s.sidxPartContexts == nil {
			s.sidxPartContexts = make(map[string]*sidx.SyncPartContext)
		}
		s.sidxPartContexts[ctx.PartType] = sidxPartContext
		return nil
	}

	// Allocate partID if not already allocated by an earlier SIDX NewPartType call.
	if s.partID == 0 {
		s.partID = atomic.AddUint64(&s.tsTable.curPartID, 1)
	}
	s.partPath = partPath(s.tsTable.root, s.partID)
	s.fileSystem = s.tsTable.fileSystem

	w := generateWriters()
	w.mustInitForFilePart(s.fileSystem, s.partPath, s.tsTable.pm.ShouldCache(int64(ctx.CompressedSizeBytes)))
	s.writers = w

	s.partMeta.fillFromSyncContext(ctx)
	return nil
}

func (s *syncPartContext) FinishSync() error {
	if s.partPath == "" {
		// No core part was written (only SIDX parts arrived). Nothing to finalize;
		// Close will discard any partially-written SIDX parts too.
		return s.Close()
	}

	// Close writers first so file data is flushed before we write sidecar metadata.
	s.releaseCoreWriters()

	if len(s.traceIDFilterBuffer) > 0 {
		fs.MustFlush(s.fileSystem, s.traceIDFilterBuffer, filepath.Join(s.partPath, traceIDFilterFilename), storage.FilePerm)
	}
	if len(s.tagTypeBuffer) > 0 {
		fs.MustFlush(s.fileSystem, s.tagTypeBuffer, filepath.Join(s.partPath, tagTypeFilename), storage.FilePerm)
	}
	s.partMeta.mustWriteMetadata(s.fileSystem, s.partPath)
	s.fileSystem.SyncPath(s.partPath)

	// Finish SIDX writers and collect file paths for file-backed parts.
	sidxFilePartsMap := make(map[string]string, len(s.sidxPartContexts))
	for _, sidxPartContext := range s.sidxPartContexts {
		sidxFilePartsMap[sidxPartContext.Name()] = sidxPartContext.Finish()
	}
	s.tsTable.mustAddFilePart(s.partID, sidxFilePartsMap)
	s.partPath = ""
	s.traceIDFilterBuffer = nil
	s.tagTypeBuffer = nil
	return s.Close()
}

// releaseCoreWriters closes and releases the core writers, if any. Safe to call multiple times.
func (s *syncPartContext) releaseCoreWriters() {
	if s.writers == nil {
		return
	}
	s.writers.MustClose()
	releaseWriters(s.writers)
	s.writers = nil
}

func (s *syncPartContext) Close() error {
	s.releaseCoreWriters()
	// Clean up incomplete filePart on error (partPath is cleared on success in FinishSync)
	if s.partPath != "" && s.fileSystem != nil {
		s.fileSystem.MustRMAll(s.partPath)
		s.partPath = ""
	}
	if s.sidxPartContexts != nil {
		for _, sidxPartContext := range s.sidxPartContexts {
			sidxPartContext.Close()
		}
		s.sidxPartContexts = nil
	}
	if s.segment != nil {
		s.segment.DecRef()
		s.segment = nil
	}
	s.tsTable = nil
	s.fileSystem = nil
	s.traceIDFilterBuffer = nil
	s.tagTypeBuffer = nil
	return nil
}

type syncSeriesContext struct {
	streamer index.ExternalSegmentStreamer
	segment  storage.Segment[*tsTable, *commonv1.ResourceOpts]
	l        *logger.Logger
	fileName string
}

func (s *syncSeriesContext) NewPartType(_ *queue.ChunkedSyncPartContext) error {
	logger.Panicf("new part type is not supported for trace")
	return nil
}

func (s *syncSeriesContext) FinishSync() error {
	if s.streamer != nil {
		if err := s.streamer.CompleteSegment(); err != nil {
			s.l.Error().Err(err).Msg("failed to complete external segment")
			return err
		}
	}
	return s.Close()
}

func (s *syncSeriesContext) Close() error {
	if s.segment != nil {
		s.segment.DecRef()
	}
	s.streamer = nil
	s.fileName = ""
	s.segment = nil
	return nil
}

type syncSeriesCallback struct {
	l          *logger.Logger
	schemaRepo *schemaRepo
}

func setUpSeriesSyncCallback(l *logger.Logger, s *schemaRepo) queue.ChunkedSyncHandler {
	return &syncSeriesCallback{
		l:          l,
		schemaRepo: s,
	}
}

func (s *syncSeriesCallback) CheckHealth() *common.Error {
	return nil
}

func (s *syncSeriesCallback) CreatePartHandler(ctx *queue.ChunkedSyncPartContext) (queue.PartHandler, error) {
	if ctx.MinTimestamp <= 0 {
		return nil, fmt.Errorf("invalid MinTimestamp %d in series sync context for group %s", ctx.MinTimestamp, ctx.Group)
	}
	tsdb, err := s.schemaRepo.loadTSDB(ctx.Group)
	if err != nil {
		s.l.Error().Err(err).Str("group", ctx.Group).Msg("failed to load TSDB for group")
		return nil, err
	}
	segmentTime := time.Unix(0, ctx.MinTimestamp)
	segment, err := tsdb.CreateSegmentIfNotExist(segmentTime)
	if err != nil {
		s.l.Error().Err(err).Str("group", ctx.Group).Time("segmentTime", segmentTime).Msg("failed to create segment")
		return nil, err
	}
	return &syncSeriesContext{
		l:       s.l,
		segment: segment,
	}, nil
}

// HandleFileChunk implements queue.ChunkedSyncHandler for streaming series index chunks.
func (s *syncSeriesCallback) HandleFileChunk(ctx *queue.ChunkedSyncPartContext, chunk []byte) error {
	if ctx.Handler == nil {
		return fmt.Errorf("part handler is nil")
	}
	seriesCtx := ctx.Handler.(*syncSeriesContext)

	if seriesCtx.segment == nil {
		return fmt.Errorf("segment is nil")
	}
	if seriesCtx.fileName != ctx.FileName {
		if seriesCtx.streamer != nil {
			if err := seriesCtx.streamer.CompleteSegment(); err != nil {
				s.l.Error().Err(err).Str("group", ctx.Group).Msg("failed to complete external segment")
				return err
			}
		}
		indexDB := seriesCtx.segment.IndexDB()
		streamer, err := indexDB.EnableExternalSegments()
		if err != nil {
			s.l.Error().Err(err).Str("group", ctx.Group).Msg("failed to enable external segments")
			return err
		}
		if err := streamer.StartSegment(); err != nil {
			s.l.Error().Err(err).Str("group", ctx.Group).Msg("failed to start external segment")
			return err
		}
		seriesCtx.fileName = ctx.FileName
		seriesCtx.streamer = streamer
	}
	if err := seriesCtx.streamer.WriteChunk(chunk); err != nil {
		return fmt.Errorf("failed to write chunk (size: %d) to file %q: %w", len(chunk), ctx.FileName, err)
	}
	return nil
}

type syncChunkCallback struct {
	l          *logger.Logger
	schemaRepo *schemaRepo
}

func setUpChunkedSyncCallback(l *logger.Logger, schemaRepo *schemaRepo) queue.ChunkedSyncHandler {
	return &syncChunkCallback{
		l:          l,
		schemaRepo: schemaRepo,
	}
}

func (s *syncChunkCallback) CheckHealth() *common.Error {
	return nil
}

// CreatePartHandler implements queue.ChunkedSyncHandler.
func (s *syncChunkCallback) CreatePartHandler(ctx *queue.ChunkedSyncPartContext) (queue.PartHandler, error) {
	if ctx.MinTimestamp <= 0 {
		return nil, fmt.Errorf("invalid MinTimestamp %d in chunk sync context for group %s", ctx.MinTimestamp, ctx.Group)
	}
	tsdb, err := s.schemaRepo.loadTSDB(ctx.Group)
	if err != nil {
		s.l.Error().Err(err).Str("group", ctx.Group).Msg("failed to load TSDB for group")
		return nil, err
	}
	segmentTime := time.Unix(0, ctx.MinTimestamp)
	segment, err := tsdb.CreateSegmentIfNotExist(segmentTime)
	if err != nil {
		s.l.Error().Err(err).Str("group", ctx.Group).Time("segmentTime", segmentTime).Msg("failed to create segment")
		return nil, err
	}
	tsTable, err := segment.CreateTSTableIfNotExist(common.ShardID(ctx.ShardID))
	if err != nil {
		segment.DecRef()
		s.l.Error().Err(err).Str("group", ctx.Group).Uint32("shardID", ctx.ShardID).Msg("failed to create ts table")
		return nil, err
	}

	tsdb.Tick(ctx.MaxTimestamp)
	partCtx := &syncPartContext{
		tsTable: tsTable,
		segment: segment,
		l:       s.l,
	}
	if err := partCtx.NewPartType(ctx); err != nil {
		if closeErr := partCtx.Close(); closeErr != nil {
			return nil, fmt.Errorf("new part type: %w; close part context: %w", err, closeErr)
		}
		return nil, err
	}
	return partCtx, nil
}

// HandleFileChunk implements queue.ChunkedSyncHandler for streaming file chunks.
func (s *syncChunkCallback) HandleFileChunk(ctx *queue.ChunkedSyncPartContext, chunk []byte) error {
	if ctx.Handler == nil {
		return fmt.Errorf("part handler is nil")
	}
	if ctx.PartType != PartTypeCore {
		return s.handleSidxFileChunk(ctx, chunk)
	}
	return s.handleTraceFileChunk(ctx, chunk)
}

func (s *syncChunkCallback) handleSidxFileChunk(ctx *queue.ChunkedSyncPartContext, chunk []byte) error {
	sidxName := ctx.PartType
	fileName := ctx.FileName
	partCtx := ctx.Handler.(*syncPartContext)
	writers := partCtx.sidxPartContexts[sidxName].GetWriters()
	switch {
	case fileName == sidx.SidxPrimaryName:
		writers.SidxPrimaryWriter().MustWrite(chunk)
	case fileName == sidx.SidxDataName:
		writers.SidxDataWriter().MustWrite(chunk)
	case fileName == sidx.SidxKeysName:
		writers.SidxKeysWriter().MustWrite(chunk)
	case fileName == sidx.SidxMetaName:
		writers.SidxMetaWriter().MustWrite(chunk)
	case strings.HasPrefix(fileName, sidx.TagDataPrefix):
		tagName := fileName[len(sidx.TagDataPrefix):]
		_, tagDataWriter, _ := writers.GetTagWriters(tagName)
		tagDataWriter.MustWrite(chunk)
	case strings.HasPrefix(fileName, sidx.TagMetadataPrefix):
		tagName := fileName[len(sidx.TagMetadataPrefix):]
		tagMetadataWriter, _, _ := writers.GetTagWriters(tagName)
		tagMetadataWriter.MustWrite(chunk)
	case strings.HasPrefix(fileName, sidx.TagFilterPrefix):
		tagName := fileName[len(sidx.TagFilterPrefix):]
		_, _, tagFilterWriter := writers.GetTagWriters(tagName)
		tagFilterWriter.MustWrite(chunk)
	default:
		s.l.Warn().Str("fileName", fileName).Str("sidxName", sidxName).Msg("unknown sidx file type")
		return fmt.Errorf("unknown sidx file type: %s for sidx: %s", fileName, sidxName)
	}
	return nil
}

func (s *syncChunkCallback) handleTraceFileChunk(ctx *queue.ChunkedSyncPartContext, chunk []byte) error {
	fileName := ctx.FileName
	partCtx := ctx.Handler.(*syncPartContext)
	switch {
	case fileName == traceMetaName:
		partCtx.writers.metaWriter.MustWrite(chunk)
	case fileName == tracePrimaryName:
		partCtx.writers.primaryWriter.MustWrite(chunk)
	case fileName == traceSpansName:
		partCtx.writers.spanWriter.MustWrite(chunk)
	case fileName == traceIDFilterFilename:
		s.handleTraceIDFilterChunk(partCtx, chunk)
	case fileName == tagTypeFilename:
		s.handleTagTypeChunk(partCtx, chunk)
	case strings.HasPrefix(fileName, traceTagsPrefix):
		tagName := fileName[len(traceTagsPrefix):]
		_, tagWriter := partCtx.writers.getWriters(tagName)
		tagWriter.MustWrite(chunk)
	case strings.HasPrefix(fileName, traceTagMetadataPrefix):
		tagName := fileName[len(traceTagMetadataPrefix):]
		tagMetadataWriter, _ := partCtx.writers.getWriters(tagName)
		tagMetadataWriter.MustWrite(chunk)
	default:
		s.l.Warn().Str("fileName", fileName).Msg("unknown file type in chunked sync")
		return fmt.Errorf("unknown file type: %s", fileName)
	}
	return nil
}

func (s *syncChunkCallback) handleTraceIDFilterChunk(partCtx *syncPartContext, chunk []byte) {
	partCtx.traceIDFilterBuffer = append(partCtx.traceIDFilterBuffer, chunk...)
}

func (s *syncChunkCallback) handleTagTypeChunk(partCtx *syncPartContext, chunk []byte) {
	partCtx.tagTypeBuffer = append(partCtx.tagTypeBuffer, chunk...)
}
