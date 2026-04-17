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

package measure

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

type syncPartContext struct {
	tsTable    *tsTable
	fileSystem fs.FileSystem
	writers    *writers
	partPath   string
	partMeta   partMetadata
	partID     uint64
}

func (s *syncPartContext) NewPartType(_ *queue.ChunkedSyncPartContext) error {
	logger.Panicf("new part type is not supported for measure")
	return nil
}

func (s *syncPartContext) FinishSync() error {
	// Close writers first so file data is flushed before we write metadata.
	s.releaseCoreWriters()

	s.partMeta.mustWriteMetadata(s.fileSystem, s.partPath)
	s.fileSystem.SyncPath(s.partPath)

	s.tsTable.mustAddFilePart(s.partID)
	s.partPath = ""
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
	s.tsTable = nil
	s.fileSystem = nil
	return nil
}

type syncCallback struct {
	l          *logger.Logger
	schemaRepo *schemaRepo
}

func setUpChunkedSyncCallback(l *logger.Logger, schemaRepo *schemaRepo) queue.ChunkedSyncHandler {
	return &syncCallback{
		l:          l,
		schemaRepo: schemaRepo,
	}
}

func (s *syncCallback) CheckHealth() *common.Error {
	return nil
}

// CreatePartHandler implements queue.ChunkedSyncHandler.
func (s *syncCallback) CreatePartHandler(ctx *queue.ChunkedSyncPartContext) (queue.PartHandler, error) {
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
	defer segment.DecRef()
	tsTable, err := segment.CreateTSTableIfNotExist(common.ShardID(ctx.ShardID))
	if err != nil {
		s.l.Error().Err(err).Str("group", ctx.Group).Uint32("shardID", ctx.ShardID).Msg("failed to create ts table")
		return nil, err
	}

	tsdb.Tick(ctx.MaxTimestamp)

	partID := atomic.AddUint64(&tsTable.curPartID, 1)
	pp := partPath(tsTable.root, partID)
	fileSystem := tsTable.fileSystem

	w := generateWriters()
	w.mustInitForFilePart(fileSystem, pp, tsTable.pm.ShouldCache(int64(ctx.CompressedSizeBytes)))

	partCtx := &syncPartContext{
		tsTable:    tsTable,
		fileSystem: fileSystem,
		writers:    w,
		partPath:   pp,
		partID:     partID,
	}
	partCtx.partMeta.fillFromSyncContext(ctx)
	return partCtx, nil
}

// HandleFileChunk implements queue.ChunkedSyncHandler for streaming file chunks.
func (s *syncCallback) HandleFileChunk(ctx *queue.ChunkedSyncPartContext, chunk []byte) error {
	if ctx.Handler == nil {
		return fmt.Errorf("part handler is nil")
	}
	partCtx := ctx.Handler.(*syncPartContext)

	// Select the appropriate writer based on the filename and write the chunk.
	fileName := ctx.FileName
	switch {
	case fileName == measureMetaName:
		partCtx.writers.metaWriter.MustWrite(chunk)
	case fileName == measurePrimaryName:
		partCtx.writers.primaryWriter.MustWrite(chunk)
	case fileName == measureTimestampsName:
		partCtx.writers.timestampsWriter.MustWrite(chunk)
	case fileName == measureFieldValuesName:
		partCtx.writers.fieldValuesWriter.MustWrite(chunk)
	case strings.HasPrefix(fileName, measureTagFamiliesPrefix):
		tagName := fileName[len(measureTagFamiliesPrefix):]
		_, tagWriter := partCtx.writers.getColumnMetadataWriterAndColumnWriter(tagName)
		tagWriter.MustWrite(chunk)
	case strings.HasPrefix(fileName, measureTagMetadataPrefix):
		tagName := fileName[len(measureTagMetadataPrefix):]
		tagMetadataWriter, _ := partCtx.writers.getColumnMetadataWriterAndColumnWriter(tagName)
		tagMetadataWriter.MustWrite(chunk)
	default:
		s.l.Warn().Str("fileName", fileName).Msg("unknown file type in chunked sync")
		return fmt.Errorf("unknown file type: %s", fileName)
	}

	return nil
}

// HandlePartComplete implements queue.ChunkedSyncHandler for part completion.
func (s *syncCallback) HandlePartComplete(ctx *queue.ChunkedSyncPartContext) error {
	if ctx.Handler == nil {
		return fmt.Errorf("part handler is nil")
	}
	partCtx := ctx.Handler.(*syncPartContext)
	return partCtx.FinishSync()
}

func generateWriters() *writers {
	v := writersPool.Get()
	if v == nil {
		return &writers{
			tagFamilyMetadataWriters: make(map[string]*writer),
			tagFamilyWriters:         make(map[string]*writer),
		}
	}
	return v
}

func releaseWriters(sw *writers) {
	sw.reset()
	writersPool.Put(sw)
}

var writersPool = pool.Register[*writers]("measure-writers")

type syncSeriesContext struct {
	streamer index.ExternalSegmentStreamer
	segment  storage.Segment[*tsTable, *commonv1.ResourceOpts]
	l        *logger.Logger
	fileName string
}

func (s *syncSeriesContext) NewPartType(_ *queue.ChunkedSyncPartContext) error {
	logger.Panicf("new part type is not supported for measure")
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

func setUpSyncSeriesCallback(l *logger.Logger, schemaRepo *schemaRepo) queue.ChunkedSyncHandler {
	return &syncSeriesCallback{
		l:          l,
		schemaRepo: schemaRepo,
	}
}

func (s *syncSeriesCallback) CheckHealth() *common.Error {
	return nil
}

// CreatePartHandler implements queue.ChunkedSyncHandler for series index synchronization.
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
	return seriesCtx.streamer.WriteChunk(chunk)
}
