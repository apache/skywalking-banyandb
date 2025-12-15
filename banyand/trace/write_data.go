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
	"strings"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type syncPartContext struct {
	tsTable             *tsTable
	l                   *logger.Logger
	writers             *writers
	memPart             *memPart
	sidxPartContexts    map[string]*sidx.SyncPartContext
	traceIDFilterBuffer []byte
	tagTypeBuffer       []byte
}

func (s *syncPartContext) NewPartType(ctx *queue.ChunkedSyncPartContext) error {
	if ctx.PartType != PartTypeCore {
		sidxPartContext := sidx.NewSyncPartContext()
		memPart := sidx.GenerateMemPart()
		memPart.SetPartMetadata(ctx.CompressedSizeBytes, ctx.UncompressedSizeBytes, ctx.TotalCount, ctx.BlocksCount, ctx.MinKey, ctx.MaxKey, ctx.ID)
		writers := sidx.GenerateWriters()
		writers.MustInitForMemPart(memPart)
		sidxPartContext.Set(ctx.PartType, memPart, writers)
		if s.sidxPartContexts == nil {
			s.sidxPartContexts = make(map[string]*sidx.SyncPartContext)
		}
		s.sidxPartContexts[ctx.PartType] = sidxPartContext
		return nil
	}

	memPart := generateMemPart()
	memPart.partMetadata.CompressedSizeBytes = ctx.CompressedSizeBytes
	memPart.partMetadata.UncompressedSpanSizeBytes = ctx.UncompressedSizeBytes
	memPart.partMetadata.TotalCount = ctx.TotalCount
	memPart.partMetadata.BlocksCount = ctx.BlocksCount
	memPart.partMetadata.MinTimestamp = ctx.MinTimestamp
	memPart.partMetadata.MaxTimestamp = ctx.MaxTimestamp
	memPart.partMetadata.ID = ctx.ID
	writers := generateWriters()
	writers.mustInitForMemPart(memPart)
	s.writers = writers
	s.memPart = memPart
	return nil
}

func (s *syncPartContext) FinishSync() error {
	if len(s.traceIDFilterBuffer) > 0 && s.memPart != nil {
		bf := generateTraceIDBloomFilter()
		s.memPart.traceIDFilter.filter = decodeBloomFilter(s.traceIDFilterBuffer, bf)
	}
	if len(s.tagTypeBuffer) > 0 && s.memPart != nil {
		if s.memPart.tagType == nil {
			s.memPart.tagType = make(tagType)
		}
		if err := s.memPart.tagType.unmarshal(s.tagTypeBuffer); err != nil {
			s.l.Error().Err(err).Msg("failed to unmarshal tag type data")
			return err
		}
	}

	if s.memPart != nil {
		sidxPartContexts := make(map[string]*sidx.MemPart, len(s.sidxPartContexts))
		for _, sidxPartContext := range s.sidxPartContexts {
			sidxPartContexts[sidxPartContext.Name()] = sidxPartContext.GetMemPart()
		}
		s.tsTable.mustAddMemPart(s.memPart, sidxPartContexts)
	}
	return s.Close()
}

func (s *syncPartContext) Close() error {
	if s.writers != nil {
		s.writers.MustClose()
		releaseWriters(s.writers)
		s.writers = nil
	}
	if s.memPart != nil {
		s.memPart = nil
	}
	if s.sidxPartContexts != nil {
		for _, sidxPartContext := range s.sidxPartContexts {
			sidxPartContext.Close()
		}
		s.sidxPartContexts = nil
	}
	s.tsTable = nil
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
	partCtx := &syncPartContext{
		tsTable: tsTable,
		l:       s.l,
	}
	return partCtx, partCtx.NewPartType(ctx)
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
		if partCtx.memPart != nil {
			s.handleTraceIDFilterChunk(partCtx, chunk)
		}
	case fileName == tagTypeFilename:
		if partCtx.memPart != nil {
			s.handleTagTypeChunk(partCtx, chunk)
		}
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
