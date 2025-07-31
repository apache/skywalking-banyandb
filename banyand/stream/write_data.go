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

package stream

import (
	"fmt"
	"strings"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type syncPartContext struct {
	tsTable *tsTable
	writers *writers
	memPart *memPart
}

func (s *syncPartContext) FinishSync() error {
	s.tsTable.mustAddMemPart(s.memPart)
	return s.Close()
}

func (s *syncPartContext) Close() error {
	s.writers.MustClose()
	releaseWriters(s.writers)
	s.writers = nil
	s.memPart = nil
	s.tsTable = nil
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
	memPart := generateMemPart()
	memPart.partMetadata.CompressedSizeBytes = ctx.CompressedSizeBytes
	memPart.partMetadata.UncompressedSizeBytes = ctx.UncompressedSizeBytes
	memPart.partMetadata.TotalCount = ctx.TotalCount
	memPart.partMetadata.BlocksCount = ctx.BlocksCount
	memPart.partMetadata.MinTimestamp = ctx.MinTimestamp
	memPart.partMetadata.MaxTimestamp = ctx.MaxTimestamp
	writers := generateWriters()
	writers.mustInitForMemPart(memPart)
	return &syncPartContext{
		tsTable: tsTable,
		writers: writers,
		memPart: memPart,
	}, nil
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
	case fileName == streamMetaName:
		partCtx.writers.metaWriter.MustWrite(chunk)
	case fileName == streamPrimaryName:
		partCtx.writers.primaryWriter.MustWrite(chunk)
	case fileName == streamTimestampsName:
		partCtx.writers.timestampsWriter.MustWrite(chunk)
	case strings.HasPrefix(fileName, streamTagFamiliesPrefix):
		tagName := fileName[len(streamTagFamiliesPrefix):]
		_, tagWriter, _ := partCtx.writers.getWriters(tagName)
		tagWriter.MustWrite(chunk)
	case strings.HasPrefix(fileName, streamTagMetadataPrefix):
		tagName := fileName[len(streamTagMetadataPrefix):]
		tagMetadataWriter, _, _ := partCtx.writers.getWriters(tagName)
		tagMetadataWriter.MustWrite(chunk)
	case strings.HasPrefix(fileName, streamTagFilterPrefix):
		tagName := fileName[len(streamTagFilterPrefix):]
		_, _, tagFilterWriter := partCtx.writers.getWriters(tagName)
		tagFilterWriter.MustWrite(chunk)
	default:
		s.l.Warn().Str("fileName", fileName).Msg("unknown file type in chunked sync")
		return fmt.Errorf("unknown file type: %s", fileName)
	}

	return nil
}

type syncSeriesContext struct {
	streamer index.ExternalSegmentStreamer
}

func (s *syncSeriesContext) FinishSync() error {
	if s.streamer != nil {
		return s.streamer.CompleteSegment()
	}
	return nil
}

func (s *syncSeriesContext) Close() error {
	s.streamer = nil
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

	indexDB := segment.IndexDB()
	streamer, err := indexDB.EnableExternalSegments()
	if err != nil {
		s.l.Error().Err(err).Str("group", ctx.Group).Msg("failed to enable external segments")
		return nil, err
	}

	if err := streamer.StartSegment(); err != nil {
		s.l.Error().Err(err).Str("group", ctx.Group).Msg("failed to start external segment")
		return nil, err
	}

	s.l.Debug().Str("group", ctx.Group).Uint32("shardID", ctx.ShardID).Msg("created series sync context")
	return &syncSeriesContext{
		streamer: streamer,
	}, nil
}

// HandleFileChunk implements queue.ChunkedSyncHandler for streaming series index chunks.
func (s *syncSeriesCallback) HandleFileChunk(ctx *queue.ChunkedSyncPartContext, chunk []byte) error {
	if ctx.Handler == nil {
		return fmt.Errorf("part handler is nil")
	}
	seriesCtx := ctx.Handler.(*syncSeriesContext)

	if seriesCtx.streamer == nil {
		return fmt.Errorf("external segment streamer is nil")
	}

	err := seriesCtx.streamer.WriteChunk(chunk)
	if err != nil {
		s.l.Error().Err(err).
			Str("group", ctx.Group).
			Int("chunk_size", len(chunk)).
			Msg("failed to write chunk to external segment streamer")
		return err
	}

	s.l.Debug().
		Str("group", ctx.Group).
		Int("chunk_size", len(chunk)).
		Uint64("bytes_received", seriesCtx.streamer.BytesReceived()).
		Msg("wrote chunk to external segment streamer")

	return nil
}
