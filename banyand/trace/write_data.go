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
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type syncCallback struct {
	l          *logger.Logger
	schemaRepo *schemaRepo
}

func setUpSyncCallback(l *logger.Logger, schemaRepo *schemaRepo) bus.MessageListener {
	return &syncCallback{
		l:          l,
		schemaRepo: schemaRepo,
	}
}

func (s *syncCallback) CheckHealth() *common.Error {
	return nil
}

func (s *syncCallback) Rev(_ context.Context, message bus.Message) (resp bus.Message) {
	data, ok := message.Data().([]byte)
	if !ok {
		s.l.Warn().Msg("invalid sync message data type")
		return
	}

	// Decode group name
	tail, groupBytes, err := encoding.DecodeBytes(data)
	if err != nil {
		s.l.Error().Err(err).Msg("failed to decode group name from sync message")
		return
	}
	group := string(groupBytes)

	// Decode shardID (uint32)
	if len(tail) < 4 {
		s.l.Error().Msg("insufficient data for shardID in sync message")
		return
	}
	shardID := encoding.BytesToUint32(tail[:4])
	tail = tail[4:]

	// Decode memPart
	memPart := generateMemPart()

	if err = memPart.Unmarshal(tail); err != nil {
		s.l.Error().Err(err).Msg("failed to unmarshal memPart from sync message")
		return
	}

	// Get the group schema from schemaRepo by the string group
	tsdb, err := s.schemaRepo.loadTSDB(group)
	if err != nil {
		s.l.Error().Err(err).Str("group", group).Msg("failed to load TSDB for group")
		return
	}

	// Get the segment using memPart.partMetadata.MinTimestamp
	segmentTime := time.Unix(0, memPart.partMetadata.MinTimestamp)
	segment, err := tsdb.CreateSegmentIfNotExist(segmentTime)
	if err != nil {
		s.l.Error().Err(err).Str("group", group).Time("segmentTime", segmentTime).Msg("failed to create segment")
		return
	}
	defer segment.DecRef()

	// Get Shard from a segment using shardID
	tsTable, err := segment.CreateTSTableIfNotExist(common.ShardID(shardID))
	if err != nil {
		s.l.Error().Err(err).Str("group", group).Uint32("shardID", shardID).Msg("failed to create ts table")
		return
	}

	tsdb.Tick(memPart.partMetadata.MaxTimestamp)
	// Use tsTable.mustAddMemPart to introduce memPart to tsTable
	tsTable.mustAddMemPart(memPart)

	if e := s.l.Debug(); e.Enabled() {
		e.
			Str("group", group).
			Uint32("shardID", shardID).
			Uint64("partID", memPart.partMetadata.ID).
			Msg("successfully stored memPart to tsTable")
	}

	return
}

type seriesIndexCallback struct {
	l          *logger.Logger
	schemaRepo *schemaRepo
}

func setUpSeriesIndexCallback(l *logger.Logger, schemaRepo *schemaRepo) bus.MessageListener {
	return &seriesIndexCallback{
		l:          l,
		schemaRepo: schemaRepo,
	}
}

func (s *seriesIndexCallback) CheckHealth() *common.Error {
	return nil
}

func (s *seriesIndexCallback) Rev(_ context.Context, message bus.Message) (resp bus.Message) {
	data, ok := message.Data().([]byte)
	if !ok {
		s.l.Warn().Msg("invalid series index message data type")
		return
	}

	// Decode group name (first part of the message)
	tail, groupBytes, err := encoding.DecodeBytes(data)
	if err != nil {
		s.l.Error().Err(err).Msg("failed to decode group name from series index message")
		return
	}
	group := string(groupBytes)

	// Decode timestamp (uint64) - second part of the message
	if len(tail) < 8 {
		s.l.Error().Msg("insufficient data for timestamp in series index message")
		return
	}
	timestamp := encoding.BytesToInt64(tail[:8])
	tail = tail[8:]

	// Unmarshal Documents from the remaining data
	var documents index.Documents
	if err = documents.Unmarshal(tail); err != nil {
		s.l.Error().Err(err).Msg("failed to unmarshal documents from series index message")
		return
	}

	if len(documents) == 0 {
		s.l.Warn().Msg("empty documents in series index message")
		return
	}

	// Get TSDB by group name using schemaRepo
	tsdb, err := s.schemaRepo.loadTSDB(group)
	if err != nil {
		s.l.Error().Err(err).Str("group", group).Msg("failed to load TSDB for group")
		return
	}

	// Use the timestamp to get the segment directly
	segmentTime := time.Unix(0, timestamp)
	segment, err := tsdb.CreateSegmentIfNotExist(segmentTime)
	if err != nil {
		s.l.Error().Err(err).Str("group", group).Time("segmentTime", segmentTime).Msg("failed to create segment")
		return
	}
	defer segment.DecRef()

	// Insert all documents into the segment's index
	if err := segment.IndexDB().Insert(documents); err != nil {
		s.l.Error().Err(err).Str("group", group).Int("documentCount", len(documents)).Msg("failed to insert documents to index")
		return
	}

	s.l.Info().
		Str("group", group).
		Int("documentCount", len(documents)).
		Time("segmentTime", segmentTime).
		Msg("successfully inserted documents to series index")

	return
}

type localIndexCallback struct {
	l          *logger.Logger
	schemaRepo *schemaRepo
}

func setUpLocalIndexCallback(l *logger.Logger, schemaRepo *schemaRepo) bus.MessageListener {
	return &localIndexCallback{
		l:          l,
		schemaRepo: schemaRepo,
	}
}

func (l *localIndexCallback) CheckHealth() *common.Error {
	return nil
}

func (l *localIndexCallback) Rev(_ context.Context, message bus.Message) (resp bus.Message) {
	data, ok := message.Data().([]byte)
	if !ok {
		l.l.Warn().Msg("invalid local index message data type")
		return
	}

	// Decode group name (first part of the message)
	tail, groupBytes, err := encoding.DecodeBytes(data)
	if err != nil {
		l.l.Error().Err(err).Msg("failed to decode group name from local index message")
		return
	}
	group := string(groupBytes)

	// Decode shardID (uint32)
	if len(tail) < 4 {
		l.l.Error().Msg("insufficient data for shardID in local index message")
		return
	}
	shardID := encoding.BytesToUint32(tail[:4])
	tail = tail[4:]

	// Unmarshal Documents from the remaining data
	var documents index.Documents
	if err = documents.Unmarshal(tail); err != nil {
		l.l.Error().Err(err).Msg("failed to unmarshal documents from local index message")
		return
	}

	if len(documents) == 0 {
		l.l.Warn().Msg("empty documents in local index message")
		return
	}

	// Get TSDB by group name using schemaRepo
	tsdb, err := l.schemaRepo.loadTSDB(group)
	if err != nil {
		l.l.Error().Err(err).Str("group", group).Msg("failed to load TSDB for group")
		return
	}

	// Use the first document's timestamp to get the segment
	firstDocTime := time.Unix(0, documents[0].Timestamp)
	segment, err := tsdb.CreateSegmentIfNotExist(firstDocTime)
	if err != nil {
		l.l.Error().Err(err).Str("group", group).Time("docTime", firstDocTime).Msg("failed to create segment for documents")
		return
	}
	defer segment.DecRef()

	// Get the tsTable using shardID
	_, err = segment.CreateTSTableIfNotExist(common.ShardID(shardID))
	if err != nil {
		l.l.Error().Err(err).Str("group", group).Uint32("shardID", shardID).Msg("failed to create ts table")
		return
	}
	return
}
