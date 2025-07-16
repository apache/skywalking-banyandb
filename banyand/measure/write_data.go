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
	"context"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
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

type indexCallback struct {
	l          *logger.Logger
	schemaRepo *schemaRepo
	topic      bus.Topic
}

func setUpIndexCallback(l *logger.Logger, schemaRepo *schemaRepo, topic bus.Topic) bus.MessageListener {
	return &indexCallback{
		l:          l,
		schemaRepo: schemaRepo,
		topic:      topic,
	}
}

func (i *indexCallback) CheckHealth() *common.Error {
	return nil
}

func (i *indexCallback) Rev(_ context.Context, message bus.Message) (resp bus.Message) {
	msgData, ok := message.Data().([]byte)
	if !ok {
		i.l.Warn().Msg("invalid index insert message data type")
		return
	}

	// Decode group name (first part of the message)
	tail, groupBytes, err := encoding.DecodeBytes(msgData)
	if err != nil {
		i.l.Error().Err(err).Msg("failed to decode group name from index insert message")
		return
	}
	group := string(groupBytes)

	// Decode timestamp (uint64) - second part of the message
	if len(tail) < 8 {
		i.l.Error().Msg("insufficient data for timestamp in index insert message")
		return
	}
	timestamp := encoding.BytesToInt64(tail[:8])
	tail = tail[8:]

	// Unmarshal Documents from the remaining data
	var documents index.Documents
	if err = documents.Unmarshal(tail); err != nil {
		i.l.Error().Err(err).Msg("failed to unmarshal documents from index insert message")
		return
	}

	if len(documents) == 0 {
		i.l.Warn().Msg("empty documents in index insert message")
		return
	}

	// Get TSDB by group name using schemaRepo
	tsdb, err := i.schemaRepo.loadTSDB(group)
	if err != nil {
		i.l.Error().Err(err).Str("group", group).Msg("failed to load TSDB for group")
		return
	}

	// Use the timestamp to get the segment directly
	segmentTime := time.Unix(0, timestamp)
	segment, err := tsdb.CreateSegmentIfNotExist(segmentTime)
	if err != nil {
		i.l.Error().Err(err).Str("group", group).Time("segmentTime", segmentTime).Msg("failed to create segment")
		return
	}
	defer segment.DecRef()

	switch i.topic {
	case data.TopicMeasureSeriesIndexInsert:
		err = segment.IndexDB().Insert(documents)
	case data.TopicMeasureSeriesIndexUpdate:
		err = segment.IndexDB().Update(documents)
	}
	if err != nil {
		i.l.Error().Err(err).Str("group", group).Int("documentCount", len(documents)).Msg("failed to insert documents to index")
		return
	}

	if e := i.l.Debug(); e.Enabled() {
		e.
			Str("group", group).
			Int("documentCount", len(documents)).
			Time("segmentTime", segmentTime).
			Msg("successfully inserted documents to index")
	}

	return
}
