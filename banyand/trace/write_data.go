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
