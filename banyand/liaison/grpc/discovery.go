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

package grpc

import (
	"sync"

	commonv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	databasev2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v2"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

type identity struct {
	name  string
	group string
}

type shardRepo struct {
	log            *logger.Logger
	shardEventsMap map[identity]uint32
	sync.RWMutex
}

func (s *shardRepo) Rev(message bus.Message) (resp bus.Message) {
	e, ok := message.Data().(*databasev2.ShardEvent)
	if !ok {
		s.log.Warn().Msg("invalid e data type")
		return
	}
	s.setShardNum(e)
	s.log.Info().
		Str("action", databasev2.Action_name[int32(e.Action)]).
		Uint64("shardID", e.Shard.Id).
		Msg("received a shard e")
	return
}

func (s *shardRepo) setShardNum(eventVal *databasev2.ShardEvent) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	idx := getID(eventVal.GetShard().GetMetadata())
	if eventVal.Action == databasev2.Action_ACTION_PUT {
		s.shardEventsMap[idx] = eventVal.Shard.Total
	} else if eventVal.Action == databasev2.Action_ACTION_DELETE {
		delete(s.shardEventsMap, idx)
	}
}

func (s *shardRepo) shardNum(idx identity) (uint32, bool) {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	sn, ok := s.shardEventsMap[idx]
	if !ok {
		return 0, false
	}
	return sn, true
}

func getID(metadata *commonv2.Metadata) identity {
	return identity{
		name:  metadata.GetName(),
		group: metadata.GetGroup(),
	}
}

type entityRepo struct {
	log         *logger.Logger
	entitiesMap map[identity]partition.EntityLocator
	sync.RWMutex
}

func (s *entityRepo) Rev(message bus.Message) (resp bus.Message) {
	e, ok := message.Data().(*databasev2.EntityEvent)
	if !ok {
		s.log.Warn().Msg("invalid e data type")
		return
	}
	id := getID(e.GetSubject())
	s.log.Info().
		Str("action", databasev2.Action_name[int32(e.Action)]).
		Interface("subject", id).
		Msg("received an entity event")
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	switch e.Action {
	case databasev2.Action_ACTION_PUT:
		en := make(partition.EntityLocator, 0, len(e.GetEntityLocator()))
		for _, l := range e.GetEntityLocator() {
			en = append(en, partition.TagLocator{
				FamilyOffset: int(l.FamilyOffset),
				TagOffset:    int(l.TagOffset),
			})
		}
		s.entitiesMap[id] = en
	case databasev2.Action_ACTION_DELETE:
		delete(s.entitiesMap, id)
	}
	return
}

func (s *entityRepo) getLocator(id identity) (partition.EntityLocator, bool) {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	el, ok := s.entitiesMap[id]
	if !ok {
		return nil, false
	}
	return el, true
}
