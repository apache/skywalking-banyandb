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

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

var errNotExist = errors.New("the object doesn't exist")

type discoveryService struct {
	shardRepo  *shardRepo
	entityRepo *entityRepo
	pipeline   queue.Queue
	log        *logger.Logger
}

func newDiscoveryService(pipeline queue.Queue) *discoveryService {
	return &discoveryService{
		shardRepo:  &shardRepo{shardEventsMap: make(map[identity]uint32)},
		entityRepo: &entityRepo{entitiesMap: make(map[identity]partition.EntityLocator)},
		pipeline:   pipeline,
	}
}

func (ds *discoveryService) SetLogger(log *logger.Logger) {
	ds.log = log
	ds.shardRepo.log = log
	ds.entityRepo.log = log
}

func (ds *discoveryService) navigate(metadata *commonv1.Metadata, tagFamilies []*modelv1.TagFamilyForWrite) (tsdb.Entity, tsdb.EntityValues, common.ShardID, error) {
	shardNum, existed := ds.shardRepo.shardNum(getID(&commonv1.Metadata{
		Name: metadata.Group,
	}))
	if !existed {
		return nil, nil, common.ShardID(0), errors.Wrapf(errNotExist, "finding the shard num by: %v", metadata)
	}
	locator, existed := ds.entityRepo.getLocator(getID(metadata))
	if !existed {
		return nil, nil, common.ShardID(0), errors.Wrapf(errNotExist, "finding the locator by: %v", metadata)
	}
	return locator.Locate(metadata.Name, tagFamilies, shardNum)
}

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
	e, ok := message.Data().(*databasev1.ShardEvent)
	if !ok {
		s.log.Warn().Msg("invalid e data type")
		return
	}
	s.setShardNum(e)

	if le := s.log.Debug(); le.Enabled() {
		le.
			Str("action", databasev1.Action_name[int32(e.Action)]).
			Uint64("shardID", e.Shard.Id).
			Msg("received a shard e")
	}
	return
}

func (s *shardRepo) setShardNum(eventVal *databasev1.ShardEvent) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	idx := getID(eventVal.GetShard().GetMetadata())
	if eventVal.Action == databasev1.Action_ACTION_PUT {
		s.shardEventsMap[idx] = eventVal.Shard.Total
	} else if eventVal.Action == databasev1.Action_ACTION_DELETE {
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

func getID(metadata *commonv1.Metadata) identity {
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
	e, ok := message.Data().(*databasev1.EntityEvent)
	if !ok {
		s.log.Warn().Msg("invalid e data type")
		return
	}
	id := getID(e.GetSubject())
	if le := s.log.Debug(); le.Enabled() {
		le.
			Str("action", databasev1.Action_name[int32(e.Action)]).
			Interface("subject", id).
			Msg("received an entity event")
	}
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	switch e.Action {
	case databasev1.Action_ACTION_PUT:
		en := make(partition.EntityLocator, 0, len(e.GetEntityLocator()))
		for _, l := range e.GetEntityLocator() {
			en = append(en, partition.TagLocator{
				FamilyOffset: int(l.FamilyOffset),
				TagOffset:    int(l.TagOffset),
			})
		}
		s.entitiesMap[id] = en
	case databasev1.Action_ACTION_DELETE:
		delete(s.entitiesMap, id)
	case databasev1.Action_ACTION_UNSPECIFIED:
		s.log.Warn().RawJSON("event", logger.Proto(e)).Msg("ignored unspecified event")
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
