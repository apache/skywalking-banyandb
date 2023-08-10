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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

var errNotExist = errors.New("the object doesn't exist")

type discoveryService struct {
	pipeline     queue.Queue
	metadataRepo metadata.Repo
	shardRepo    *shardRepo
	entityRepo   *entityRepo
	log          *logger.Logger
	kind         schema.Kind
}

func newDiscoveryService(pipeline queue.Queue, kind schema.Kind, metadataRepo metadata.Repo) *discoveryService {
	sr := &shardRepo{shardEventsMap: make(map[identity]uint32)}
	er := &entityRepo{entitiesMap: make(map[identity]partition.EntityLocator)}
	return &discoveryService{
		shardRepo:    sr,
		entityRepo:   er,
		pipeline:     pipeline,
		kind:         kind,
		metadataRepo: metadataRepo,
	}
}

func (ds *discoveryService) initialize(ctx context.Context) error {
	ctxLocal, cancel := context.WithTimeout(ctx, 5*time.Second)
	groups, err := ds.metadataRepo.GroupRegistry().ListGroup(ctxLocal)
	cancel()
	if err != nil {
		return err
	}
	for _, g := range groups {
		switch ds.kind {
		case schema.KindMeasure:
		case schema.KindStream:
		default:
			continue
		}
		ctxLocal, cancel := context.WithTimeout(ctx, 5*time.Second)
		shards, innerErr := ds.metadataRepo.ShardRegistry().ListShard(ctxLocal, schema.ListOpt{Group: g.Metadata.Name})
		cancel()
		if innerErr != nil {
			return innerErr
		}
		for _, s := range shards {
			ds.shardRepo.OnAddOrUpdate(schema.Metadata{
				TypeMeta: schema.TypeMeta{
					Kind:  schema.KindShard,
					Name:  s.Metadata.Name,
					Group: s.Metadata.Group,
				},
				Spec: s,
			})
		}

		switch ds.kind {
		case schema.KindMeasure:
			ctxLocal, cancel = context.WithTimeout(ctx, 5*time.Second)
			mm, innerErr := ds.metadataRepo.MeasureRegistry().ListMeasure(ctxLocal, schema.ListOpt{Group: g.Metadata.Name})
			cancel()
			if innerErr != nil {
				return innerErr
			}
			for _, m := range mm {
				ds.entityRepo.OnAddOrUpdate(schema.Metadata{
					TypeMeta: schema.TypeMeta{
						Kind:  schema.KindMeasure,
						Name:  m.Metadata.Name,
						Group: m.Metadata.Group,
					},
					Spec: m,
				})
			}
		case schema.KindStream:
			ctxLocal, cancel = context.WithTimeout(ctx, 5*time.Second)
			ss, innerErr := ds.metadataRepo.StreamRegistry().ListStream(ctxLocal, schema.ListOpt{Group: g.Metadata.Name})
			cancel()
			if innerErr != nil {
				return innerErr
			}
			for _, s := range ss {
				ds.entityRepo.OnAddOrUpdate(schema.Metadata{
					TypeMeta: schema.TypeMeta{
						Kind:  schema.KindStream,
						Name:  s.Metadata.Name,
						Group: s.Metadata.Group,
					},
					Spec: s,
				})
			}
		default:
			return fmt.Errorf("unsupported kind: %d", ds.kind)
		}
	}
	ds.metadataRepo.RegisterHandler(schema.KindShard, ds.shardRepo)
	ds.metadataRepo.RegisterHandler(ds.kind, ds.entityRepo)
	return nil
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

func (i identity) String() string {
	return fmt.Sprintf("%s/%s", i.group, i.name)
}

var _ schema.EventHandler = (*shardRepo)(nil)

type shardRepo struct {
	log            *logger.Logger
	shardEventsMap map[identity]uint32
	sync.RWMutex
}

// OnAddOrUpdate implements schema.EventHandler.
func (s *shardRepo) OnAddOrUpdate(schemaMetadata schema.Metadata) {
	if schemaMetadata.Kind != schema.KindShard {
		return
	}
	shard := schemaMetadata.Spec.(*databasev1.Shard)
	idx := getID(shard.GetMetadata())
	if le := s.log.Debug(); le.Enabled() {
		le.Stringer("id", idx).Uint32("total", shard.Total).Msg("shard added or updated")
	}
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.shardEventsMap[idx] = shard.Total
}

// OnDelete implements schema.EventHandler.
func (s *shardRepo) OnDelete(schemaMetadata schema.Metadata) {
	if schemaMetadata.Kind != schema.KindShard {
		return
	}
	shard := schemaMetadata.Spec.(*databasev1.Shard)
	idx := getID(shard.GetMetadata())
	if le := s.log.Debug(); le.Enabled() {
		le.Stringer("id", idx).Msg("shard deleted")
	}
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	delete(s.shardEventsMap, idx)
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

var _ schema.EventHandler = (*entityRepo)(nil)

type entityRepo struct {
	log         *logger.Logger
	entitiesMap map[identity]partition.EntityLocator
	sync.RWMutex
}

// OnAddOrUpdate implements schema.EventHandler.
func (e *entityRepo) OnAddOrUpdate(schemaMetadata schema.Metadata) {
	var el partition.EntityLocator
	var id identity
	switch schemaMetadata.Kind {
	case schema.KindMeasure:
		measure := schemaMetadata.Spec.(*databasev1.Measure)
		el = partition.NewEntityLocator(measure.TagFamilies, measure.Entity)
		id = getID(measure.GetMetadata())
	case schema.KindStream:
		stream := schemaMetadata.Spec.(*databasev1.Stream)
		el = partition.NewEntityLocator(stream.TagFamilies, stream.Entity)
		id = getID(stream.GetMetadata())
	default:
		return
	}
	if le := e.log.Debug(); le.Enabled() {
		var kind string
		switch schemaMetadata.Kind {
		case schema.KindMeasure:
			kind = "measure"
		case schema.KindStream:
			kind = "stream"
		default:
			kind = "unknown"
		}
		le.
			Str("action", "add_or_update").
			Stringer("subject", id).
			Str("kind", kind).
			Msg("entity added or updated")
	}
	en := make(partition.EntityLocator, 0, len(el))
	for _, l := range el {
		en = append(en, partition.TagLocator{
			FamilyOffset: l.FamilyOffset,
			TagOffset:    l.TagOffset,
		})
	}
	e.RWMutex.Lock()
	defer e.RWMutex.Unlock()
	e.entitiesMap[id] = en
}

// OnDelete implements schema.EventHandler.
func (e *entityRepo) OnDelete(schemaMetadata schema.Metadata) {
	var id identity
	switch schemaMetadata.Kind {
	case schema.KindMeasure:
		measure := schemaMetadata.Spec.(*databasev1.Measure)
		id = getID(measure.GetMetadata())
	case schema.KindStream:
		stream := schemaMetadata.Spec.(*databasev1.Stream)
		id = getID(stream.GetMetadata())
	default:
		return
	}
	if le := e.log.Debug(); le.Enabled() {
		var kind string
		switch schemaMetadata.Kind {
		case schema.KindMeasure:
			kind = "measure"
		case schema.KindStream:
			kind = "stream"
		default:
			kind = "unknown"
		}
		le.
			Str("action", "delete").
			Stringer("subject", id).
			Str("kind", kind).
			Msg("entity deleted")
	}
	e.RWMutex.Lock()
	defer e.RWMutex.Unlock()
	delete(e.entitiesMap, id)
}

func (e *entityRepo) getLocator(id identity) (partition.EntityLocator, bool) {
	e.RWMutex.RLock()
	defer e.RWMutex.RUnlock()
	el, ok := e.entitiesMap[id]
	if !ok {
		return nil, false
	}
	return el, true
}
