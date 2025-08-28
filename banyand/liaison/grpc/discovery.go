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
	"fmt"
	"sync"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

var errNotExist = errors.New("the object doesn't exist")

type discoveryService struct {
	metadataRepo    metadata.Repo
	nodeRegistry    NodeRegistry
	groupRepo       *groupRepo
	entityRepo      *entityRepo
	shardingKeyRepo *shardingKeyRepo
	log             *logger.Logger
	kind            schema.Kind
}

func newDiscoveryService(kind schema.Kind, metadataRepo metadata.Repo, nodeRegistry NodeRegistry, gr *groupRepo) *discoveryService {
	er := &entityRepo{
		entitiesMap:     make(map[identity]partition.Locator),
		measureMap:      make(map[identity]*databasev1.Measure),
		traceMap:        make(map[identity]*databasev1.Trace),
		traceIDIndexMap: make(map[identity]int),
	}
	return newDiscoveryServiceWithEntityRepo(kind, metadataRepo, nodeRegistry, gr, er)
}

func newDiscoveryServiceWithEntityRepo(kind schema.Kind, metadataRepo metadata.Repo, nodeRegistry NodeRegistry, gr *groupRepo, er *entityRepo) *discoveryService {
	sr := &shardingKeyRepo{shardingKeysMap: make(map[identity]partition.Locator)}
	return &discoveryService{
		groupRepo:       gr,
		entityRepo:      er,
		shardingKeyRepo: sr,
		kind:            kind,
		metadataRepo:    metadataRepo,
		nodeRegistry:    nodeRegistry,
	}
}

func (ds *discoveryService) initialize() error {
	ds.metadataRepo.RegisterHandler("liaison", ds.kind, ds.entityRepo)
	if ds.kind == schema.KindMeasure {
		ds.metadataRepo.RegisterHandler("liaison", ds.kind, ds.shardingKeyRepo)
	}
	return nil
}

func (ds *discoveryService) SetLogger(log *logger.Logger) {
	ds.log = log
	ds.groupRepo.log = log
	ds.entityRepo.log = log
	ds.shardingKeyRepo.log = log
}

func (ds *discoveryService) navigate(metadata *commonv1.Metadata, tagFamilies []*modelv1.TagFamilyForWrite) (pbv1.EntityValues, common.ShardID, error) {
	shardNum, existed := ds.groupRepo.shardNum(metadata.Group)
	if !existed {
		return nil, common.ShardID(0), errors.Wrapf(errNotExist, "finding the shard num by: %v", metadata)
	}
	id := getID(metadata)
	entityLocator, existed := ds.entityRepo.getLocator(id)
	if !existed {
		return nil, common.ShardID(0), errors.Wrapf(errNotExist, "finding the entity locator by: %v", metadata)
	}
	entityValues, shardID, err := entityLocator.Locate(metadata.Name, tagFamilies, shardNum)
	if err != nil {
		return nil, common.ShardID(0), err
	}
	shardingKeyLocator, existed := ds.shardingKeyRepo.getLocator(id)
	if !existed {
		return entityValues, shardID, nil
	}
	_, shardID, err = shardingKeyLocator.Locate(metadata.Name, tagFamilies, shardNum)
	if err != nil {
		return nil, common.ShardID(0), err
	}
	return entityValues, shardID, nil
}

type identity struct {
	name  string
	group string
}

func (i identity) String() string {
	return fmt.Sprintf("%s/%s", i.group, i.name)
}

var _ schema.EventHandler = (*groupRepo)(nil)

type groupRepo struct {
	schema.UnimplementedOnInitHandler
	log          *logger.Logger
	resourceOpts map[string]*commonv1.ResourceOpts
	sync.RWMutex
}

func (s *groupRepo) OnAddOrUpdate(schemaMetadata schema.Metadata) {
	if schemaMetadata.Kind != schema.KindGroup {
		return
	}
	group := schemaMetadata.Spec.(*commonv1.Group)
	if group.ResourceOpts == nil || group.Catalog == commonv1.Catalog_CATALOG_UNSPECIFIED {
		return
	}
	if le := s.log.Debug(); le.Enabled() {
		le.Stringer("id", group.Metadata).Uint32("total", group.ResourceOpts.ShardNum).Msg("shard added or updated")
	}
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.resourceOpts[group.Metadata.GetName()] = group.ResourceOpts
}

func (s *groupRepo) OnDelete(schemaMetadata schema.Metadata) {
	if schemaMetadata.Kind != schema.KindGroup {
		return
	}
	group := schemaMetadata.Spec.(*commonv1.Group)
	if group.ResourceOpts == nil || group.Catalog == commonv1.Catalog_CATALOG_UNSPECIFIED {
		return
	}
	if le := s.log.Debug(); le.Enabled() {
		le.Stringer("id", group.Metadata).Msg("shard deletedTime")
	}
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	delete(s.resourceOpts, group.Metadata.GetName())
}

func (s *groupRepo) shardNum(groupName string) (uint32, bool) {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	r, ok := s.resourceOpts[groupName]
	if !ok {
		return 0, false
	}
	return r.ShardNum, true
}

func (s *groupRepo) copies(groupName string) (uint32, bool) {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	r, ok := s.resourceOpts[groupName]
	if !ok {
		return 0, false
	}
	return r.Replicas + 1, true
}

func getID(metadata *commonv1.Metadata) identity {
	return identity{
		name:  metadata.GetName(),
		group: metadata.GetGroup(),
	}
}

var _ schema.EventHandler = (*entityRepo)(nil)

type entityRepo struct {
	schema.UnimplementedOnInitHandler
	log             *logger.Logger
	entitiesMap     map[identity]partition.Locator
	measureMap      map[identity]*databasev1.Measure
	traceMap        map[identity]*databasev1.Trace
	traceIDIndexMap map[identity]int // Cache trace ID tag index
	sync.RWMutex
}

// OnAddOrUpdate implements schema.EventHandler.
func (e *entityRepo) OnAddOrUpdate(schemaMetadata schema.Metadata) {
	var l partition.Locator
	var id identity
	var modRevision int64
	if le := e.log.Debug(); le.Enabled() {
		var kind string
		switch schemaMetadata.Kind {
		case schema.KindMeasure:
			kind = "measure"
		case schema.KindStream:
			kind = "stream"
		case schema.KindTrace:
			kind = "trace"
		default:
			kind = "unknown"
		}
		le.
			Str("action", "add_or_update").
			Stringer("subject", id).
			Str("kind", kind).
			Msg("entity added or updated")
	}
	switch schemaMetadata.Kind {
	case schema.KindMeasure:
		measure := schemaMetadata.Spec.(*databasev1.Measure)
		modRevision = measure.GetMetadata().GetModRevision()
		l = partition.NewEntityLocator(measure.TagFamilies, measure.Entity, modRevision)
		id = getID(measure.GetMetadata())
	case schema.KindStream:
		stream := schemaMetadata.Spec.(*databasev1.Stream)
		modRevision = stream.GetMetadata().GetModRevision()
		l = partition.NewEntityLocator(stream.TagFamilies, stream.Entity, modRevision)
		id = getID(stream.GetMetadata())
	case schema.KindTrace:
		trace := schemaMetadata.Spec.(*databasev1.Trace)
		id = getID(trace.GetMetadata())
		e.traceMap[id] = trace
		// Pre-compute trace ID tag index
		traceIDTagName := trace.GetTraceIdTagName()
		traceIDIndex := -1
		for i, tagSpec := range trace.GetTags() {
			if tagSpec.GetName() == traceIDTagName {
				traceIDIndex = i
				break
			}
		}
		e.traceIDIndexMap[id] = traceIDIndex
		return
	default:
		return
	}

	e.RWMutex.Lock()
	defer e.RWMutex.Unlock()
	e.entitiesMap[id] = partition.Locator{TagLocators: l.TagLocators, ModRevision: modRevision}
	if schemaMetadata.Kind == schema.KindMeasure {
		measure := schemaMetadata.Spec.(*databasev1.Measure)
		e.measureMap[id] = measure
	} else {
		delete(e.measureMap, id) // Ensure measure is not stored for streams
	}
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
	case schema.KindTrace:
		trace := schemaMetadata.Spec.(*databasev1.Trace)
		id = getID(trace.GetMetadata())
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
		case schema.KindTrace:
			kind = "trace"
		default:
			kind = "unknown"
		}
		le.
			Str("action", "delete").
			Stringer("subject", id).
			Str("kind", kind).
			Msg("entity deletedTime")
	}
	e.RWMutex.Lock()
	defer e.RWMutex.Unlock()
	delete(e.entitiesMap, id)
	delete(e.measureMap, id)      // Clean up measure
	delete(e.traceMap, id)        // Clean up trace
	delete(e.traceIDIndexMap, id) // Clean up trace ID index
}

func (e *entityRepo) getLocator(id identity) (partition.Locator, bool) {
	e.RWMutex.RLock()
	defer e.RWMutex.RUnlock()
	el, ok := e.entitiesMap[id]
	if !ok {
		return partition.Locator{}, false
	}
	return el, true
}

func (e *entityRepo) getTrace(id identity) (*databasev1.Trace, bool) {
	e.RWMutex.RLock()
	defer e.RWMutex.RUnlock()
	trace, ok := e.traceMap[id]
	if !ok {
		return nil, false
	}
	return trace, true
}

func (e *entityRepo) getTraceIDIndex(id identity) (int, bool) {
	e.RWMutex.RLock()
	defer e.RWMutex.RUnlock()
	index, ok := e.traceIDIndexMap[id]
	if !ok {
		return -1, false
	}
	return index, true
}

var _ schema.EventHandler = (*shardingKeyRepo)(nil)

type shardingKeyRepo struct {
	schema.UnimplementedOnInitHandler
	log             *logger.Logger
	shardingKeysMap map[identity]partition.Locator
	sync.RWMutex
}

// OnAddOrUpdate implements schema.EventHandler.
func (s *shardingKeyRepo) OnAddOrUpdate(schemaMetadata schema.Metadata) {
	if schemaMetadata.Kind != schema.KindMeasure {
		return
	}
	measure := schemaMetadata.Spec.(*databasev1.Measure)
	shardingKey := measure.GetShardingKey()
	if shardingKey == nil || len(shardingKey.GetTagNames()) == 0 {
		return
	}
	l := partition.NewShardingKeyLocator(measure.TagFamilies, measure.ShardingKey)
	id := getID(measure.GetMetadata())
	if le := s.log.Debug(); le.Enabled() {
		le.
			Str("action", "add_or_update").
			Stringer("subject", id).
			Str("kind", "measure").
			Msg("sharding key added or updated")
	}
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.shardingKeysMap[id] = partition.Locator{TagLocators: l.TagLocators}
}

// OnDelete implements schema.EventHandler.
func (s *shardingKeyRepo) OnDelete(schemaMetadata schema.Metadata) {
	if schemaMetadata.Kind != schema.KindMeasure {
		return
	}
	measure := schemaMetadata.Spec.(*databasev1.Measure)
	shardingKey := measure.GetShardingKey()
	if shardingKey == nil || len(shardingKey.GetTagNames()) == 0 {
		return
	}
	id := getID(measure.GetMetadata())
	if le := s.log.Debug(); le.Enabled() {
		le.
			Str("action", "delete").
			Stringer("subject", id).
			Str("kind", "measure").
			Msg("sharding key deletedTime")
	}
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	delete(s.shardingKeysMap, id)
}

func (s *shardingKeyRepo) getLocator(id identity) (partition.Locator, bool) {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	sl, ok := s.shardingKeysMap[id]
	if !ok {
		return partition.Locator{}, false
	}
	return sl, true
}
