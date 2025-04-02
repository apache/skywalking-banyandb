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

func newDiscoveryService(kind schema.Kind, metadataRepo metadata.Repo, nodeRegistry NodeRegistry) *discoveryService {
	gr := &groupRepo{resourceOpts: make(map[string]*commonv1.ResourceOpts)}
	er := &entityRepo{entitiesMap: make(map[identity]partition.EntityLocator)}
	sr := &shardingKeyRepo{shardingKeysMap: make(map[identity]partition.ShardingKeyLocator)}
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
	ds.metadataRepo.RegisterHandler("liaison", schema.KindGroup, ds.groupRepo)
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

func (ds *discoveryService) navigate(metadata *commonv1.Metadata, tagFamilies []*modelv1.TagFamilyForWrite) (pbv1.Entity, pbv1.EntityValues, common.ShardID, error) {
	shardNum, existed := ds.groupRepo.shardNum(metadata.Group)
	if !existed {
		return nil, nil, common.ShardID(0), errors.Wrapf(errNotExist, "finding the shard num by: %v", metadata)
	}
	entityLocator, existed := ds.entityRepo.getLocator(getID(metadata))
	if !existed {
		return nil, nil, common.ShardID(0), errors.Wrapf(errNotExist, "finding the entity locator by: %v", metadata)
	}
	entity, entityValues, shardID, err := entityLocator.Locate(metadata.Name, tagFamilies, shardNum)
	if err != nil {
		return nil, nil, common.ShardID(0), err
	}

	topNAggregations, err := ds.metadataRepo.TopNAggregationRegistry().ListTopNAggregation(context.TODO(), schema.ListOpt{Group: metadata.Group})
	if err != nil {
		return nil, nil, common.ShardID(0), err
	}
	isTopNSourceMeaure := func(topNAggregations []*databasev1.TopNAggregation, measureName string) bool {
		for _, topNAggregation := range topNAggregations {
			if measureName == topNAggregation.SourceMeasure.Name {
				return true
			}
		}
		return false
	}
	if isTopNSourceMeaure(topNAggregations, metadata.Name) {
		shardingKeyLocator, existed := ds.shardingKeyRepo.getLocator(getID(metadata))
		if !existed {
			return nil, nil, common.ShardID(0), errors.Wrapf(errNotExist, "finding the sharding key locator by: %v", metadata)
		}
		if shardingKeyLocator.IsEmpty() {
			return entity, entityValues, shardID, nil
		}
		shardID, err = shardingKeyLocator.Locate(metadata.Name, tagFamilies, shardNum)
		if err != nil {
			return nil, nil, common.ShardID(0), err
		}
	}
	return entity, entityValues, shardID, nil
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
		le.Stringer("id", group.Metadata).Msg("shard deleted")
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

func (s *groupRepo) getNodeSelector(groupName string) (string, bool) {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	r, ok := s.resourceOpts[groupName]
	if !ok {
		return "", false
	}
	return r.DefaultNodeSelector, true
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
	log         *logger.Logger
	entitiesMap map[identity]partition.EntityLocator
	sync.RWMutex
}

// OnAddOrUpdate implements schema.EventHandler.
func (e *entityRepo) OnAddOrUpdate(schemaMetadata schema.Metadata) {
	var el partition.EntityLocator
	var id identity
	var modRevision int64
	switch schemaMetadata.Kind {
	case schema.KindMeasure:
		measure := schemaMetadata.Spec.(*databasev1.Measure)
		modRevision = measure.GetMetadata().GetModRevision()
		el = partition.NewEntityLocator(measure.TagFamilies, measure.Entity, modRevision)
		id = getID(measure.GetMetadata())
	case schema.KindStream:
		stream := schemaMetadata.Spec.(*databasev1.Stream)
		modRevision = stream.GetMetadata().GetModRevision()
		el = partition.NewEntityLocator(stream.TagFamilies, stream.Entity, modRevision)
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
	en := make([]partition.TagLocator, 0, len(el.TagLocators))
	for _, l := range el.TagLocators {
		en = append(en, partition.TagLocator{
			FamilyOffset: l.FamilyOffset,
			TagOffset:    l.TagOffset,
		})
	}
	e.RWMutex.Lock()
	defer e.RWMutex.Unlock()
	e.entitiesMap[id] = partition.EntityLocator{TagLocators: en, ModRevision: modRevision}
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
		return partition.EntityLocator{}, false
	}
	return el, true
}

var _ schema.EventHandler = (*shardingKeyRepo)(nil)

type shardingKeyRepo struct {
	schema.UnimplementedOnInitHandler
	log             *logger.Logger
	shardingKeysMap map[identity]partition.ShardingKeyLocator
	sync.RWMutex
}

// OnAddOrUpdate implements schema.EventHandler.
func (s *shardingKeyRepo) OnAddOrUpdate(schemaMetadata schema.Metadata) {
	var sl partition.ShardingKeyLocator
	var id identity
	var modRevision int64
	if schemaMetadata.Kind != schema.KindMeasure {
		return
	}
	measure := schemaMetadata.Spec.(*databasev1.Measure)
	modRevision = measure.GetMetadata().GetModRevision()
	sl = partition.NewShardingKeyLocator(measure.TagFamilies, measure.ShardingKey, modRevision)
	id = getID(measure.GetMetadata())
	if le := s.log.Debug(); le.Enabled() {
		le.
			Str("action", "add_or_update").
			Stringer("subject", id).
			Str("kind", "measure").
			Msg("entity added or updated")
	}
	tl := make([]partition.TagLocator, 0, len(sl.TagLocators))
	for _, l := range sl.TagLocators {
		tl = append(tl, partition.TagLocator{
			FamilyOffset: l.FamilyOffset,
			TagOffset:    l.TagOffset,
		})
	}
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.shardingKeysMap[id] = partition.ShardingKeyLocator{TagLocators: tl, ModRevision: modRevision}
}

// OnDelete implements schema.EventHandler.
func (s *shardingKeyRepo) OnDelete(schemaMetadata schema.Metadata) {
	var id identity
	if schemaMetadata.Kind != schema.KindMeasure {
		return
	}
	measure := schemaMetadata.Spec.(*databasev1.Measure)
	id = getID(measure.GetMetadata())
	if le := s.log.Debug(); le.Enabled() {
		le.
			Str("action", "delete").
			Stringer("subject", id).
			Str("kind", "measure").
			Msg("entity deleted")
	}
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	delete(s.shardingKeysMap, id)
}

func (s *shardingKeyRepo) getLocator(id identity) (partition.ShardingKeyLocator, bool) {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	sl, ok := s.shardingKeysMap[id]
	if !ok {
		return partition.ShardingKeyLocator{}, false
	}
	return sl, true
}
