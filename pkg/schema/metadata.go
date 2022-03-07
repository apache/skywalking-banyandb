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

package schema

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

type EventType uint8

const (
	EventAddOrUpdate EventType = iota
	EventDelete
)

type EventKind uint8

const (
	EventKindGroup EventKind = iota
	EventKindResource
)

type Group interface {
	GetSchema() *commonv1.Group
	StoreResource(resourceSchema ResourceSchema) (Resource, error)
	LoadResource(name string) (Resource, bool)
}

type MetadataEvent struct {
	Typ      EventType
	Kind     EventKind
	Metadata *commonv1.Metadata
}

type ResourceSchema interface {
	GetMetadata() *commonv1.Metadata
}

type ResourceSpec struct {
	Schema     ResourceSchema
	IndexRules []*databasev1.IndexRule
}

type Resource interface {
	GetIndexRules() []*databasev1.IndexRule
	MaxObservedModRevision() int64
	EntityLocator() partition.EntityLocator
	ResourceSchema
	io.Closer
}

type ResourceSupplier interface {
	OpenResource(shardNum uint32, db tsdb.Supplier, spec ResourceSpec) (Resource, error)
	ResourceSchema(repo metadata.Repo, metdata *commonv1.Metadata) (ResourceSchema, error)
	OpenDB(groupSchema *commonv1.Group) (tsdb.Database, error)
}

type Repository interface {
	Watcher()
	SendMetadataEvent(MetadataEvent)
	StoreGroup(groupMeta *commonv1.Metadata) (*group, error)
	LoadGroup(name string) (Group, bool)
	LoadResource(metadata *commonv1.Metadata) (Resource, bool)
	NotifyAll() (err error)
	Close()
}

var _ Repository = (*schemaRepo)(nil)

type schemaRepo struct {
	sync.RWMutex
	metadata         metadata.Repo
	repo             discovery.ServiceRepo
	l                *logger.Logger
	resourceSupplier ResourceSupplier
	shardTopic       bus.Topic
	entityTopic      bus.Topic
	data             map[string]*group

	// stop channel for the inner worker
	workerStopCh chan struct{}
	eventCh      chan MetadataEvent
}

func NewRepository(
	metadata metadata.Repo,
	repo discovery.ServiceRepo,
	l *logger.Logger,
	resourceSupplier ResourceSupplier,
	shardTopic bus.Topic,
	entityTopic bus.Topic,
) Repository {
	return &schemaRepo{
		metadata:         metadata,
		repo:             repo,
		l:                l,
		resourceSupplier: resourceSupplier,
		shardTopic:       shardTopic,
		entityTopic:      entityTopic,
		data:             make(map[string]*group),
		eventCh:          make(chan MetadataEvent),
		workerStopCh:     make(chan struct{}),
	}
}

func (sr *schemaRepo) SendMetadataEvent(event MetadataEvent) {
	sr.eventCh <- event
}

func (sr *schemaRepo) Watcher() {
	defer func() {
		if err := recover(); err != nil {
			sr.l.Warn().Interface("err", err).Msg("watching the events")
		}
	}()
	for {
		select {
		case evt := <-sr.eventCh:
			var err error
			switch evt.Typ {
			case EventAddOrUpdate:
				switch evt.Kind {
				case EventKindGroup:
					_, err = sr.StoreGroup(evt.Metadata)
				case EventKindResource:
					_, err = sr.storeResource(evt.Metadata)
				}
			case EventDelete:
				switch evt.Kind {
				case EventKindGroup:
					err = sr.deleteGroup(evt.Metadata)
				case EventKindResource:
					err = sr.deleteResource(evt.Metadata)
				}
			}
			if err != nil {
				sr.l.Err(err).Interface("event", evt).Msg("fail to handle the metadata event. retry...")
				sr.eventCh <- evt
			}
		case <-sr.workerStopCh:
			return
		}
	}
}

func (sr *schemaRepo) StoreGroup(groupMeta *commonv1.Metadata) (*group, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	groupSchema, err := sr.metadata.GroupRegistry().GetGroup(ctx, groupMeta.GetName())
	cancel()
	if err != nil {
		return nil, err
	}
	name := groupSchema.GetMetadata().GetName()
	sr.Lock()
	defer sr.Unlock()
	g, ok := sr.getGroup(name)
	if !ok {
		sr.l.Info().Str("group", name).Msg("creating a tsdb")
		var db tsdb.Database
		db, err = sr.resourceSupplier.OpenDB(groupSchema)
		if err != nil {
			return nil, err
		}
		g = newGroup(groupSchema, sr.repo, sr.metadata, db, sr.l, sr.resourceSupplier, sr.entityTopic)
		sr.data[name] = g
		return g, sr.notify(groupSchema, databasev1.Action_ACTION_PUT)
	}
	prevGroupSchema := g.groupSchema
	if groupSchema.GetMetadata().GetModRevision() <= prevGroupSchema.Metadata.ModRevision {
		return g, nil
	}
	sr.l.Info().Str("group", name).Msg("closing the previous tsdb")
	db := g.SupplyTSDB()
	db.Close()
	err = sr.notify(prevGroupSchema, databasev1.Action_ACTION_DELETE)
	if err != nil {
		return nil, err
	}
	sr.l.Info().Str("group", name).Msg("creating a new tsdb")
	newDB, err := sr.resourceSupplier.OpenDB(groupSchema)
	if err != nil {
		return nil, err
	}
	g.setDB(newDB)
	err = sr.notify(groupSchema, databasev1.Action_ACTION_PUT)
	if err != nil {
		return nil, err
	}
	g.groupSchema = groupSchema
	return g, nil
}

func (sr *schemaRepo) deleteGroup(groupMeta *commonv1.Metadata) error {
	name := groupMeta.GetName()
	sr.Lock()
	defer sr.Unlock()
	var ok bool
	g, ok := sr.getGroup(name)
	if !ok {
		return nil
	}
	err := g.close()
	if err != nil {
		return err
	}
	_ = sr.notify(g.groupSchema, databasev1.Action_ACTION_DELETE)
	delete(sr.data, name)
	return nil
}

func (sr *schemaRepo) getGroup(name string) (*group, bool) {
	g := sr.data[name]
	if g == nil {
		return nil, false
	}
	return g, true
}

func (sr *schemaRepo) LoadGroup(name string) (Group, bool) {
	sr.RLock()
	defer sr.RUnlock()
	return sr.getGroup(name)
}

func (sr *schemaRepo) LoadResource(metadata *commonv1.Metadata) (Resource, bool) {
	g, ok := sr.LoadGroup(metadata.Group)
	if !ok {
		return nil, false
	}
	return g.LoadResource(metadata.Name)
}

func (sr *schemaRepo) storeResource(metadata *commonv1.Metadata) (Resource, error) {
	group, ok := sr.LoadGroup(metadata.Group)
	if !ok {
		return nil, errors.Errorf("unknown group")
	}
	stm, err := sr.resourceSupplier.ResourceSchema(sr.metadata, metadata)
	if err != nil {
		return nil, errors.Errorf("fails to get the resource")
	}
	return group.StoreResource(stm)
}

func (sr *schemaRepo) deleteResource(metadata *commonv1.Metadata) error {
	g, ok := sr.LoadGroup(metadata.Group)
	if !ok {
		return nil
	}
	return g.(*group).deleteResource(metadata)
}

func (sr *schemaRepo) notify(groupSchema *commonv1.Group, action databasev1.Action) (err error) {
	now := time.Now()
	nowPb := timestamppb.New(now)
	shardNum := groupSchema.GetResourceOpts().GetShardNum()
	for i := 0; i < int(shardNum); i++ {
		_, errInternal := sr.repo.Publish(sr.shardTopic, bus.NewMessage(bus.MessageID(now.UnixNano()), &databasev1.ShardEvent{
			Shard: &databasev1.Shard{
				Id:    uint64(i),
				Total: shardNum,
				Metadata: &commonv1.Metadata{
					Name: groupSchema.GetMetadata().GetName(),
				},
				Node: &databasev1.Node{
					Id:        sr.repo.NodeID(),
					CreatedAt: nowPb,
					UpdatedAt: nowPb,
					Addr:      "localhost",
				},
				UpdatedAt: nowPb,
				CreatedAt: nowPb,
			},
			Time:   nowPb,
			Action: action,
		}))
		if errors.Is(errInternal, bus.ErrTopicNotExist) {
			return nil
		}
		if errInternal != nil {
			err = multierr.Append(err, errInternal)
		}
	}
	return err
}

func (sr *schemaRepo) NotifyAll() (err error) {
	for _, g := range sr.getMap() {
		err = multierr.Append(err, sr.notify(g.groupSchema, databasev1.Action_ACTION_PUT))
		for _, s := range g.getMap() {
			err = multierr.Append(err, g.notify(s, databasev1.Action_ACTION_PUT))
		}
	}
	return err
}

func (sr *schemaRepo) Close() {
	defer func() {
		if err := recover(); err != nil {
			sr.l.Warn().Interface("err", err).Msg("closing resource")
		}
	}()
	if sr.eventCh != nil {
		close(sr.eventCh)
	}
	if sr.workerStopCh != nil {
		close(sr.workerStopCh)
	}
	for _, g := range sr.getMap() {
		err := g.close()
		if err != nil {
			sr.l.Err(err).Stringer("group", g.groupSchema.Metadata).Msg("closing")
		}
	}
}

func (sr *schemaRepo) getMap() map[string]*group {
	sr.RLock()
	defer sr.RUnlock()
	return sr.data
}

var _ Group = (*group)(nil)

type group struct {
	groupSchema      *commonv1.Group
	l                *logger.Logger
	resourceSupplier ResourceSupplier
	repo             discovery.ServiceRepo
	metadata         metadata.Repo
	entityTopic      bus.Topic
	db               atomic.Value
	mapMutex         sync.RWMutex
	schemaMap        map[string]Resource
}

func newGroup(
	groupSchema *commonv1.Group,
	repo discovery.ServiceRepo,
	metadata metadata.Repo,
	db tsdb.Database,
	l *logger.Logger,
	resourceSupplier ResourceSupplier,
	entityTopic bus.Topic,
) *group {
	g := &group{
		groupSchema:      groupSchema,
		repo:             repo,
		metadata:         metadata,
		l:                l,
		schemaMap:        make(map[string]Resource),
		resourceSupplier: resourceSupplier,
		entityTopic:      entityTopic,
	}
	g.db.Store(db)
	return g
}

func (g *group) GetSchema() *commonv1.Group {
	return g.groupSchema
}

func (g *group) SupplyTSDB() tsdb.Database {
	return g.db.Load().(tsdb.Database)
}

func (g *group) setDB(db tsdb.Database) {
	g.db.Store(db)
}

func (g *group) StoreResource(resourceSchema ResourceSchema) (Resource, error) {
	g.mapMutex.Lock()
	defer g.mapMutex.Unlock()
	key := resourceSchema.GetMetadata().GetName()
	preResource := g.schemaMap[key]
	if preResource != nil &&
		resourceSchema.GetMetadata().GetModRevision() <= preResource.GetMetadata().GetModRevision() {
		// we only need to check the max modifications revision observed for index rules
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		idxRules, errIndexRules := g.metadata.IndexRules(ctx, resourceSchema.GetMetadata())
		cancel()
		if errIndexRules != nil {
			return nil, errIndexRules
		}
		if len(idxRules) == len(preResource.GetIndexRules()) {
			maxModRevision := pbv1.ParseMaxModRevision(idxRules)
			if preResource.MaxObservedModRevision() >= maxModRevision {
				return preResource, nil
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	idxRules, errIndexRules := g.metadata.IndexRules(ctx, resourceSchema.GetMetadata())
	cancel()
	if errIndexRules != nil {
		return nil, errIndexRules
	}
	sm, errTS := g.resourceSupplier.OpenResource(g.groupSchema.GetResourceOpts().ShardNum, g, ResourceSpec{
		Schema:     resourceSchema,
		IndexRules: idxRules,
	})
	if errTS != nil {
		return nil, errTS
	}
	if err := g.notify(sm, databasev1.Action_ACTION_PUT); err != nil {
		return nil, err
	}
	g.schemaMap[key] = sm
	if preResource != nil {
		_ = preResource.Close()
	}
	return sm, nil
}

func (g *group) deleteResource(metadata *commonv1.Metadata) error {
	g.mapMutex.Lock()
	defer g.mapMutex.Unlock()
	key := metadata.GetName()
	preResource := g.schemaMap[key]
	if preResource == nil {
		return nil
	}
	if err := g.notify(preResource, databasev1.Action_ACTION_DELETE); err != nil {
		return err
	}
	delete(g.schemaMap, key)
	_ = preResource.Close()
	return nil
}

func (g *group) LoadResource(name string) (Resource, bool) {
	data := g.getMap()
	s := data[name]
	if s == nil {
		return nil, false
	}
	return s, true
}

func (g *group) notify(resource Resource, action databasev1.Action) error {
	now := time.Now()
	nowPb := timestamppb.New(now)
	entityLocator := resource.EntityLocator()
	locator := make([]*databasev1.EntityEvent_TagLocator, 0, len(entityLocator))
	for _, tagLocator := range entityLocator {
		locator = append(locator, &databasev1.EntityEvent_TagLocator{
			FamilyOffset: uint32(tagLocator.FamilyOffset),
			TagOffset:    uint32(tagLocator.TagOffset),
		})
	}
	_, err := g.repo.Publish(g.entityTopic, bus.NewMessage(bus.MessageID(now.UnixNano()), &databasev1.EntityEvent{
		Subject:       resource.GetMetadata(),
		EntityLocator: locator,
		Time:          nowPb,
		Action:        action,
	}))
	if errors.Is(err, bus.ErrTopicNotExist) {
		return nil
	}
	return err
}

func (g *group) close() (err error) {
	for _, s := range g.getMap() {
		err = multierr.Append(err, s.Close())
	}
	return multierr.Append(err, g.SupplyTSDB().Close())
}

func (g *group) getMap() map[string]Resource {
	g.mapMutex.RLock()
	defer g.mapMutex.RUnlock()
	return g.schemaMap
}
