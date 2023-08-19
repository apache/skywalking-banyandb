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

// Package schema implements a framework to sync schema info from the metadata repository.
package schema

import (
	"context"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// EventType defines actions of events.
type EventType uint8

// EventType support Add/Update and Delete.
// All events are idempotent.
const (
	EventAddOrUpdate EventType = iota
	EventDelete
)

// EventKind defines category of events.
type EventKind uint8

// This framework groups events to a hierarchy. A group is the root node.
const (
	EventKindGroup EventKind = iota
	EventKindResource
)

// Group is the root node, allowing get resources from its sub nodes.
type Group interface {
	GetSchema() *commonv1.Group
	StoreResource(ctx context.Context, resourceSchema ResourceSchema) (Resource, error)
	LoadResource(name string) (Resource, bool)
}

// MetadataEvent is the syncing message between metadata repo and this framework.
type MetadataEvent struct {
	Metadata *commonv1.Metadata
	Typ      EventType
	Kind     EventKind
}

// ResourceSchema allows get the metadata.
type ResourceSchema interface {
	GetMetadata() *commonv1.Metadata
}

// ResourceSpec wraps required fields to open a resource.
type ResourceSpec struct {
	Schema ResourceSchema
	// IndexRules are index rules bound to the Schema
	IndexRules []*databasev1.IndexRule
	// Aggregations are topN aggregation bound to the Schema, e.g. TopNAggregation
	Aggregations []*databasev1.TopNAggregation
}

// Resource allows access metadata from a local cache.
type Resource interface {
	GetIndexRules() []*databasev1.IndexRule
	GetTopN() []*databasev1.TopNAggregation
	MaxObservedModRevision() int64
	ResourceSchema
	io.Closer
}

// ResourceSupplier allows open a resource and its embedded tsdb.
type ResourceSupplier interface {
	OpenResource(shardNum uint32, db tsdb.Supplier, spec ResourceSpec) (Resource, error)
	ResourceSchema(metdata *commonv1.Metadata) (ResourceSchema, error)
	OpenDB(groupSchema *commonv1.Group) (tsdb.Database, error)
}

// Repository is the collection of several hierarchies groups by a "Group".
type Repository interface {
	Watcher()
	SendMetadataEvent(MetadataEvent)
	StoreGroup(groupMeta *commonv1.Metadata) (*group, error)
	LoadGroup(name string) (Group, bool)
	LoadResource(metadata *commonv1.Metadata) (Resource, bool)
	Close()
	StopCh() <-chan struct{}
}

const defaultWorkerNum = 10

var _ Repository = (*schemaRepo)(nil)

type schemaRepo struct {
	metadata         metadata.Repo
	resourceSupplier ResourceSupplier
	l                *logger.Logger
	data             map[string]*group
	workerCloser     *run.Closer
	closer           *run.Closer
	eventCh          chan MetadataEvent
	workerNum        int
	sync.RWMutex
}

// StopCh implements Repository.
func (sr *schemaRepo) StopCh() <-chan struct{} {
	return sr.closer.CloseNotify()
}

// NewRepository return a new Repository.
func NewRepository(
	metadata metadata.Repo,
	l *logger.Logger,
	resourceSupplier ResourceSupplier,
) Repository {
	return &schemaRepo{
		metadata:         metadata,
		l:                l,
		resourceSupplier: resourceSupplier,
		data:             make(map[string]*group),
		eventCh:          make(chan MetadataEvent, defaultWorkerNum),
		workerNum:        defaultWorkerNum,
		workerCloser:     run.NewCloser(defaultWorkerNum),
		closer:           run.NewCloser(1),
	}
}

func (sr *schemaRepo) SendMetadataEvent(event MetadataEvent) {
	sr.eventCh <- event
}

func (sr *schemaRepo) Watcher() {
	for i := 0; i < sr.workerNum; i++ {
		go func() {
			defer func() {
				sr.workerCloser.Done()
				if err := recover(); err != nil {
					sr.l.Warn().Interface("err", err).Msg("watching the events")
				}
			}()
			for {
				select {
				case evt, more := <-sr.eventCh:
					if !more {
						return
					}
					if e := sr.l.Debug(); e.Enabled() {
						e.Interface("event", evt).Msg("received an event")
					}
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
					if err != nil && !errors.Is(err, schema.ErrClosed) {
						sr.l.Err(err).Interface("event", evt).Msg("fail to handle the metadata event. retry...")
						select {
						case sr.eventCh <- evt:
						case <-sr.workerCloser.CloseNotify():
							return
						}
					}
				case <-sr.workerCloser.CloseNotify():
					return
				}
			}
		}()
	}
}

func (sr *schemaRepo) StoreGroup(groupMeta *commonv1.Metadata) (*group, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	groupSchema, err := sr.metadata.GroupRegistry().GetGroup(ctx, groupMeta.GetName())
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
		g = newGroup(groupSchema, sr.metadata, db, sr.l, sr.resourceSupplier)
		sr.data[name] = g
		return g, nil
	}
	prevGroupSchema := g.GetSchema()
	if groupSchema.GetMetadata().GetModRevision() <= prevGroupSchema.Metadata.ModRevision {
		return g, nil
	}
	sr.l.Info().Str("group", name).Msg("closing the previous tsdb")
	db := g.SupplyTSDB()
	db.Close()
	sr.l.Info().Str("group", name).Msg("creating a new tsdb")
	newDB, err := sr.resourceSupplier.OpenDB(groupSchema)
	if err != nil {
		return nil, err
	}
	g.setDB(newDB)
	g.groupSchema.Store(groupSchema)
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
		var err error
		if group, err = sr.StoreGroup(&commonv1.Metadata{Name: metadata.Group}); err != nil {
			return nil, errors.WithMessagef(err, "create unknown group:%s", metadata.Group)
		}
	}
	stm, err := sr.resourceSupplier.ResourceSchema(metadata)
	if err != nil {
		return nil, errors.WithMessage(err, "fails to get the resource")
	}
	return group.StoreResource(context.Background(), stm)
}

func (sr *schemaRepo) deleteResource(metadata *commonv1.Metadata) error {
	g, ok := sr.LoadGroup(metadata.Group)
	if !ok {
		return nil
	}
	return g.(*group).deleteResource(metadata)
}

func (sr *schemaRepo) Close() {
	defer func() {
		if err := recover(); err != nil {
			sr.l.Warn().Interface("err", err).Msg("closing resource")
		}
	}()
	sr.workerCloser.CloseThenWait()

	sr.RLock()
	defer sr.RUnlock()
	for _, g := range sr.data {
		err := g.close()
		if err != nil {
			sr.l.Err(err).RawJSON("group", logger.Proto(g.GetSchema().Metadata)).Msg("closing")
		}
	}
	sr.closer.Done()
	sr.closer.CloseThenWait()
}

var _ Group = (*group)(nil)

type group struct {
	resourceSupplier ResourceSupplier
	metadata         metadata.Repo
	db               atomic.Value
	groupSchema      atomic.Pointer[commonv1.Group]
	l                *logger.Logger
	schemaMap        map[string]Resource
	mapMutex         sync.RWMutex
}

func newGroup(
	groupSchema *commonv1.Group,
	metadata metadata.Repo,
	db tsdb.Database,
	l *logger.Logger,
	resourceSupplier ResourceSupplier,
) *group {
	g := &group{
		groupSchema:      atomic.Pointer[commonv1.Group]{},
		metadata:         metadata,
		l:                l,
		schemaMap:        make(map[string]Resource),
		resourceSupplier: resourceSupplier,
	}
	g.groupSchema.Store(groupSchema)
	g.db.Store(db)
	return g
}

func (g *group) GetSchema() *commonv1.Group {
	return g.groupSchema.Load()
}

func (g *group) SupplyTSDB() tsdb.Database {
	return g.db.Load().(tsdb.Database)
}

func (g *group) setDB(db tsdb.Database) {
	g.db.Store(db)
}

func (g *group) StoreResource(ctx context.Context, resourceSchema ResourceSchema) (Resource, error) {
	g.mapMutex.Lock()
	defer g.mapMutex.Unlock()
	key := resourceSchema.GetMetadata().GetName()
	preResource := g.schemaMap[key]
	var localCtx context.Context
	var cancel context.CancelFunc
	if preResource != nil &&
		resourceSchema.GetMetadata().GetModRevision() <= preResource.GetMetadata().GetModRevision() {
		// we only need to check the max modifications revision observed for index rules
		localCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
		idxRules, errIndexRules := g.metadata.IndexRules(localCtx, resourceSchema.GetMetadata())
		cancel()
		if errIndexRules != nil {
			return nil, errIndexRules
		}
		localCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
		topNAggrs, errTopN := g.metadata.MeasureRegistry().TopNAggregations(localCtx, resourceSchema.GetMetadata())
		cancel()
		if errTopN != nil {
			return nil, errTopN
		}
		if len(idxRules) == len(preResource.GetIndexRules()) && len(topNAggrs) == len(preResource.GetTopN()) {
			maxModRevision := int64(math.Max(float64(ParseMaxModRevision(idxRules)), float64(ParseMaxModRevision(topNAggrs))))
			if preResource.MaxObservedModRevision() >= maxModRevision {
				return preResource, nil
			}
		}
	}
	localCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	idxRules, err := g.metadata.IndexRules(localCtx, resourceSchema.GetMetadata())
	if err != nil {
		return nil, err
	}

	var topNAggrs []*databasev1.TopNAggregation
	if _, ok := resourceSchema.(*databasev1.Measure); ok {
		localCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
		var innerErr error
		topNAggrs, innerErr = g.metadata.MeasureRegistry().TopNAggregations(localCtx, resourceSchema.GetMetadata())
		cancel()
		if innerErr != nil {
			return nil, innerErr
		}
	}

	sm, errTS := g.resourceSupplier.OpenResource(g.GetSchema().GetResourceOpts().ShardNum, g, ResourceSpec{
		Schema:       resourceSchema,
		IndexRules:   idxRules,
		Aggregations: topNAggrs,
	})
	if errTS != nil {
		return nil, errTS
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
	delete(g.schemaMap, key)
	_ = preResource.Close()
	return nil
}

func (g *group) LoadResource(name string) (Resource, bool) {
	g.mapMutex.RLock()
	s := g.schemaMap[name]
	g.mapMutex.RUnlock()
	if s == nil {
		return nil, false
	}
	return s, true
}

func (g *group) close() (err error) {
	g.mapMutex.RLock()
	for _, s := range g.schemaMap {
		err = multierr.Append(err, s.Close())
	}
	g.mapMutex.RUnlock()
	return multierr.Append(err, g.SupplyTSDB().Close())
}

// ParseMaxModRevision gives the max revision from resources' metadata.
func ParseMaxModRevision[T ResourceSchema](indexRules []T) (maxRevisionForIdxRules int64) {
	maxRevisionForIdxRules = int64(0)
	for _, idxRule := range indexRules {
		if idxRule.GetMetadata().GetModRevision() > maxRevisionForIdxRules {
			maxRevisionForIdxRules = idxRule.GetMetadata().GetModRevision()
		}
	}
	return
}
