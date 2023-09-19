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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var _ Resource = (*resourceSpec)(nil)

type resourceSpec struct {
	schema       ResourceSchema
	delegated    io.Closer
	indexRules   []*databasev1.IndexRule
	aggregations []*databasev1.TopNAggregation
}

func (rs *resourceSpec) Delegated() io.Closer {
	return rs.delegated
}

func (rs *resourceSpec) Close() error {
	return rs.delegated.Close()
}

func (rs *resourceSpec) Schema() ResourceSchema {
	return rs.schema
}

func (rs *resourceSpec) IndexRules() []*databasev1.IndexRule {
	return rs.indexRules
}

func (rs *resourceSpec) TopN() []*databasev1.TopNAggregation {
	return rs.aggregations
}

func (rs *resourceSpec) maxRevision() int64 {
	return rs.schema.GetMetadata().GetModRevision()
}

func (rs *resourceSpec) isNewThan(other *resourceSpec) bool {
	if other.maxRevision() > rs.maxRevision() {
		return false
	}
	if len(rs.indexRules) != len(other.indexRules) {
		return false
	}
	if len(rs.aggregations) != len(other.aggregations) {
		return false
	}
	if parseMaxModRevision(other.indexRules) > parseMaxModRevision(rs.indexRules) {
		return false
	}
	if parseMaxModRevision(other.aggregations) > parseMaxModRevision(rs.aggregations) {
		return false
	}
	return true
}

const defaultWorkerNum = 10

var _ Repository = (*schemaRepo)(nil)

type schemaRepo struct {
	metadata               metadata.Repo
	resourceSupplier       ResourceSupplier
	resourceSchemaSupplier ResourceSchemaSupplier
	l                      *logger.Logger
	data                   map[string]*group
	closer                 *run.ChannelCloser
	eventCh                chan MetadataEvent
	workerNum              int
	sync.RWMutex
}

func (sr *schemaRepo) SendMetadataEvent(event MetadataEvent) {
	if !sr.closer.AddSender() {
		return
	}
	defer sr.closer.SenderDone()
	select {
	case sr.eventCh <- event:
	case <-sr.closer.CloseNotify():
	}
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
		metadata:               metadata,
		l:                      l,
		resourceSupplier:       resourceSupplier,
		resourceSchemaSupplier: resourceSupplier,
		data:                   make(map[string]*group),
		eventCh:                make(chan MetadataEvent, defaultWorkerNum),
		workerNum:              defaultWorkerNum,
		closer:                 run.NewChannelCloser(),
	}
}

// NewPortableRepository return a new Repository without tsdb.
func NewPortableRepository(
	metadata metadata.Repo,
	l *logger.Logger,
	supplier ResourceSchemaSupplier,
) Repository {
	return &schemaRepo{
		metadata:               metadata,
		l:                      l,
		resourceSchemaSupplier: supplier,
		data:                   make(map[string]*group),
		eventCh:                make(chan MetadataEvent, defaultWorkerNum),
		workerNum:              defaultWorkerNum,
		closer:                 run.NewChannelCloser(),
	}
}

func (sr *schemaRepo) Watcher() {
	for i := 0; i < sr.workerNum; i++ {
		go func() {
			if !sr.closer.AddReceiver() {
				return
			}
			defer func() {
				sr.closer.ReceiverDone()
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
							err = sr.storeResource(evt.Metadata)
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
						select {
						case <-sr.closer.CloseNotify():
							return
						default:
						}
						sr.l.Err(err).Interface("event", evt).Msg("fail to handle the metadata event. retry...")
						sr.SendMetadataEvent(evt)
					}
				case <-sr.closer.CloseNotify():
					return
				}
			}
		}()
	}
}

func (sr *schemaRepo) StoreGroup(groupMeta *commonv1.Metadata) (*group, error) {
	name := groupMeta.GetName()
	sr.Lock()
	defer sr.Unlock()
	g, ok := sr.getGroup(name)
	if !ok {
		sr.l.Info().Str("group", name).Msg("creating a tsdb")
		g = sr.createGroup(name)
		if err := g.init(name); err != nil {
			return nil, err
		}
		return g, nil
	}
	if !g.isInit() {
		if err := g.init(name); err != nil {
			return nil, err
		}
		return g, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	groupSchema, err := g.metadata.GroupRegistry().GetGroup(ctx, name)
	if err != nil {
		return nil, err
	}
	prevGroupSchema := g.GetSchema()
	if groupSchema.GetMetadata().GetModRevision() <= prevGroupSchema.Metadata.ModRevision {
		return g, nil
	}
	sr.l.Info().Str("group", name).Msg("closing the previous tsdb")
	db := g.SupplyTSDB()
	db.Close()
	sr.l.Info().Str("group", name).Msg("creating a new tsdb")
	if err := g.init(name); err != nil {
		return nil, err
	}
	return g, nil
}

func (sr *schemaRepo) createGroup(name string) (g *group) {
	if sr.resourceSupplier != nil {
		g = newGroup(sr.metadata, sr.l, sr.resourceSupplier)
	} else {
		g = newPortableGroup(sr.metadata, sr.l, sr.resourceSchemaSupplier)
	}
	sr.data[name] = g
	return
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
	g, ok := sr.getGroup(name)
	if !ok {
		return nil, false
	}
	return g, g.isInit()
}

func (sr *schemaRepo) LoadResource(metadata *commonv1.Metadata) (Resource, bool) {
	g, ok := sr.LoadGroup(metadata.Group)
	if !ok {
		return nil, false
	}
	return g.LoadResource(metadata.Name)
}

func (sr *schemaRepo) storeResource(metadata *commonv1.Metadata) error {
	g, ok := sr.LoadGroup(metadata.Group)
	if !ok {
		var err error
		if g, err = sr.StoreGroup(&commonv1.Metadata{Name: metadata.Group}); err != nil {
			return errors.WithMessagef(err, "create unknown group:%s", metadata.Group)
		}
	}
	stm, err := sr.resourceSchemaSupplier.ResourceSchema(metadata)
	if err != nil {
		if errors.Is(err, schema.ErrGRPCResourceNotFound) {
			if dl := sr.l.Debug(); dl.Enabled() {
				dl.Interface("metadata", metadata).Msg("resource not found")
			}
			return nil
		}
		return errors.WithMessage(err, "fails to get the resource")
	}
	_, err = g.(*group).storeResource(context.Background(), stm)
	return err
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
	sr.closer.CloseThenWait()
	close(sr.eventCh)

	sr.RLock()
	defer sr.RUnlock()
	for _, g := range sr.data {
		err := g.close()
		if err != nil {
			sr.l.Err(err).RawJSON("group", logger.Proto(g.GetSchema().Metadata)).Msg("closing")
		}
	}
}

var _ Group = (*group)(nil)

type group struct {
	resourceSupplier       ResourceSupplier
	resourceSchemaSupplier ResourceSchemaSupplier
	metadata               metadata.Repo
	db                     atomic.Value
	groupSchema            atomic.Pointer[commonv1.Group]
	l                      *logger.Logger
	schemaMap              map[string]*resourceSpec
	mapMutex               sync.RWMutex
}

func newGroup(
	metadata metadata.Repo,
	l *logger.Logger,
	resourceSupplier ResourceSupplier,
) *group {
	g := &group{
		groupSchema:            atomic.Pointer[commonv1.Group]{},
		metadata:               metadata,
		l:                      l,
		schemaMap:              make(map[string]*resourceSpec),
		resourceSupplier:       resourceSupplier,
		resourceSchemaSupplier: resourceSupplier,
	}
	return g
}

func newPortableGroup(
	metadata metadata.Repo,
	l *logger.Logger,
	resourceSchemaSupplier ResourceSchemaSupplier,
) *group {
	g := &group{
		groupSchema:            atomic.Pointer[commonv1.Group]{},
		metadata:               metadata,
		l:                      l,
		schemaMap:              make(map[string]*resourceSpec),
		resourceSchemaSupplier: resourceSchemaSupplier,
	}
	return g
}

func (g *group) init(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	groupSchema, err := g.metadata.GroupRegistry().GetGroup(ctx, name)
	if errors.As(err, schema.ErrGRPCResourceNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	g.groupSchema.Store(groupSchema)
	if g.isPortable() {
		return nil
	}
	db, err := g.resourceSupplier.OpenDB(groupSchema)
	if err != nil {
		return err
	}
	g.db.Store(db)
	return nil
}

func (g *group) isInit() bool {
	return g.GetSchema() != nil
}

func (g *group) GetSchema() *commonv1.Group {
	return g.groupSchema.Load()
}

func (g *group) SupplyTSDB() tsdb.Database {
	return g.db.Load().(tsdb.Database)
}

func (g *group) storeResource(ctx context.Context, resourceSchema ResourceSchema) (Resource, error) {
	g.mapMutex.Lock()
	defer g.mapMutex.Unlock()
	localCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
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
	resource := &resourceSpec{
		schema:       resourceSchema,
		indexRules:   idxRules,
		aggregations: topNAggrs,
	}
	key := resourceSchema.GetMetadata().GetName()
	preResource := g.schemaMap[key]
	if preResource != nil && preResource.isNewThan(resource) {
		return preResource, nil
	}
	var dbSupplier tsdb.Supplier
	if !g.isPortable() {
		dbSupplier = g
	}
	sm, errTS := g.resourceSchemaSupplier.OpenResource(g.GetSchema().GetResourceOpts().ShardNum, dbSupplier, resource)
	if errTS != nil {
		return nil, errTS
	}
	resource.delegated = sm
	g.schemaMap[key] = resource
	if preResource != nil {
		_ = preResource.Close()
	}
	return resource, nil
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

func (g *group) isPortable() bool {
	return g.resourceSupplier == nil
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
	if !g.isInit() || g.isPortable() {
		return nil
	}
	return multierr.Append(err, g.SupplyTSDB().Close())
}

func parseMaxModRevision[T ResourceSchema](indexRules []T) (maxRevisionForIdxRules int64) {
	maxRevisionForIdxRules = int64(0)
	for _, idxRule := range indexRules {
		if idxRule.GetMetadata().GetModRevision() > maxRevisionForIdxRules {
			maxRevisionForIdxRules = idxRule.GetMetadata().GetModRevision()
		}
	}
	return
}
