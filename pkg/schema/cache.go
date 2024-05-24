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
	"path"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
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

var defaultWorkerNum = runtime.GOMAXPROCS(-1)

var _ Repository = (*schemaRepo)(nil)

type schemaRepo struct {
	metadata               metadata.Repo
	resourceSupplier       ResourceSupplier
	resourceSchemaSupplier ResourceSchemaSupplier
	l                      *logger.Logger
	closer                 *run.ChannelCloser
	eventCh                chan MetadataEvent
	groupMap               sync.Map
	resourceMap            sync.Map
	workerNum              int
	resourceMutex          sync.Mutex
	groupMux               sync.Mutex
}

func (sr *schemaRepo) SendMetadataEvent(event MetadataEvent) {
	sr.sendMetadataEvent(event, false)
}

func (sr *schemaRepo) sendMetadataEvent(event MetadataEvent, retry bool) {
	if !sr.closer.AddSender() {
		return
	}
	defer sr.closer.SenderDone()
	if retry {
		sr.l.Error().Msgf("sending metadata event: %v", event)
	}
	select {
	case sr.eventCh <- event:
	case <-sr.closer.CloseNotify():
	}
	if retry {
		sr.l.Error().Msgf("sent metadata event done: %v", event)
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
							_, err = sr.storeGroup(evt.Metadata.GetMetadata())
						case EventKindResource:
							err = sr.initResource(evt.Metadata.GetMetadata())
						case EventKindTopNAgg:
							topNSchema := evt.Metadata.(*databasev1.TopNAggregation)
							_, err = createOrUpdateTopNMeasure(context.Background(), sr.metadata.MeasureRegistry(), topNSchema)
							if err != nil {
								break
							}
							err = sr.initResource(topNSchema.SourceMeasure)
						}
					case EventDelete:
						switch evt.Kind {
						case EventKindGroup:
							err = sr.deleteGroup(evt.Metadata.GetMetadata())
						case EventKindResource:
							err = sr.deleteResource(evt.Metadata.GetMetadata())
						case EventKindTopNAgg:
							topNSchema := evt.Metadata.(*databasev1.TopNAggregation)
							err = multierr.Combine(
								sr.deleteResource(topNSchema.SourceMeasure),
								sr.initResource(topNSchema.SourceMeasure),
							)
						}
					}
					if err != nil && !errors.Is(err, schema.ErrClosed) {
						select {
						case <-sr.closer.CloseNotify():
							return
						default:
						}
						// TODO: Reconcile when the retry times is more than 3.
						sr.l.Err(err).Interface("event", evt).Msg("fail to handle the metadata event. retry...")
						go func() {
							sr.sendMetadataEvent(evt, true)
						}()
					}
				case <-sr.closer.CloseNotify():
					return
				}
			}
		}()
	}
}

func (sr *schemaRepo) storeGroup(groupMeta *commonv1.Metadata) (*group, error) {
	name := groupMeta.GetName()
	sr.groupMux.Lock()
	defer sr.groupMux.Unlock()
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
	if db != nil {
		db.Close()
	}
	sr.l.Info().Str("group", name).Msg("creating a new tsdb")
	if err := g.init(name); err != nil {
		return nil, err
	}
	return g, nil
}

func (sr *schemaRepo) createGroup(name string) (g *group) {
	g = newGroup(sr.metadata, sr.l, sr.resourceSupplier)
	sr.groupMap.Store(name, g)
	return
}

func (sr *schemaRepo) deleteGroup(groupMeta *commonv1.Metadata) error {
	name := groupMeta.GetName()
	g, loaded := sr.groupMap.LoadAndDelete(name)
	if !loaded {
		return nil
	}
	return g.(*group).close()
}

func (sr *schemaRepo) getGroup(name string) (*group, bool) {
	g, ok := sr.groupMap.Load(name)
	if !ok {
		return nil, false
	}
	return g.(*group), true
}

func (sr *schemaRepo) LoadGroup(name string) (Group, bool) {
	g, ok := sr.getGroup(name)
	if !ok {
		return nil, false
	}
	return g, g.isInit()
}

func (sr *schemaRepo) LoadResource(metadata *commonv1.Metadata) (Resource, bool) {
	k := getKey(metadata)
	s, ok := sr.resourceMap.Load(k)
	if !ok {
		return nil, false
	}
	return s.(Resource), true
}

func (sr *schemaRepo) storeResource(g Group, stm ResourceSchema,
	idxRules []*databasev1.IndexRule, topNAggrs []*databasev1.TopNAggregation,
) error {
	sr.resourceMutex.Lock()
	defer sr.resourceMutex.Unlock()
	resource := &resourceSpec{
		schema:       stm,
		indexRules:   idxRules,
		aggregations: topNAggrs,
	}
	key := getKey(stm.GetMetadata())
	pre, loadedPre := sr.resourceMap.Load(key)
	var preResource *resourceSpec
	if loadedPre {
		preResource = pre.(*resourceSpec)
	}
	if loadedPre && preResource.isNewThan(resource) {
		return nil
	}
	var dbSupplier Supplier
	if !g.(*group).isPortable() {
		dbSupplier = g
	}
	sm, err := sr.resourceSchemaSupplier.OpenResource(g.GetSchema().GetResourceOpts().ShardNum, dbSupplier, resource)
	if err != nil {
		return errors.WithMessage(err, "fails to open the resource")
	}
	resource.delegated = sm
	sr.resourceMap.Store(key, resource)
	if loadedPre {
		return preResource.Close()
	}
	return nil
}

func getKey(metadata *commonv1.Metadata) string {
	return path.Join(metadata.GetGroup(), metadata.GetName())
}

func (sr *schemaRepo) initResource(metadata *commonv1.Metadata) error {
	g, ok := sr.LoadGroup(metadata.Group)
	if !ok {
		var err error
		if g, err = sr.storeGroup(&commonv1.Metadata{Name: metadata.Group}); err != nil {
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
	ctx := context.Background()
	localCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	idxRules, err := sr.metadata.IndexRules(localCtx, stm.GetMetadata())
	if err != nil {
		return err
	}
	var topNAggrs []*databasev1.TopNAggregation
	if _, ok := stm.(*databasev1.Measure); ok {
		localCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
		var innerErr error
		topNAggrs, innerErr = sr.metadata.MeasureRegistry().TopNAggregations(localCtx, stm.GetMetadata())
		cancel()
		if innerErr != nil {
			return errors.WithMessage(innerErr, "fails to get the topN aggregations")
		}
	}
	return sr.storeResource(g, stm, idxRules, topNAggrs)
}

func (sr *schemaRepo) deleteResource(metadata *commonv1.Metadata) error {
	key := getKey(metadata)
	pre, loaded := sr.resourceMap.LoadAndDelete(key)
	if !loaded {
		return nil
	}
	return pre.(Resource).Close()
}

func (sr *schemaRepo) Close() {
	defer func() {
		if err := recover(); err != nil {
			sr.l.Warn().Interface("err", err).Msg("closing resource")
		}
	}()
	sr.closer.CloseThenWait()
	close(sr.eventCh)

	sr.resourceMutex.Lock()
	sr.resourceMap.Range(func(_, value any) bool {
		if value == nil {
			return true
		}
		r, ok := value.(*resourceSpec)
		if !ok {
			return true
		}
		err := r.Close()
		if err != nil {
			sr.l.Err(err).RawJSON("resource", logger.Proto(r.Schema().GetMetadata())).Msg("closing")
		}
		return true
	})
	sr.resourceMutex.Unlock()

	sr.groupMux.Lock()
	defer sr.groupMux.Unlock()
	sr.groupMap.Range(func(_, value any) bool {
		if value == nil {
			return true
		}
		g, ok := value.(*group)
		if !ok {
			return true
		}
		err := g.close()
		if err != nil {
			sr.l.Err(err).RawJSON("group", logger.Proto(g.GetSchema().Metadata)).Msg("closing")
		}
		return true
	})
	sr.groupMap = sync.Map{}
}

var _ Group = (*group)(nil)

type group struct {
	resourceSupplier ResourceSupplier
	metadata         metadata.Repo
	db               atomic.Value
	groupSchema      atomic.Pointer[commonv1.Group]
	l                *logger.Logger
}

func newGroup(
	metadata metadata.Repo,
	l *logger.Logger,
	resourceSupplier ResourceSupplier,
) *group {
	g := &group{
		groupSchema:      atomic.Pointer[commonv1.Group]{},
		metadata:         metadata,
		l:                l,
		resourceSupplier: resourceSupplier,
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
	return g.initBySchema(groupSchema)
}

func (g *group) initBySchema(groupSchema *commonv1.Group) error {
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

func (g *group) SupplyTSDB() io.Closer {
	if v := g.db.Load(); v != nil {
		return v.(io.Closer)
	}
	return nil
}

func (g *group) isPortable() bool {
	return g.resourceSupplier == nil
}

func (g *group) close() (err error) {
	if !g.isInit() || g.isPortable() {
		return nil
	}
	db := g.SupplyTSDB()
	if db != nil {
		err = multierr.Append(err, db.Close())
	}
	return err
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
