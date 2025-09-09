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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var _ Resource = (*resourceSpec)(nil)

type resourceSpec struct {
	schema    ResourceSchema
	delegated IndexListener
}

func (rs *resourceSpec) Delegated() IndexListener {
	return rs.delegated
}

func (rs *resourceSpec) Schema() ResourceSchema {
	return rs.schema
}

func (rs *resourceSpec) maxRevision() int64 {
	return rs.schema.GetMetadata().GetModRevision()
}

func (rs *resourceSpec) isNewThan(other *resourceSpec) bool {
	return other.maxRevision() <= rs.maxRevision()
}

const maxWorkerNum = 8

func getWorkerNum() int {
	maxProcs := cgroups.CPUs()
	if maxProcs > maxWorkerNum {
		return maxWorkerNum
	}
	return maxProcs
}

var _ Repository = (*schemaRepo)(nil)

type schemaRepo struct {
	metadata               metadata.Repo
	resourceSupplier       ResourceSupplier
	resourceSchemaSupplier ResourceSchemaSupplier
	l                      *logger.Logger
	closer                 *run.ChannelCloser
	eventCh                chan MetadataEvent
	metrics                *Metrics
	groupMap               sync.Map
	resourceMap            sync.Map
	indexRuleMap           sync.Map
	bindingForwardMap      sync.Map
	bindingBackwardMap     sync.Map
	workerNum              int
	resourceMutex          sync.Mutex
	groupMux               sync.Mutex
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
	metrics *Metrics,
) Repository {
	workNum := getWorkerNum()
	return &schemaRepo{
		metadata:               metadata,
		l:                      l,
		resourceSupplier:       resourceSupplier,
		resourceSchemaSupplier: resourceSupplier,
		eventCh:                make(chan MetadataEvent, workNum),
		workerNum:              workNum,
		closer:                 run.NewChannelCloser(),
		metrics:                metrics,
	}
}

// NewPortableRepository return a new Repository without tsdb.
func NewPortableRepository(
	metadata metadata.Repo,
	l *logger.Logger,
	supplier ResourceSchemaSupplier,
	metrics *Metrics,
) Repository {
	workNum := getWorkerNum()
	return &schemaRepo{
		metadata:               metadata,
		l:                      l,
		resourceSchemaSupplier: supplier,
		eventCh:                make(chan MetadataEvent, workNum),
		workerNum:              workNum,
		closer:                 run.NewChannelCloser(),
		metrics:                metrics,
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
				sr.metrics.totalPanics.Inc(1)
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
							if errors.As(err, schema.ErrGRPCResourceNotFound) {
								err = nil
							}
						case EventKindResource:
							err = sr.storeResource(evt.Metadata)
						case EventKindIndexRule:
							indexRule := evt.Metadata.(*databasev1.IndexRule)
							if indexRule.GetMetadata().GetGroup() == "test-trace-group" {
								sr.l.Info().Str("group", indexRule.GetMetadata().GetGroup()).Msg("index rule")
							}
							sr.storeIndexRule(indexRule)
						case EventKindIndexRuleBinding:
							indexRuleBinding := evt.Metadata.(*databasev1.IndexRuleBinding)
							if indexRuleBinding.GetMetadata().GetGroup() == "test-trace-group" {
								sr.l.Info().Str("group", indexRuleBinding.GetMetadata().GetGroup()).Msg("index rule binding")
							}
							sr.storeIndexRuleBinding(indexRuleBinding)
						}
					case EventDelete:
						switch evt.Kind {
						case EventKindGroup:
							err = sr.deleteGroup(evt.Metadata.GetMetadata())
						case EventKindResource:
							sr.deleteResource(evt.Metadata.GetMetadata())
						case EventKindIndexRule:
							key := getKey(evt.Metadata.GetMetadata())
							sr.indexRuleMap.Delete(key)
						case EventKindIndexRuleBinding:
							indexRuleBinding := evt.Metadata.(*databasev1.IndexRuleBinding)
							col, _ := sr.bindingForwardMap.Load(getKey(&commonv1.Metadata{
								Name:  indexRuleBinding.Subject.GetName(),
								Group: indexRuleBinding.GetMetadata().GetGroup(),
							}))
							if col == nil {
								break
							}
							tMap := col.(*sync.Map)
							key := getKey(indexRuleBinding.GetMetadata())
							tMap.Delete(key)
							for i := range indexRuleBinding.Rules {
								col, _ := sr.bindingBackwardMap.Load(getKey(&commonv1.Metadata{
									Name:  indexRuleBinding.Rules[i],
									Group: indexRuleBinding.GetMetadata().GetGroup(),
								}))
								if col == nil {
									continue
								}
								tMap := col.(*sync.Map)
								tMap.Delete(key)
							}
						}
					}
					if err != nil && !errors.Is(err, schema.ErrClosed) {
						select {
						case <-sr.closer.CloseNotify():
							return
						default:
						}
						sr.l.Err(err).Interface("event", evt).Msg("fail to handle the metadata event. retry...")
						sr.metrics.totalErrs.Inc(1)
						go func() {
							sr.SendMetadataEvent(evt)
							sr.metrics.totalRetries.Inc(1)
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
	g.groupSchema.Store(groupSchema)
	if proto.Equal(groupSchema, prevGroupSchema) {
		return g, nil
	}
	sr.l.Info().Str("group", name).Msg("updating the group resource options")
	g.db.Load().(DB).UpdateOptions(groupSchema.ResourceOpts)
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

func (sr *schemaRepo) LoadAllGroups() []Group {
	var groups []Group
	sr.groupMap.Range(func(_, value any) bool {
		groups = append(groups, value.(*group))
		return true
	})
	return groups
}

func (sr *schemaRepo) LoadResource(metadata *commonv1.Metadata) (Resource, bool) {
	k := getKey(metadata)
	s, ok := sr.resourceMap.Load(k)
	if !ok {
		return nil, false
	}
	return s.(Resource), true
}

func (sr *schemaRepo) storeResource(resourceSchema ResourceSchema) error {
	sr.resourceMutex.Lock()
	defer sr.resourceMutex.Unlock()
	resource := &resourceSpec{
		schema: resourceSchema,
	}
	key := getKey(resourceSchema.GetMetadata())
	pre, loadedPre := sr.resourceMap.Load(key)
	var preResource *resourceSpec
	if loadedPre {
		preResource = pre.(*resourceSpec)
	}
	if loadedPre && preResource.isNewThan(resource) {
		return nil
	}
	sm, err := sr.resourceSchemaSupplier.OpenResource(resource)
	if err != nil {
		return errors.WithMessage(err, "fails to open the resource")
	}
	sm.OnIndexUpdate(sr.indexRules(resourceSchema))
	resource.delegated = sm
	sr.resourceMap.Store(key, resource)
	return nil
}

func (sr *schemaRepo) storeIndexRule(indexRule *databasev1.IndexRule) {
	key := getKey(indexRule.GetMetadata())
	if prev, loaded := sr.indexRuleMap.LoadOrStore(key, indexRule); loaded {
		if prev.(*databasev1.IndexRule).GetMetadata().ModRevision <= indexRule.GetMetadata().ModRevision {
			sr.indexRuleMap.Store(key, indexRule)
			if col, _ := sr.bindingBackwardMap.Load(key); col != nil {
				col.(*sync.Map).Range(func(_, value any) bool {
					sr.updateIndex(value.(*databasev1.IndexRuleBinding))
					return true
				})
			}
		}
	} else {
		if col, _ := sr.bindingBackwardMap.Load(key); col != nil {
			col.(*sync.Map).Range(func(_, value any) bool {
				sr.updateIndex(value.(*databasev1.IndexRuleBinding))
				return true
			})
		}
	}
}

func (sr *schemaRepo) storeIndexRuleBinding(indexRuleBinding *databasev1.IndexRuleBinding) {
	var changed bool
	col, _ := sr.bindingForwardMap.LoadOrStore(getKey(&commonv1.Metadata{
		Name:  indexRuleBinding.Subject.GetName(),
		Group: indexRuleBinding.GetMetadata().GetGroup(),
	}), &sync.Map{})
	tMap := col.(*sync.Map)
	key := getKey(indexRuleBinding.GetMetadata())
	if prev, loaded := tMap.LoadOrStore(key, indexRuleBinding); loaded {
		if prev.(*databasev1.IndexRuleBinding).GetMetadata().ModRevision <= indexRuleBinding.GetMetadata().ModRevision {
			tMap.Store(key, indexRuleBinding)
			changed = true
		}
	} else {
		changed = true
	}
	for i := range indexRuleBinding.Rules {
		col, _ := sr.bindingBackwardMap.LoadOrStore(getKey(&commonv1.Metadata{
			Name:  indexRuleBinding.Rules[i],
			Group: indexRuleBinding.GetMetadata().GetGroup(),
		}), &sync.Map{})
		tMap := col.(*sync.Map)
		key := getKey(indexRuleBinding.GetMetadata())
		if prev, loaded := tMap.LoadOrStore(key, indexRuleBinding); loaded {
			if prev.(*databasev1.IndexRuleBinding).GetMetadata().ModRevision <= indexRuleBinding.GetMetadata().ModRevision {
				tMap.Store(key, indexRuleBinding)
				changed = true
			}
		} else {
			changed = true
		}
	}
	if !changed {
		return
	}
	sr.updateIndex(indexRuleBinding)
}

func (sr *schemaRepo) updateIndex(binding *databasev1.IndexRuleBinding) {
	if r, ok := sr.LoadResource(&commonv1.Metadata{
		Name:  binding.Subject.GetName(),
		Group: binding.GetMetadata().GetGroup(),
	}); ok {
		r.Delegated().OnIndexUpdate(sr.indexRules(r.Schema()))
	}
}

func (sr *schemaRepo) indexRules(schema ResourceSchema) []*databasev1.IndexRule {
	n := schema.GetMetadata().GetName()
	g := schema.GetMetadata().GetGroup()
	col, _ := sr.bindingForwardMap.Load(getKey(&commonv1.Metadata{
		Name:  n,
		Group: g,
	}))
	if col == nil {
		return nil
	}
	tMap := col.(*sync.Map)
	var indexRules []*databasev1.IndexRule
	tMap.Range(func(_, value any) bool {
		indexRuleBinding := value.(*databasev1.IndexRuleBinding)
		for i := range indexRuleBinding.Rules {
			if r, _ := sr.indexRuleMap.Load(getKey(&commonv1.Metadata{
				Name:  indexRuleBinding.Rules[i],
				Group: g,
			})); r != nil {
				indexRules = append(indexRules, r.(*databasev1.IndexRule))
			}
		}
		return true
	})
	return indexRules
}

func getKey(metadata *commonv1.Metadata) string {
	return path.Join(metadata.GetGroup(), metadata.GetName())
}

func (sr *schemaRepo) deleteResource(metadata *commonv1.Metadata) {
	key := getKey(metadata)
	_, _ = sr.resourceMap.LoadAndDelete(key)
}

func (sr *schemaRepo) Close() {
	defer func() {
		if err := recover(); err != nil {
			sr.l.Warn().Interface("err", err).Msg("closing resource")
		}
	}()
	sr.closer.CloseThenWait()
	close(sr.eventCh)

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
	return err
}

func (g *group) isInit() bool {
	return g.GetSchema() != nil
}

func (g *group) GetSchema() *commonv1.Group {
	return g.groupSchema.Load()
}

func (g *group) SupplyTSDB() io.Closer {
	return g.db.Load().(io.Closer)
}

func (g *group) isPortable() bool {
	return g.resourceSupplier == nil
}

func (g *group) close() (err error) {
	if !g.isInit() || g.isPortable() {
		return nil
	}
	return multierr.Append(err, g.SupplyTSDB().Close())
}
