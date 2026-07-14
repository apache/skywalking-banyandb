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
	"runtime/debug"
	"strings"
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
	"github.com/apache/skywalking-banyandb/pkg/initerror"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	_ Resource           = (*resourceSpec)(nil)
	_ RevisionRepository = (*schemaRepo)(nil)
)

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
	latestModRevision      atomic.Int64
	workerNum              int
	closeEventChOnce       sync.Once
	resourceMutex          sync.Mutex
	groupMux               sync.Mutex
}

// SendMetadataEvent applies the given event synchronously. When the downstream
// caches (e.g. pkg/schema.schemaRepo.groupMap and resourceMap) must be coherent
// before the caller returns — for example, when SchemaBarrierService delegates
// through notifyHandlers → this handler → SendMetadataEvent — the caller relies
// on the event being fully applied here. Only transient errors queue the event
// for retry via the background worker; the success path does not touch the
// channel.
func (sr *schemaRepo) SendMetadataEvent(event MetadataEvent) {
	sr.sendMetadataEvent(sr.closer.Ctx(), event)
}

func (sr *schemaRepo) sendMetadataEvent(ctx context.Context, event MetadataEvent) {
	if !sr.closer.AddSender() {
		return
	}
	defer sr.closer.SenderDone()
	// SendMetadataEvent runs synchronously on the caller goroutine (e.g. an
	// etcd-watch handler such as (*schemaRepo).OnAddOrUpdate). A panic from
	// processEvent — string-typed today, error-typed once Step 1b lands —
	// would otherwise propagate to a caller that has no recover() of its own.
	// The classifier branch escalates permanent errors to .Fatal() so
	// runtime registration of an incompatible group exits the process the
	// same way the boot path does; non-permanent panics are logged and
	// requeued onto eventCh so the Watcher worker can retry, mirroring the
	// transient error-return path below.
	defer func() {
		if recovered := recover(); recovered != nil {
			if recErr, isErr := recovered.(error); isErr && initerror.IsPermanent(recErr) {
				sr.l.Fatal().Err(recErr).Interface("event", event).Str("stack", string(debug.Stack())).
					Msg("SendMetadataEvent hit a permanent error, refusing to continue")
			}
			sr.l.Warn().Interface("err", recovered).Interface("event", event).Str("stack", string(debug.Stack())).
				Msg("recovered from SendMetadataEvent panic; requeueing")
			sr.metrics.totalErrs.Inc(1)
			select {
			case sr.eventCh <- event:
			case <-sr.closer.CloseNotify():
			}
		}
	}()
	if err := sr.processEvent(ctx, event); err != nil && !errors.Is(err, schema.ErrClosed) {
		if initerror.IsPermanent(err) {
			sr.l.Fatal().Err(err).Interface("event", event).
				Msg("SendMetadataEvent hit a permanent error, refusing to continue")
		}
		select {
		case <-sr.closer.CloseNotify():
			return
		default:
		}
		sr.l.Err(err).Interface("event", event).Msg("fail to handle the metadata event. retry...")
		sr.metrics.totalErrs.Inc(1)
		select {
		case sr.eventCh <- event:
		case <-sr.closer.CloseNotify():
		}
	}
}

// processEvent applies the event in-place, bumping latestModRevision on success
// so the barrier's downstream-handler watermark catches up before notifyHandlers
// returns to its caller.
func (sr *schemaRepo) processEvent(ctx context.Context, evt MetadataEvent) error {
	if e := sr.l.Debug(); e.Enabled() {
		e.Interface("event", evt).Msg("received an event")
	}
	var err error
	switch evt.Typ {
	case EventAddOrUpdate:
		switch evt.Kind {
		case EventKindGroup:
			_, err = sr.storeGroup(ctx, evt.Metadata.GetMetadata())
			if errors.Is(err, schema.ErrGRPCResourceNotFound) {
				err = nil
			}
		case EventKindResource:
			err = sr.storeResource(evt.Metadata)
		case EventKindIndexRule:
			indexRule := evt.Metadata.(*databasev1.IndexRule)
			sr.storeIndexRule(indexRule)
		case EventKindIndexRuleBinding:
			indexRuleBinding := evt.Metadata.(*databasev1.IndexRuleBinding)
			sr.storeIndexRuleBinding(indexRuleBinding)
		}
	case EventDelete:
		switch evt.Kind {
		case EventKindGroup:
			err = sr.deleteGroup(evt.Metadata.GetMetadata())
		case EventKindResource:
			sr.deleteResource(evt)
		case EventKindIndexRule:
			key := getKey(evt.Metadata.GetMetadata())
			sr.indexRuleMap.Delete(key)
			// Re-index resources bound to this rule so the deleted rule is dropped from
			// their in-memory index; the add path (storeIndexRule) refreshes the same
			// way, the delete path previously did not, leaving stale index config.
			sr.reindexBoundSubjects(key)
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
			// Refresh the subject's index from the remaining bindings so the unbound
			// rules stop being applied (mirrors storeIndexRuleBinding's add-path refresh).
			sr.updateIndex(indexRuleBinding)
		}
	}
	return err
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
		run.Go(sr.closer.Ctx(), "schema-watcher", sr.l, func(ctx context.Context) {
			if !sr.closer.AddReceiver() {
				return
			}
			defer func() {
				sr.closer.ReceiverDone()
				if recovered := recover(); recovered != nil {
					// Bump the panic metric before the classifier branch:
					// .Fatal() calls os.Exit(1), so a deferred Inc would be
					// skipped on the permanent-error escalation path.
					sr.metrics.totalPanics.Inc(1)
					// Classifier is dormant on string-typed panics (today's
					// Panicf sites outside this PR's scope still panic with
					// strings); tsdb.go:176/242 are converted in Step 1b so
					// permanent errors reaching this recover are error-typed.
					// We use .Fatal() not panic() because this is a worker
					// goroutine: panic() would only kill this goroutine and
					// leave the Watcher partially alive.
					if recErr, isErr := recovered.(error); isErr && initerror.IsPermanent(recErr) {
						sr.l.Fatal().Err(recErr).Str("stack", string(debug.Stack())).
							Msg("Watcher hit a permanent error, refusing to continue")
					}
					sr.l.Warn().Interface("err", recovered).Str("stack", string(debug.Stack())).Msg("watching the events")
				}
			}()
			// The Watcher drains events retried via the channel after transient
			// processing errors. The fast path runs synchronously in SendMetadataEvent
			// so the barrier's downstream-handler watermark advances correctly.
			for {
				select {
				case evt, more := <-sr.eventCh:
					if !more {
						return
					}
					if retryErr := sr.processEvent(ctx, evt); retryErr != nil && !errors.Is(retryErr, schema.ErrClosed) {
						if initerror.IsPermanent(retryErr) {
							sr.l.Fatal().Err(retryErr).Interface("event", evt).
								Msg("Watcher hit a permanent error, refusing to continue")
						}
						select {
						case <-sr.closer.CloseNotify():
							return
						default:
						}
						sr.l.Err(retryErr).Interface("event", evt).Msg("retry processing failed, requeueing")
						sr.metrics.totalRetries.Inc(1)
						retryEvent := evt
						run.Go(ctx, "schema-watcher-retry", sr.l, func(retryCtx context.Context) {
							sr.sendMetadataEvent(retryCtx, retryEvent)
						})
					}
				case <-sr.closer.CloseNotify():
					return
				}
			}
		}, run.WithReporter(func(_ context.Context, _ panicdiag.RecoveryResult) {
			sr.metrics.totalPanics.Inc(1)
		}))
	}
}

func (sr *schemaRepo) storeGroup(ctx context.Context, groupMeta *commonv1.Metadata) (*group, error) {
	name := groupMeta.GetName()
	sr.groupMux.Lock()
	defer sr.groupMux.Unlock()
	g, ok := sr.getGroup(name)
	if !ok {
		sr.l.Info().Str("group", name).Msg("creating a tsdb")
		g = sr.createGroup(name)
		if err := g.init(ctx, name); err != nil {
			return nil, err
		}
		if gs := g.GetSchema(); gs != nil {
			sr.updateLatestModRevision(gs.GetMetadata().GetModRevision())
		}
		return g, nil
	}
	if !g.isInit() {
		if err := g.init(ctx, name); err != nil {
			return nil, err
		}
		if gs := g.GetSchema(); gs != nil {
			sr.updateLatestModRevision(gs.GetMetadata().GetModRevision())
		}
		return g, nil
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	groupSchema, err := g.metadata.GroupRegistry().GetGroup(timeoutCtx, name)
	if err != nil {
		return nil, err
	}
	prevGroupSchema := g.GetSchema()
	if groupSchema.GetMetadata().GetModRevision() <= prevGroupSchema.Metadata.ModRevision {
		return g, nil
	}
	g.groupSchema.Store(groupSchema)
	sr.updateLatestModRevision(groupSchema.GetMetadata().GetModRevision())
	if proto.Equal(groupSchema, prevGroupSchema) {
		return g, nil
	}
	sr.l.Info().Str("group", name).Msg("updating the group resource options")
	// Resolve through the supplier so a data node re-applies the matched lifecycle
	// stage's interval/ttl/shardNum; passing the raw group default here would silently
	// clobber a warm/cold node's stage-resolved values. A portable group has no db (and a
	// nil supplier), so guard on db before touching either.
	if db := g.db.Load(); db != nil {
		db.(DB).UpdateOptions(g.resourceSupplier.ResolveResourceOpts(groupSchema))
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
	// Defensive cascade: the orchestrated deletion flow removes every child before the
	// group, but if a group delete arrives without that (a crash mid-sequence, or a
	// non-orchestrated caller) the child entries would dangle in these caches and keep
	// serving a closed tsdb. Purge everything under this group.
	sr.purgeGroupFromCaches(name)
	grp := g.(*group)
	return grp.close()
}

// purgeGroupFromCaches removes every resource/index-rule/binding cache entry belonging
// to the group. Keys are "group/name" (getKey), so the "group/" prefix cannot collide
// with a differently named group.
func (sr *schemaRepo) purgeGroupFromCaches(group string) {
	prefix := group + "/"
	deleteByPrefix := func(m *sync.Map) {
		m.Range(func(k, _ any) bool {
			if ks, ok := k.(string); ok && strings.HasPrefix(ks, prefix) {
				m.Delete(k)
			}
			return true
		})
	}
	sr.resourceMutex.Lock()
	deleteByPrefix(&sr.resourceMap)
	sr.resourceMutex.Unlock()
	deleteByPrefix(&sr.indexRuleMap)
	deleteByPrefix(&sr.bindingForwardMap)
	deleteByPrefix(&sr.bindingBackwardMap)
}

func (sr *schemaRepo) DropGroup(name string) error {
	if g, ok := sr.groupMap.Load(name); ok {
		return g.(*group).drop()
	}
	return nil
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
	sr.updateLatestModRevision(resourceSchema.GetMetadata().GetModRevision())
	return nil
}

func (sr *schemaRepo) storeIndexRule(indexRule *databasev1.IndexRule) {
	key := getKey(indexRule.GetMetadata())
	if prev, loaded := sr.indexRuleMap.LoadOrStore(key, indexRule); loaded {
		if prev.(*databasev1.IndexRule).GetMetadata().ModRevision <= indexRule.GetMetadata().ModRevision {
			sr.indexRuleMap.Store(key, indexRule)
			sr.updateLatestModRevision(indexRule.GetMetadata().GetModRevision())
			sr.reindexBoundSubjects(key)
		}
	} else {
		sr.updateLatestModRevision(indexRule.GetMetadata().GetModRevision())
		sr.reindexBoundSubjects(key)
	}
}

// reindexBoundSubjects refreshes the in-memory index of every resource bound to the index
// rule identified by ruleKey (through the backward binding map). Called whenever the rule
// is added, updated, or deleted, so a stored resource always reflects the current rule set.
func (sr *schemaRepo) reindexBoundSubjects(ruleKey string) {
	col, _ := sr.bindingBackwardMap.Load(ruleKey)
	if col == nil {
		return
	}
	col.(*sync.Map).Range(func(_, value any) bool {
		sr.updateIndex(value.(*databasev1.IndexRuleBinding))
		return true
	})
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
	sr.updateLatestModRevision(indexRuleBinding.GetMetadata().GetModRevision())
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

func (sr *schemaRepo) deleteResource(evt MetadataEvent) {
	key := getKey(evt.Metadata.GetMetadata())
	// Hold resourceMutex across the load/check/delete to keep the revision-guard atomic.
	// storeResource takes the same lock, so a concurrent newer store cannot interleave
	// between the staleness check and LoadAndDelete and have its entry wiped.
	sr.resourceMutex.Lock()
	defer sr.resourceMutex.Unlock()
	if evt.DeleteRevision != 0 {
		if v, ok := sr.resourceMap.Load(key); ok {
			stored := v.(*resourceSpec)
			if evt.DeleteRevision < stored.maxRevision() {
				return
			}
		}
	}
	_, _ = sr.resourceMap.LoadAndDelete(key)
}

func (sr *schemaRepo) updateLatestModRevision(incoming int64) {
	for {
		cur := sr.latestModRevision.Load()
		if incoming <= cur {
			return
		}
		if sr.latestModRevision.CompareAndSwap(cur, incoming) {
			return
		}
	}
}

// LatestModRevision returns the highest mod_revision seen across all stored kinds.
func (sr *schemaRepo) LatestModRevision() int64 {
	return sr.latestModRevision.Load()
}

// ResourceRevision returns the mod_revision for a stored resource and whether it was found.
func (sr *schemaRepo) ResourceRevision(kind schema.Kind, groupName, name string) (int64, bool) {
	key := path.Join(groupName, name)
	switch kind {
	case schema.KindStream, schema.KindMeasure, schema.KindTrace:
		if v, ok := sr.resourceMap.Load(key); ok {
			return v.(*resourceSpec).maxRevision(), true
		}
	case schema.KindIndexRule:
		if v, ok := sr.indexRuleMap.Load(key); ok {
			return v.(*databasev1.IndexRule).GetMetadata().GetModRevision(), true
		}
	case schema.KindGroup:
		if v, ok := sr.groupMap.Load(name); ok {
			grp := v.(*group)
			if gs := grp.GetSchema(); gs != nil {
				return gs.GetMetadata().GetModRevision(), true
			}
		}
	case schema.KindIndexRuleBinding, schema.KindTopNAggregation,
		schema.KindNode, schema.KindProperty, schema.KindMask:
		// schemaRepo only caches resources, index rules, and groups; other kinds
		// (bindings, top-n aggregations, nodes, properties, masks) have no entry here.
	}
	return 0, false
}

// IsAbsent returns true when the given resource is not present in the local cache.
func (sr *schemaRepo) IsAbsent(kind schema.Kind, groupName, name string) bool {
	_, ok := sr.ResourceRevision(kind, groupName, name)
	return !ok
}

func (sr *schemaRepo) Close() {
	defer func() {
		if err := recover(); err != nil {
			sr.l.Warn().Interface("err", err).Msg("closing resource")
		}
	}()
	sr.closer.Close()
	sr.closeEventChOnce.Do(func() {
		close(sr.eventCh)
	})
	sr.closer.Wait()

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

func (g *group) init(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
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
	if g.isPortable() {
		g.groupSchema.Store(groupSchema)
		return nil
	}
	db, err := g.resourceSupplier.OpenDB(groupSchema)
	if err != nil {
		return err
	}
	g.db.Store(db)
	g.groupSchema.Store(groupSchema)
	return nil
}

func (g *group) isInit() bool {
	return g.GetSchema() != nil
}

func (g *group) GetSchema() *commonv1.Group {
	return g.groupSchema.Load()
}

func (g *group) SupplyTSDB() io.Closer {
	if g.db.Load() == nil {
		return nil
	}
	return g.db.Load().(io.Closer)
}

func (g *group) isPortable() bool {
	return g.resourceSupplier == nil
}

func (g *group) close() (err error) {
	if !g.isInit() || g.isPortable() {
		return nil
	}
	tsdb := g.SupplyTSDB()
	if tsdb == nil {
		return nil
	}
	return multierr.Append(err, tsdb.Close())
}

func (g *group) drop() error {
	if !g.isInit() || g.isPortable() {
		return nil
	}
	db := g.db.Load()
	if db == nil {
		return nil
	}
	return db.(DB).Drop()
}
