// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/api/validate"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/internal/wqueue"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var metadataScope = streamScope.SubScope("metadata")

// SchemaService allows querying schema information.
type SchemaService interface {
	Query
	Close()
}
type schemaRepo struct {
	resourceSchema.Repository
	l        *logger.Logger
	metadata metadata.Repo
	path     string
}

func newSchemaRepo(path string, svc *standalone, nodeLabels map[string]string) schemaRepo {
	sr := schemaRepo{
		l:        svc.l,
		path:     path,
		metadata: svc.metadata,
		Repository: resourceSchema.NewRepository(
			svc.metadata,
			svc.l,
			newSupplier(path, svc, nodeLabels),
			resourceSchema.NewMetrics(svc.omr.With(metadataScope)),
		),
	}
	sr.start()
	return sr
}

func newLiaisonSchemaRepo(path string, svc *liaison, streamDataNodeRegistry grpc.NodeRegistry) schemaRepo {
	sr := schemaRepo{
		l:        svc.l,
		path:     path,
		metadata: svc.metadata,
		Repository: resourceSchema.NewRepository(
			svc.metadata,
			svc.l,
			newQueueSupplier(path, svc, streamDataNodeRegistry),
			resourceSchema.NewMetrics(svc.omr.With(metadataScope)),
		),
	}
	sr.start()
	return sr
}

func (sr *schemaRepo) start() {
	sr.Watcher()
	sr.metadata.
		RegisterHandler("stream", schema.KindGroup|schema.KindStream|schema.KindIndexRuleBinding|schema.KindIndexRule,
			sr)
}

func (sr *schemaRepo) Stream(metadata *commonv1.Metadata) (Stream, error) {
	sm, ok := sr.loadStream(metadata)
	if !ok {
		return nil, errors.WithStack(ErrStreamNotExist)
	}
	return sm, nil
}

func (sr *schemaRepo) GetRemovalSegmentsTimeRange(group string) *timestamp.TimeRange {
	g, ok := sr.LoadGroup(group)
	if !ok {
		return nil
	}
	db := g.SupplyTSDB()
	if db == nil {
		return nil
	}
	return db.(storage.TSDB[*tsTable, option]).GetExpiredSegmentsTimeRange()
}

func (sr *schemaRepo) OnInit(kinds []schema.Kind) (bool, []int64) {
	if len(kinds) != 4 {
		logger.Panicf("invalid kinds: %v", kinds)
		return false, nil
	}
	_, revs := sr.Repository.Init(schema.KindStream)
	return true, revs
}

func (sr *schemaRepo) OnAddOrUpdate(metadata schema.Metadata) {
	switch metadata.Kind {
	case schema.KindGroup:
		g := metadata.Spec.(*commonv1.Group)
		if g.Catalog != commonv1.Catalog_CATALOG_STREAM {
			return
		}
		if err := validate.GroupForStreamOrMeasure(g); err != nil {
			sr.l.Warn().Err(err).Msg("group is ignored")
			return
		}
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventAddOrUpdate,
			Kind:     resourceSchema.EventKindGroup,
			Metadata: g,
		})
	case schema.KindStream:
		if err := validate.Stream(metadata.Spec.(*databasev1.Stream)); err != nil {
			sr.l.Warn().Err(err).Msg("stream is ignored")
			return
		}
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventAddOrUpdate,
			Kind:     resourceSchema.EventKindResource,
			Metadata: metadata.Spec.(*databasev1.Stream),
		})
	case schema.KindIndexRuleBinding:
		if irb, ok := metadata.Spec.(*databasev1.IndexRuleBinding); ok {
			if err := validate.IndexRuleBinding(irb); err != nil {
				sr.l.Warn().Err(err).Msg("index rule binding is ignored")
				return
			}
			if irb.GetSubject().Catalog == commonv1.Catalog_CATALOG_STREAM {
				sr.SendMetadataEvent(resourceSchema.MetadataEvent{
					Typ:      resourceSchema.EventAddOrUpdate,
					Kind:     resourceSchema.EventKindIndexRuleBinding,
					Metadata: irb,
				})
			}
		}
	case schema.KindIndexRule:
		if ir, ok := metadata.Spec.(*databasev1.IndexRule); ok {
			if err := validate.IndexRule(metadata.Spec.(*databasev1.IndexRule)); err != nil {
				sr.l.Warn().Err(err).Msg("index rule is ignored")
				return
			}
			sr.SendMetadataEvent(resourceSchema.MetadataEvent{
				Typ:      resourceSchema.EventAddOrUpdate,
				Kind:     resourceSchema.EventKindIndexRule,
				Metadata: ir,
			})
		}
	default:
	}
}

func (sr *schemaRepo) OnDelete(metadata schema.Metadata) {
	switch metadata.Kind {
	case schema.KindGroup:
		g := metadata.Spec.(*commonv1.Group)
		if g.Catalog != commonv1.Catalog_CATALOG_STREAM {
			return
		}
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventDelete,
			Kind:     resourceSchema.EventKindGroup,
			Metadata: g,
		})
	case schema.KindStream:
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventDelete,
			Kind:     resourceSchema.EventKindResource,
			Metadata: metadata.Spec.(*databasev1.Stream),
		})
	case schema.KindIndexRuleBinding:
		if binding, ok := metadata.Spec.(*databasev1.IndexRuleBinding); ok {
			if binding.GetSubject().Catalog == commonv1.Catalog_CATALOG_MEASURE {
				sr.SendMetadataEvent(resourceSchema.MetadataEvent{
					Typ:      resourceSchema.EventDelete,
					Kind:     resourceSchema.EventKindIndexRuleBinding,
					Metadata: metadata.Spec.(*databasev1.IndexRuleBinding),
				})
			}
		}
	case schema.KindIndexRule:
		if rule, ok := metadata.Spec.(*databasev1.IndexRule); ok {
			sr.SendMetadataEvent(resourceSchema.MetadataEvent{
				Typ:      resourceSchema.EventDelete,
				Kind:     resourceSchema.EventKindIndexRule,
				Metadata: rule,
			})
		}
	default:
	}
}

func (sr *schemaRepo) loadStream(metadata *commonv1.Metadata) (*stream, bool) {
	r, ok := sr.LoadResource(metadata)
	if !ok {
		return nil, false
	}
	s, ok := r.Delegated().(*stream)
	return s, ok
}

func (sr *schemaRepo) loadTSDB(groupName string) (storage.TSDB[*tsTable, option], error) {
	g, ok := sr.LoadGroup(groupName)
	if !ok {
		return nil, fmt.Errorf("group %s not found", groupName)
	}
	db := g.SupplyTSDB()
	if db == nil {
		return nil, fmt.Errorf("tsdb for group %s not found", groupName)
	}
	return db.(storage.TSDB[*tsTable, option]), nil
}

func (sr *schemaRepo) loadQueue(groupName string) (*wqueue.Queue[*tsTable, option], error) {
	g, ok := sr.LoadGroup(groupName)
	if !ok {
		return nil, fmt.Errorf("group %s not found", groupName)
	}
	db := g.SupplyTSDB()
	if db == nil {
		return nil, fmt.Errorf("queue for group %s not found", groupName)
	}
	return db.(*wqueue.Queue[*tsTable, option]), nil
}

var _ resourceSchema.ResourceSupplier = (*supplier)(nil)

type supplier struct {
	metadata   metadata.Repo
	omr        observability.MetricsRegistry
	pm         protector.Memory
	l          *logger.Logger
	schemaRepo *schemaRepo
	nodeLabels map[string]string
	path       string
	option     option
}

func newSupplier(path string, svc *standalone, nodeLabels map[string]string) *supplier {
	if svc.pm == nil {
		svc.l.Panic().Msg("CRITICAL: svc.pm is nil in newSupplier")
	}
	opt := svc.option
	opt.protector = svc.pm

	if opt.protector == nil {
		svc.l.Panic().Msg("CRITICAL: opt.protector is still nil after assignment")
	}

	return &supplier{
		metadata:   svc.metadata,
		l:          svc.l,
		option:     opt,
		omr:        svc.omr,
		pm:         svc.pm,
		path:       path,
		schemaRepo: &svc.schemaRepo,
		nodeLabels: nodeLabels,
	}
}

func (s *supplier) OpenResource(spec resourceSchema.Resource) (resourceSchema.IndexListener, error) {
	streamSchema := spec.Schema().(*databasev1.Stream)
	return openStream(streamSpec{
		schema: streamSchema,
	}, s.l, s.pm, s.schemaRepo), nil
}

func (s *supplier) ResourceSchema(md *commonv1.Metadata) (resourceSchema.ResourceSchema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.metadata.StreamRegistry().GetStream(ctx, md)
}

func (s *supplier) OpenDB(groupSchema *commonv1.Group) (resourceSchema.DB, error) {
	name := groupSchema.Metadata.Name
	p := common.Position{
		Module:   "stream",
		Database: name,
	}
	ro := groupSchema.ResourceOpts
	if ro == nil {
		return nil, fmt.Errorf("no resource opts in group %s", name)
	}
	shardNum := ro.ShardNum
	ttl := ro.Ttl
	segInterval := ro.SegmentInterval
	segmentIdleTimeout := time.Duration(0)
	if len(ro.Stages) > 0 && len(s.nodeLabels) > 0 {
		var ttlNum uint32
		for _, st := range ro.Stages {
			if st.Ttl.Unit != ro.Ttl.Unit {
				return nil, fmt.Errorf("ttl unit %s is not consistent with stage %s", ro.Ttl.Unit, st.Ttl.Unit)
			}
			selector, err := pub.ParseLabelSelector(st.NodeSelector)
			if err != nil {
				return nil, errors.WithMessagef(err, "failed to parse node selector %s", st.NodeSelector)
			}
			ttlNum += st.Ttl.Num
			if !selector.Matches(s.nodeLabels) {
				continue
			}
			ttl.Num += ttlNum
			shardNum = st.ShardNum
			segInterval = st.SegmentInterval
			if st.Close {
				segmentIdleTimeout = 5 * time.Minute
			}
			break
		}
	}
	group := groupSchema.Metadata.Name
	opts := storage.TSDBOpts[*tsTable, option]{
		ShardNum:                       shardNum,
		Location:                       path.Join(s.path, group),
		TSTableCreator:                 newTSTable,
		TableMetrics:                   s.newMetrics(p),
		SegmentInterval:                storage.MustToIntervalRule(segInterval),
		TTL:                            storage.MustToIntervalRule(ttl),
		Option:                         s.option,
		SeriesIndexFlushTimeoutSeconds: s.option.flushTimeout.Nanoseconds() / int64(time.Second),
		SeriesIndexCacheMaxBytes:       int(s.option.seriesCacheMaxSize),
		StorageMetricsFactory:          s.omr.With(storageScope.ConstLabels(meter.ToLabelPairs(common.DBLabelNames(), p.DBLabelValues()))),
		SegmentIdleTimeout:             segmentIdleTimeout,
		MemoryLimit:                    s.pm.GetLimit(),
	}
	return storage.OpenTSDB(
		common.SetPosition(context.Background(), func(_ common.Position) common.Position {
			return p
		}),
		opts, nil, group,
	)
}

var _ resourceSchema.ResourceSupplier = (*queueSupplier)(nil)

type queueSupplier struct {
	metadata               metadata.Repo
	omr                    observability.MetricsRegistry
	pm                     protector.Memory
	streamDataNodeRegistry grpc.NodeRegistry
	l                      *logger.Logger
	schemaRepo             *schemaRepo
	path                   string
	option                 option
}

func newQueueSupplier(path string, svc *liaison, streamDataNodeRegistry grpc.NodeRegistry) *queueSupplier {
	if svc.pm == nil {
		svc.l.Panic().Msg("CRITICAL: svc.pm is nil in newSupplier")
	}
	opt := svc.option
	opt.protector = svc.pm

	if opt.protector == nil {
		svc.l.Panic().Msg("CRITICAL: opt.protector is still nil after assignment")
	}

	return &queueSupplier{
		metadata:               svc.metadata,
		l:                      svc.l,
		option:                 opt,
		omr:                    svc.omr,
		pm:                     svc.pm,
		path:                   path,
		schemaRepo:             &svc.schemaRepo,
		streamDataNodeRegistry: streamDataNodeRegistry,
	}
}

func (s *queueSupplier) OpenResource(spec resourceSchema.Resource) (resourceSchema.IndexListener, error) {
	streamSchema := spec.Schema().(*databasev1.Stream)
	return openStream(streamSpec{
		schema: streamSchema,
	}, s.l, s.pm, s.schemaRepo), nil
}

func (s *queueSupplier) ResourceSchema(md *commonv1.Metadata) (resourceSchema.ResourceSchema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.metadata.StreamRegistry().GetStream(ctx, md)
}

func (s *queueSupplier) OpenDB(groupSchema *commonv1.Group) (resourceSchema.DB, error) {
	name := groupSchema.Metadata.Name
	p := common.Position{
		Module:   "stream",
		Database: name,
	}
	ro := groupSchema.ResourceOpts
	if ro == nil {
		return nil, fmt.Errorf("no resource opts in group %s", name)
	}
	shardNum := ro.ShardNum
	group := groupSchema.Metadata.Name
	opts := wqueue.Opts[*tsTable, option]{
		Group:           group,
		ShardNum:        shardNum,
		SegmentInterval: storage.MustToIntervalRule(ro.SegmentInterval),
		Location:        path.Join(s.path, group),
		Option:          s.option,
		Metrics:         s.newMetrics(p),
		SubQueueCreator: newWriteQueue,
		GetNodes: func(shardID common.ShardID) []string {
			copies := ro.Replicas + 1
			nodes := make([]string, 0, copies)
			for i := uint32(0); i < copies; i++ {
				nodeID, err := s.streamDataNodeRegistry.Locate(group, "", uint32(shardID), i)
				if err != nil {
					s.l.Error().Err(err).Str("group", group).Uint32("shard", uint32(shardID)).Uint32("copy", i).Msg("failed to locate node")
					return nil
				}
				nodes = append(nodes, nodeID)
			}
			return nodes
		},
	}
	return wqueue.Open(
		common.SetPosition(context.Background(), func(_ common.Position) common.Position {
			return p
		}),
		opts, group,
	)
}
