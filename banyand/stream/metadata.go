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
	"io"
	"path"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/api/validate"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
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
}

func newSchemaRepo(path string, svc *service) schemaRepo {
	sr := schemaRepo{
		l:        svc.l,
		metadata: svc.metadata,
		Repository: resourceSchema.NewRepository(
			svc.metadata,
			svc.l,
			newSupplier(path, svc),
			resourceSchema.NewMetrics(svc.omr.With(metadataScope)),
		),
	}
	sr.start()
	return sr
}

// NewPortableRepository creates a new portable repository.
func NewPortableRepository(metadata metadata.Repo, l *logger.Logger, metrics *resourceSchema.Metrics) SchemaService {
	r := &schemaRepo{
		l:        l,
		metadata: metadata,
		Repository: resourceSchema.NewPortableRepository(
			metadata,
			l,
			newPortableSupplier(metadata, l),
			metrics,
		),
	}
	r.start()
	return r
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

func (sr *schemaRepo) OnInit(kinds []schema.Kind) (bool, []int64) {
	if len(kinds) != 4 {
		logger.Panicf("invalid kinds: %v", kinds)
		return false, nil
	}
	return true, sr.Repository.Init(schema.KindStream)
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
		irb, ok := metadata.Spec.(*databasev1.IndexRuleBinding)
		if !ok {
			sr.l.Warn().Msg("fail to convert message to IndexRuleBinding")
			return
		}
		if err := validate.IndexRuleBinding(irb); err != nil {
			sr.l.Warn().Err(err).Msg("index rule binding is ignored")
			return
		}
		if irb.GetSubject().Catalog == commonv1.Catalog_CATALOG_STREAM {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			stm, err := sr.metadata.StreamRegistry().GetStream(ctx, &commonv1.Metadata{
				Name:  irb.GetSubject().GetName(),
				Group: metadata.Group,
			})
			cancel()
			if err != nil {
				return
			}
			sr.SendMetadataEvent(resourceSchema.MetadataEvent{
				Typ:      resourceSchema.EventAddOrUpdate,
				Kind:     resourceSchema.EventKindResource,
				Metadata: stm,
			})
		}
	case schema.KindIndexRule:
		if err := validate.IndexRule(metadata.Spec.(*databasev1.IndexRule)); err != nil {
			sr.l.Warn().Err(err).Msg("index rule is ignored")
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		subjects, err := sr.metadata.Subjects(ctx, metadata.Spec.(*databasev1.IndexRule), commonv1.Catalog_CATALOG_STREAM)
		if err != nil {
			return
		}
		for _, sub := range subjects {
			sr.SendMetadataEvent(resourceSchema.MetadataEvent{
				Typ:      resourceSchema.EventAddOrUpdate,
				Kind:     resourceSchema.EventKindResource,
				Metadata: sub.(*databasev1.Stream),
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
		if metadata.Spec.(*databasev1.IndexRuleBinding).GetSubject().Catalog == commonv1.Catalog_CATALOG_STREAM {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			m, err := sr.metadata.StreamRegistry().GetStream(ctx, &commonv1.Metadata{
				Name:  metadata.Name,
				Group: metadata.Group,
			})
			if err != nil {
				return
			}
			// we should update instead of delete
			sr.SendMetadataEvent(resourceSchema.MetadataEvent{
				Typ:      resourceSchema.EventAddOrUpdate,
				Kind:     resourceSchema.EventKindResource,
				Metadata: m,
			})
		}
	case schema.KindIndexRule:
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

var _ resourceSchema.ResourceSupplier = (*supplier)(nil)

type supplier struct {
	metadata metadata.Repo
	pipeline queue.Queue
	omr      observability.MetricsRegistry
	l        *logger.Logger
	path     string
	option   option
}

func newSupplier(path string, svc *service) *supplier {
	return &supplier{
		path:     path,
		metadata: svc.metadata,
		l:        svc.l,
		pipeline: svc.localPipeline,
		option:   svc.option,
		omr:      svc.omr,
	}
}

func (s *supplier) OpenResource(shardNum uint32, supplier resourceSchema.Supplier, spec resourceSchema.Resource) (io.Closer, error) {
	streamSchema := spec.Schema().(*databasev1.Stream)
	return openStream(shardNum, supplier, streamSpec{
		schema:     streamSchema,
		indexRules: spec.IndexRules(),
	}, s.l), nil
}

func (s *supplier) ResourceSchema(md *commonv1.Metadata) (resourceSchema.ResourceSchema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.metadata.StreamRegistry().GetStream(ctx, md)
}

func (s *supplier) OpenDB(groupSchema *commonv1.Group) (io.Closer, error) {
	name := groupSchema.Metadata.Name
	p := common.Position{
		Module:   "stream",
		Database: name,
	}

	opts := storage.TSDBOpts[*tsTable, option]{
		ShardNum:                       groupSchema.ResourceOpts.ShardNum,
		Location:                       path.Join(s.path, groupSchema.Metadata.Name),
		TSTableCreator:                 newTSTable,
		TableMetrics:                   s.newMetrics(p),
		SegmentInterval:                storage.MustToIntervalRule(groupSchema.ResourceOpts.SegmentInterval),
		TTL:                            storage.MustToIntervalRule(groupSchema.ResourceOpts.Ttl),
		Option:                         s.option,
		SeriesIndexFlushTimeoutSeconds: s.option.flushTimeout.Nanoseconds() / int64(time.Second),
		StorageMetricsFactory:          s.omr.With(storageScope.ConstLabels(meter.ToLabelPairs(common.DBLabelNames(), p.DBLabelValues()))),
	}
	return storage.OpenTSDB(
		common.SetPosition(context.Background(), func(_ common.Position) common.Position {
			return p
		}),
		opts)
}

type portableSupplier struct {
	metadata metadata.Repo
	l        *logger.Logger
}

func newPortableSupplier(metadata metadata.Repo, l *logger.Logger) *portableSupplier {
	return &portableSupplier{
		metadata: metadata,
		l:        l,
	}
}

func (s *portableSupplier) ResourceSchema(md *commonv1.Metadata) (resourceSchema.ResourceSchema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.metadata.StreamRegistry().GetStream(ctx, md)
}

func (*portableSupplier) OpenDB(_ *commonv1.Group) (io.Closer, error) {
	panic("do not support open db")
}

func (s *portableSupplier) OpenResource(shardNum uint32, _ resourceSchema.Supplier, spec resourceSchema.Resource) (io.Closer, error) {
	streamSchema := spec.Schema().(*databasev1.Stream)
	return openStream(shardNum, nil, streamSpec{
		schema:     streamSchema,
		indexRules: spec.IndexRules(),
	}, s.l), nil
}
