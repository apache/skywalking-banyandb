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

package measure

import (
	"context"
	"fmt"
	"io"
	"path"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/api/validate"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	// TopNSchemaName is the name of the top n result schema.
	TopNSchemaName = "_top_n_result"
	// TopNTagFamily is the tag family name of the topN result measure.
	TopNTagFamily = "_topN"
	// TopNFieldName is the field name of the topN result measure.
	TopNFieldName = "value"
)

var (
	metadataScope = measureScope.SubScope("metadata")

	topNFieldsSpec = []*databasev1.FieldSpec{{
		Name:              TopNFieldName,
		FieldType:         databasev1.FieldType_FIELD_TYPE_DATA_BINARY,
		EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
		CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
	}}
	// TopNTagNames is the tag names of the topN result measure.
	TopNTagNames = []string{"name", "direction", "group"}
)

// SchemaService allows querying schema information.
type SchemaService interface {
	Query
	Close()
}
type schemaRepo struct {
	resourceSchema.Repository
	metadata         metadata.Repo
	pipeline         queue.Queue
	l                *logger.Logger
	topNProcessorMap sync.Map
	path             string
}

func newSchemaRepo(path string, svc *service, nodeLabels map[string]string) *schemaRepo {
	sr := &schemaRepo{
		path:     path,
		l:        svc.l,
		metadata: svc.metadata,
		pipeline: svc.localPipeline,
	}
	sr.Repository = resourceSchema.NewRepository(
		svc.metadata,
		svc.l,
		newSupplier(path, svc, sr, nodeLabels),
		resourceSchema.NewMetrics(svc.omr.With(metadataScope)),
	)
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
		RegisterHandler("measure", schema.KindGroup|schema.KindMeasure|schema.KindIndexRuleBinding|schema.KindIndexRule|schema.KindTopNAggregation,
			sr)
}

func (sr *schemaRepo) Measure(metadata *commonv1.Metadata) (Measure, error) {
	sm, ok := sr.loadMeasure(metadata)
	if !ok {
		return nil, errors.WithStack(ErrMeasureNotExist)
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
	if len(kinds) != 5 {
		logger.Panicf("unexpected kinds: %v", kinds)
		return false, nil
	}
	groupNames, revs := sr.Repository.Init(schema.KindMeasure)
	for i := range groupNames {
		sr.createTopNResultMeasure(context.Background(), sr.metadata.MeasureRegistry(), groupNames[i])
	}
	return true, revs
}

func (sr *schemaRepo) OnAddOrUpdate(metadata schema.Metadata) {
	switch metadata.Kind {
	case schema.KindGroup:
		g := metadata.Spec.(*commonv1.Group)
		if g.Catalog != commonv1.Catalog_CATALOG_MEASURE {
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
		sr.createTopNResultMeasure(context.Background(), sr.metadata.MeasureRegistry(), g.Metadata.Name)
	case schema.KindMeasure:
		m := metadata.Spec.(*databasev1.Measure)
		if err := validate.Measure(m); err != nil {
			sr.l.Warn().Err(err).Msg("measure is ignored")
			return
		}
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventAddOrUpdate,
			Kind:     resourceSchema.EventKindResource,
			Metadata: m,
		})
	case schema.KindIndexRuleBinding:
		if irb, ok := metadata.Spec.(*databasev1.IndexRuleBinding); ok {
			if err := validate.IndexRuleBinding(irb); err != nil {
				sr.l.Warn().Err(err).Msg("index rule binding is ignored")
				return
			}
			if irb.GetSubject().Catalog == commonv1.Catalog_CATALOG_MEASURE {
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
	case schema.KindTopNAggregation:
		topNSchema := metadata.Spec.(*databasev1.TopNAggregation)
		if err := validate.TopNAggregation(topNSchema); err != nil {
			sr.l.Warn().Err(err).Msg("topNAggregation is ignored")
			return
		}
		manager := sr.getSteamingManager(topNSchema.SourceMeasure, sr.pipeline)
		manager.register(topNSchema)
	default:
	}
}

func (sr *schemaRepo) OnDelete(metadata schema.Metadata) {
	switch metadata.Kind {
	case schema.KindGroup:
		g := metadata.Spec.(*commonv1.Group)
		if g.Catalog != commonv1.Catalog_CATALOG_MEASURE {
			return
		}
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventDelete,
			Kind:     resourceSchema.EventKindGroup,
			Metadata: g,
		})
	case schema.KindMeasure:
		m := metadata.Spec.(*databasev1.Measure)
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventDelete,
			Kind:     resourceSchema.EventKindResource,
			Metadata: m,
		})
		sr.stopSteamingManager(m.GetMetadata())
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
	case schema.KindTopNAggregation:
		topNAggregation := metadata.Spec.(*databasev1.TopNAggregation)
		sr.stopSteamingManager(topNAggregation.SourceMeasure)
	default:
	}
}

func (sr *schemaRepo) Close() {
	var err error
	sr.topNProcessorMap.Range(func(_, val any) bool {
		manager := val.(*topNProcessorManager)
		err = multierr.Append(err, manager.Close())
		return true
	})
	if err != nil {
		sr.l.Error().Err(err).Msg("faced error when closing schema repository")
	}
	sr.Repository.Close()
}

func (sr *schemaRepo) loadMeasure(metadata *commonv1.Metadata) (*measure, bool) {
	r, ok := sr.LoadResource(metadata)
	if !ok {
		return nil, false
	}
	s, ok := r.Delegated().(*measure)
	return s, ok
}

func (sr *schemaRepo) loadTSDB(groupName string) (storage.TSDB[*tsTable, option], error) {
	if sr == nil {
		return nil, fmt.Errorf("schemaRepo is nil")
	}
	g, ok := sr.LoadGroup(groupName)
	if !ok {
		return nil, fmt.Errorf("group %s not found", groupName)
	}
	db := g.SupplyTSDB()
	if db == nil {
		return nil, fmt.Errorf("group %s not found", groupName)
	}
	return db.(storage.TSDB[*tsTable, option]), nil
}

func (sr *schemaRepo) createTopNResultMeasure(ctx context.Context, measureSchemaRegistry schema.Measure, group string) {
	md := GetTopNSchemaMetadata(group)
	operation := func() error {
		m, err := measureSchemaRegistry.GetMeasure(ctx, md)
		if err != nil && !errors.Is(err, schema.ErrGRPCResourceNotFound) {
			return errors.WithMessagef(err, "fail to get %s", md)
		}
		if m != nil {
			return nil
		}

		m = GetTopNSchema(md)
		if _, innerErr := measureSchemaRegistry.CreateMeasure(ctx, m); innerErr != nil {
			if !errors.Is(innerErr, schema.ErrGRPCAlreadyExists) {
				return errors.WithMessagef(innerErr, "fail to create new topN measure %s", m)
			}
		}
		return nil
	}

	backoffStrategy := backoff.NewExponentialBackOff()
	backoffStrategy.MaxElapsedTime = 2 * time.Minute

	err := backoff.Retry(operation, backoffStrategy)
	if err != nil {
		logger.Panicf("fail to create topN measure %s: %v", md, err)
	}
}

var _ resourceSchema.ResourceSupplier = (*supplier)(nil)

type supplier struct {
	metadata   metadata.Repo
	omr        observability.MetricsRegistry
	l          *logger.Logger
	pm         *protector.Memory
	schemaRepo *schemaRepo
	nodeLabels map[string]string
	path       string
	option     option
}

func newSupplier(path string, svc *service, sr *schemaRepo, nodeLabels map[string]string) *supplier {
	return &supplier{
		path:       path,
		metadata:   svc.metadata,
		l:          svc.l,
		option:     svc.option,
		omr:        svc.omr,
		pm:         svc.pm,
		schemaRepo: sr,
		nodeLabels: nodeLabels,
	}
}

func (s *supplier) OpenResource(spec resourceSchema.Resource) (resourceSchema.IndexListener, error) {
	measureSchema := spec.Schema().(*databasev1.Measure)
	return openMeasure(measureSpec{
		schema: measureSchema,
	}, s.l, s.pm, s.schemaRepo)
}

func (s *supplier) ResourceSchema(md *commonv1.Metadata) (resourceSchema.ResourceSchema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.metadata.MeasureRegistry().GetMeasure(ctx, md)
}

func (s *supplier) OpenDB(groupSchema *commonv1.Group) (resourceSchema.DB, error) {
	name := groupSchema.Metadata.Name
	p := common.Position{
		Module:   "measure",
		Database: name,
	}
	metrics, factory := s.newMetrics(p)
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
	opts := storage.TSDBOpts[*tsTable, option]{
		ShardNum:                       shardNum,
		Location:                       path.Join(s.path, groupSchema.Metadata.Name),
		TSTableCreator:                 newTSTable,
		TableMetrics:                   metrics,
		SegmentInterval:                storage.MustToIntervalRule(segInterval),
		TTL:                            storage.MustToIntervalRule(ttl),
		Option:                         s.option,
		SeriesIndexFlushTimeoutSeconds: s.option.flushTimeout.Nanoseconds() / int64(time.Second),
		SeriesIndexCacheMaxBytes:       int(s.option.seriesCacheMaxSize),
		StorageMetricsFactory:          factory,
		SegmentBoundaryUpdateFn:        s.metadata.UpdateSegmentsBoundary,
		SegmentIdleTimeout:             segmentIdleTimeout,
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
	return s.metadata.MeasureRegistry().GetMeasure(ctx, md)
}

func (*portableSupplier) OpenDB(_ *commonv1.Group) (io.Closer, error) {
	panic("do not support open db")
}

func (s *portableSupplier) OpenResource(spec resourceSchema.Resource) (resourceSchema.IndexListener, error) {
	measureSchema := spec.Schema().(*databasev1.Measure)
	return openMeasure(measureSpec{
		schema: measureSchema,
	}, s.l, nil, nil)
}

// GetTopNSchema returns the schema of the topN result measure.
func GetTopNSchema(md *commonv1.Metadata) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: md,
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: TopNTagFamily,
				Tags: []*databasev1.TagSpec{
					{Name: TopNTagNames[0], Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: TopNTagNames[1], Type: databasev1.TagType_TAG_TYPE_INT},
					{Name: TopNTagNames[2], Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
		Fields: topNFieldsSpec,
		Entity: &databasev1.Entity{
			TagNames: TopNTagNames,
		},
	}
}

// GetTopNSchemaMetadata returns the metadata of the topN result measure.
func GetTopNSchemaMetadata(group string) *commonv1.Metadata {
	return &commonv1.Metadata{
		Name:  TopNSchemaName,
		Group: group,
	}
}

func getKey(metadata *commonv1.Metadata) string {
	return path.Join(metadata.GetGroup(), metadata.GetName())
}
