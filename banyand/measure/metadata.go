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
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
)

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
		),
	}
	sr.start()
	return sr
}

// NewPortableRepository creates a new portable repository.
func NewPortableRepository(metadata metadata.Repo, l *logger.Logger) SchemaService {
	r := &schemaRepo{
		l:        l,
		metadata: metadata,
		Repository: resourceSchema.NewPortableRepository(
			metadata,
			l,
			newPortableSupplier(metadata, l),
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

func (sr *schemaRepo) OnAddOrUpdate(metadata schema.Metadata) {
	switch metadata.Kind {
	case schema.KindGroup:
		g := metadata.Spec.(*commonv1.Group)
		if g.Catalog != commonv1.Catalog_CATALOG_MEASURE {
			return
		}
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventAddOrUpdate,
			Kind:     resourceSchema.EventKindGroup,
			Metadata: g.GetMetadata(),
		})
	case schema.KindMeasure:
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventAddOrUpdate,
			Kind:     resourceSchema.EventKindResource,
			Metadata: metadata.Spec.(*databasev1.Measure).GetMetadata(),
		})
	case schema.KindIndexRuleBinding:
		irb, ok := metadata.Spec.(*databasev1.IndexRuleBinding)
		if !ok {
			sr.l.Warn().Msg("fail to convert message to IndexRuleBinding")
			return
		}
		if irb.GetSubject().Catalog == commonv1.Catalog_CATALOG_MEASURE {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			stm, err := sr.metadata.MeasureRegistry().GetMeasure(ctx, &commonv1.Metadata{
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
				Metadata: stm.GetMetadata(),
			})
		}
	case schema.KindIndexRule:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		subjects, err := sr.metadata.Subjects(ctx, metadata.Spec.(*databasev1.IndexRule), commonv1.Catalog_CATALOG_MEASURE)
		if err != nil {
			return
		}
		for _, sub := range subjects {
			sr.SendMetadataEvent(resourceSchema.MetadataEvent{
				Typ:      resourceSchema.EventAddOrUpdate,
				Kind:     resourceSchema.EventKindResource,
				Metadata: sub.(*databasev1.Measure).GetMetadata(),
			})
		}
	case schema.KindTopNAggregation:
		// createOrUpdate TopN schemas in advance
		_, err := createOrUpdateTopNMeasure(context.Background(), sr.metadata.MeasureRegistry(), metadata.Spec.(*databasev1.TopNAggregation))
		if err != nil {
			return
		}
		// reload source measure
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventAddOrUpdate,
			Kind:     resourceSchema.EventKindResource,
			Metadata: metadata.Spec.(*databasev1.TopNAggregation).GetSourceMeasure(),
		})
	default:
	}
}

func createOrUpdateTopNMeasure(ctx context.Context, measureSchemaRegistry schema.Measure, topNSchema *databasev1.TopNAggregation) (*databasev1.Measure, error) {
	oldTopNSchema, err := measureSchemaRegistry.GetMeasure(ctx, topNSchema.GetMetadata())
	if err != nil && !errors.Is(err, schema.ErrGRPCResourceNotFound) {
		return nil, err
	}

	sourceMeasureSchema, err := measureSchemaRegistry.GetMeasure(ctx, topNSchema.GetSourceMeasure())
	if err != nil {
		return nil, err
	}

	tagNames := sourceMeasureSchema.GetEntity().GetTagNames()
	seriesSpecs := make([]*databasev1.TagSpec, 0, len(tagNames))

	for _, tagName := range tagNames {
		var found bool
		for _, fSpec := range sourceMeasureSchema.GetTagFamilies() {
			for _, tSpec := range fSpec.GetTags() {
				if tSpec.GetName() == tagName {
					seriesSpecs = append(seriesSpecs, tSpec)
					found = true
					goto CHECK
				}
			}
		}

	CHECK:
		if !found {
			return nil, fmt.Errorf("fail to find tag spec %s", tagName)
		}
	}

	// create a new "derived" measure for TopN result
	newTopNMeasure := &databasev1.Measure{
		Metadata: topNSchema.GetMetadata(),
		Interval: sourceMeasureSchema.GetInterval(),
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: topNTagFamily,
				Tags: append([]*databasev1.TagSpec{
					{
						Name: "measure_id",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
				}, seriesSpecs...),
			},
		},
		Fields: []*databasev1.FieldSpec{topNValueFieldSpec},
		Entity: sourceMeasureSchema.GetEntity(),
	}
	if oldTopNSchema == nil {
		if _, innerErr := measureSchemaRegistry.CreateMeasure(ctx, newTopNMeasure); innerErr != nil {
			return nil, innerErr
		}
		return newTopNMeasure, nil
	}
	// compare with the old one
	if cmp.Diff(newTopNMeasure, oldTopNSchema,
		protocmp.IgnoreUnknown(),
		protocmp.IgnoreFields(&databasev1.Measure{}, "updated_at"),
		protocmp.IgnoreFields(&commonv1.Metadata{}, "id", "create_revision", "mod_revision"),
		protocmp.Transform()) == "" {
		return oldTopNSchema, nil
	}
	// update
	if _, err = measureSchemaRegistry.UpdateMeasure(ctx, newTopNMeasure); err != nil {
		return nil, err
	}
	return newTopNMeasure, nil
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
			Metadata: g.GetMetadata(),
		})
	case schema.KindMeasure:
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventDelete,
			Kind:     resourceSchema.EventKindResource,
			Metadata: metadata.Spec.(*databasev1.Measure).GetMetadata(),
		})
	case schema.KindIndexRuleBinding:
		if metadata.Spec.(*databasev1.IndexRuleBinding).GetSubject().Catalog == commonv1.Catalog_CATALOG_MEASURE {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			m, err := sr.metadata.MeasureRegistry().GetMeasure(ctx, &commonv1.Metadata{
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
				Metadata: m.GetMetadata(),
			})
		}
	case schema.KindIndexRule:
	case schema.KindTopNAggregation:
		err := sr.removeTopNMeasure(metadata.Spec.(*databasev1.TopNAggregation).GetSourceMeasure())
		if err != nil {
			return
		}
		// we should update instead of delete
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventAddOrUpdate,
			Kind:     resourceSchema.EventKindResource,
			Metadata: metadata.Spec.(*databasev1.TopNAggregation).GetSourceMeasure(),
		})
	default:
	}
}

func (sr *schemaRepo) removeTopNMeasure(metadata *commonv1.Metadata) error {
	_, err := sr.metadata.MeasureRegistry().DeleteMeasure(context.Background(), metadata)
	return err
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
	g, ok := sr.LoadGroup(groupName)
	if !ok {
		return nil, fmt.Errorf("group %s not found", groupName)
	}
	return g.SupplyTSDB().(storage.TSDB[*tsTable, option]), nil
}

var _ resourceSchema.ResourceSupplier = (*supplier)(nil)

type supplier struct {
	metadata metadata.Repo
	pipeline queue.Queue
	option   option
	l        *logger.Logger
	path     string
}

func newSupplier(path string, svc *service) *supplier {
	return &supplier{
		path:     path,
		metadata: svc.metadata,
		l:        svc.l,
		pipeline: svc.localPipeline,
		option:   svc.option,
	}
}

func (s *supplier) OpenResource(shardNum uint32, supplier resourceSchema.Supplier, spec resourceSchema.Resource) (io.Closer, error) {
	measureSchema := spec.Schema().(*databasev1.Measure)
	return openMeasure(shardNum, supplier, measureSpec{
		schema:           measureSchema,
		indexRules:       spec.IndexRules(),
		topNAggregations: spec.TopN(),
	}, s.l, s.pipeline)
}

func (s *supplier) ResourceSchema(md *commonv1.Metadata) (resourceSchema.ResourceSchema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.metadata.MeasureRegistry().GetMeasure(ctx, md)
}

func (s *supplier) OpenDB(groupSchema *commonv1.Group) (io.Closer, error) {
	opts := storage.TSDBOpts[*tsTable, option]{
		ShardNum:                       groupSchema.ResourceOpts.ShardNum,
		Location:                       path.Join(s.path, groupSchema.Metadata.Name),
		TSTableCreator:                 newTSTable,
		SegmentInterval:                storage.MustToIntervalRule(groupSchema.ResourceOpts.SegmentInterval),
		TTL:                            storage.MustToIntervalRule(groupSchema.ResourceOpts.Ttl),
		Option:                         s.option,
		SeriesIndexFlushTimeoutSeconds: s.option.flushTimeout.Nanoseconds() / int64(time.Second),
	}
	name := groupSchema.Metadata.Name
	return storage.OpenTSDB(
		common.SetPosition(context.Background(), func(p common.Position) common.Position {
			p.Module = "measure"
			p.Database = name
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

func (s *portableSupplier) OpenResource(shardNum uint32, _ resourceSchema.Supplier, spec resourceSchema.Resource) (io.Closer, error) {
	measureSchema := spec.Schema().(*databasev1.Measure)
	return openMeasure(shardNum, nil, measureSpec{
		schema:           measureSchema,
		indexRules:       spec.IndexRules(),
		topNAggregations: spec.TopN(),
	}, s.l, nil)
}
