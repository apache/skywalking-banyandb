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
	"path"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/event"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	pb_v1 "github.com/apache/skywalking-banyandb/pkg/pb/v1/tsdb"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
)

type schemaRepo struct {
	resourceSchema.Repository
	l        *logger.Logger
	metadata metadata.Repo
}

func newSchemaRepo(path string, metadata metadata.Repo, repo discovery.ServiceRepo,
	dbOpts tsdb.DatabaseOpts, l *logger.Logger, pipeline queue.Queue, encoderBufferSize, bufferSize int64,
) schemaRepo {
	return schemaRepo{
		l:        l,
		metadata: metadata,
		Repository: resourceSchema.NewRepository(
			metadata,
			repo,
			l,
			newSupplier(path, metadata, dbOpts, l, pipeline, encoderBufferSize, bufferSize),
			event.MeasureTopicShardEvent,
			event.MeasureTopicEntityEvent,
		),
	}
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
				sr.l.Error().Err(err).Msg("fail to get subject")
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
			sr.l.Error().Err(err).Msg("fail to get subjects(measure)")
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
		_, err := createOrUpdateTopNMeasure(sr.metadata.MeasureRegistry(), metadata.Spec.(*databasev1.TopNAggregation))
		if err != nil {
			sr.l.Error().Err(err).Msg("fail to create/update topN measure")
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

func createOrUpdateTopNMeasure(measureSchemaRegistry schema.Measure, topNSchema *databasev1.TopNAggregation) (*databasev1.Measure, error) {
	oldTopNSchema, err := measureSchemaRegistry.GetMeasure(context.TODO(), topNSchema.GetMetadata())
	if err != nil && !errors.Is(err, schema.ErrGRPCResourceNotFound) {
		return nil, err
	}

	sourceMeasureSchema, err := measureSchemaRegistry.GetMeasure(context.Background(), topNSchema.GetSourceMeasure())
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
				Name: TopNTagFamily,
				Tags: append([]*databasev1.TagSpec{
					{
						Name: "measure_id",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
				}, seriesSpecs...),
			},
		},
		Fields: []*databasev1.FieldSpec{TopNValueFieldSpec},
	}
	if oldTopNSchema == nil {
		if innerErr := measureSchemaRegistry.CreateMeasure(context.Background(), newTopNMeasure); innerErr != nil {
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
	if err = measureSchemaRegistry.UpdateMeasure(context.Background(), newTopNMeasure); err != nil {
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
				sr.l.Error().Err(err).Msg("fail to get subject")
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
			sr.l.Error().Err(err).Msg("fail to remove topN measure")
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
	s, ok := r.(*measure)
	return s, ok
}

var _ resourceSchema.ResourceSupplier = (*supplier)(nil)

type supplier struct {
	metadata                      metadata.Repo
	pipeline                      queue.Queue
	l                             *logger.Logger
	path                          string
	dbOpts                        tsdb.DatabaseOpts
	encoderBufferSize, bufferSize int64
}

func newSupplier(path string, metadata metadata.Repo, dbOpts tsdb.DatabaseOpts, l *logger.Logger,
	pipeline queue.Queue, encoderBufferSize, bufferSize int64,
) *supplier {
	return &supplier{
		path:              path,
		dbOpts:            dbOpts,
		metadata:          metadata,
		l:                 l,
		pipeline:          pipeline,
		encoderBufferSize: encoderBufferSize,
		bufferSize:        bufferSize,
	}
}

func (s *supplier) OpenResource(shardNum uint32, db tsdb.Supplier, spec resourceSchema.ResourceSpec) (resourceSchema.Resource, error) {
	measureSchema := spec.Schema.(*databasev1.Measure)
	return openMeasure(shardNum, db, measureSpec{
		schema:           measureSchema,
		indexRules:       spec.IndexRules,
		topNAggregations: spec.Aggregations,
	}, s.l, s.pipeline)
}

func (s *supplier) ResourceSchema(md *commonv1.Metadata) (resourceSchema.ResourceSchema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.metadata.MeasureRegistry().GetMeasure(ctx, md)
}

func (s *supplier) OpenDB(groupSchema *commonv1.Group) (tsdb.Database, error) {
	opts := s.dbOpts
	opts.ShardNum = groupSchema.ResourceOpts.ShardNum
	opts.Location = path.Join(s.path, groupSchema.Metadata.Name)
	name := groupSchema.Metadata.Name
	opts.TSTableFactory = &tsTableFactory{
		bufferSize:        s.bufferSize,
		encoderBufferSize: s.encoderBufferSize,
		encoderPool:       encoding.NewEncoderPool(name, intChunkNum, intervalFn),
		decoderPool:       encoding.NewDecoderPool(name, intChunkNum, intervalFn),
		compressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		encodingChunkSize: intChunkSize,
		plainChunkSize:    plainChunkSize,
	}

	var err error
	if opts.BlockInterval, err = pb_v1.ToIntervalRule(groupSchema.ResourceOpts.BlockInterval); err != nil {
		return nil, err
	}
	if opts.SegmentInterval, err = pb_v1.ToIntervalRule(groupSchema.ResourceOpts.SegmentInterval); err != nil {
		return nil, err
	}
	if opts.TTL, err = pb_v1.ToIntervalRule(groupSchema.ResourceOpts.Ttl); err != nil {
		return nil, err
	}

	return tsdb.OpenDatabase(
		common.SetPosition(context.Background(), func(p common.Position) common.Position {
			p.Module = "measure"
			p.Database = name
			return p
		}),
		opts)
}

func intervalFn(key []byte) time.Duration {
	_, interval, err := pbv1.DecodeFieldFlag(key)
	if err != nil {
		panic(err)
	}
	return interval
}
