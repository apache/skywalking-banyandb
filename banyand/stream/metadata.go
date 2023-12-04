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

package stream

import (
	"context"
	"io"
	"path"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pb_v1 "github.com/apache/skywalking-banyandb/pkg/pb/v1/tsdb"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
)

var _ Query = (*schemaRepo)(nil)

// SchemaService allows querying stream schema.
type SchemaService interface {
	Query
	Close()
}

type schemaRepo struct {
	resourceSchema.Repository
	l        *logger.Logger
	metadata metadata.Repo
}

func newSchemaRepo(path string, metadata metadata.Repo,
	bufferSize int64, dbOpts tsdb.DatabaseOpts, l *logger.Logger,
) schemaRepo {
	r := schemaRepo{
		l:        l,
		metadata: metadata,
		Repository: resourceSchema.NewRepository(
			metadata,
			l,
			newSupplier(path, metadata, bufferSize, dbOpts, l),
		),
	}
	r.start()
	return r
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
	sr.metadata.RegisterHandler("stream",
		schema.KindGroup|schema.KindStream|schema.KindIndexRuleBinding|schema.KindIndexRule,
		sr)
}

func (sr *schemaRepo) Stream(metadata *commonv1.Metadata) (Stream, error) {
	sm, ok := sr.loadStream(metadata)
	if !ok {
		return nil, errors.WithStack(errStreamNotExist)
	}
	return sm, nil
}

func (sr *schemaRepo) OnAddOrUpdate(m schema.Metadata) {
	switch m.Kind {
	case schema.KindGroup:
		g := m.Spec.(*commonv1.Group)
		if g.Catalog != commonv1.Catalog_CATALOG_STREAM {
			return
		}
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventAddOrUpdate,
			Kind:     resourceSchema.EventKindGroup,
			Metadata: g.GetMetadata(),
		})
	case schema.KindStream:
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventAddOrUpdate,
			Kind:     resourceSchema.EventKindResource,
			Metadata: m.Spec.(*databasev1.Stream).GetMetadata(),
		})
	case schema.KindIndexRuleBinding:
		irb, ok := m.Spec.(*databasev1.IndexRuleBinding)
		if !ok {
			sr.l.Warn().Msg("fail to convert message to IndexRuleBinding")
			return
		}
		if irb.GetSubject().Catalog == commonv1.Catalog_CATALOG_STREAM {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			stm, err := sr.metadata.StreamRegistry().GetStream(ctx, &commonv1.Metadata{
				Name:  irb.GetSubject().GetName(),
				Group: m.Group,
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
		subjects, err := sr.metadata.Subjects(ctx, m.Spec.(*databasev1.IndexRule), commonv1.Catalog_CATALOG_STREAM)
		cancel()
		if err != nil {
			return
		}
		for _, sub := range subjects {
			sr.SendMetadataEvent(resourceSchema.MetadataEvent{
				Typ:      resourceSchema.EventAddOrUpdate,
				Kind:     resourceSchema.EventKindResource,
				Metadata: sub.(*databasev1.Stream).GetMetadata(),
			})
		}
	default:
	}
}

func (sr *schemaRepo) OnDelete(m schema.Metadata) {
	switch m.Kind {
	case schema.KindGroup:
		g := m.Spec.(*commonv1.Group)
		if g.Catalog != commonv1.Catalog_CATALOG_STREAM {
			return
		}
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventDelete,
			Kind:     resourceSchema.EventKindGroup,
			Metadata: g.GetMetadata(),
		})
	case schema.KindStream:
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventDelete,
			Kind:     resourceSchema.EventKindResource,
			Metadata: m.Spec.(*databasev1.Stream).GetMetadata(),
		})
	case schema.KindIndexRuleBinding:
		if m.Spec.(*databasev1.IndexRuleBinding).GetSubject().Catalog == commonv1.Catalog_CATALOG_STREAM {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			stm, err := sr.metadata.StreamRegistry().GetStream(ctx, &commonv1.Metadata{
				Name:  m.Name,
				Group: m.Group,
			})
			cancel()
			if err != nil {
				return
			}
			sr.SendMetadataEvent(resourceSchema.MetadataEvent{
				Typ:      resourceSchema.EventDelete,
				Kind:     resourceSchema.EventKindResource,
				Metadata: stm.GetMetadata(),
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

var _ resourceSchema.ResourceSupplier = (*supplier)(nil)

type supplier struct {
	metadata   metadata.Repo
	l          *logger.Logger
	path       string
	dbOpts     tsdb.DatabaseOpts
	bufferSize int64
}

func newSupplier(path string, metadata metadata.Repo, bufferSize int64, dbOpts tsdb.DatabaseOpts, l *logger.Logger) *supplier {
	return &supplier{
		path:       path,
		bufferSize: bufferSize,
		dbOpts:     dbOpts,
		metadata:   metadata,
		l:          l,
	}
}

func (s *supplier) OpenResource(shardNum uint32, db resourceSchema.Supplier, resource resourceSchema.Resource) (io.Closer, error) {
	streamSchema := resource.Schema().(*databasev1.Stream)
	return openStream(shardNum, &tsdbSupplier{db: db.(tsdb.Database)}, streamSpec{
		schema:     streamSchema,
		indexRules: resource.IndexRules(),
	}, s.l), nil
}

type tsdbSupplier struct {
	db tsdb.Database
}

func (s *tsdbSupplier) SupplyTSDB() tsdb.Database {
	return s.db
}

func (s *supplier) ResourceSchema(md *commonv1.Metadata) (resourceSchema.ResourceSchema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.metadata.StreamRegistry().GetStream(ctx, md)
}

func (s *supplier) OpenDB(groupSchema *commonv1.Group) (io.Closer, error) {
	name := groupSchema.Metadata.Name
	opts := s.dbOpts
	opts.ShardNum = groupSchema.ResourceOpts.ShardNum
	opts.Location = path.Join(s.path, groupSchema.Metadata.Name)
	opts.TSTableFactory = &tsTableFactory{
		bufferSize:        s.bufferSize,
		compressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		chunkSize:         chunkSize,
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
			p.Module = "stream"
			p.Database = name
			return p
		}),
		opts)
}

type portableSupplier struct {
	metadata metadata.Repo
	l        *logger.Logger
}

func (*portableSupplier) OpenDB(_ *commonv1.Group) (io.Closer, error) {
	panic("do not support open db")
}

func (s *portableSupplier) OpenResource(shardNum uint32, _ resourceSchema.Supplier, resource resourceSchema.Resource) (io.Closer, error) {
	streamSchema := resource.Schema().(*databasev1.Stream)
	return openStream(shardNum, nil, streamSpec{
		schema:     streamSchema,
		indexRules: resource.IndexRules(),
	}, s.l), nil
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
