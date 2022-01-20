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

package measure

import (
	"context"
	"path"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/index"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

// a chunk is 1MB
const chunkSize = 1 << 20

type measure struct {
	name          string
	group         string
	l             *logger.Logger
	schema        *databasev1.Measure
	db            tsdb.Database
	entityLocator partition.EntityLocator
	indexRules    []*databasev1.IndexRule
	indexWriter   *index.Writer
}

func (s *measure) Close() error {
	_ = s.indexWriter.Close()
	return s.db.Close()
}

func (s *measure) parseSchema() {
	sm := s.schema
	meta := sm.GetMetadata()
	s.name, s.group = meta.GetName(), meta.GetGroup()
	s.entityLocator = partition.NewEntityLocator(sm.TagFamilies, sm.Entity)
}

type measureSpec struct {
	schema     *databasev1.Measure
	indexRules []*databasev1.IndexRule
}

func openMeasure(root string, spec measureSpec, l *logger.Logger) (*measure, error) {
	sm := &measure{
		schema:     spec.schema,
		indexRules: spec.indexRules,
		l:          l,
	}
	sm.parseSchema()
	ctx := context.WithValue(context.Background(), logger.ContextKey, l)

	db, err := tsdb.OpenDatabase(
		ctx,
		tsdb.DatabaseOpts{
			Location:   path.Join(root, spec.schema.GetMetadata().GetGroup(), spec.schema.GetMetadata().GetName()),
			ShardNum:   sm.schema.GetOpts().GetShardNum(),
			IndexRules: spec.indexRules,
			EncodingMethod: tsdb.EncodingMethod{
				EncoderPool: encoding.NewPlainEncoderPool(chunkSize),
				DecoderPool: encoding.NewPlainDecoderPool(chunkSize),
			},
		})
	if err != nil {
		return nil, err
	}
	sm.db = db
	sm.indexWriter = index.NewWriter(ctx, index.WriterOptions{
		DB:         db,
		ShardNum:   spec.schema.GetOpts().ShardNum,
		Families:   spec.schema.TagFamilies,
		IndexRules: spec.indexRules,
	})
	return sm, nil
}
