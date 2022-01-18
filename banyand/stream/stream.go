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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/index"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

// a chunk is 1MB
const chunkSize = 1 << 20

type stream struct {
	name  string
	group string
	l     *logger.Logger
	// schema is the reference to the spec of the stream
	schema *databasev1.Stream
	// maxObservedModRevision is the max observed revision of index rules in the spec
	maxObservedModRevision int64
	db                     tsdb.Database
	entityLocator          partition.EntityLocator
	indexRules             []*databasev1.IndexRule
	indexWriter            *index.Writer
}

func (s *stream) Close() error {
	_ = s.indexWriter.Close()
	return s.db.Close()
}

func parseMaxModRevision(indexRules []*databasev1.IndexRule) (maxRevisionForIdxRules int64) {
	maxRevisionForIdxRules = int64(0)
	for _, idxRule := range indexRules {
		if idxRule.GetMetadata().GetModRevision() > maxRevisionForIdxRules {
			maxRevisionForIdxRules = idxRule.GetMetadata().GetModRevision()
		}
	}
	return
}

func (s *stream) parseSpec() {
	s.name, s.group = s.schema.GetMetadata().GetName(), s.schema.GetMetadata().GetGroup()
	s.entityLocator = partition.NewEntityLocator(s.schema.GetTagFamilies(), s.schema.GetEntity())
	s.maxObservedModRevision = parseMaxModRevision(s.indexRules)
}

type streamSpec struct {
	schema     *databasev1.Stream
	indexRules []*databasev1.IndexRule
}

func openStream(root string, spec streamSpec, l *logger.Logger) (*stream, error) {
	sm := &stream{
		schema:     spec.schema,
		indexRules: spec.indexRules,
		l:          l,
	}
	sm.parseSpec()
	ctx := context.WithValue(context.Background(), logger.ContextKey, l)
	db, err := tsdb.OpenDatabase(
		ctx,
		tsdb.DatabaseOpts{
			Location:   root,
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

func formatStreamID(name, group string) string {
	return name + ":" + group
}
