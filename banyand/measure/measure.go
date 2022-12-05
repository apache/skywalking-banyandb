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

// Package measure implements a time-series-based storage which is consists of a sequence of data points.
// Each data point contains tags and fields. They arrive in a fixed interval. A data point could be updated
// by one with the identical entity(series_id) and timestamp.
package measure

import (
	"context"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	plainChunkSize = 1 << 20
	intChunkSize   = 120
)

type measure struct {
	databaseSupplier       tsdb.Supplier
	l                      *logger.Logger
	schema                 *databasev1.Measure
	indexWriter            *index.Writer
	processorManager       *topNProcessorManager
	name                   string
	group                  string
	entityLocator          partition.EntityLocator
	indexRules             []*databasev1.IndexRule
	maxObservedModRevision int64
	interval               time.Duration
	shardNum               uint32
}

func (s *measure) GetSchema() *databasev1.Measure {
	return s.schema
}

func (s *measure) GetMetadata() *commonv1.Metadata {
	return s.schema.Metadata
}

func (s *measure) GetIndexRules() []*databasev1.IndexRule {
	return s.indexRules
}

func (s *measure) MaxObservedModRevision() int64 {
	return s.maxObservedModRevision
}

func (s *measure) EntityLocator() partition.EntityLocator {
	return s.entityLocator
}

func (s *measure) Close() error {
	return s.processorManager.Close()
}

func (s *measure) parseSpec() (err error) {
	s.name, s.group = s.schema.GetMetadata().GetName(), s.schema.GetMetadata().GetGroup()
	s.entityLocator = partition.NewEntityLocator(s.schema.GetTagFamilies(), s.schema.GetEntity())
	s.maxObservedModRevision = pbv1.ParseMaxModRevision(s.indexRules)
	if s.schema.Interval != "" {
		s.interval, err = timestamp.ParseDuration(s.schema.Interval)
	}
	return err
}

type measureSpec struct {
	schema           *databasev1.Measure
	indexRules       []*databasev1.IndexRule
	topNAggregations []*databasev1.TopNAggregation
}

func openMeasure(shardNum uint32, db tsdb.Supplier, spec measureSpec, l *logger.Logger) (*measure, error) {
	m := &measure{
		shardNum:   shardNum,
		schema:     spec.schema,
		indexRules: spec.indexRules,
		l:          l,
	}
	if err := m.parseSpec(); err != nil {
		return nil, err
	}
	ctx := context.WithValue(context.Background(), logger.ContextKey, l)

	m.databaseSupplier = db
	m.indexWriter = index.NewWriter(ctx, index.WriterOptions{
		DB:         db,
		ShardNum:   shardNum,
		Families:   spec.schema.TagFamilies,
		IndexRules: spec.indexRules,
	})

	m.processorManager = &topNProcessorManager{
		l:            l,
		m:            m,
		topNSchemas:  spec.topNAggregations,
		processorMap: make(map[*commonv1.Metadata][]*topNStreamingProcessor),
	}

	err := m.processorManager.start()
	if err != nil {
		return nil, err
	}

	return m, nil
}
