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

// Package stream implements a time-series-based storage which is consists of a sequence of element.
// Each element drops in a arbitrary interval. They are immutable, can not be updated or overwritten.
package stream

import (
	"context"
	"sync/atomic"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/schema"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	maxValuesBlockSize              = 8 * 1024 * 1024
	maxElementIDsBlockSize          = 8 * 1024 * 1024
	maxTagFamiliesMetadataSize      = 8 * 1024 * 1024
	maxUncompressedBlockSize        = 2 * 1024 * 1024
	maxUncompressedPrimaryBlockSize = 128 * 1024

	defaultFlushTimeout = time.Second
)

type option struct {
	mergePolicy              *mergePolicy
	protector                protector.Memory
	seriesCacheMaxSize       run.Bytes
	flushTimeout             time.Duration
	elementIndexFlushTimeout time.Duration
}

// Query allow to retrieve elements in a series of streams.
type Query interface {
	LoadGroup(name string) (schema.Group, bool)
	Stream(stream *commonv1.Metadata) (Stream, error)
	GetRemovalSegmentsTimeRange(group string) *timestamp.TimeRange
}

// Stream allows inspecting elements' details.
type Stream interface {
	GetSchema() *databasev1.Stream
	GetIndexRules() []*databasev1.IndexRule
	Query(ctx context.Context, opts model.StreamQueryOptions) (model.StreamQueryResult, error)
}

type indexSchema struct {
	tagMap            map[string]*databasev1.TagSpec
	indexRuleLocators partition.IndexRuleLocator
	indexRules        []*databasev1.IndexRule
}

func (i *indexSchema) parse(schema *databasev1.Stream) {
	i.indexRuleLocators, _ = partition.ParseIndexRuleLocators(schema.GetEntity(), schema.GetTagFamilies(), i.indexRules, false)
	i.tagMap = make(map[string]*databasev1.TagSpec)
	for _, tf := range schema.GetTagFamilies() {
		for _, tag := range tf.GetTags() {
			i.tagMap[tag.GetName()] = tag
		}
	}
}

var _ Stream = (*stream)(nil)

type stream struct {
	indexSchema atomic.Value
	tsdb        atomic.Value
	l           *logger.Logger
	schema      *databasev1.Stream
	pm          protector.Memory
	schemaRepo  *schemaRepo
	name        string
	group       string
}

func (s *stream) GetSchema() *databasev1.Stream {
	return s.schema
}

func (s *stream) GetIndexRules() []*databasev1.IndexRule {
	is := s.indexSchema.Load()
	if is == nil {
		return nil
	}
	return is.(indexSchema).indexRules
}

func (s *stream) OnIndexUpdate(index []*databasev1.IndexRule) {
	var is indexSchema
	is.indexRules = index
	is.parse(s.schema)
	s.indexSchema.Store(is)
}

func (s *stream) parseSpec() {
	s.name, s.group = s.schema.GetMetadata().GetName(), s.schema.GetMetadata().GetGroup()
	var is indexSchema
	is.parse(s.schema)
	s.indexSchema.Store(is)
}

type streamSpec struct {
	schema *databasev1.Stream
}

func openStream(spec streamSpec,
	l *logger.Logger, pm protector.Memory, schemaRepo *schemaRepo,
) *stream {
	s := &stream{
		schema:     spec.schema,
		l:          l,
		pm:         pm,
		schemaRepo: schemaRepo,
	}
	s.parseSpec()
	return s
}
