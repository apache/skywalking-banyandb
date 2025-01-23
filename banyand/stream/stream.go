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
	"io"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/schema"
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
	flushTimeout             time.Duration
	elementIndexFlushTimeout time.Duration
	seriesCacheMaxSize       run.Bytes
}

// Query allow to retrieve elements in a series of streams.
type Query interface {
	LoadGroup(name string) (schema.Group, bool)
	Stream(stream *commonv1.Metadata) (Stream, error)
}

// Stream allows inspecting elements' details.
type Stream interface {
	io.Closer
	GetSchema() *databasev1.Stream
	GetIndexRules() []*databasev1.IndexRule
	Query(ctx context.Context, opts model.StreamQueryOptions) (model.StreamQueryResult, error)
}

var _ Stream = (*stream)(nil)

type stream struct {
	databaseSupplier  schema.Supplier
	l                 *logger.Logger
	schema            *databasev1.Stream
	tagMap            map[string]*databasev1.TagSpec
	entityMap         map[string]int
	pm                *protector.Memory
	name              string
	group             string
	indexRuleLocators partition.IndexRuleLocator
	indexRules        []*databasev1.IndexRule
	shardNum          uint32
}

func (s *stream) GetSchema() *databasev1.Stream {
	return s.schema
}

func (s *stream) GetIndexRules() []*databasev1.IndexRule {
	return s.indexRules
}

func (s *stream) Close() error {
	return nil
}

func (s *stream) parseSpec() {
	s.name, s.group = s.schema.GetMetadata().GetName(), s.schema.GetMetadata().GetGroup()
	s.indexRuleLocators, _ = partition.ParseIndexRuleLocators(s.schema.GetEntity(), s.schema.GetTagFamilies(), s.indexRules, false)
	s.tagMap = make(map[string]*databasev1.TagSpec)
	for _, tf := range s.schema.GetTagFamilies() {
		for _, tag := range tf.GetTags() {
			s.tagMap[tag.GetName()] = tag
		}
	}
	s.entityMap = make(map[string]int)
	for idx, entity := range s.schema.GetEntity().GetTagNames() {
		s.entityMap[entity] = idx + 1
	}
}

type streamSpec struct {
	schema     *databasev1.Stream
	indexRules []*databasev1.IndexRule
}

func openStream(shardNum uint32, db schema.Supplier,
	spec streamSpec, l *logger.Logger, pm *protector.Memory,
) *stream {
	s := &stream{
		shardNum:   shardNum,
		schema:     spec.schema,
		indexRules: spec.indexRules,
		l:          l,
		pm:         pm,
	}
	s.parseSpec()
	if db == nil {
		return s
	}

	s.databaseSupplier = db
	return s
}
