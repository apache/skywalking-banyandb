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

// Package trace implements a trace-based storage which consists of trace data.
// Traces are composed of spans and support querying by trace ID and various tags.
// TODO: Remove this once trace partitioning is implemented
// nolint:unused
package trace

import (
	"sync/atomic"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/schema"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	maxValuesBlockSize              = 8 * 1024 * 1024
	maxTagsMetadataSize             = 8 * 1024 * 1024
	maxUncompressedSpanSize         = 2 * 1024 * 1024
	maxUncompressedPrimaryBlockSize = 128 * 1024

	defaultFlushTimeout = time.Second
)

var traceScope = observability.RootScope.SubScope("trace")

type option struct {
	mergePolicy        *mergePolicy
	protector          protector.Memory
	tire2Client        queue.Client
	seriesCacheMaxSize run.Bytes
	flushTimeout       time.Duration
}

// Service allows inspecting the trace data.
type Service interface {
	run.PreRunner
	run.Config
	run.Service
	Query
}

// Query allows retrieving traces.
type Query interface {
	LoadGroup(name string) (schema.Group, bool)
	Trace(metadata *commonv1.Metadata) (Trace, error)
	GetRemovalSegmentsTimeRange(group string) *timestamp.TimeRange
}

// Trace allows inspecting trace details.
type Trace interface {
	GetSchema() *databasev1.Trace
	GetIndexRules() []*databasev1.IndexRule
}

type indexSchema struct {
	tagMap            map[string]*databasev1.TraceTagSpec
	indexRuleLocators map[string]*databasev1.IndexRule
	indexRules        []*databasev1.IndexRule
}

func (i *indexSchema) parse(schema *databasev1.Trace) {
	// Note: This will need proper implementation when trace partition support is added
	// For now, we'll just parse the tag map
	i.tagMap = make(map[string]*databasev1.TraceTagSpec)
	for _, tag := range schema.GetTags() {
		i.tagMap[tag.GetName()] = tag
	}
}

type trace struct {
	pm          protector.Memory
	indexSchema atomic.Value
	tsdb        atomic.Value
	l           *logger.Logger
	schema      *databasev1.Trace
	schemaRepo  *schemaRepo
	name        string
	group       string
}

type traceSpec struct {
	schema *databasev1.Trace
}

func (t *trace) GetSchema() *databasev1.Trace {
	return t.schema
}

func (t *trace) GetIndexRules() []*databasev1.IndexRule {
	if is := t.indexSchema.Load(); is != nil {
		return is.(*indexSchema).indexRules
	}
	return nil
}

func (t *trace) OnIndexUpdate(index []*databasev1.IndexRule) {
	var is indexSchema
	is.indexRules = index
	is.parse(t.schema)
	t.indexSchema.Store(is)
}

func (t *trace) parseSpec() {
	t.name, t.group = t.schema.GetMetadata().GetName(), t.schema.GetMetadata().GetGroup()
	var is indexSchema
	is.parse(t.schema)
	t.indexSchema.Store(is)
}

func openTrace(schema *databasev1.Trace, l *logger.Logger, pm protector.Memory, schemaRepo *schemaRepo) *trace {
	t := &trace{
		schema:     schema,
		l:          l,
		pm:         pm,
		schemaRepo: schemaRepo,
	}
	t.parseSpec()
	return t
}
