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
	"sync/atomic"
	"time"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	maxValuesBlockSize              = 8 * 1024 * 1024
	maxTagFamiliesMetadataSize      = 8 * 1024 * 1024
	maxUncompressedBlockSize        = 2 * 1024 * 1024
	maxUncompressedPrimaryBlockSize = 128 * 1024

	maxBlockLength = 8 * 1024

	defaultFlushTimeout = 5 * time.Second
)

type option struct {
	mergePolicy        *mergePolicy
	flushTimeout       time.Duration
	seriesCacheMaxSize run.Bytes
}

type indexSchema struct {
	indexTagMap        map[string]struct{}
	fieldIndexLocation partition.FieldIndexLocation
	indexRuleLocators  partition.IndexRuleLocator
	indexRules         []*databasev1.IndexRule
}

func (i *indexSchema) parse(schema *databasev1.Measure) {
	i.indexRuleLocators, i.fieldIndexLocation = partition.ParseIndexRuleLocators(schema.GetEntity(), schema.GetTagFamilies(), i.indexRules, schema.IndexMode)
	i.indexTagMap = make(map[string]struct{})
	for j := range i.indexRules {
		for k := range i.indexRules[j].Tags {
			i.indexTagMap[i.indexRules[j].Tags[k]] = struct{}{}
		}
	}
}

type measure struct {
	indexSchema atomic.Value
	tsdb        atomic.Value
	pm          protector.MemoryProtector
	l           *logger.Logger
	schema      *databasev1.Measure
	schemaRepo  *schemaRepo
	name        string
	group       string
	interval    time.Duration
}

func (s *measure) GetSchema() *databasev1.Measure {
	return s.schema
}

func (s *measure) GetIndexRules() []*databasev1.IndexRule {
	is := s.indexSchema.Load()
	if is == nil {
		return nil
	}
	return is.(indexSchema).indexRules
}

func (s *measure) OnIndexUpdate(index []*databasev1.IndexRule) {
	var is indexSchema
	is.indexRules = index
	is.parse(s.schema)
	s.indexSchema.Store(is)
}

func (s *measure) parseSpec() (err error) {
	s.name, s.group = s.schema.GetMetadata().GetName(), s.schema.GetMetadata().GetGroup()
	if s.schema.Interval != "" {
		s.interval, err = timestamp.ParseDuration(s.schema.Interval)
	}
	var is indexSchema
	is.parse(s.schema)
	s.indexSchema.Store(is)
	return err
}

type measureSpec struct {
	schema *databasev1.Measure
}

func openMeasure(spec measureSpec,
	l *logger.Logger, pm *protector.Memory, schemaRepo *schemaRepo,
) (*measure, error) {
	m := &measure{
		schema:     spec.schema,
		l:          l,
		pm:         pm,
		schemaRepo: schemaRepo,
	}
	if err := m.parseSpec(); err != nil {
		return nil, err
	}
	return m, nil
}
