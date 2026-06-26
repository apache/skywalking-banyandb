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
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
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
	defaultSyncInterval = 30 * time.Second
)

type option struct {
	protector                    protector.Memory
	tire2Client                  queue.Client
	mergePolicy                  *mergePolicy
	seriesCacheMaxSize           run.Bytes
	flushTimeout                 time.Duration
	syncInterval                 time.Duration
	memWaitTimeout               time.Duration
	failedPartsMaxTotalSizeBytes uint64
	vectorized                   vmeasure.VectorizedConfig
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
	indexSchema  atomic.Value
	tsdb         atomic.Value
	c            storage.Cache
	pm           protector.Memory
	l            *logger.Logger
	schema       *databasev1.Measure
	schemaRepo   *schemaRepo
	queryMetrics *queryMetrics
	name         string
	group        string
	interval     time.Duration
	vectorized   vmeasure.VectorizedConfig
}

func (m *measure) GetSchema() *databasev1.Measure {
	return m.schema
}

func (m *measure) GetIndexRules() []*databasev1.IndexRule {
	is := m.indexSchema.Load()
	if is == nil {
		return nil
	}
	return is.(indexSchema).indexRules
}

func (m *measure) OnIndexUpdate(index []*databasev1.IndexRule) {
	var is indexSchema
	is.indexRules = index
	is.parse(m.schema)
	m.indexSchema.Store(is)
}

func (m *measure) parseSpec() (err error) {
	m.name, m.group = m.schema.GetMetadata().GetName(), m.schema.GetMetadata().GetGroup()
	if m.schema.Interval != "" {
		m.interval, err = timestamp.ParseDuration(m.schema.Interval)
	}
	var is indexSchema
	is.parse(m.schema)
	m.indexSchema.Store(is)
	return err
}

type measureSpec struct {
	schema *databasev1.Measure
}

type queryMetrics struct {
	queryLatency             meter.Histogram
	queryErrors              meter.Counter
	resultPoints             meter.Histogram
	totalQueryResultStarted  meter.Counter
	totalQueryResultFinished meter.Counter
}

func newQueryMetrics(factory observability.Factory) *queryMetrics {
	if factory == nil {
		return nil
	}
	return &queryMetrics{
		queryLatency:             factory.NewHistogram("query_latency", meter.DefBuckets),
		queryErrors:              factory.NewCounter("total_query_errors"),
		resultPoints:             factory.NewHistogram("result_points", meter.DefBuckets),
		totalQueryResultStarted:  factory.NewCounter("total_query_result_started"),
		totalQueryResultFinished: factory.NewCounter("total_query_result_finished"),
	}
}

func openMeasure(spec measureSpec,
	l *logger.Logger, c storage.Cache, pm protector.Memory, schemaRepo *schemaRepo, qm *queryMetrics,
	vectorized vmeasure.VectorizedConfig,
) (*measure, error) {
	m := &measure{
		schema:       spec.schema,
		l:            l,
		c:            c,
		pm:           pm,
		schemaRepo:   schemaRepo,
		queryMetrics: qm,
		vectorized:   vectorized,
	}
	if err := m.parseSpec(); err != nil {
		return nil, err
	}
	return m, nil
}

// VectorizedConfig returns the per-Measure vectorized query configuration. It
// is consumed by the logical query layer to decide whether to wrap the row
// MeasureQueryResult in a vectorized iterator.
func (m *measure) VectorizedConfig() vmeasure.VectorizedConfig {
	return m.vectorized
}

// bindVectorizedFlags wires VectorizedConfig fields to a run.FlagSet. The
// defaults match vmeasure.DefaultConfig.
func bindVectorizedFlags(flagS *run.FlagSet, cfg *vmeasure.VectorizedConfig) {
	defaults := vmeasure.DefaultConfig()
	flagS.BoolVar(&cfg.Enabled, "measure-vectorized-enabled", defaults.Enabled,
		"enable the vectorized measure query path")
	flagS.IntVar(&cfg.BatchSize, "measure-vectorized-batch-size", defaults.BatchSize,
		"row count per vectorized batch")
	flagS.IntVar(&cfg.QueryMemoryMiB, "measure-vectorized-query-memory-mib", defaults.QueryMemoryMiB,
		"per-query memory budget for the vectorized path, in MiB")
}
