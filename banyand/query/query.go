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

// Package query implement the query module for liaison and other modules to retrieve data.
package query

import (
	"context"
	"errors"
	"time"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type queryService struct {
	metaService metadata.Repo
	pipeline    queue.Server
	log         *logger.Logger
	sqp         *streamQueryProcessor
	mqp         *measureQueryProcessor
	imqp        *measureInternalQueryProcessor
	nqp         *topNQueryProcessor
	tqp         *traceQueryProcessor
	nodeID      string
	slowQuery   time.Duration
}

// NewService return a new query service.
func NewService(_ context.Context, streamService stream.Service, measureService measure.Service, traceService trace.Service,
	metaService metadata.Repo, pipeline queue.Server,
) (run.Unit, error) {
	svc := &queryService{
		metaService: metaService,
		pipeline:    pipeline,
	}
	// measure query processor
	svc.mqp = &measureQueryProcessor{
		measureService: measureService,
		queryService:   svc,
	}
	// internal measure query processor for distributed query
	svc.imqp = &measureInternalQueryProcessor{
		measureService: measureService,
		queryService:   svc,
	}
	// stream query processor
	svc.sqp = &streamQueryProcessor{
		streamService: streamService,
		queryService:  svc,
	}
	// topN query processor
	svc.nqp = &topNQueryProcessor{
		measureService: measureService,
		queryService:   svc,
	}
	// trace query processor
	svc.tqp = &traceQueryProcessor{
		traceService: traceService,
		queryService: svc,
	}
	return svc, nil
}

func (q *queryService) Name() string {
	return moduleName
}

func (q *queryService) PreRun(ctx context.Context) error {
	val := ctx.Value(common.ContextNodeKey)
	if val == nil {
		return errors.New("node id is empty")
	}
	node := val.(common.Node)
	q.nodeID = node.NodeID
	q.log = logger.GetLogger(moduleName)
	return multierr.Combine(
		q.pipeline.Subscribe(data.TopicStreamQuery, q.sqp),
		q.pipeline.Subscribe(data.TopicMeasureQuery, q.mqp),
		q.pipeline.Subscribe(data.TopicInternalMeasureQuery, q.imqp),
		q.pipeline.Subscribe(data.TopicTopNQuery, q.nqp),
		q.pipeline.Subscribe(data.TopicTraceQuery, q.tqp),
	)
}

func (q *queryService) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("query")
	fs.DurationVar(&q.slowQuery, "slow-query", 0, "slow query threshold, 0 means no slow query log")
	return fs
}

func (q *queryService) Validate() error {
	return nil
}
