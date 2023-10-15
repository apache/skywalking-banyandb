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

// Package dquery implement the distributed query.
package dquery

import (
	"context"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	moduleName = "distributed-query"
)

var _ run.Service = (*queryService)(nil)

type queryService struct {
	log         *logger.Logger
	metaService metadata.Repo
	sqp         *streamQueryProcessor
	mqp         *measureQueryProcessor
	tqp         *topNQueryProcessor
	closer      *run.Closer
	pipeline    queue.Server
}

// NewService return a new query service.
func NewService(metaService metadata.Repo, pipeline queue.Server, broadcaster bus.Broadcaster,
) (run.Unit, error) {
	svc := &queryService{
		metaService: metaService,
		closer:      run.NewCloser(1),
		pipeline:    pipeline,
	}
	svc.sqp = &streamQueryProcessor{
		queryService: svc,
		broadcaster:  broadcaster,
	}
	svc.mqp = &measureQueryProcessor{
		queryService: svc,
		broadcaster:  broadcaster,
	}
	svc.tqp = &topNQueryProcessor{
		queryService: svc,
		broadcaster:  broadcaster,
	}
	return svc, nil
}

func (q *queryService) Name() string {
	return moduleName
}

func (q *queryService) PreRun(_ context.Context) error {
	q.log = logger.GetLogger(moduleName)
	q.sqp.streamService = stream.NewPortableRepository(q.metaService, q.log)
	q.mqp.measureService = measure.NewPortableRepository(q.metaService, q.log)
	return multierr.Combine(
		q.pipeline.Subscribe(data.TopicStreamQuery, q.sqp),
		q.pipeline.Subscribe(data.TopicMeasureQuery, q.mqp),
		q.pipeline.Subscribe(data.TopicTopNQuery, q.tqp),
	)
}

func (q *queryService) GracefulStop() {
	q.sqp.streamService.Close()
	q.mqp.measureService.Close()
	q.closer.Done()
	q.closer.CloseThenWait()
}

func (q *queryService) Serve() run.StopNotify {
	return q.closer.CloseNotify()
}

var _ executor.DistributedExecutionContext = (*distributedContext)(nil)

type distributedContext struct {
	bus.Broadcaster
	timeRange *modelv1.TimeRange
}

func (dc *distributedContext) TimeRange() *modelv1.TimeRange {
	return dc.timeRange
}
