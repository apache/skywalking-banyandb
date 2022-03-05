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

package query

import (
	"context"
	"time"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/data"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

const (
	moduleName = "query-processor"
)

var (
	_ Executor            = (*queryService)(nil)
	_ bus.MessageListener = (*streamQueryProcessor)(nil)
	_ bus.MessageListener = (*measureQueryProcessor)(nil)
)

type queryService struct {
	log         *logger.Logger
	metaService metadata.Service
	serviceRepo discovery.ServiceRepo
	pipeline    queue.Queue
	sqp         *streamQueryProcessor
	mqp         *measureQueryProcessor
}

type streamQueryProcessor struct {
	streamService stream.Service
	*queryService
}

func (p *streamQueryProcessor) Rev(message bus.Message) (resp bus.Message) {
	queryCriteria, ok := message.Data().(*streamv1.QueryRequest)
	if !ok {
		p.log.Warn().Msg("invalid event data type")
		return
	}
	p.log.Info().Msg("received a query event")

	meta := queryCriteria.GetMetadata()
	ec, err := p.streamService.Stream(meta)
	if err != nil {
		p.log.Error().Err(err).
			Str("stream", meta.GetName()).
			Msg("fail to get execution context for stream")
		return
	}

	analyzer, err := logical.CreateStreamAnalyzerFromMetaService(p.metaService)
	if err != nil {
		p.log.Error().Err(err).Msg("fail to build analyzer")
		return
	}

	s, err := analyzer.BuildStreamSchema(context.TODO(), meta)
	if err != nil {
		p.log.Error().Err(err).Msg("fail to build")
		return
	}

	plan, err := analyzer.Analyze(context.TODO(), queryCriteria, meta, s)
	if err != nil {
		p.log.Error().Err(err).Msg("fail to analyze the query request")
		return
	}

	p.log.Debug().Str("plan", plan.String()).Msg("query plan")

	entities, err := plan.(executor.StreamExecutable).Execute(ec)
	if err != nil {
		p.log.Error().Err(err).Msg("fail to execute the query plan")
		return
	}

	now := time.Now().UnixNano()
	resp = bus.NewMessage(bus.MessageID(now), entities)

	return
}

type measureQueryProcessor struct {
	measureService measure.Service
	*queryService
}

func (p *measureQueryProcessor) Rev(message bus.Message) (resp bus.Message) {
	queryCriteria, ok := message.Data().(*measurev1.QueryRequest)
	if !ok {
		p.queryService.log.Warn().Msg("invalid event data type")
		return
	}
	p.log.Info().Msg("received a query event")

	meta := queryCriteria.GetMetadata()
	ec, err := p.measureService.Measure(meta)
	if err != nil {
		p.log.Error().Err(err).
			Str("measure", meta.GetName()).
			Msg("fail to get execution context")
		return
	}

	analyzer, err := logical.CreateMeasureAnalyzerFromMetaService(p.metaService)
	if err != nil {
		p.log.Error().Err(err).Msg("fail to build analyzer")
		return
	}

	s, err := analyzer.BuildMeasureSchema(context.TODO(), meta)
	if err != nil {
		p.queryService.log.Error().Err(err).Msg("fail to build measure schema")
		return
	}

	plan, err := analyzer.Analyze(context.TODO(), queryCriteria, meta, s)
	if err != nil {
		p.queryService.log.Error().Err(err).Msg("fail to analyze the query request")
		return
	}

	p.queryService.log.Debug().Str("plan", plan.String()).Msg("query plan")

	entities, err := plan.(executor.MeasureExecutable).Execute(ec)
	if err != nil {
		p.queryService.log.Error().Err(err).Msg("fail to execute the query plan")
		return
	}

	now := time.Now().UnixNano()
	resp = bus.NewMessage(bus.MessageID(now), entities)

	return
}

func (q *queryService) Name() string {
	return moduleName
}

func (q *queryService) PreRun() error {
	q.log = logger.GetLogger(moduleName)
	var err error
	err = multierr.Append(err, q.pipeline.Subscribe(data.TopicStreamQuery, q.sqp))
	err = multierr.Append(err, q.pipeline.Subscribe(data.TopicMeasureQuery, q.mqp))
	return err
}
