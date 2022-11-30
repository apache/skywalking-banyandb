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

	"github.com/apache/skywalking-banyandb/api/common"
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
	logical_measure "github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
	logical_stream "github.com/apache/skywalking-banyandb/pkg/query/logical/stream"
)

const (
	moduleName = "query"
)

var (
	_ Executor            = (*queryService)(nil)
	_ bus.MessageListener = (*streamQueryProcessor)(nil)
	_ bus.MessageListener = (*measureQueryProcessor)(nil)
	_ bus.MessageListener = (*topNQueryProcessor)(nil)
)

type queryService struct {
	log         *logger.Logger
	metaService metadata.Service
	serviceRepo discovery.ServiceRepo
	pipeline    queue.Queue
	sqp         *streamQueryProcessor
	mqp         *measureQueryProcessor
	tqp         *topNQueryProcessor
}

type streamQueryProcessor struct {
	streamService stream.Service
	*queryService
}

func (p *streamQueryProcessor) Rev(message bus.Message) (resp bus.Message) {
	now := time.Now().UnixNano()
	queryCriteria, ok := message.Data().(*streamv1.QueryRequest)
	if !ok {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("invalid event data type"))
		return
	}
	sl := p.log.Named("stream", queryCriteria.Metadata.Group, queryCriteria.Metadata.Name)
	if e := sl.Debug(); e.Enabled() {
		e.RawJSON("criteria", logger.Proto(queryCriteria)).Msg("received a query request")
	}

	meta := queryCriteria.GetMetadata()
	ec, err := p.streamService.Stream(meta)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to get execution context for stream %s: %v", meta.GetName(), err))
		return
	}

	analyzer, err := logical_stream.CreateAnalyzerFromMetaService(p.metaService)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to build analyzer for stream %s: %v", meta.GetName(), err))
		return
	}

	s, err := analyzer.BuildSchema(context.TODO(), meta)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to build schema for stream %s: %v", meta.GetName(), err))
		return
	}

	plan, err := analyzer.Analyze(context.TODO(), queryCriteria, meta, s)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to analyze the query request for stream %s: %v", meta.GetName(), err))
		return
	}

	if e := sl.Debug(); e.Enabled() {
		e.Str("plan", plan.String()).Msg("query plan")
	}

	entities, err := plan.(executor.StreamExecutable).Execute(ec)
	if err != nil {
		sl.Error().Err(err).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to execute the query plan")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("execute the query plan for stream %s: %v", meta.GetName(), err))
		return
	}

	resp = bus.NewMessage(bus.MessageID(now), entities)

	return
}

type measureQueryProcessor struct {
	measureService measure.Service
	*queryService
}

func (p *measureQueryProcessor) Rev(message bus.Message) (resp bus.Message) {
	queryCriteria, ok := message.Data().(*measurev1.QueryRequest)
	now := time.Now().UnixNano()
	if !ok {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("invalid event data type"))
		return
	}
	ml := p.log.Named("measure", queryCriteria.Metadata.Group, queryCriteria.Metadata.Name)
	if e := ml.Debug(); e.Enabled() {
		e.RawJSON("req", logger.Proto(queryCriteria)).Msg("received a query event")
	}

	meta := queryCriteria.GetMetadata()
	ec, err := p.measureService.Measure(meta)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to get execution context for measure %s: %v", meta.GetName(), err))
		return
	}

	analyzer, err := logical_measure.CreateAnalyzerFromMetaService(p.metaService)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to build analyzer for measure %s: %v", meta.GetName(), err))
		return
	}

	s, err := analyzer.BuildSchema(context.TODO(), meta)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to build schema for measure %s: %v", meta.GetName(), err))
		return
	}

	plan, err := analyzer.Analyze(context.TODO(), queryCriteria, meta, s)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to analyze the query request for measure %s: %v", meta.GetName(), err))
		return
	}

	if e := ml.Debug(); e.Enabled() {
		e.Str("plan", plan.String()).Msg("query plan")
	}

	mIterator, err := plan.(executor.MeasureExecutable).Execute(ec)
	if err != nil {
		ml.Error().Err(err).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to close the query plan")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to execute the query plan for measure %s: %v", meta.GetName(), err))
		return
	}
	defer func() {
		if err = mIterator.Close(); err != nil {
			ml.Error().Err(err).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to close the query plan")
		}
	}()
	result := make([]*measurev1.DataPoint, 0)
	for mIterator.Next() {
		current := mIterator.Current()
		if len(current) > 0 {
			result = append(result, current[0])
		}
	}
	if e := ml.Debug(); e.Enabled() {
		e.RawJSON("ret", logger.Proto(&measurev1.QueryResponse{DataPoints: result})).Msg("got a measure")
	}
	resp = bus.NewMessage(bus.MessageID(now), result)
	return
}

func (q *queryService) Name() string {
	return moduleName
}

func (q *queryService) PreRun() error {
	q.log = logger.GetLogger(moduleName)
	return multierr.Combine(
		q.pipeline.Subscribe(data.TopicStreamQuery, q.sqp),
		q.pipeline.Subscribe(data.TopicMeasureQuery, q.mqp),
		q.pipeline.Subscribe(data.TopicTopNQuery, q.tqp),
	)
}
