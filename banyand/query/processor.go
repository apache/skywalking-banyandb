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

	"github.com/apache/skywalking-banyandb/api/data"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

const (
	moduleName = "query-processor"
)

var (
	_ Executor            = (*queryProcessor)(nil)
	_ bus.MessageListener = (*queryProcessor)(nil)
)

type queryProcessor struct {
	streamService stream.Service
	metaService   metadata.Service
	logger        *logger.Logger
	log           *logger.Logger
	serviceRepo   discovery.ServiceRepo
	pipeline      queue.Queue
}

func (q *queryProcessor) Rev(message bus.Message) (resp bus.Message) {
	queryCriteria, ok := message.Data().(*streamv1.QueryRequest)
	if !ok {
		q.log.Warn().Msg("invalid event data type")
		return
	}
	q.log.Info().
		Msg("received a query event")

	meta := queryCriteria.GetMetadata()
	ec, err := q.streamService.Stream(meta)
	if err != nil {
		q.logger.Error().Err(err).Msg("fail to get measure execution context")
		return
	}

	analyzer, err := logical.CreateAnalyzerFromMetaService(q.metaService)
	if err != nil {
		q.logger.Error().Err(err).Msg("fail to build analyzer")
		return
	}

	s, err := analyzer.BuildStreamSchema(context.TODO(), meta)
	if err != nil {
		q.logger.Error().Err(err).Msg("fail to build trace schema")
		return
	}

	p, err := analyzer.Analyze(context.TODO(), queryCriteria, meta, s)
	if err != nil {
		q.logger.Error().Err(err).Msg("fail to analyze the query request")
		return
	}

	entities, err := p.Execute(ec)
	if err != nil {
		q.logger.Error().Err(err).Msg("fail to execute the query plan")
		return
	}

	now := time.Now().UnixNano()
	resp = bus.NewMessage(bus.MessageID(now), entities)

	return
}

func (q *queryProcessor) Name() string {
	return moduleName
}

func (q *queryProcessor) PreRun() error {
	q.log = logger.GetLogger(moduleName)
	return q.pipeline.Subscribe(data.TopicStreamQuery, q)
}
