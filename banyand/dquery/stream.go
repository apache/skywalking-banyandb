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

package dquery

import (
	"context"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	logical_stream "github.com/apache/skywalking-banyandb/pkg/query/logical/stream"
)

type streamQueryProcessor struct {
	streamService stream.SchemaService
	broadcaster   bus.Broadcaster
	*queryService
}

func (p *streamQueryProcessor) Rev(message bus.Message) (resp bus.Message) {
	now := time.Now().UnixNano()
	queryCriteria, ok := message.Data().(*streamv1.QueryRequest)
	if !ok {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("invalid event data type"))
		return
	}
	if p.log.Debug().Enabled() {
		p.log.Debug().RawJSON("criteria", logger.Proto(queryCriteria)).Msg("received a query request")
	}

	meta := queryCriteria.GetMetadata()
	ec, err := p.streamService.Stream(meta)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to get execution context for stream %s: %v", meta.GetName(), err))
		return
	}
	s, err := logical_stream.BuildSchema(ec.GetSchema(), ec.GetIndexRules())
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to build schema for stream %s: %v", meta.GetName(), err))
		return
	}

	plan, err := logical_stream.DistributedAnalyze(queryCriteria, s)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to analyze the query request for stream %s: %v", meta.GetName(), err))
		return
	}

	if p.log.Debug().Enabled() {
		p.log.Debug().Str("plan", plan.String()).Msg("query plan")
	}
	entities, err := plan.(executor.StreamExecutable).Execute(executor.WithDistributedExecutionContext(context.Background(), &distributedContext{
		Broadcaster: p.broadcaster,
		timeRange:   queryCriteria.TimeRange,
	}))
	if err != nil {
		p.log.Error().Err(err).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to execute the query plan")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("execute the query plan for stream %s: %v", meta.GetName(), err))
		return
	}

	resp = bus.NewMessage(bus.MessageID(now), &streamv1.QueryResponse{Elements: entities})

	return
}
