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
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	logical_measure "github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
)

type measureQueryProcessor struct {
	measureService measure.SchemaService
	broadcaster    bus.Broadcaster
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

	s, err := logical_measure.BuildSchema(ec.GetSchema(), ec.GetIndexRules())
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to build schema for measure %s: %v", meta.GetName(), err))
		return
	}

	plan, err := logical_measure.DistributedAnalyze(queryCriteria, s)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to analyze the query request for measure %s: %v", meta.GetName(), err))
		return
	}

	if e := ml.Debug(); e.Enabled() {
		e.Str("plan", plan.String()).Msg("query plan")
	}

	mIterator, err := plan.(executor.MeasureExecutable).Execute(executor.WithDistributedExecutionContext(context.Background(), &distributedContext{
		Broadcaster: p.broadcaster,
		timeRange:   queryCriteria.TimeRange,
	}))
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
	resp = bus.NewMessage(bus.MessageID(now), &measurev1.QueryResponse{DataPoints: result})
	return
}
