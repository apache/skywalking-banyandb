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
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	logical_measure "github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
	logical_stream "github.com/apache/skywalking-banyandb/pkg/query/logical/stream"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	moduleName = "query"
)

var (
	_ run.PreRunner       = (*queryService)(nil)
	_ bus.MessageListener = (*streamQueryProcessor)(nil)
	_ bus.MessageListener = (*measureQueryProcessor)(nil)
	_ bus.MessageListener = (*topNQueryProcessor)(nil)
)

type queryService struct {
	metaService metadata.Repo
	pipeline    queue.Server
	log         *logger.Logger
	sqp         *streamQueryProcessor
	mqp         *measureQueryProcessor
	tqp         *topNQueryProcessor
	nodeID      string
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
	if p.log.Debug().Enabled() {
		p.log.Debug().RawJSON("criteria", logger.Proto(queryCriteria)).Msg("received a query request")
	}
	defer func() {
		if err := recover(); err != nil {
			p.log.Error().Interface("err", err).RawJSON("req", logger.Proto(queryCriteria)).Str("stack", string(debug.Stack())).Msg("panic")
			resp = bus.NewMessage(bus.MessageID(time.Now().UnixNano()), common.NewError("panic"))
		}
	}()
	// TODO: support multiple groups
	if len(queryCriteria.Groups) > 1 {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("only support one group in the query request"))
		return
	}
	meta := &commonv1.Metadata{
		Name:  queryCriteria.Name,
		Group: queryCriteria.Groups[0],
	}
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

	plan, err := logical_stream.Analyze(context.TODO(), queryCriteria, meta, s)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to analyze the query request for stream %s: %v", meta.GetName(), err))
		return
	}

	if p.log.Debug().Enabled() {
		p.log.Debug().Str("plan", plan.String()).Msg("query plan")
	}
	entities, err := plan.(executor.StreamExecutable).Execute(executor.WithStreamExecutionContext(context.Background(), ec))
	if err != nil {
		p.log.Error().Err(err).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to execute the query plan")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("execute the query plan for stream %s: %v", meta.GetName(), err))
		return
	}

	resp = bus.NewMessage(bus.MessageID(now), &streamv1.QueryResponse{Elements: entities})

	return
}

type measureQueryProcessor struct {
	measureService measure.Service
	*queryService
}

func (p *measureQueryProcessor) Rev(message bus.Message) (resp bus.Message) {
	queryCriteria, ok := message.Data().(*measurev1.QueryRequest)
	n := time.Now()
	now := n.UnixNano()
	if !ok {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("invalid event data type"))
		return
	}
	defer func() {
		if err := recover(); err != nil {
			p.log.Error().Interface("err", err).RawJSON("req", logger.Proto(queryCriteria)).Str("stack", string(debug.Stack())).Msg("panic")
			resp = bus.NewMessage(bus.MessageID(time.Now().UnixNano()), common.NewError("panic"))
		}
	}()
	// TODO: support multiple groups
	if len(queryCriteria.Groups) > 1 {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("only support one group in the query request"))
		return
	}
	ml := p.log.Named("measure", queryCriteria.Groups[0], queryCriteria.Name)
	if e := ml.Debug(); e.Enabled() {
		e.RawJSON("req", logger.Proto(queryCriteria)).Msg("received a query event")
	}

	meta := &commonv1.Metadata{
		Name:  queryCriteria.Name,
		Group: queryCriteria.Groups[0],
	}
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

	plan, err := logical_measure.Analyze(context.TODO(), queryCriteria, meta, s)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to analyze the query request for measure %s: %v", meta.GetName(), err))
		return
	}
	ctx := context.Background()
	var tracer *query.Tracer
	var span *query.Span
	if queryCriteria.Trace {
		tracer, ctx = query.NewTracer(ctx, n.Format(time.RFC3339Nano))
		span, ctx = tracer.StartSpan(ctx, "data-%s", p.queryService.nodeID)
		span.Tag("plan", plan.String())
		defer func() {
			data := resp.Data()
			switch d := data.(type) {
			case *measurev1.QueryResponse:
				d.Trace = tracer.ToProto()
			case common.Error:
				span.Error(errors.New(d.Msg()))
				resp = bus.NewMessage(bus.MessageID(now), &measurev1.QueryResponse{Trace: tracer.ToProto()})
			default:
				panic("unexpected data type")
			}
			span.Stop()
		}()
	}

	if e := ml.Debug(); e.Enabled() {
		e.Str("plan", plan.String()).Msg("query plan")
	}

	mIterator, err := plan.(executor.MeasureExecutable).Execute(executor.WithMeasureExecutionContext(ctx, ec))
	if err != nil {
		ml.Error().Err(err).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to close the query plan")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to execute the query plan for measure %s: %v", meta.GetName(), err))
		return
	}
	defer func() {
		if err = mIterator.Close(); err != nil {
			ml.Error().Err(err).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to close the query plan")
			if span != nil {
				span.Error(fmt.Errorf("fail to close the query plan: %w", err))
			}
		}
	}()

	result := make([]*measurev1.DataPoint, 0)
	func() {
		var r int
		if tracer != nil {
			iterSpan, _ := tracer.StartSpan(ctx, "iterator")
			defer func() {
				iterSpan.Tag("rounds", fmt.Sprintf("%d", r))
				iterSpan.Tag("size", fmt.Sprintf("%d", len(result)))
				iterSpan.Stop()
			}()
		}
		for mIterator.Next() {
			r++
			current := mIterator.Current()
			if len(current) > 0 {
				result = append(result, current[0])
			}
		}
	}()
	qr := &measurev1.QueryResponse{DataPoints: result}
	if e := ml.Debug(); e.Enabled() {
		e.RawJSON("ret", logger.Proto(qr)).Msg("got a measure")
	}
	resp = bus.NewMessage(bus.MessageID(now), qr)
	return
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
		q.pipeline.Subscribe(data.TopicTopNQuery, q.tqp),
	)
}
