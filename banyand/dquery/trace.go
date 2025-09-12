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
	"errors"
	"slices"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/iter"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	logical_trace "github.com/apache/skywalking-banyandb/pkg/query/logical/trace"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

type traceQueryProcessor struct {
	traceService trace.Service
	broadcaster  bus.Broadcaster
	*queryService
	*bus.UnImplementedHealthyListener
}

func (p *traceQueryProcessor) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	n := time.Now()
	now := n.UnixNano()
	queryCriteria, ok := message.Data().(*tracev1.QueryRequest)
	if !ok {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("invalid event data type"))
		return
	}
	if p.log.Debug().Enabled() {
		p.log.Debug().RawJSON("criteria", logger.Proto(queryCriteria)).Msg("received a query request")
	}

	var schemas []logical.Schema
	for _, group := range queryCriteria.Groups {
		meta := &commonv1.Metadata{
			Name:  queryCriteria.Name,
			Group: group,
		}
		ec, err := p.traceService.Trace(meta)
		if err != nil {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to get execution context for trace %s: %v", meta.GetName(), err))
			return
		}
		s, err := logical_trace.BuildSchema(ec.GetSchema(), ec.GetIndexRules())
		if err != nil {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to build schema for trace %s: %v", meta.GetName(), err))
			return
		}
		schemas = append(schemas, s)
	}

	plan, err := logical_trace.DistributedAnalyze(queryCriteria, schemas)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to analyze the query request for trace %s: %v", queryCriteria.Name, err))
		return
	}

	if p.log.Debug().Enabled() {
		p.log.Debug().Str("plan", plan.String()).Msg("query plan")
	}
	nodeSelectors := make(map[string][]string)
	for _, g := range queryCriteria.Groups {
		if gs, ok := p.traceService.LoadGroup(g); ok {
			if ns, exist := p.parseNodeSelector(queryCriteria.Stages, gs.GetSchema().ResourceOpts); exist {
				nodeSelectors[g] = ns
			} else if len(gs.GetSchema().ResourceOpts.Stages) > 0 {
				p.log.Error().Strs("req_stages", queryCriteria.Stages).Strs("default_stages", gs.GetSchema().GetResourceOpts().GetDefaultStages()).Msg("no stage found")
				resp = bus.NewMessage(bus.MessageID(now), common.NewError("no stage found in request or default stages in resource opts"))
				return
			}
		} else {
			p.log.Error().RawJSON("req", logger.Proto(queryCriteria)).Msg("group not found")
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("group %s not found", g))
			return
		}
	}
	if len(queryCriteria.Stages) > 0 && len(nodeSelectors) == 0 {
		p.log.Error().RawJSON("req", logger.Proto(queryCriteria)).Msg("no stage found")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("no stage found"))
		return
	}
	if queryCriteria.Trace {
		var tracer *query.Tracer
		var span *query.Span
		tracer, ctx = query.NewTracer(ctx, n.Format(time.RFC3339Nano))
		span, ctx = tracer.StartSpan(ctx, "distributed-%s", p.queryService.nodeID)
		span.Tag("plan", plan.String())
		span.Tagf("nodeSelectors", "%v", nodeSelectors)
		defer func() {
			data := resp.Data()
			switch d := data.(type) {
			case *tracev1.QueryResponse:
				d.TraceQueryResult = tracer.ToProto()
			case *common.Error:
				span.Error(errors.New(d.Error()))
				resp = bus.NewMessage(bus.MessageID(now), &tracev1.QueryResponse{TraceQueryResult: tracer.ToProto()})
			default:
				panic("unexpected data type")
			}
			span.Stop()
		}()
	}
	te := plan.(executor.TraceExecutable)
	defer te.Close()
	resultIterator, err := te.Execute(executor.WithDistributedExecutionContext(ctx, &distributedContext{
		Broadcaster:   p.broadcaster,
		timeRange:     queryCriteria.TimeRange,
		nodeSelectors: nodeSelectors,
	}))
	if err != nil {
		p.log.Error().Err(err).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to execute the query plan")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("execute the query plan for trace %s: %v", queryCriteria.Name, err))
		return
	}

	traces := BuildTracesFromResult(resultIterator, queryCriteria)
	resp = bus.NewMessage(bus.MessageID(now), &tracev1.QueryResponse{Traces: traces})
	if !queryCriteria.Trace && p.slowQuery > 0 {
		latency := time.Since(n)
		if latency > p.slowQuery {
			spanCount := 0
			for _, trace := range traces {
				spanCount += len(trace.Spans)
			}
			p.log.Warn().Dur("latency", latency).RawJSON("req", logger.Proto(queryCriteria)).Int("resp_count", spanCount).Msg("trace slow query")
		}
	}
	return
}

// BuildTracesFromResult builds traces from the result iterator.
func BuildTracesFromResult(resultIterator iter.Iterator[model.TraceResult], queryCriteria *tracev1.QueryRequest) []*tracev1.Trace {
	traceMap := make(map[string]*tracev1.Trace)
	for {
		result, hasNext := resultIterator.Next()
		if !hasNext {
			break
		}
		traceID := result.TID
		trace, exists := traceMap[traceID]
		if !exists {
			trace = &tracev1.Trace{
				Spans: make([]*tracev1.Span, 0),
			}
			traceMap[traceID] = trace
		}
		for i, spanBytes := range result.Spans {
			var traceTags []*modelv1.Tag
			if result.Tags != nil && len(queryCriteria.TagProjection) > 0 {
				for _, tag := range result.Tags {
					if !slices.Contains(queryCriteria.TagProjection, tag.Name) {
						continue
					}
					if i < len(tag.Values) {
						traceTags = append(traceTags, &modelv1.Tag{
							Key:   tag.Name,
							Value: tag.Values[i],
						})
					}
				}
			}
			span := &tracev1.Span{
				Tags: traceTags,
				Span: spanBytes,
			}
			trace.Spans = append(trace.Spans, span)
		}
	}
	var traces []*tracev1.Trace
	for _, trace := range traceMap {
		traces = append(traces, trace)
	}
	return traces
}
