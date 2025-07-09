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

	"golang.org/x/exp/slices"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
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

type streamQueryProcessor struct {
	streamService stream.Service
	*queryService
	*bus.UnImplementedHealthyListener
}

func (p *streamQueryProcessor) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	n := time.Now()
	now := n.UnixNano()
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
	var metadata []*commonv1.Metadata
	var schemas []logical.Schema
	var ecc []executor.StreamExecutionContext
	for i := range queryCriteria.Groups {
		meta := &commonv1.Metadata{
			Name:  queryCriteria.Name,
			Group: queryCriteria.Groups[i],
		}
		ec, err := p.streamService.Stream(meta)
		if err != nil {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to get execution context for stream %s: %v", meta.GetName(), err))
			return
		}
		ecc = append(ecc, ec)
		s, err := logical_stream.BuildSchema(ec.GetSchema(), ec.GetIndexRules())
		if err != nil {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to build schema for stream %s: %v", meta.GetName(), err))
			return
		}
		schemas = append(schemas, s)
		metadata = append(metadata, meta)
	}

	plan, err := logical_stream.Analyze(queryCriteria, metadata, schemas, ecc)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to analyze the query request for stream %s: %v", queryCriteria.GetName(), err))
		return
	}

	if p.log.Debug().Enabled() {
		p.log.Debug().Str("plan", plan.String()).Msg("query plan")
	}
	var tracer *query.Tracer
	var span *query.Span
	if queryCriteria.Trace {
		tracer, ctx = query.NewTracer(ctx, n.Format(time.RFC3339Nano))
		span, ctx = tracer.StartSpan(ctx, "data-%s", p.queryService.nodeID)
		span.Tag("plan", plan.String())
		defer func() {
			data := resp.Data()
			switch d := data.(type) {
			case *streamv1.QueryResponse:
				d.Trace = tracer.ToProto()
			case *common.Error:
				span.Error(errors.New(d.Error()))
				resp = bus.NewMessage(bus.MessageID(now), &measurev1.QueryResponse{Trace: tracer.ToProto()})
			default:
				panic("unexpected data type")
			}
			span.Stop()
		}()
	}
	se := plan.(executor.StreamExecutable)
	defer se.Close()
	entities, err := se.Execute(ctx)
	if err != nil {
		p.log.Error().Err(err).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to execute the query plan")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("execute the query plan for stream %s: %v", queryCriteria.GetName(), err))
		return
	}

	resp = bus.NewMessage(bus.MessageID(now), &streamv1.QueryResponse{Elements: entities})

	if !queryCriteria.Trace && p.slowQuery > 0 {
		latency := time.Since(n)
		if latency > p.slowQuery {
			p.log.Warn().Dur("latency", latency).RawJSON("req", logger.Proto(queryCriteria)).Int("resp_count", len(entities)).Msg("stream slow query")
		}
	}
	return
}

type measureQueryProcessor struct {
	measureService measure.Service
	*queryService
	*bus.UnImplementedHealthyListener
}

func (p *measureQueryProcessor) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	queryCriteria, ok := message.Data().(*measurev1.QueryRequest)
	n := time.Now()
	now := n.UnixNano()
	if !ok {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("invalid event data type"))
		return
	}
	if queryCriteria.RewriteAggTopNResult {
		queryCriteria.Top.Number *= 2
	}
	resp = p.executeQuery(ctx, queryCriteria)

	if queryCriteria.RewriteAggTopNResult {
		result, handleErr := handleResponse(resp)
		if handleErr != nil {
			return
		}
		if len(result) == 0 {
			return
		}
		groupByTags := make([]string, 0)
		if queryCriteria.GetGroupBy() != nil {
			for _, tagFamily := range queryCriteria.GetGroupBy().GetTagProjection().GetTagFamilies() {
				groupByTags = append(groupByTags, tagFamily.GetTags()...)
			}
		}
		tagValueMap := make(map[string][]*modelv1.TagValue)
		for _, dp := range result {
			for _, tagFamily := range dp.GetTagFamilies() {
				for _, tag := range tagFamily.GetTags() {
					tagName := tag.GetKey()
					if len(groupByTags) == 0 || slices.Contains(groupByTags, tagName) {
						tagValueMap[tagName] = append(tagValueMap[tagName], tag.GetValue())
					}
				}
			}
		}
		rewriteCriteria, err := rewriteCriteria(tagValueMap)
		if err != nil {
			p.log.Error().Err(err).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to rewrite the query criteria")
			return
		}
		rewriteQueryCriteria := &measurev1.QueryRequest{
			Groups:          queryCriteria.Groups,
			Name:            queryCriteria.Name,
			TimeRange:       queryCriteria.TimeRange,
			Criteria:        rewriteCriteria,
			TagProjection:   queryCriteria.TagProjection,
			FieldProjection: queryCriteria.FieldProjection,
		}
		resp = p.executeQuery(ctx, rewriteQueryCriteria)
		dataPoints, handleErr := handleResponse(resp)
		if handleErr != nil {
			return
		}
		resp = bus.NewMessage(bus.MessageID(now), &measurev1.QueryResponse{DataPoints: dataPoints})
	}
	return
}

func (p *measureQueryProcessor) executeQuery(ctx context.Context, queryCriteria *measurev1.QueryRequest) (resp bus.Message) {
	n := time.Now()
	now := n.UnixNano()
	defer func() {
		if err := recover(); err != nil {
			p.log.Error().Interface("err", err).RawJSON("req", logger.Proto(queryCriteria)).Str("stack", string(debug.Stack())).Msg("panic")
			resp = bus.NewMessage(bus.MessageID(time.Now().UnixNano()), common.NewError("panic"))
		}
	}()
	var metadata []*commonv1.Metadata
	var schemas []logical.Schema
	var ecc []executor.MeasureExecutionContext
	for i := range queryCriteria.Groups {
		meta := &commonv1.Metadata{
			Name:  queryCriteria.Name,
			Group: queryCriteria.Groups[i],
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
		ecc = append(ecc, ec)
		schemas = append(schemas, s)
		metadata = append(metadata, meta)
	}
	ml := p.log.Named("measure", queryCriteria.Groups[0], queryCriteria.Name)
	if e := ml.Debug(); e.Enabled() {
		e.RawJSON("req", logger.Proto(queryCriteria)).Msg("received a query event")
	}

	plan, err := logical_measure.Analyze(queryCriteria, metadata, schemas, ecc)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to analyze the query request for measure %s: %v", queryCriteria.GetName(), err))
		return
	}
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
			case *common.Error:
				span.Error(errors.New(d.Error()))
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

	mIterator, err := plan.(executor.MeasureExecutable).Execute(ctx)
	if err != nil {
		ml.Error().Err(err).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to query")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to execute the query plan for measure %s: %v", queryCriteria.GetName(), err))
		return
	}
	defer func() {
		if err = mIterator.Close(); err != nil {
			ml.Error().Err(err).Dur("latency", time.Since(n)).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to close the query plan")
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
	if !queryCriteria.Trace && p.slowQuery > 0 {
		latency := time.Since(n)
		if latency > p.slowQuery {
			p.log.Warn().Dur("latency", latency).RawJSON("req", logger.Proto(queryCriteria)).Int("resp_count", len(result)).Msg("measure slow query")
		}
	}
	return
}

func handleResponse(resp bus.Message) ([]*measurev1.DataPoint, *common.Error) {
	data := resp.Data()
	switch d := data.(type) {
	case *common.Error:
		return nil, d
	case *measurev1.QueryResponse:
		return d.DataPoints, nil
	default:
		return nil, common.NewError("unexpected response data type: %T", d)
	}
}

func rewriteCriteria(tagValueMap map[string][]*modelv1.TagValue) (*modelv1.Criteria, error) {
	var tagConditions []*modelv1.Condition
	for tagName, tagValues := range tagValueMap {
		if len(tagValues) == 0 {
			continue
		}
		switch tagValues[0].GetValue().(type) {
		case *modelv1.TagValue_Str:
			valueSet := make(map[string]bool)
			for _, value := range tagValues {
				if strVal, ok := value.GetValue().(*modelv1.TagValue_Str); ok {
					valueSet[strVal.Str.GetValue()] = true
				}
			}
			values := make([]string, 0, len(valueSet))
			for value := range valueSet {
				values = append(values, value)
			}
			condition := &modelv1.Condition{
				Name: tagName,
				Op:   modelv1.Condition_BINARY_OP_IN,
				Value: &modelv1.TagValue{
					Value: &modelv1.TagValue_StrArray{
						StrArray: &modelv1.StrArray{
							Value: values,
						},
					},
				},
			}
			tagConditions = append(tagConditions, condition)
		case *modelv1.TagValue_Int:
			valueSet := make(map[int64]bool)
			for _, value := range tagValues {
				if intVal, ok := value.GetValue().(*modelv1.TagValue_Int); ok {
					valueSet[intVal.Int.GetValue()] = true
				}
			}
			values := make([]int64, 0, len(valueSet))
			for value := range valueSet {
				values = append(values, value)
			}
			condition := &modelv1.Condition{
				Name: tagName,
				Op:   modelv1.Condition_BINARY_OP_IN,
				Value: &modelv1.TagValue{
					Value: &modelv1.TagValue_IntArray{
						IntArray: &modelv1.IntArray{
							Value: values,
						},
					},
				},
			}
			tagConditions = append(tagConditions, condition)
		default:
			return nil, fmt.Errorf("unsupported tag value type: %T", tagValues[0].GetValue())
		}
	}
	return buildCriteriaTree(tagConditions), nil
}

func buildCriteriaTree(conditions []*modelv1.Condition) *modelv1.Criteria {
	if len(conditions) == 0 {
		return nil
	}
	return &modelv1.Criteria{
		Exp: &modelv1.Criteria_Le{
			Le: &modelv1.LogicalExpression{
				Op: modelv1.LogicalExpression_LOGICAL_OP_AND,
				Left: &modelv1.Criteria{
					Exp: &modelv1.Criteria_Condition{
						Condition: conditions[0],
					},
				},
				Right: buildCriteriaTree(conditions[1:]),
			},
		},
	}
}
