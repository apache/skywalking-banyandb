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
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/iter"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	logical_measure "github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
	logical_stream "github.com/apache/skywalking-banyandb/pkg/query/logical/stream"
	logical_trace "github.com/apache/skywalking-banyandb/pkg/query/logical/trace"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

const (
	moduleName = "query"
)

var (
	_ bus.MessageListener            = (*streamQueryProcessor)(nil)
	_ bus.MessageListener            = (*measureQueryProcessor)(nil)
	_ bus.MessageListener            = (*measureInternalQueryProcessor)(nil)
	_ bus.MessageListener            = (*traceQueryProcessor)(nil)
	_ executor.TraceExecutionContext = trace.Trace(nil)
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
				span.Tag("resp_count", fmt.Sprintf("%d", len(d.Elements)))
				span.Stop()
				d.Trace = tracer.ToProto()
			case *common.Error:
				span.Error(errors.New(d.Error()))
				span.Stop()
				resp = bus.NewMessage(bus.MessageID(now), &measurev1.QueryResponse{Trace: tracer.ToProto()})
			default:
				panic("unexpected data type")
			}
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

// measureExecutionContext holds the common execution context for measure queries.
type measureExecutionContext struct {
	ml       *logger.Logger
	ecc      []executor.MeasureExecutionContext
	metadata []*commonv1.Metadata
	schemas  []logical.Schema
}

// buildMeasureContext builds the execution context for a measure query.
func buildMeasureContext(measureService measure.Service, log *logger.Logger, queryCriteria *measurev1.QueryRequest, logPrefix string) (*measureExecutionContext, error) {
	var metadata []*commonv1.Metadata
	var schemas []logical.Schema
	var ecc []executor.MeasureExecutionContext
	for i := range queryCriteria.Groups {
		meta := &commonv1.Metadata{
			Name:  queryCriteria.Name,
			Group: queryCriteria.Groups[i],
		}
		ec, ecErr := measureService.Measure(meta)
		if ecErr != nil {
			return nil, fmt.Errorf("fail to get execution context for measure %s: %w", meta.GetName(), ecErr)
		}
		s, schemaErr := logical_measure.BuildSchema(ec.GetSchema(), ec.GetIndexRules())
		if schemaErr != nil {
			return nil, fmt.Errorf("fail to build schema for measure %s: %w", meta.GetName(), schemaErr)
		}
		ecc = append(ecc, ec)
		schemas = append(schemas, s)
		metadata = append(metadata, meta)
	}
	ml := log.Named(logPrefix, queryCriteria.Groups[0], queryCriteria.Name)
	return &measureExecutionContext{
		metadata: metadata,
		schemas:  schemas,
		ecc:      ecc,
		ml:       ml,
	}, nil
}

// executeMeasurePlan executes the measure query plan and returns the iterator.
func executeMeasurePlan(ctx context.Context, queryCriteria *measurev1.QueryRequest, mctx *measureExecutionContext) (executor.MIterator, logical.Plan, error) {
	plan, planErr := logical_measure.Analyze(queryCriteria, mctx.metadata, mctx.schemas, mctx.ecc)
	if planErr != nil {
		return nil, nil, fmt.Errorf("fail to analyze the query request for measure %s: %w", queryCriteria.GetName(), planErr)
	}
	if e := mctx.ml.Debug(); e.Enabled() {
		e.Str("plan", plan.String()).Msg("query plan")
	}
	mIterator, execErr := plan.(executor.MeasureExecutable).Execute(ctx)
	if execErr != nil {
		mctx.ml.Error().Err(execErr).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to query")
		return nil, nil, fmt.Errorf("fail to execute the query plan for measure %s: %w", queryCriteria.GetName(), execErr)
	}
	return mIterator, plan, nil
}

// extractTagValuesFromInternalDataPoints extracts tag values from InternalDataPoints for RewriteAggTopNResult.
func extractTagValuesFromInternalDataPoints(dataPoints []*measurev1.InternalDataPoint, groupByTags []string) map[string][]*modelv1.TagValue {
	tagValueMap := make(map[string][]*modelv1.TagValue)
	for _, idp := range dataPoints {
		if idp.DataPoint != nil {
			extractTagValuesFromDataPoint(idp.DataPoint, groupByTags, tagValueMap)
		}
	}
	return tagValueMap
}

// collectInternalDataPoints collects InternalDataPoints from the iterator.
func collectInternalDataPoints(mIterator executor.MIterator) []*measurev1.InternalDataPoint {
	result := make([]*measurev1.InternalDataPoint, 0)
	for mIterator.Next() {
		current := mIterator.Current()
		if len(current) > 0 {
			result = append(result, current[0])
		}
	}
	return result
}

// extractTagValuesFromDataPoints extracts tag values from DataPoints for RewriteAggTopNResult.
func extractTagValuesFromDataPoints(dataPoints []*measurev1.DataPoint, groupByTags []string) map[string][]*modelv1.TagValue {
	tagValueMap := make(map[string][]*modelv1.TagValue)
	for _, dp := range dataPoints {
		extractTagValuesFromDataPoint(dp, groupByTags, tagValueMap)
	}
	return tagValueMap
}

// extractTagValuesFromDataPoint extracts tag values from a single DataPoint and appends to tagValueMap.
func extractTagValuesFromDataPoint(dp *measurev1.DataPoint, groupByTags []string, tagValueMap map[string][]*modelv1.TagValue) {
	for _, tagFamily := range dp.GetTagFamilies() {
		for _, tag := range tagFamily.GetTags() {
			tagName := tag.GetKey()
			if len(groupByTags) == 0 || slices.Contains(groupByTags, tagName) {
				tagValueMap[tagName] = append(tagValueMap[tagName], tag.GetValue())
			}
		}
	}
}

// getGroupByTags extracts group by tag names from query criteria.
func getGroupByTags(queryCriteria *measurev1.QueryRequest) []string {
	groupByTags := make([]string, 0)
	if queryCriteria.GetGroupBy() != nil {
		for _, tagFamily := range queryCriteria.GetGroupBy().GetTagProjection().GetTagFamilies() {
			groupByTags = append(groupByTags, tagFamily.GetTags()...)
		}
	}
	return groupByTags
}

// buildRewriteQueryCriteria builds the rewrite query criteria for RewriteAggTopNResult.
func buildRewriteQueryCriteria(queryCriteria *measurev1.QueryRequest, rewrittenCriteria *modelv1.Criteria) *measurev1.QueryRequest {
	return &measurev1.QueryRequest{
		Groups:          queryCriteria.Groups,
		Name:            queryCriteria.Name,
		TimeRange:       queryCriteria.TimeRange,
		Criteria:        rewrittenCriteria,
		TagProjection:   queryCriteria.TagProjection,
		FieldProjection: queryCriteria.FieldProjection,
	}
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
		groupByTags := getGroupByTags(queryCriteria)
		tagValueMap := extractTagValuesFromDataPoints(result, groupByTags)
		rewrittenCriteria, rewriteErr := rewriteCriteria(tagValueMap)
		if rewriteErr != nil {
			p.log.Error().Err(rewriteErr).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to rewrite the query criteria")
			return
		}
		rewriteQueryCriteria := buildRewriteQueryCriteria(queryCriteria, rewrittenCriteria)
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
		if recoverErr := recover(); recoverErr != nil {
			p.log.Error().Interface("err", recoverErr).RawJSON("req", logger.Proto(queryCriteria)).Str("stack", string(debug.Stack())).Msg("panic")
			resp = bus.NewMessage(bus.MessageID(time.Now().UnixNano()), common.NewError("panic"))
		}
	}()

	mctx, buildErr := buildMeasureContext(p.measureService, p.log, queryCriteria, "measure")
	if buildErr != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("%v", buildErr))
		return
	}
	if e := mctx.ml.Debug(); e.Enabled() {
		e.RawJSON("req", logger.Proto(queryCriteria)).Msg("received a query event")
	}

	mIterator, plan, execErr := executeMeasurePlan(ctx, queryCriteria, mctx)
	if execErr != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("%v", execErr))
		return
	}
	defer func() {
		if closeErr := mIterator.Close(); closeErr != nil {
			mctx.ml.Error().Err(closeErr).Dur("latency", time.Since(n)).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to close the query plan")
		}
	}()

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
				span.Tag("resp_count", fmt.Sprintf("%d", len(d.DataPoints)))
				d.Trace = tracer.ToProto()
				span.Stop()
			case *common.Error:
				span.Error(errors.New(d.Error()))
				span.Stop()
				resp = bus.NewMessage(bus.MessageID(now), &measurev1.QueryResponse{Trace: tracer.ToProto()})
			default:
				panic("unexpected data type")
			}
		}()
	}

	var result []*measurev1.DataPoint
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
				result = append(result, current[0].GetDataPoint())
			}
		}
	}()

	qr := &measurev1.QueryResponse{DataPoints: result}
	if e := mctx.ml.Debug(); e.Enabled() {
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

type measureInternalQueryProcessor struct {
	measureService measure.Service
	*queryService
	*bus.UnImplementedHealthyListener
}

func (p *measureInternalQueryProcessor) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	internalRequest, ok := message.Data().(*measurev1.InternalQueryRequest)
	n := time.Now()
	now := n.UnixNano()
	if !ok {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("invalid event data type"))
		return
	}
	queryCriteria := internalRequest.GetRequest()
	if queryCriteria == nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("query request is nil"))
		return
	}
	// Handle RewriteAggTopNResult: double the top number for initial query
	if queryCriteria.RewriteAggTopNResult {
		queryCriteria.Top.Number *= 2
	}
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			p.log.Error().Interface("err", recoverErr).RawJSON("req", logger.Proto(queryCriteria)).Str("stack", string(debug.Stack())).Msg("panic")
			resp = bus.NewMessage(bus.MessageID(time.Now().UnixNano()), common.NewError("panic"))
		}
	}()

	mctx, buildErr := buildMeasureContext(p.measureService, p.log, queryCriteria, "internal-measure")
	if buildErr != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("%v", buildErr))
		return
	}
	if e := mctx.ml.Debug(); e.Enabled() {
		e.RawJSON("req", logger.Proto(queryCriteria)).Msg("received an internal query event")
	}

	mIterator, plan, execErr := executeMeasurePlan(ctx, queryCriteria, mctx)
	if execErr != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("%v", execErr))
		return
	}
	defer func() {
		if closeErr := mIterator.Close(); closeErr != nil {
			mctx.ml.Error().Err(closeErr).Dur("latency", time.Since(n)).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to close the query plan")
		}
	}()

	var tracer *query.Tracer
	var span *query.Span
	if queryCriteria.Trace {
		tracer, ctx = query.NewTracer(ctx, n.Format(time.RFC3339Nano))
		span, ctx = tracer.StartSpan(ctx, "data-%s", p.queryService.nodeID)
		span.Tag("plan", plan.String())
		defer func() {
			respData := resp.Data()
			switch d := respData.(type) {
			case *measurev1.InternalQueryResponse:
				span.Tag("resp_count", fmt.Sprintf("%d", len(d.DataPoints)))
				d.Trace = tracer.ToProto()
				span.Stop()
			case *common.Error:
				span.Error(errors.New(d.Error()))
				span.Stop()
				resp = bus.NewMessage(bus.MessageID(now), &measurev1.InternalQueryResponse{Trace: tracer.ToProto()})
			default:
				panic("unexpected data type")
			}
		}()
	}

	result := collectInternalDataPoints(mIterator)

	// Handle RewriteAggTopNResult: rewrite query to get original data with Timestamp
	if queryCriteria.RewriteAggTopNResult && len(result) > 0 {
		groupByTags := getGroupByTags(queryCriteria)
		tagValueMap := extractTagValuesFromInternalDataPoints(result, groupByTags)
		rewrittenCriteria, rewriteErr := rewriteCriteria(tagValueMap)
		if rewriteErr != nil {
			mctx.ml.Error().Err(rewriteErr).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to rewrite the query criteria")
		} else {
			rewriteQueryCriteria := buildRewriteQueryCriteria(queryCriteria, rewrittenCriteria)
			rewriteIterator, _, rewriteExecErr := executeMeasurePlan(ctx, rewriteQueryCriteria, mctx)
			if rewriteExecErr != nil {
				mctx.ml.Error().Err(rewriteExecErr).RawJSON("req", logger.Proto(rewriteQueryCriteria)).Msg("fail to execute the rewrite query plan")
			} else {
				defer func() {
					if closeErr := rewriteIterator.Close(); closeErr != nil {
						mctx.ml.Error().Err(closeErr).Msg("fail to close the rewrite query plan")
					}
				}()
				result = collectInternalDataPoints(rewriteIterator)
			}
		}
	}

	qr := &measurev1.InternalQueryResponse{DataPoints: result}
	if e := mctx.ml.Debug(); e.Enabled() {
		e.RawJSON("ret", logger.Proto(qr)).Msg("got an internal measure response")
	}
	resp = bus.NewMessage(bus.MessageID(now), qr)
	if !queryCriteria.Trace && p.slowQuery > 0 {
		latency := time.Since(n)
		if latency > p.slowQuery {
			p.log.Warn().Dur("latency", latency).RawJSON("req", logger.Proto(queryCriteria)).Int("resp_count", len(result)).Msg("internal measure slow query")
		}
	}
	return
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

type traceQueryProcessor struct {
	traceService trace.Service
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
		p.log.Debug().RawJSON("criteria", logger.Proto(queryCriteria)).Msg("received a trace query request")
	}
	defer func() {
		if err := recover(); err != nil {
			p.log.Error().Interface("err", err).RawJSON("req", logger.Proto(queryCriteria)).Str("stack", string(debug.Stack())).Msg("panic")
			resp = bus.NewMessage(bus.MessageID(time.Now().UnixNano()), common.NewError("panic"))
		}
	}()

	resp = p.executeQuery(ctx, queryCriteria)
	return
}

type traceExecutionPlan struct {
	traceIDTagNames   []string
	spanIDTagNames    []string
	timestampTagNames []string
	metadata          []*commonv1.Metadata
	schemas           []logical.Schema
	executionContexts []trace.Trace
}

func (p *traceQueryProcessor) setupTraceExecutionPlan(queryCriteria *tracev1.QueryRequest) (*traceExecutionPlan, *common.Error) {
	plan := &traceExecutionPlan{
		metadata:          make([]*commonv1.Metadata, 0, len(queryCriteria.Groups)),
		schemas:           make([]logical.Schema, 0, len(queryCriteria.Groups)),
		executionContexts: make([]trace.Trace, 0, len(queryCriteria.Groups)),
		traceIDTagNames:   make([]string, 0, len(queryCriteria.Groups)),
		spanIDTagNames:    make([]string, 0, len(queryCriteria.Groups)),
		timestampTagNames: make([]string, 0, len(queryCriteria.Groups)),
	}

	for i := range queryCriteria.Groups {
		meta := &commonv1.Metadata{
			Name:  queryCriteria.Name,
			Group: queryCriteria.Groups[i],
		}
		ec, err := p.traceService.Trace(meta)
		if err != nil {
			return nil, common.NewError("fail to get execution context for trace %s: %v", meta.GetName(), err)
		}

		s, err := logical_trace.BuildSchema(ec.GetSchema(), ec.GetIndexRules())
		if err != nil {
			return nil, common.NewError("fail to build schema for trace %s: %v", meta.GetName(), err)
		}

		// Validate tag name consistency
		if errMsg := p.validateTagNames(plan, ec, meta); errMsg != nil {
			return nil, errMsg
		}

		plan.executionContexts = append(plan.executionContexts, ec)
		plan.schemas = append(plan.schemas, s)
		plan.metadata = append(plan.metadata, meta)
		plan.traceIDTagNames = append(plan.traceIDTagNames, ec.GetSchema().GetTraceIdTagName())
		plan.spanIDTagNames = append(plan.spanIDTagNames, ec.GetSchema().GetSpanIdTagName())
		plan.timestampTagNames = append(plan.timestampTagNames, ec.GetSchema().GetTimestampTagName())
	}

	return plan, nil
}

func (p *traceQueryProcessor) validateTagNames(plan *traceExecutionPlan, ec trace.Trace, meta *commonv1.Metadata) *common.Error {
	if len(plan.traceIDTagNames) > 0 && plan.traceIDTagNames[0] != ec.GetSchema().GetTraceIdTagName() {
		return common.NewError("trace id tag name mismatch for trace %s: %s != %s",
			meta.GetName(), plan.traceIDTagNames[0], ec.GetSchema().GetTraceIdTagName())
	}
	if len(plan.spanIDTagNames) > 0 && plan.spanIDTagNames[0] != ec.GetSchema().GetSpanIdTagName() {
		return common.NewError("span id tag name mismatch for trace %s: %s != %s",
			meta.GetName(), plan.spanIDTagNames[0], ec.GetSchema().GetSpanIdTagName())
	}
	if len(plan.timestampTagNames) > 0 && plan.timestampTagNames[0] != ec.GetSchema().GetTimestampTagName() {
		return common.NewError("timestamp tag name mismatch for trace %s: %s != %s",
			meta.GetName(), plan.timestampTagNames[0], ec.GetSchema().GetTimestampTagName())
	}
	return nil
}

type traceMonitor struct {
	tracer *query.Tracer
	span   *query.Span
}

func (p *traceQueryProcessor) setupTraceMonitor(ctx context.Context, queryCriteria *tracev1.QueryRequest,
	plan logical.Plan, startTime time.Time,
) (context.Context, *traceMonitor) {
	if !queryCriteria.Trace {
		return ctx, nil
	}

	tracer, newCtx := query.NewTracer(ctx, startTime.Format(time.RFC3339Nano))
	span, newCtx := tracer.StartSpan(newCtx, "data-%s", p.queryService.nodeID)
	span.Tag("plan", plan.String())

	return newCtx, &traceMonitor{
		tracer: tracer,
		span:   span,
	}
}

func (tm *traceMonitor) finishTrace(resp *bus.Message, messageID int64) {
	if tm == nil {
		return
	}

	data := resp.Data()
	switch d := data.(type) {
	case *tracev1.InternalQueryResponse:
		tm.span.Tag("resp_count", fmt.Sprintf("%d", len(d.InternalTraces)))
		tm.span.Stop()
		d.TraceQueryResult = tm.tracer.ToProto()
	case *common.Error:
		tm.span.Error(errors.New(d.Error()))
		tm.span.Stop()
		*resp = bus.NewMessage(bus.MessageID(messageID), &tracev1.QueryResponse{TraceQueryResult: tm.tracer.ToProto()})
	default:
		panic("unexpected data type")
	}
}

func (p *traceQueryProcessor) processTraceResults(resultIterator iter.Iterator[model.TraceResult],
	queryCriteria *tracev1.QueryRequest, execPlan *traceExecutionPlan,
) ([]*tracev1.InternalTrace, error) {
	var traces []*tracev1.InternalTrace

	// Build tag inclusion maps for each group
	traceIDInclusionMap := make(map[int]bool)
	spanIDInclusionMap := make(map[int]bool)
	for i, tagName := range execPlan.traceIDTagNames {
		if slices.Contains(queryCriteria.TagProjection, tagName) {
			traceIDInclusionMap[i] = true
		}
	}
	for i, tagName := range execPlan.spanIDTagNames {
		if slices.Contains(queryCriteria.TagProjection, tagName) {
			spanIDInclusionMap[i] = true
		}
	}

	for {
		result, hasNext := resultIterator.Next()
		if !hasNext {
			break
		}
		if result.Error != nil {
			return nil, result.Error
		}
		if result.TID == "" {
			// Skip spans without trace ID
			continue
		}

		// Create a trace for this result
		trace := &tracev1.InternalTrace{
			TraceId: result.TID,
			Key:     result.Key,
			Spans:   make([]*tracev1.Span, 0, len(result.Spans)),
		}
		// Convert each span in the trace result
		for i, spanBytes := range result.Spans {
			traceTags := p.buildTraceTags(&result, queryCriteria, execPlan, i, traceIDInclusionMap, spanIDInclusionMap)

			span := &tracev1.Span{
				Tags:   traceTags,
				Span:   spanBytes,
				SpanId: result.SpanIDs[i],
			}
			trace.Spans = append(trace.Spans, span)
		}
		traces = append(traces, trace)
	}

	return traces, nil
}

func (p *traceQueryProcessor) buildTraceTags(result *model.TraceResult, queryCriteria *tracev1.QueryRequest, execPlan *traceExecutionPlan,
	spanIndex int, traceIDInclusionMap, spanIDInclusionMap map[int]bool,
) []*modelv1.Tag {
	var traceTags []*modelv1.Tag

	// Create trace tags from the result
	if result.Tags != nil && len(queryCriteria.TagProjection) > 0 {
		for _, tag := range result.Tags {
			if !slices.Contains(queryCriteria.TagProjection, tag.Name) {
				continue
			}
			if spanIndex < len(tag.Values) {
				traceTags = append(traceTags, &modelv1.Tag{
					Key:   tag.Name,
					Value: tag.Values[spanIndex],
				})
			}
		}
	}

	// Use group index to select traceIDTagName
	if traceIDInclusionMap[result.GroupIndex] && result.TID != "" {
		traceTags = append(traceTags, &modelv1.Tag{
			Key: execPlan.traceIDTagNames[result.GroupIndex],
			Value: &modelv1.TagValue{
				Value: &modelv1.TagValue_Str{
					Str: &modelv1.Str{
						Value: result.TID,
					},
				},
			},
		})
	}

	// Add span ID tag to each span if it should be included
	// Use group index to select spanIDTagName
	if spanIDInclusionMap[result.GroupIndex] && spanIndex < len(result.SpanIDs) {
		traceTags = append(traceTags, &modelv1.Tag{
			Key: execPlan.spanIDTagNames[result.GroupIndex],
			Value: &modelv1.TagValue{
				Value: &modelv1.TagValue_Str{
					Str: &modelv1.Str{
						Value: result.SpanIDs[spanIndex],
					},
				},
			},
		})
	}

	return traceTags
}

func (p *traceQueryProcessor) logSlowQuery(queryCriteria *tracev1.QueryRequest, traces []*tracev1.InternalTrace, startTime time.Time) {
	if queryCriteria.Trace || p.slowQuery <= 0 {
		return
	}

	latency := time.Since(startTime)
	if latency <= p.slowQuery {
		return
	}

	spanCount := 0
	for _, trace := range traces {
		spanCount += len(trace.Spans)
	}
	p.log.Warn().Dur("latency", latency).RawJSON("req", logger.Proto(queryCriteria)).Int("resp_count", spanCount).Msg("trace slow query")
}

func (p *traceQueryProcessor) executeQuery(ctx context.Context, queryCriteria *tracev1.QueryRequest) (resp bus.Message) {
	n := time.Now()
	now := n.UnixNano()
	defer func() {
		if err := recover(); err != nil {
			p.log.Error().Interface("err", err).RawJSON("req", logger.Proto(queryCriteria)).Str("stack", string(debug.Stack())).Msg("panic")
			resp = bus.NewMessage(bus.MessageID(time.Now().UnixNano()), common.NewError("panic"))
		}
	}()

	execPlan, setupErr := p.setupTraceExecutionPlan(queryCriteria)
	if setupErr != nil {
		resp = bus.NewMessage(bus.MessageID(now), setupErr)
		return
	}
	traceExecContexts := make([]executor.TraceExecutionContext, len(execPlan.executionContexts))
	for i, ec := range execPlan.executionContexts {
		traceExecContexts[i] = ec
	}

	plan, err := logical_trace.Analyze(queryCriteria, execPlan.metadata, execPlan.schemas, traceExecContexts,
		execPlan.traceIDTagNames, execPlan.spanIDTagNames, execPlan.timestampTagNames)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to analyze the query request for trace %s: %v", queryCriteria.GetName(), err))
		return
	}
	if p.log.Debug().Enabled() {
		p.log.Debug().Str("plan", plan.String()).Msg("query plan")
	}

	ctx, traceMonitor := p.setupTraceMonitor(ctx, queryCriteria, plan, n)
	if traceMonitor != nil {
		defer traceMonitor.finishTrace(&resp, now)
	}

	te := plan.(executor.TraceExecutable)
	defer te.Close()
	resultIterator, err := te.Execute(ctx)
	if err != nil {
		p.log.Error().Err(err).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to execute the trace query plan")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("execute the query plan for trace %s: %v", queryCriteria.GetName(), err))
		return
	}

	traces, err := p.processTraceResults(resultIterator, queryCriteria, execPlan)
	if err != nil {
		p.log.Error().Err(err).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to process trace results")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("process trace results for trace %s: %v", queryCriteria.GetName(), err))
		return
	}

	resp = bus.NewMessage(bus.MessageID(now), &tracev1.InternalQueryResponse{InternalTraces: traces})

	p.logSlowQuery(queryCriteria, traces, n)
	return
}
