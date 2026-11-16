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
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	logical_measure "github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
)

type topNQueryProcessor struct {
	measureService measure.Service
	*queryService
	*bus.UnImplementedHealthyListener
}

type topNQueryContext struct {
	sourceMeasureSchemas []*databasev1.Measure
	topNSchemas          []*databasev1.TopNAggregation
	ecc                  []executor.MeasureExecutionContext
}

func (t *topNQueryProcessor) prepareGroupsContext(ctx context.Context, request *measurev1.TopNRequest, ml *logger.Logger) (*topNQueryContext, error) {
	qc := &topNQueryContext{
		sourceMeasureSchemas: make([]*databasev1.Measure, 0, len(request.Groups)),
		topNSchemas:          make([]*databasev1.TopNAggregation, 0, len(request.Groups)),
		ecc:                  make([]executor.MeasureExecutionContext, 0, len(request.Groups)),
	}

	for _, group := range request.Groups {
		topNMetadata := &commonv1.Metadata{
			Name:  request.Name,
			Group: group,
		}
		topNSchema, err := t.metaService.TopNAggregationRegistry().GetTopNAggregation(ctx, topNMetadata)
		if err != nil {
			t.log.Error().Err(err).
				Str("group", group).
				Msg("fail to get execution context")
			return nil, err
		}
		if topNSchema.GetFieldValueSort() != modelv1.Sort_SORT_UNSPECIFIED &&
			topNSchema.GetFieldValueSort() != request.GetFieldValueSort() {
			t.log.Warn().Str("group", group).Msg("unmatched sort direction")
			return nil, err
		}
		sourceMeasure, err := t.measureService.Measure(topNSchema.GetSourceMeasure())
		if err != nil {
			t.log.Error().Err(err).
				Str("group", group).
				Msg("fail to find source measure")
			return nil, err
		}
		topNResultMeasure, err := t.measureService.Measure(measure.GetTopNSchemaMetadata(group))
		if err != nil {
			ml.Error().Err(err).Str("group", group).Msg("fail to find topn result measure")
			return nil, err
		}

		qc.sourceMeasureSchemas = append(qc.sourceMeasureSchemas, sourceMeasure.GetSchema())
		qc.topNSchemas = append(qc.topNSchemas, topNSchema)
		qc.ecc = append(qc.ecc, topNResultMeasure)
	}

	return qc, nil
}

func (t *topNQueryProcessor) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	// A TopNRequest comes from a client of a standalone server, or from a liaison that predates the internal
	// TopN query; an InternalTopNRequest comes from a liaison that merges per-shard partial aggregates.
	var request *measurev1.TopNRequest
	var partialLimit int32
	switch d := message.Data().(type) {
	case *measurev1.TopNRequest:
		request = d
	case *measurev1.InternalTopNRequest:
		request = d.GetRequest()
		partialLimit = d.GetPartialLimit()
	}
	n := time.Now()
	now := n.UnixNano()
	if request == nil {
		t.log.Warn().Msg("invalid event data type")
		return
	}
	partial := partialLimit > 0
	ml := t.log.Named("topn", strings.Join(request.Groups, ","), request.Name)
	if e := ml.Debug(); e.Enabled() {
		e.RawJSON("req", logger.Proto(request)).Int32("partial_limit", partialLimit).Msg("received a topn event for groups: " + strings.Join(request.Groups, ","))
	}
	if request.GetFieldValueSort() == modelv1.Sort_SORT_UNSPECIFIED {
		t.log.Warn().Msg("invalid requested sort direction")
		return
	}
	if e := t.log.Debug(); e.Enabled() {
		e.Stringer("req", request).Msg("received a topN query event")
	}
	if partial && (len(request.GetGroups()) != 1 || request.GetAgg() == modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED) {
		// Shard IDs identify a shard only within one group, and a partial needs an aggregation function.
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("an internal topn query needs one group and an aggregation function, got %d groups and %s",
			len(request.GetGroups()), request.GetAgg()))
		return
	}
	// Process all groups
	qc, err := t.prepareGroupsContext(ctx, request, ml)
	if err != nil {
		return
	}

	plan, err := logical_measure.TopNAnalyze(request, partialLimit, qc.sourceMeasureSchemas, qc.topNSchemas, qc.ecc)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), protector.QueryFailure(err, "fail to analyze the query request for topn %s: %v", request.Name, err))
		return
	}

	if e := ml.Debug(); e.Enabled() {
		e.Str("plan", plan.String()).Msg("topn plan")
	}

	var tracer *query.Tracer
	var span *query.Span
	if request.Trace {
		tracer, ctx = query.NewTracer(ctx, n.Format(time.RFC3339Nano))
		span, ctx = tracer.StartSpan(ctx, "data-%s", t.queryService.nodeID)
		span.Tag("plan", plan.String())
		defer func() {
			data := resp.Data()
			switch d := data.(type) {
			case *measurev1.TopNResponse:
				d.Trace = tracer.ToProto()
			case *measurev1.InternalTopNResponse:
				d.Trace = tracer.ToProto()
			case *common.Error:
				span.Error(errors.New(d.Error()))
				if partial {
					resp = bus.NewMessage(bus.MessageID(now), &measurev1.InternalTopNResponse{Trace: tracer.ToProto()})
				} else {
					resp = bus.NewMessage(bus.MessageID(now), &measurev1.QueryResponse{Trace: tracer.ToProto()})
				}
			default:
				panic("unexpected data type")
			}
			span.Stop()
		}()
	}
	mIterator, err := plan.(executor.MeasureExecutable).Execute(ctx)
	if err != nil {
		ml.Error().Err(err).RawJSON("req", logger.Proto(request)).Msg("fail to close the topn plan")
		resp = bus.NewMessage(bus.MessageID(now), protector.QueryFailure(err, "fail to execute the topn plan for measure %s: %v", request.Name, err))
		return
	}
	defer func() {
		if err = mIterator.Close(); err != nil {
			ml.Error().Err(err).RawJSON("req", logger.Proto(request))
		}
	}()

	result := make([]*measurev1.InternalDataPoint, 0)
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
			result = append(result, mIterator.Current()...)
		}
	}()

	if partial {
		resp = bus.NewMessage(bus.MessageID(now), logical_measure.ToInternalTopNResponse(result))
	} else {
		// With an aggregation function the plan has already aggregated every value of each item and
		// ranked the items by the aggregated value, so the result is returned as is.
		resp = bus.NewMessage(bus.MessageID(now), toTopNResponse(result, request.GetAgg() != modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED))
	}
	if !request.Trace && t.slowQuery > 0 {
		latency := time.Since(n)
		if latency > t.slowQuery {
			t.log.Warn().Dur("latency", latency).RawJSON("req", logger.Proto(request)).Int("resp_count", len(result)).Msg("top_n slow query")
		}
	}
	return
}

func toTopNResponse(idps []*measurev1.InternalDataPoint, aggregated bool) *measurev1.TopNResponse {
	if aggregated && len(idps) == 0 {
		// An aggregation over no data answers with no list, as the distributed liaison does.
		return &measurev1.TopNResponse{}
	}
	topNList := make([]*measurev1.TopNList, 0)
	topNItems := make([]*measurev1.TopNList_Item, len(idps))
	for i, idp := range idps {
		dp := idp.GetDataPoint()
		topNItems[i] = &measurev1.TopNList_Item{
			Entity:    dp.GetTagFamilies()[0].GetTags(),
			Value:     dp.GetFields()[0].GetValue(),
			Version:   dp.GetVersion(),
			Timestamp: dp.GetTimestamp(),
		}
	}
	list := &measurev1.TopNList{Items: topNItems}
	if aggregated {
		list.Timestamp = timestamppb.Now()
	}
	topNList = append(topNList, list)
	return &measurev1.TopNResponse{Lists: topNList}
}
