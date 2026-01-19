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

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
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
	request, ok := message.Data().(*measurev1.TopNRequest)
	n := time.Now()
	now := n.UnixNano()
	if !ok {
		t.log.Warn().Msg("invalid event data type")
		return
	}
	ml := t.log.Named("topn", strings.Join(request.Groups, ","), request.Name)
	if e := ml.Debug(); e.Enabled() {
		e.RawJSON("req", logger.Proto(request)).Msg("received a topn event for groups: " + strings.Join(request.Groups, ","))
	}
	if request.GetFieldValueSort() == modelv1.Sort_SORT_UNSPECIFIED {
		t.log.Warn().Msg("invalid requested sort direction")
		return
	}
	if e := t.log.Debug(); e.Enabled() {
		e.Stringer("req", request).Msg("received a topN query event")
	}
	// Process all groups
	qc, err := t.prepareGroupsContext(ctx, request, ml)
	if err != nil {
		return
	}

	plan, err := logical_measure.TopNAnalyze(request, qc.sourceMeasureSchemas, qc.topNSchemas, qc.ecc)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to analyze the query request for topn %s: %v", request.Name, err))
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
			case *common.Error:
				span.Error(errors.New(d.Error()))
				resp = bus.NewMessage(bus.MessageID(now), &measurev1.QueryResponse{Trace: tracer.ToProto()})
			default:
				panic("unexpected data type")
			}
			span.Stop()
		}()
	}
	mIterator, err := plan.(executor.MeasureExecutable).Execute(ctx)
	if err != nil {
		ml.Error().Err(err).RawJSON("req", logger.Proto(request)).Msg("fail to close the topn plan")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to execute the topn plan for measure %s: %v", request.Name, err))
		return
	}
	defer func() {
		if err = mIterator.Close(); err != nil {
			ml.Error().Err(err).RawJSON("req", logger.Proto(request))
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
				result = append(result, current[0].GetDataPoint())
			}
		}
	}()

	resp = bus.NewMessage(bus.MessageID(now), toTopNResponse(result))
	if !request.Trace && t.slowQuery > 0 {
		latency := time.Since(n)
		if latency > t.slowQuery {
			t.log.Warn().Dur("latency", latency).RawJSON("req", logger.Proto(request)).Int("resp_count", len(result)).Msg("top_n slow query")
		}
	}
	return
}

func toTopNResponse(dps []*measurev1.DataPoint) *measurev1.TopNResponse {
	topNList := make([]*measurev1.TopNList, 0)
	topNItems := make([]*measurev1.TopNList_Item, len(dps))
	for i, dp := range dps {
		topNItems[i] = &measurev1.TopNList_Item{
			Entity:    dp.GetTagFamilies()[0].GetTags(),
			Value:     dp.GetFields()[0].GetValue(),
			Version:   dp.GetVersion(),
			Timestamp: dp.GetTimestamp(),
		}
	}
	topNList = append(topNList, &measurev1.TopNList{
		Items: topNItems,
	})
	return &measurev1.TopNResponse{Lists: topNList}
}
