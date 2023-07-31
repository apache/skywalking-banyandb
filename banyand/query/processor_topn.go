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

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	logical_topn "github.com/apache/skywalking-banyandb/pkg/query/logical/topn"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type topNQueryProcessor struct {
	measureService measure.Service
	*queryService
}

func (t *topNQueryProcessor) Rev(message bus.Message) (resp bus.Message) {
	request, ok := message.Data().(*measurev1.TopNRequest)
	now := time.Now().UnixNano()
	if !ok {
		t.log.Warn().Msg("invalid event data type")
		return
	}
	ml := t.log.Named("topn", request.Metadata.Group, request.Metadata.Name)
	if e := ml.Debug(); e.Enabled() {
		e.RawJSON("req", logger.Proto(request)).Msg("received a topn event")
	}
	if request.GetFieldValueSort() == modelv1.Sort_SORT_UNSPECIFIED {
		t.log.Warn().Msg("invalid requested sort direction")
		return
	}
	if e := t.log.Debug(); e.Enabled() {
		e.Stringer("req", request).Msg("received a topN query event")
	}
	topNMetadata := request.GetMetadata()
	topNSchema, err := t.metaService.TopNAggregationRegistry().GetTopNAggregation(context.TODO(), topNMetadata)
	if err != nil {
		t.log.Error().Err(err).
			Str("topN", topNMetadata.GetName()).
			Msg("fail to get execution context")
		return
	}
	if topNSchema.GetFieldValueSort() != modelv1.Sort_SORT_UNSPECIFIED &&
		topNSchema.GetFieldValueSort() != request.GetFieldValueSort() {
		t.log.Warn().Msg("unmatched sort direction")
		return
	}
	meta := topNSchema.GetSourceMeasure()
	sourceMeasure, err := t.measureService.Measure(meta)
	if err != nil {
		t.log.Error().Err(err).
			Str("topN", topNMetadata.GetName()).
			Msg("fail to find source measure")
		return
	}

	s, err := logical_topn.BuildSchema(sourceMeasure, topNSchema)
	if err != nil {
		t.log.Error().Err(err).
			Str("topN", topNMetadata.GetName()).
			Msg("fail to build schema")
	}

	plan, err := logical_topn.Analyze(context.TODO(), request, topNMetadata, s)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to analyze the query request for topn %s: %v", meta.GetName(), err))
		return
	}

	if e := ml.Debug(); e.Enabled() {
		e.Str("plan", plan.String()).Msg("topn plan")
	}

	mIterator, err := plan.(executor.MeasureExecutable).Execute(sourceMeasure)
	if err != nil {
		ml.Error().Err(err).RawJSON("req", logger.Proto(request)).Msg("fail to close the topn plan")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to execute the topn plan for measure %s: %v", meta.GetName(), err))
	}
	defer func() {
		if err = mIterator.Close(); err != nil {
			ml.Error().Err(err).RawJSON("req", logger.Proto(request))
		}
	}()

	for mIterator.Next() {
	}

	return
}
