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

package batch

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/flow/api"
	batchApi "github.com/apache/skywalking-banyandb/pkg/flow/batch/api"
	"github.com/apache/skywalking-banyandb/pkg/flow/batch/sink"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

var _ api.Flow = (*batchFlow)(nil)

type batchFlow struct {
	src      batchApi.Source
	snkParam interface{}

	// services
	ctx      context.Context
	metaSvc  metadata.Service
	analyzer *logical.MeasureAnalyzer

	// unresolvedOps
	conditionOp   *conditionalFilterOperator
	timeRangeOp   *timeRangeOperator
	limit, offset int

	// resolvedOps
	filter    tsdb.ItemFilter
	entityMap map[string]int
	entity    []tsdb.Entry

	err error
}

func New(src batchApi.Source, metaSvc metadata.Service) api.Flow {
	return &batchFlow{
		src:       src,
		metaSvc:   metaSvc,
		filter:    tsdb.EmptyFilter(),
		entityMap: make(map[string]int),
	}
}

func (flow *batchFlow) Filter(f interface{}) api.Flow {
	if unresolvedCriteria, ok := f.([]*modelv1.Criteria); ok {
		flow.conditionOp = &conditionalFilterOperator{
			f:                  flow,
			unresolvedCriteria: unresolvedCriteria,
		}
	} else {
		flow.appendErr(errors.New("Filter can only accept []*modelv1.Criteria"))
	}
	return flow
}

func (flow *batchFlow) Map(f interface{}) api.Flow {
	// TODO implement me
	panic("implement me")
}

func (flow *batchFlow) Window(api.WindowAssigner) api.WindowedFlow {
	panic("Window is not supported for batchFlow flow")
}

func (flow *batchFlow) Offset(offset int) api.Flow {
	flow.offset = offset
	return flow
}

func (flow *batchFlow) Limit(limit int) api.Flow {
	flow.limit = limit
	return flow
}

func (flow *batchFlow) To(snkParam interface{}) api.Flow {
	flow.snkParam = snkParam
	return flow
}

func (flow *batchFlow) prepareContext() {
	if flow.ctx == nil {
		flow.ctx = context.TODO()
	}

	// TODO: add more runtime utilities
}

func (flow *batchFlow) initSchema() (logical.Schema, error) {
	flow.prepareContext()

	var err error
	flow.analyzer, err = logical.CreateMeasureAnalyzerFromMetaService(flow.metaSvc)
	if err != nil {
		return nil, err
	}

	// build and check source
	md := flow.src.Metadata()
	if md == nil {
		return nil, errors.New("metadata is nil")
	}

	schema, err := flow.analyzer.BuildMeasureSchema(flow.ctx, md)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

func (flow *batchFlow) OpenAsync() <-chan error {
	panic("fail to open batch flow in async mode")
}

func (flow *batchFlow) OpenSync() error {
	if flow.err != nil {
		return flow.err
	}

	schema, err := flow.initSchema()
	if err != nil {
		return err
	}

	// Analyze
	// 1. create timeRange op
	flow.timeRangeOp = &timeRangeOperator{
		f: flow,
	}
	schema, err = flow.timeRangeOp.Analyze(schema)
	if err != nil {
		return err
	}

	// 2. create filter op
	// TODO: schema should be used in the execution context
	_, err = flow.conditionOp.Analyze(schema)
	if err != nil {
		return err
	}

	// link all "Transformer" operators together
	// source () -> Iter[tsdb.Series]
	// ==> timeRangeOp (Iter[tsdb.Series]) -> Iter[tsdb.SeriesSpan]
	//     ==> conditionOp (Iter[tsdb.SeriesSpan]) -> Iter[tsdb.Item]
	if err = flow.snkParam.(*sink.ItemSlice).Drain(flow.conditionOp.Transform(
		flow.timeRangeOp.Transform(
			flow.src.Shards(flow.entity),
		),
	)); err != nil {
		return err
	}

	return nil
}

func (flow *batchFlow) appendErr(err error) {
	flow.err = multierr.Append(flow.err, err)
}
