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

package plan

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

const distributedQueryTimeout = 15 * time.Second

// SupportsDistributedRows reports whether a non-aggregation request can use
// the native vectorized distributed row merge.
func SupportsDistributedRows(req *measurev1.QueryRequest) bool {
	if req == nil || req.GetAgg() != nil || req.GetGroupBy() != nil || req.GetTop() != nil {
		return false
	}
	if len(req.GetGroups()) > 1 {
		return false
	}
	orderBy := req.GetOrderBy()
	return orderBy == nil || orderBy.GetIndexRuleName() == ""
}

// DistributedPlan is the vectorized liaison-side distributed measure plan.
// It consumes data-node raw frame bodies as []byte values from RawFrameCodec,
// decodes them into vectorized batches, and runs the liaison operators without
// routing through the row-compatible logical distributedPlan.
type DistributedPlan struct {
	queryTemplate *measurev1.QueryRequest
	nodeTemplate  *measurev1.QueryRequest
	measureSchema *databasev1.Measure
	cfg           vmeasure.VectorizedConfig
}

// AnalyzeDistributed builds the vectorized distributed liaison plan.
func AnalyzeDistributed(req *measurev1.QueryRequest, measureSchema *databasev1.Measure, cfg vmeasure.VectorizedConfig) (*DistributedPlan, error) {
	if req == nil {
		return nil, fmt.Errorf("vec distributed analyze: nil request")
	}
	if measureSchema == nil {
		return nil, fmt.Errorf("vec distributed analyze: nil measure schema")
	}
	if cfgErr := cfg.Validate(); cfgErr != nil {
		return nil, fmt.Errorf("vec distributed analyze: %w", cfgErr)
	}
	if req.GetAgg() == nil && !SupportsDistributedRows(req) {
		return nil, fmt.Errorf("vec distributed analyze: unsupported non-aggregation scan requires row distributed merge")
	}
	queryTemplate := proto.Clone(req).(*measurev1.QueryRequest)
	nodeTemplate := proto.Clone(req).(*measurev1.QueryRequest)
	limit := nodeTemplate.GetLimit()
	if limit == 0 {
		limit = defaultLimit
	}
	nodeTemplate.Limit = limit + nodeTemplate.GetOffset()
	nodeTemplate.Offset = 0
	// hadTop is captured before clearing Top so the data-node Limit can
	// be unbounded when the original query is Top-over-Agg. Without this,
	// each node aggregates its local rows and prunes to Limit before
	// returning, so the liaison's global Top sees only a per-node-truncated
	// subset of the groups and may miss the global winners entirely (e.g.
	// a node returns 2 groups out of 16, and svc15 - the actual top -
	// never crosses the wire).
	hadTop := nodeTemplate.GetTop() != nil
	nodeTemplate.Top = nil
	if nodeTemplate.GetAgg() == nil {
		nodeTemplate.GroupBy = nil
	}
	if hadTop && nodeTemplate.GetAgg() != nil {
		nodeTemplate.Limit = math.MaxUint32
	}
	return &DistributedPlan{queryTemplate: queryTemplate, nodeTemplate: nodeTemplate, measureSchema: measureSchema, cfg: cfg}, nil
}

// Execute broadcasts the internal query and executes the liaison-side vectorized plan.
func (p *DistributedPlan) Execute(ctx context.Context) (executor.MIterator, error) {
	if !data.MeasureWireModeRaw() {
		return nil, fmt.Errorf("vec distributed plan requires raw measure wire mode")
	}
	dctx := executor.FromDistributedExecutionContext(ctx)
	queryRequest := proto.Clone(p.queryTemplate).(*measurev1.QueryRequest)
	nodeRequest := proto.Clone(p.nodeTemplate).(*measurev1.QueryRequest)
	queryRequest.TimeRange = dctx.TimeRange()
	nodeRequest.TimeRange = dctx.TimeRange()
	internalRequest := &measurev1.InternalQueryRequest{Request: nodeRequest, AggReturnPartial: queryRequest.GetAgg() != nil}
	ff, broadcastErr := dctx.Broadcast(distributedQueryTimeout, data.TopicInternalMeasureQuery,
		bus.NewMessageWithNodeSelectors(bus.MessageID(dctx.TimeRange().Begin.Nanos), dctx.NodeSelectors(), dctx.TimeRange(), internalRequest))
	if broadcastErr != nil {
		return nil, fmt.Errorf("vec distributed plan: broadcast: %w", broadcastErr)
	}
	frames, responseErr := collectRawFrameResponses(ff)
	if responseErr != nil {
		return nil, responseErr
	}
	if queryRequest.GetAgg() == nil {
		return p.executeRows(ctx, frames, queryRequest)
	}
	return p.executeAgg(ctx, frames, queryRequest)
}

func collectRawFrameResponses(ff []bus.Future) ([][]byte, error) {
	frames := make([][]byte, 0, len(ff))
	var err error
	for _, future := range ff {
		message, getErr := future.Get()
		if getErr != nil {
			err = multierr.Append(err, getErr)
			continue
		}
		switch response := message.Data().(type) {
		case nil:
			// Empty-result carve-out: emptyMIterator (and any
			// FrameEmitter that has no rows to emit) returns a nil
			// rawBody, which the bus wire layer surfaces as an
			// untyped-nil Data interface. Treat as zero rows from this
			// source — there is no frame to decode.
		case []byte:
			if len(response) > 0 {
				frames = append(frames, response)
			}
		case *common.Error:
			err = multierr.Append(err, fmt.Errorf("data node error: %s", response.Error()))
		case *measurev1.InternalQueryResponse:
			err = multierr.Append(err, fmt.Errorf("vec distributed plan: got proto response under raw wire mode"))
		default:
			err = multierr.Append(err, fmt.Errorf("vec distributed plan: unexpected response %T", response))
		}
	}
	return frames, err
}

func (p *DistributedPlan) executeAgg(ctx context.Context, frames [][]byte, req *measurev1.QueryRequest) (executor.MIterator, error) {
	keyTagNames := distributedGroupByTagNames(req.GetGroupBy())
	aggFunc, aggErr := distributedAggFunc(req.GetAgg().GetFunction())
	if aggErr != nil {
		return nil, aggErr
	}
	aggSpecs := []vmeasure.AggReduceSpec{{OutputName: req.GetAgg().GetFieldName(), Func: aggFunc}}
	var topSpec *vmeasure.ReduceTopSpec
	if top := req.GetTop(); top != nil {
		topSpec = &vmeasure.ReduceTopSpec{FieldName: top.GetFieldName(), N: int(top.GetNumber()), Asc: top.GetFieldValueSort() == modelv1.Sort_SORT_ASC}
	}
	tracker := vectorized.NewMemoryTracker(int64(p.cfg.QueryMemoryMiB) * 1024 * 1024)
	batches, reduceErr := vmeasure.ReduceRawFrames(frames, keyTagNames, aggSpecs, p.cfg.BatchSize, tracker)
	if reduceErr != nil {
		return nil, fmt.Errorf("vec distributed plan: reduce raw frames: %w", reduceErr)
	}
	if topSpec != nil && topSpec.N > 0 && len(batches) > 0 {
		topped, topErr := vmeasure.ApplyTopToReduce(batches, *topSpec, p.cfg.BatchSize)
		if topErr != nil {
			return nil, fmt.Errorf("vec distributed plan: top reduced frames: %w", topErr)
		}
		batches = topped
	}
	return p.iteratorFromBatches(ctx, batches, req)
}

func (p *DistributedPlan) executeRows(ctx context.Context, frames [][]byte, req *measurev1.QueryRequest) (executor.MIterator, error) {
	if !SupportsDistributedRows(req) {
		return nil, fmt.Errorf("vec distributed plan: unsupported non-aggregation scan requires row distributed merge")
	}
	tracker := vectorized.NewMemoryTracker(int64(p.cfg.QueryMemoryMiB) * 1024 * 1024)
	batches, mergeErr := mergeDistributedRows(frames, distributedRowsSpec{
		Desc:      req.GetOrderBy().GetSort() == modelv1.Sort_SORT_DESC,
		IndexMode: p.measureSchema.GetIndexMode(),
		BatchSize: p.cfg.BatchSize,
		Tracker:   tracker,
	})
	if mergeErr != nil {
		return nil, fmt.Errorf("vec distributed plan: merge rows: %w", mergeErr)
	}
	return p.iteratorFromBatches(ctx, batches, req)
}

func (p *DistributedPlan) iteratorFromBatches(ctx context.Context, batches []*vectorized.RecordBatch, req *measurev1.QueryRequest) (executor.MIterator, error) {
	var schema *vectorized.BatchSchema
	for _, batch := range batches {
		if batch != nil && batch.Schema != nil {
			schema = batch.Schema
			break
		}
	}
	if schema == nil {
		vecPlan, analyzeErr := Analyze(req, p.measureSchema, vmeasure.AggModeAll)
		if analyzeErr != nil {
			return nil, fmt.Errorf("vec distributed plan: analyze empty output: %w", analyzeErr)
		}
		schema = vecPlan.Schema()
	}
	builder := vectorized.NewPipelineBuilder().WithMemoryTracker(vectorized.NewMemoryTracker(int64(p.cfg.QueryMemoryMiB) * 1024 * 1024))
	builder.From(&batchSliceSource{batches: batches, schema: schema})
	limit := req.GetLimit()
	if limit == 0 {
		limit = defaultLimit
	}
	builder.Apply(vmeasure.NewBatchLimit(schema, req.GetOffset(), limit))
	pipeline, buildErr := builder.Build()
	if buildErr != nil {
		return nil, fmt.Errorf("vec distributed plan: build output pipeline: %w", buildErr)
	}
	if initErr := pipeline.Init(ctx); initErr != nil {
		_ = pipeline.Close()
		return nil, fmt.Errorf("vec distributed plan: init output pipeline: %w", initErr)
	}
	pool := vectorized.NewBatchPool(schema, p.cfg.BatchSize)
	return vmeasure.NewIteratorFromPipeline(ctx, pipeline, schema, pool), nil
}

func distributedGroupByTagNames(groupBy *measurev1.QueryRequest_GroupBy) []string {
	if groupBy == nil || groupBy.GetTagProjection() == nil {
		return nil
	}
	families := groupBy.GetTagProjection().GetTagFamilies()
	if len(families) == 0 {
		return nil
	}
	return append([]string(nil), families[0].GetTags()...)
}

func distributedAggFunc(fn modelv1.AggregationFunction) (vmeasure.AggFunc, error) {
	switch fn {
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM:
		return vmeasure.AggSum, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT:
		return vmeasure.AggCount, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN:
		return vmeasure.AggMin, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX:
		return vmeasure.AggMax, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN:
		return vmeasure.AggMean, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED:
		return 0, fmt.Errorf("vec distributed plan: aggregation function is unspecified")
	}
	return 0, fmt.Errorf("vec distributed plan: unknown aggregation function %v", fn)
}

// String returns a concise plan rendering for tracing.
func (p *DistributedPlan) String() string {
	parts := []string{"vec-distributed"}
	if p.queryTemplate.GetAgg() != nil {
		parts = append(parts, "agg")
	}
	if p.queryTemplate.GetGroupBy() != nil {
		parts = append(parts, "group-by")
	}
	if p.queryTemplate.GetTop() != nil {
		parts = append(parts, "top")
	}
	return strings.Join(parts, ":")
}

type batchSliceSource struct {
	batches []*vectorized.RecordBatch
	schema  *vectorized.BatchSchema
	idx     int
}

func (s *batchSliceSource) Init(context.Context) error { return nil }

func (s *batchSliceSource) OutputSchema() *vectorized.BatchSchema { return s.schema }

func (s *batchSliceSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	for s.idx < len(s.batches) {
		batch := s.batches[s.idx]
		s.idx++
		if batch == nil || batch.Len == 0 {
			continue
		}
		return batch, nil
	}
	return nil, nil
}

func (s *batchSliceSource) Close() error { return nil }
