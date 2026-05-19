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
	"fmt"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	logical_measure "github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	vecplan "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/plan"
)

type measureQueryProcessor struct {
	measureService measure.Service
	broadcaster    bus.Broadcaster
	*queryService
	*bus.UnImplementedHealthyListener
}

type measureVectorizedExecutionContext interface {
	VectorizedConfig() vmeasure.VectorizedConfig
}

type measureDistributedExecutable interface {
	executor.MeasureExecutable
	fmt.Stringer
}

func useVecDistributedMeasurePlan(rawWireMode bool, req *measurev1.QueryRequest) bool {
	return rawWireMode && req != nil && (req.GetAgg() != nil || vecplan.SupportsDistributedRows(req))
}

func (p *measureQueryProcessor) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	queryCriteria, ok := message.Data().(*measurev1.QueryRequest)
	n := time.Now()
	now := n.UnixNano()
	if !ok {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("invalid event data type"))
		return
	}
	ml := p.log.Named("measure", queryCriteria.Groups[0], queryCriteria.Name)
	if e := ml.Debug(); e.Enabled() {
		e.RawJSON("req", logger.Proto(queryCriteria)).Msg("received a query event")
	}

	var schemas []logical.Schema
	var measureSchemas []*databasev1.Measure
	var measureIndexRules [][]*databasev1.IndexRule
	var vecCfg vmeasure.VectorizedConfig
	for groupIdx, g := range queryCriteria.Groups {
		meta := &commonv1.Metadata{
			Name:  queryCriteria.Name,
			Group: g,
		}
		ec, measureErr := p.measureService.Measure(meta)
		if measureErr != nil {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to get execution context for measure %s: %v", meta.GetName(), measureErr))
			return
		}
		// nolint:staticcheck // SA1019 - row schema is still used for the flag-off rollback path.
		s, schemaErr := logical_measure.BuildSchema(ec.GetSchema(), ec.GetIndexRules())
		if schemaErr != nil {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to build schema for measure %s: %v", meta.GetName(), schemaErr))
			return
		}
		schemas = append(schemas, s)
		measureSchemas = append(measureSchemas, ec.GetSchema())
		measureIndexRules = append(measureIndexRules, ec.GetIndexRules())
		if vecEC, ok := ec.(measureVectorizedExecutionContext); ok {
			if groupIdx == 0 {
				vecCfg = vecEC.VectorizedConfig()
			}
		} else if data.MeasureWireModeRaw() {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("measure %s is not vectorized-capable under raw wire mode", meta.GetName()))
			return
		}
	}

	var plan measureDistributedExecutable
	var err error
	// Routing contract:
	//   - Raw wire mode + Agg -> vec distributed plan. executeAgg consumes
	//     per-(shard,group) partials emitted by AggModeMap nodes and is the
	//     only code path that finalises distributed aggregation correctly
	//     under flag-on.
	//   - Raw wire mode + supported non-agg -> vec distributed plan.
	//     executeRows performs a native columnar cross-source merge/dedup
	//     before liaison-side paging.
	//   - Otherwise (unsupported raw non-agg, or flag-off) -> row-path
	//     DistributedAnalyze. It still accepts raw vec frames via
	//     vmeasure.DecodeFramesPerSource and runs the sortedMIterator
	//     fallback for shapes not yet covered by native row merge.
	useVecDistributed := useVecDistributedMeasurePlan(data.MeasureWireModeRaw(), queryCriteria)
	if useVecDistributed {
		if len(measureSchemas) == 0 {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("vec distributed plan requires at least one measure schema"))
			return
		}
		plan, err = vecplan.AnalyzeDistributed(queryCriteria, measureSchemas, measureIndexRules, vecCfg)
	} else {
		// nolint:staticcheck // SA1019 - row distributed plan also serves
		// non-agg distributed scans under raw wire mode (see routing
		// contract above).
		rowPlan, analyzeErr := logical_measure.DistributedAnalyze(queryCriteria, schemas)
		if analyzeErr != nil {
			err = analyzeErr
		} else {
			var ok bool
			plan, ok = rowPlan.(measureDistributedExecutable)
			if !ok {
				err = fmt.Errorf("distributed measure plan %T is not executable", rowPlan)
			}
		}
	}
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to analyze the query request for measure %s: %v", queryCriteria.Name, err))
		return
	}

	if e := ml.Debug(); e.Enabled() {
		e.Str("plan", plan.String()).Msg("query plan")
	}
	nodeSelectors := make(map[string][]string)
	for _, g := range queryCriteria.Groups {
		if gs, ok := p.measureService.LoadGroup(g); ok {
			if ns, exist := p.parseNodeSelector(queryCriteria.Stages, gs.GetSchema().ResourceOpts); exist {
				nodeSelectors[g] = ns
			} else if len(gs.GetSchema().ResourceOpts.Stages) > 0 {
				ml.Error().Strs("req_stages", queryCriteria.Stages).Strs("default_stages", gs.GetSchema().GetResourceOpts().GetDefaultStages()).Msg("no stage found")
				resp = bus.NewMessage(bus.MessageID(now), common.NewError("no stage found in request or default stages in resource opts"))
				return
			}
		} else {
			ml.Error().RawJSON("req", logger.Proto(queryCriteria)).Msg("group not found")
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("group %s not found", g))
			return
		}
	}
	if len(queryCriteria.Stages) > 0 && len(nodeSelectors) == 0 {
		ml.Error().RawJSON("req", logger.Proto(queryCriteria)).Msg("no stage found")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("no stage found"))
		return
	}
	var tracer *query.Tracer
	var span *query.Span
	if queryCriteria.Trace {
		tracer, ctx = query.NewTracer(ctx, n.Format(time.RFC3339Nano))
		span, ctx = tracer.StartSpan(ctx, "distributed-%s", p.queryService.nodeID)
		span.Tag("plan", plan.String())
		span.Tagf("nodeSelectors", "%v", nodeSelectors)
		defer func() {
			data := resp.Data()
			switch d := data.(type) {
			case *measurev1.QueryResponse:
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

	mIterator, err := plan.(executor.MeasureExecutable).Execute(executor.WithDistributedExecutionContext(ctx, &distributedContext{
		Broadcaster:   p.broadcaster,
		timeRange:     queryCriteria.TimeRange,
		nodeSelectors: nodeSelectors,
	}))
	if err != nil {
		ml.Error().Err(err).Dur("latency", time.Since(n)).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to query")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to execute the query plan for measure %s: %v", queryCriteria.Name, err))
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
				result = append(result, current[0].GetDataPoint())
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
