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

package schema

import (
	"context"
	"fmt"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var initTimeout = 10 * time.Second

type revisionContext struct {
	group            int64
	measure          int64
	stream           int64
	trace            int64
	indexRule        int64
	indexRuleBinding int64
	topNAgg          int64
}

func (r revisionContext) String() string {
	return fmt.Sprintf("Group: %d, Measure: %d, Stream: %d, Trace: %d, IndexRule: %d, IndexRuleBinding: %d, TopNAgg: %d",
		r.group, r.measure, r.stream, r.trace, r.indexRule, r.indexRuleBinding, r.topNAgg)
}

type revisionContextKey struct{}

var revCtxKey = revisionContextKey{}

func (sr *schemaRepo) Init(kind schema.Kind) ([]string, []int64) {
	if kind != schema.KindMeasure && kind != schema.KindStream && kind != schema.KindTrace {
		return nil, nil
	}
	catalog := sr.getCatalog(kind)
	ctx := context.Background()
	groups, err := sr.metadata.GroupRegistry().ListGroup(ctx)
	if err != nil {
		logger.Panicf("fails to get the groups: %v", err)
		return nil, nil
	}
	var revCtx revisionContext
	groupNames := make([]string, 0, len(groups))
	for _, g := range groups {
		if g.Catalog != catalog {
			continue
		}
		if g.Metadata.ModRevision > revCtx.group {
			revCtx.group = g.Metadata.ModRevision
		}
		sr.processGroup(context.WithValue(ctx, revCtxKey, &revCtx), g, catalog)
		groupNames = append(groupNames, g.Metadata.Name)
	}
	if kind == schema.KindMeasure {
		sr.l.Info().Stringer("revision", revCtx).Msg("init measures")
		return groupNames, []int64{revCtx.group, revCtx.measure, revCtx.indexRuleBinding, revCtx.indexRule, revCtx.topNAgg}
	}
	if kind == schema.KindTrace {
		sr.l.Info().Stringer("revision", revCtx).Msg("init trace")
		return groupNames, []int64{revCtx.group, revCtx.trace, revCtx.indexRuleBinding, revCtx.indexRule}
	}
	sr.l.Info().Stringer("revision", revCtx).Msg("init stream")
	return groupNames, []int64{revCtx.group, revCtx.stream, revCtx.indexRuleBinding, revCtx.indexRule}
}

func (sr *schemaRepo) getCatalog(kind schema.Kind) commonv1.Catalog {
	if kind == schema.KindMeasure {
		return commonv1.Catalog_CATALOG_MEASURE
	}
	if kind == schema.KindTrace {
		return commonv1.Catalog_CATALOG_TRACE
	}
	return commonv1.Catalog_CATALOG_STREAM
}

func (sr *schemaRepo) processGroup(ctx context.Context, g *commonv1.Group, catalog commonv1.Catalog) {
	_, err := sr.initGroup(g)
	if err != nil {
		logger.Panicf("fails to init the group: %v", err)
	}
	sr.processRules(ctx, g.Metadata.GetName())
	sr.processBindings(ctx, g.Metadata.GetName())
	if catalog == commonv1.Catalog_CATALOG_MEASURE {
		sr.processMeasure(ctx, g.Metadata.Name)
		return
	}
	if catalog == commonv1.Catalog_CATALOG_TRACE {
		sr.processTrace(ctx, g.Metadata.Name)
		return
	}
	sr.processStream(ctx, g.Metadata.Name)
}

func (sr *schemaRepo) processBindings(ctx context.Context, gName string) {
	ctx, cancel := context.WithTimeout(ctx, initTimeout)
	defer cancel()
	start := time.Now()
	ibb, err := sr.metadata.IndexRuleBindingRegistry().ListIndexRuleBinding(ctx, schema.ListOpt{Group: gName})
	if err != nil {
		logger.Panicf("fails to get the index rule bindings: %v", err)
	}
	revCtx := ctx.Value(revCtxKey).(*revisionContext)
	for _, ib := range ibb {
		sr.storeIndexRuleBinding(ib)
		if ib.Metadata.ModRevision > revCtx.indexRuleBinding {
			revCtx.indexRuleBinding = ib.Metadata.ModRevision
		}
	}
	sr.l.Info().Str("group", gName).Dur("duration", time.Since(start)).Int("size", len(ibb)).Msg("get index rule bindings")
}

func (sr *schemaRepo) processRules(ctx context.Context, gName string) {
	ctx, cancel := context.WithTimeout(ctx, initTimeout)
	defer cancel()
	start := time.Now()
	rr, err := sr.metadata.IndexRuleRegistry().ListIndexRule(ctx, schema.ListOpt{Group: gName})
	if err != nil {
		logger.Panicf("fails to get the index rules: %v", err)
	}
	revCtx := ctx.Value(revCtxKey).(*revisionContext)
	for _, r := range rr {
		sr.storeIndexRule(r)
		if r.Metadata.ModRevision > revCtx.indexRule {
			revCtx.indexRule = r.Metadata.ModRevision
		}
	}
	sr.l.Info().Str("group", gName).Dur("duration", time.Since(start)).Int("size", len(rr)).Msg("get index rules")
}

func (sr *schemaRepo) processMeasure(ctx context.Context, gName string) {
	ctx, cancel := context.WithTimeout(ctx, initTimeout)
	defer cancel()
	start := time.Now()
	mm, err := sr.metadata.MeasureRegistry().ListMeasure(ctx, schema.ListOpt{Group: gName})
	if err != nil {
		logger.Panicf("fails to get the measures: %v", err)
		return
	}

	revCtx := ctx.Value(revCtxKey).(*revisionContext)
	for _, m := range mm {
		if m.Metadata.ModRevision > revCtx.measure {
			revCtx.measure = m.Metadata.ModRevision
		}
		if err := sr.storeResource(m); err != nil {
			logger.Panicf("fails to store the measure: %v", err)
		}
	}
	sr.l.Info().Str("group", gName).Dur("duration", time.Since(start)).Int("size", len(mm)).Msg("store measures")
}

func (sr *schemaRepo) processStream(ctx context.Context, gName string) {
	ctx, cancel := context.WithTimeout(ctx, initTimeout)
	defer cancel()
	start := time.Now()
	ss, err := sr.metadata.StreamRegistry().ListStream(ctx, schema.ListOpt{Group: gName})
	if err != nil {
		logger.Panicf("fails to get the streams: %v", err)
		return
	}
	revCtx := ctx.Value(revCtxKey).(*revisionContext)
	for _, s := range ss {
		if err := sr.storeResource(s); err != nil {
			logger.Panicf("fails to store the stream: %v", err)
		}
		if s.Metadata.ModRevision > revCtx.stream {
			revCtx.stream = s.Metadata.ModRevision
		}
	}
	sr.l.Info().Str("group", gName).Dur("duration", time.Since(start)).Int("size", len(ss)).Msg("store streams")
}

func (sr *schemaRepo) processTrace(ctx context.Context, gName string) {
	ctx, cancel := context.WithTimeout(ctx, initTimeout)
	defer cancel()
	start := time.Now()
	tt, err := sr.metadata.TraceRegistry().ListTrace(ctx, schema.ListOpt{Group: gName})
	if err != nil {
		logger.Panicf("fails to get the traces: %v", err)
		return
	}
	revCtx := ctx.Value(revCtxKey).(*revisionContext)
	for _, t := range tt {
		if err := sr.storeResource(t); err != nil {
			logger.Panicf("fails to store the trace: %v", err)
		}
		if t.Metadata.ModRevision > revCtx.trace {
			revCtx.trace = t.Metadata.ModRevision
		}
	}
	sr.l.Info().Str("group", gName).Dur("duration", time.Since(start)).Int("size", len(tt)).Msg("store traces")
}

func (sr *schemaRepo) initGroup(groupSchema *commonv1.Group) (*group, error) {
	g, ok := sr.getGroup(groupSchema.Metadata.Name)
	if ok {
		return g, nil
	}
	sr.l.Info().Str("group", groupSchema.Metadata.Name).Msg("creating a tsdb")
	g = sr.createGroup(groupSchema.Metadata.Name)
	if err := g.initBySchema(groupSchema); err != nil {
		return nil, err
	}
	return g, nil
}
