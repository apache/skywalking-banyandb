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

	"github.com/pkg/errors"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	// TopNTagFamily is the tag family name of the topN result measure.
	TopNTagFamily = "_topN"
	// TopNFieldName is the field name of the topN result measure.
	TopNFieldName = "value"
)

var (
	initTimeout    = 10 * time.Second
	topNFieldsSpec = []*databasev1.FieldSpec{{
		Name:              TopNFieldName,
		FieldType:         databasev1.FieldType_FIELD_TYPE_DATA_BINARY,
		EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
		CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
	}}
	// TopNTagNames is the tag names of the topN result measure.
	TopNTagNames = []string{"name", "direction", "group"}
)

type revisionContext struct {
	group            int64
	measure          int64
	stream           int64
	indexRule        int64
	indexRuleBinding int64
	topNAgg          int64
}

func (r revisionContext) String() string {
	return fmt.Sprintf("Group: %d, Measure: %d, Stream: %d, IndexRule: %d, IndexRuleBinding: %d, TopNAgg: %d",
		r.group, r.measure, r.stream, r.indexRule, r.indexRuleBinding, r.topNAgg)
}

type revisionContextKey struct{}

var revCtxKey = revisionContextKey{}

func (sr *schemaRepo) Init(kind schema.Kind) []int64 {
	if kind != schema.KindMeasure && kind != schema.KindStream {
		return nil
	}
	catalog := sr.getCatalog(kind)
	ctx := context.Background()
	groups, err := sr.metadata.GroupRegistry().ListGroup(ctx)
	if err != nil {
		logger.Panicf("fails to get the groups: %v", err)
		return nil
	}
	var revCtx revisionContext
	for _, g := range groups {
		if g.Catalog != catalog {
			continue
		}
		if g.Metadata.ModRevision > revCtx.group {
			revCtx.group = g.Metadata.ModRevision
		}
		sr.processGroup(context.WithValue(ctx, revCtxKey, &revCtx), g, catalog)
	}
	if kind == schema.KindMeasure {
		sr.l.Info().Stringer("revision", revCtx).Msg("init measures")
		return []int64{revCtx.group, revCtx.measure, revCtx.indexRuleBinding, revCtx.indexRule, revCtx.topNAgg}
	}
	sr.l.Info().Stringer("revision", revCtx).Msg("init stream")
	return []int64{revCtx.group, revCtx.stream, revCtx.indexRuleBinding, revCtx.indexRule}
}

func (sr *schemaRepo) getCatalog(kind schema.Kind) commonv1.Catalog {
	if kind == schema.KindMeasure {
		return commonv1.Catalog_CATALOG_MEASURE
	}
	return commonv1.Catalog_CATALOG_STREAM
}

func (sr *schemaRepo) processGroup(ctx context.Context, g *commonv1.Group, catalog commonv1.Catalog) {
	group, err := sr.initGroup(g)
	if err != nil {
		logger.Panicf("fails to init the group: %v", err)
	}
	bindings := sr.getBindings(ctx, g.Metadata.GetName())
	rules := sr.getRules(ctx, g.Metadata.GetName())
	if catalog == commonv1.Catalog_CATALOG_MEASURE {
		sr.processMeasure(ctx, g.Metadata.Name, group, bindings, rules)
		return
	}
	sr.processStream(ctx, g.Metadata.Name, group, bindings, rules)
}

func (sr *schemaRepo) getBindings(ctx context.Context, gName string) map[string][]string {
	ctx, cancel := context.WithTimeout(ctx, initTimeout)
	defer cancel()
	start := time.Now()
	ibb, err := sr.metadata.IndexRuleBindingRegistry().ListIndexRuleBinding(ctx, schema.ListOpt{Group: gName})
	if err != nil {
		logger.Panicf("fails to get the index rule bindings: %v", err)
	}
	revCtx := ctx.Value(revCtxKey).(*revisionContext)
	bindings := make(map[string][]string, len(ibb))
	for _, ib := range ibb {
		bindings[ib.GetSubject().GetName()] = ib.GetRules()
		if ib.Metadata.ModRevision > revCtx.indexRuleBinding {
			revCtx.indexRuleBinding = ib.Metadata.ModRevision
		}
	}
	sr.l.Info().Str("group", gName).Dur("duration", time.Since(start)).Int("size", len(bindings)).Msg("get index rule bindings")
	return bindings
}

func (sr *schemaRepo) getRules(ctx context.Context, gName string) map[string]*databasev1.IndexRule {
	ctx, cancel := context.WithTimeout(ctx, initTimeout)
	defer cancel()
	start := time.Now()
	rr, err := sr.metadata.IndexRuleRegistry().ListIndexRule(ctx, schema.ListOpt{Group: gName})
	if err != nil {
		logger.Panicf("fails to get the index rules: %v", err)
	}
	revCtx := ctx.Value(revCtxKey).(*revisionContext)
	rules := make(map[string]*databasev1.IndexRule, len(rr))
	for _, r := range rr {
		rules[r.Metadata.Name] = r
		if r.Metadata.ModRevision > revCtx.indexRule {
			revCtx.indexRule = r.Metadata.ModRevision
		}
	}
	sr.l.Info().Str("group", gName).Dur("duration", time.Since(start)).Int("size", len(rules)).Msg("get index rules")
	return rules
}

func (sr *schemaRepo) processMeasure(ctx context.Context, gName string, group *group, bindings map[string][]string, rules map[string]*databasev1.IndexRule) {
	aggMap := sr.getAggMap(ctx, gName)
	if len(aggMap) > 0 {
		if err := createTopNResultMeasure(ctx, sr.metadata.MeasureRegistry(), gName); err != nil {
			logger.Panicf("fails to create the topN result measure: %v", err)
			return
		}
	}

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
		sr.storeMeasure(m, group, bindings, rules, aggMap)
	}
	sr.l.Info().Str("group", gName).Dur("duration", time.Since(start)).Int("size", len(mm)).Msg("store measures")
}

func (sr *schemaRepo) getAggMap(ctx context.Context, gName string) map[string][]*databasev1.TopNAggregation {
	ctx, cancel := context.WithTimeout(ctx, initTimeout)
	defer cancel()
	start := time.Now()
	agg, err := sr.metadata.TopNAggregationRegistry().ListTopNAggregation(ctx, schema.ListOpt{Group: gName})
	if err != nil {
		logger.Panicf("fails to get the topN aggregations: %v", err)
	}
	revCtx := ctx.Value(revCtxKey).(*revisionContext)
	aggMap := make(map[string][]*databasev1.TopNAggregation, len(agg))
	for _, a := range agg {
		aggs, ok := aggMap[a.SourceMeasure.Name]
		if ok {
			aggs = append(aggs, a)
		} else {
			aggs = []*databasev1.TopNAggregation{a}
		}
		aggMap[a.SourceMeasure.Name] = aggs
		if a.Metadata.ModRevision > revCtx.topNAgg {
			revCtx.topNAgg = a.Metadata.ModRevision
		}
	}
	sr.l.Info().Str("group", gName).Dur("duration", time.Since(start)).Int("size", len(agg)).Msg("get topN aggregations")
	return aggMap
}

func (sr *schemaRepo) storeMeasure(m *databasev1.Measure, group *group, bindings map[string][]string,
	rules map[string]*databasev1.IndexRule, aggMap map[string][]*databasev1.TopNAggregation,
) {
	var indexRules []*databasev1.IndexRule
	if rr, ok := bindings[m.Metadata.GetName()]; ok {
		for _, r := range rr {
			if rule, ok := rules[r]; ok {
				indexRules = append(indexRules, rule)
			}
		}
	}
	var topNAggr []*databasev1.TopNAggregation
	if aa, ok := aggMap[m.Metadata.GetName()]; ok {
		topNAggr = append(topNAggr, aa...)
	}
	if err := sr.storeResource(group, m, indexRules, topNAggr); err != nil {
		logger.Panicf("fails to store the measure: %v", err)
	}
}

func (sr *schemaRepo) processStream(ctx context.Context, gName string, group *group, bindings map[string][]string, rules map[string]*databasev1.IndexRule) {
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
		sr.storeStream(s, group, bindings, rules)
		if s.Metadata.ModRevision > revCtx.stream {
			revCtx.stream = s.Metadata.ModRevision
		}
	}
	sr.l.Info().Str("group", gName).Dur("duration", time.Since(start)).Int("size", len(ss)).Msg("store streams")
}

func (sr *schemaRepo) storeStream(s *databasev1.Stream, group *group, bindings map[string][]string, rules map[string]*databasev1.IndexRule) {
	var indexRules []*databasev1.IndexRule
	if rr, ok := bindings[s.Metadata.GetName()]; ok {
		for _, r := range rr {
			if rule, ok := rules[r]; ok {
				indexRules = append(indexRules, rule)
			}
		}
	}
	if err := sr.storeResource(group, s, indexRules, nil); err != nil {
		logger.Panicf("fails to store the stream: %v", err)
	}
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

func createTopNResultMeasure(ctx context.Context, measureSchemaRegistry schema.Measure, group string) error {
	md := GetTopNSchemaMetadata(group)
	m, err := measureSchemaRegistry.GetMeasure(ctx, md)
	if err != nil && !errors.Is(err, schema.ErrGRPCResourceNotFound) {
		return errors.WithMessagef(err, "fail to get %s", md)
	}
	if m != nil {
		return nil
	}

	m = GetTopNSchema(md)
	if _, innerErr := measureSchemaRegistry.CreateMeasure(ctx, m); innerErr != nil {
		if !errors.Is(innerErr, schema.ErrGRPCAlreadyExists) {
			return errors.WithMessagef(innerErr, "fail to create new topN measure %s", m)
		}
	}
	return nil
}

// GetTopNSchema returns the schema of the topN result measure.
func GetTopNSchema(md *commonv1.Metadata) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: md,
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: TopNTagFamily,
				Tags: []*databasev1.TagSpec{
					{Name: TopNTagNames[0], Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: TopNTagNames[1], Type: databasev1.TagType_TAG_TYPE_INT},
					{Name: TopNTagNames[2], Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
		Fields: topNFieldsSpec,
		Entity: &databasev1.Entity{
			TagNames: TopNTagNames,
		},
	}
}

// GetTopNSchemaMetadata returns the metadata of the topN result measure.
func GetTopNSchemaMetadata(group string) *commonv1.Metadata {
	return &commonv1.Metadata{
		Name:  TopNSchemaName,
		Group: group,
	}
}
