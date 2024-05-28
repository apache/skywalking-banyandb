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

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/testing/protocmp"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const topNTagFamily = "__topN__"

var (
	initTimeout        = 10 * time.Second
	topNValueFieldSpec = &databasev1.FieldSpec{
		Name:              "value",
		FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
		EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
		CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
	}
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

	ctx, cancel := context.WithTimeout(ctx, initTimeout)
	defer cancel()
	start := time.Now()
	mm, err := sr.metadata.MeasureRegistry().ListMeasure(ctx, schema.ListOpt{Group: gName})
	if err != nil {
		logger.Panicf("fails to get the measures: %v", err)
		return
	}
	revCtx := ctx.Value(revCtxKey).(*revisionContext)
	measureMap := make(map[string]*databasev1.Measure, len(mm))
	for _, m := range mm {
		measureMap[m.Metadata.Name] = m
	}
	for _, aa := range aggMap {
		for _, a := range aa {
			sourceMeasre, ok := measureMap[a.SourceMeasure.Name]
			if !ok {
				sr.l.Warn().Str("group", gName).Str("measure", a.SourceMeasure.Name).Str("agg", a.Metadata.Name).Msg("source measure not found")
				continue
			}
			if _, ok := measureMap[a.Metadata.Name]; !ok {
				sr.l.Info().Str("group", gName).Str("measure", a.SourceMeasure.Name).Str("agg", a.Metadata.Name).Msg("remove topN aggregation")
				m, err := createTopNMeasure(ctx, sr.metadata.MeasureRegistry(), a, sourceMeasre)
				if err != nil {
					logger.Panicf("fails to create or update the topN measure: %v", err)
					return
				}
				mm = append(mm, m)
			}
		}
	}

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
			indexRules = append(indexRules, rules[r])
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
			indexRules = append(indexRules, rules[r])
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

func createOrUpdateTopNMeasure(ctx context.Context, measureSchemaRegistry schema.Measure, topNSchema *databasev1.TopNAggregation) (*databasev1.Measure, error) {
	oldTopNSchema, err := measureSchemaRegistry.GetMeasure(ctx, topNSchema.GetMetadata())
	if err != nil && !errors.Is(err, schema.ErrGRPCResourceNotFound) {
		return nil, errors.WithMessagef(err, "fail to get current topN measure %s", topNSchema.GetMetadata().GetName())
	}

	sourceMeasureSchema, err := measureSchemaRegistry.GetMeasure(ctx, topNSchema.GetSourceMeasure())
	if err != nil {
		return nil, errors.WithMessagef(err, "fail to get source measure %s", topNSchema.GetSourceMeasure().GetName())
	}

	// create a new "derived" measure for TopN result
	newTopNMeasure, err := buildTopNSourceMeasure(topNSchema, sourceMeasureSchema)
	if err != nil {
		return nil, err
	}
	if oldTopNSchema == nil {
		if _, innerErr := measureSchemaRegistry.CreateMeasure(ctx, newTopNMeasure); innerErr != nil {
			if !errors.Is(innerErr, schema.ErrGRPCAlreadyExists) {
				return nil, errors.WithMessagef(innerErr, "fail to create new topN measure %s", newTopNMeasure.GetMetadata().GetName())
			}
			newTopNMeasure, err = measureSchemaRegistry.GetMeasure(ctx, topNSchema.GetMetadata())
			if err != nil {
				return nil, errors.WithMessagef(err, "fail to get created topN measure %s", topNSchema.GetMetadata().GetName())
			}
		}
		return newTopNMeasure, nil
	}
	// compare with the old one
	if cmp.Diff(newTopNMeasure, oldTopNSchema,
		protocmp.IgnoreUnknown(),
		protocmp.IgnoreFields(&databasev1.Measure{}, "updated_at"),
		protocmp.IgnoreFields(&commonv1.Metadata{}, "id", "create_revision", "mod_revision"),
		protocmp.Transform()) == "" {
		return oldTopNSchema, nil
	}
	// update
	if _, err = measureSchemaRegistry.UpdateMeasure(ctx, newTopNMeasure); err != nil {
		return nil, errors.WithMessagef(err, "fail to update topN measure %s", newTopNMeasure.GetMetadata().GetName())
	}
	return newTopNMeasure, nil
}

func createTopNMeasure(ctx context.Context, measureSchemaRegistry schema.Measure, topNSchema *databasev1.TopNAggregation,
	sourceMeasureSchema *databasev1.Measure,
) (*databasev1.Measure, error) {
	newTopNMeasure, err := buildTopNSourceMeasure(topNSchema, sourceMeasureSchema)
	if err != nil {
		return nil, err
	}
	if _, err := measureSchemaRegistry.CreateMeasure(ctx, newTopNMeasure); err != nil {
		return nil, err
	}
	return newTopNMeasure, nil
}

func buildTopNSourceMeasure(topNSchema *databasev1.TopNAggregation, sourceMeasureSchema *databasev1.Measure) (*databasev1.Measure, error) {
	tagNames := sourceMeasureSchema.GetEntity().GetTagNames()
	seriesSpecs := make([]*databasev1.TagSpec, 0, len(tagNames))

	for _, tagName := range tagNames {
		var found bool
		for _, fSpec := range sourceMeasureSchema.GetTagFamilies() {
			for _, tSpec := range fSpec.GetTags() {
				if tSpec.GetName() == tagName {
					seriesSpecs = append(seriesSpecs, tSpec)
					found = true
					goto CHECK
				}
			}
		}

	CHECK:
		if !found {
			return nil, fmt.Errorf("fail to find tag spec %s", tagName)
		}
	}
	// create a new "derived" measure for TopN result
	return &databasev1.Measure{
		Metadata: topNSchema.Metadata,
		Interval: sourceMeasureSchema.GetInterval(),
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: topNTagFamily,
				Tags: append([]*databasev1.TagSpec{
					{
						Name: "measure_id",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
					{
						Name: "sortDirection",
						Type: databasev1.TagType_TAG_TYPE_INT,
					},
					{
						Name: "rankNumber",
						Type: databasev1.TagType_TAG_TYPE_INT,
					},
				}, seriesSpecs...),
			},
		},
		Fields: []*databasev1.FieldSpec{topNValueFieldSpec},
		Entity: &databasev1.Entity{
			TagNames: append(topNSchema.GetGroupByTagNames(),
				"sortDirection",
				"rankNumber"),
		},
	}, nil
}
