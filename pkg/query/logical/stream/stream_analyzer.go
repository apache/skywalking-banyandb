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

package stream

import (
	"context"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

type StreamAnalyzer struct {
	metadataRepoImpl metadata.Repo
}

func CreateStreamAnalyzerFromMetaService(metaSvc metadata.Service) (*StreamAnalyzer, error) {
	return &StreamAnalyzer{
		metaSvc,
	}, nil
}

func (a *StreamAnalyzer) BuildStreamSchema(ctx context.Context, metadata *commonv1.Metadata) (logical.Schema, error) {
	group, err := a.metadataRepoImpl.GroupRegistry().GetGroup(ctx, metadata.GetGroup())
	if err != nil {
		return nil, err
	}
	stream, err := a.metadataRepoImpl.StreamRegistry().GetStream(ctx, metadata)
	if err != nil {
		return nil, err
	}

	indexRules, err := a.metadataRepoImpl.IndexRules(context.TODO(), metadata)
	if err != nil {
		return nil, err
	}

	s := &streamSchema{
		common: &logical.CommonSchema{
			Group:      group,
			IndexRules: indexRules,
			TagMap:     make(map[string]*logical.TagSpec),
			EntityList: stream.GetEntity().GetTagNames(),
		},
		stream: stream,
	}

	// generate the streamSchema of the fields for the traceSeries
	for tagFamilyIdx, tagFamily := range stream.GetTagFamilies() {
		for tagIdx, spec := range tagFamily.GetTags() {
			s.registerTag(tagFamilyIdx, tagIdx, spec)
		}
	}

	return s, nil
}

func (a *StreamAnalyzer) Analyze(_ context.Context, criteria *streamv1.QueryRequest, metadata *commonv1.Metadata, s logical.Schema) (logical.Plan, error) {
	// parse fields
	plan, err := parseStreamFields(criteria, metadata, s)
	if err != nil {
		return nil, err
	}

	// parse orderBy
	queryOrder := criteria.GetOrderBy()
	if queryOrder != nil {
		if v, ok := plan.(*unresolvedTagFilter); ok {
			v.unresolvedOrderBy = logical.NewOrderBy(queryOrder.GetIndexRuleName(), queryOrder.GetSort())
		}
	}

	// parse offset
	plan = Offset(plan, criteria.GetOffset())

	// parse limit
	limitParameter := criteria.GetLimit()
	if limitParameter == 0 {
		limitParameter = logical.DefaultLimit
	}
	plan = Limit(plan, limitParameter)

	return plan.Analyze(s)
}

// parseFields parses the query request to decide which kind of plan should be generated
// Basically,
// 1 - If no criteria is given, we can only scan all shards
// 2 - If criteria is given, but all of those fields exist in the "entity" definition,
//
//	i.e. they are top-level sharding keys. For example, for the current skywalking's streamSchema,
//	we use service_id + service_instance_id + state as the compound sharding keys.
func parseStreamFields(criteria *streamv1.QueryRequest, metadata *commonv1.Metadata, s logical.Schema) (logical.UnresolvedPlan, error) {
	timeRange := criteria.GetTimeRange()

	projTags := make([][]*logical.Tag, len(criteria.GetProjection().GetTagFamilies()))
	for i, tagFamily := range criteria.GetProjection().GetTagFamilies() {
		var projTagInFamily []*logical.Tag
		for _, tagName := range tagFamily.GetTags() {
			projTagInFamily = append(projTagInFamily, logical.NewTag(tagFamily.GetName(), tagName))
		}
		projTags[i] = projTagInFamily
	}

	return TagFilter(timeRange.GetBegin().AsTime(), timeRange.GetEnd().AsTime(), metadata,
		criteria.Criteria, nil, projTags...), nil
}
