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
	"fmt"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

const defaultLimit uint32 = 20

// BuildSchema returns Schema loaded from the metadata repository.
func BuildSchema(sm *databasev1.Stream, indexRules []*databasev1.IndexRule) (logical.Schema, error) {
	s := &schema{
		common: &logical.CommonSchema{
			IndexRules: indexRules,
			TagSpecMap: make(map[string]*logical.TagSpec),
			EntityList: sm.GetEntity().GetTagNames(),
		},
		stream: sm,
	}

	s.common.RegisterTagFamilies(sm.GetTagFamilies())

	return s, nil
}

// Analyze converts logical expressions to executable operation tree represented by Plan.
func Analyze(_ context.Context, criteria *streamv1.QueryRequest, metadata *commonv1.Metadata, s logical.Schema) (logical.Plan, error) {
	// parse fields
	plan := parseTags(criteria, metadata)

	// parse limit
	limitParameter := criteria.GetLimit()
	if limitParameter == 0 {
		limitParameter = defaultLimit
	}
	plan = newLimit(plan, criteria.GetOffset(), limitParameter)

	p, err := plan.Analyze(s)
	if err != nil {
		return nil, err
	}
	rules := []logical.OptimizeRule{
		logical.NewPushDownOrder(criteria.OrderBy),
		logical.NewPushDownMaxSize(int(limitParameter + criteria.GetOffset())),
	}
	if err := logical.ApplyRules(p, rules...); err != nil {
		return nil, err
	}
	return p, nil
}

// DistributedAnalyze converts logical expressions to executable operation tree represented by Plan.
func DistributedAnalyze(criteria *streamv1.QueryRequest, s logical.Schema) (logical.Plan, error) {
	// parse fields
	plan := newUnresolvedDistributed(criteria)

	// parse limit
	limitParameter := criteria.GetLimit()
	if limitParameter == 0 {
		limitParameter = defaultLimit
	}
	plan = newDistributedLimit(plan, criteria.Offset, limitParameter)
	return plan.Analyze(s)
}

var (
	_ logical.Plan              = (*limit)(nil)
	_ logical.UnresolvedPlan    = (*limit)(nil)
	_ executor.StreamExecutable = (*limit)(nil)
)

// Parent refers to a parent node in the execution tree(plan).
type Parent struct {
	UnresolvedInput logical.UnresolvedPlan
	Input           logical.Plan
}

type limit struct {
	*Parent
	limitNum  uint32
	offsetNum uint32
}

func (l *limit) Close() {
	l.Parent.Input.(executor.StreamExecutable).Close()
}

func (l *limit) Execute(ec context.Context) ([]*streamv1.Element, error) {
	var allEntities []*streamv1.Element
	targetCount := int(l.limitNum)
	offset := int(l.offsetNum)

	for len(allEntities) < targetCount+offset {
		entities, err := l.Parent.Input.(executor.StreamExecutable).Execute(ec)
		if err != nil {
			return nil, err
		}
		if len(entities) == 0 {
			break
		}

		needed := targetCount + offset - len(allEntities)
		if len(entities) > needed {
			allEntities = append(allEntities, entities[:needed]...)
		} else {
			allEntities = append(allEntities, entities...)
		}
	}

	if len(allEntities) <= offset {
		return []*streamv1.Element{}, nil
	}

	endIndex := offset + targetCount
	if endIndex > len(allEntities) {
		endIndex = len(allEntities)
	}

	return allEntities[offset:endIndex], nil
}

func (l *limit) Analyze(s logical.Schema) (logical.Plan, error) {
	var err error
	l.Input, err = l.UnresolvedInput.Analyze(s)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *limit) Schema() logical.Schema {
	return l.Input.Schema()
}

func (l *limit) String() string {
	return fmt.Sprintf("%s Limit: %d", l.Input.String(), l.limitNum)
}

func (l *limit) Children() []logical.Plan {
	return []logical.Plan{l.Input}
}

func newLimit(input logical.UnresolvedPlan, offset, num uint32) logical.UnresolvedPlan {
	return &limit{
		Parent: &Parent{
			UnresolvedInput: input,
		},
		offsetNum: offset,
		limitNum:  num,
	}
}

// parseTags parses the query request to decide which kind of plan should be generated
// Basically,
// 1 - If no criteria is given, we can only scan all shards
// 2 - If criteria is given, but all of those fields exist in the "entity" definition,
//
//	i.e. they are top-level sharding keys. For example, for the current skywalking's streamSchema,
//	we use service_id + service_instance_id + state as the compound sharding keys.
func parseTags(criteria *streamv1.QueryRequest, metadata *commonv1.Metadata) logical.UnresolvedPlan {
	timeRange := criteria.GetTimeRange()
	return tagFilter(timeRange.GetBegin().AsTime(), timeRange.GetEnd().AsTime(), metadata,
		criteria.Criteria, logical.ToTags(criteria.GetProjection()))
}
