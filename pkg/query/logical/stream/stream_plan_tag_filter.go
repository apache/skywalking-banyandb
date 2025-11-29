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
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ logical.UnresolvedPlan = (*unresolvedTagFilter)(nil)

type unresolvedTagFilter struct {
	startTime      time.Time
	endTime        time.Time
	ec             executor.StreamExecutionContext
	metadata       *commonv1.Metadata
	criteria       *modelv1.Criteria
	projectionTags [][]*logical.Tag
}

func (uis *unresolvedTagFilter) Analyze(s logical.Schema) (logical.Plan, error) {
	ctx := newAnalyzerContext(s)
	entityList := s.EntityList()
	entityDict := make(map[string]int)
	entity := make([]*modelv1.TagValue, len(entityList))
	for idx, e := range entityList {
		entityDict[e] = idx
		// fill AnyEntry by default
		entity[idx] = pbv1.AnyTagValue
	}
	var err error
	ctx.invertedFilter, ctx.entities, err = buildLocalFilter(uis.criteria, s, entityDict, entity, databasev1.IndexRule_TYPE_INVERTED)
	if err != nil {
		return nil, err
	}
	ctx.skippingFilter, _, err = buildLocalFilter(uis.criteria, s, entityDict, entity, databasev1.IndexRule_TYPE_SKIPPING)
	if err != nil {
		return nil, err
	}

	projBuilder := newProjectionBuilder(uis.projectionTags)
	criteriaTagNames := make(map[string]struct{})
	logical.CollectCriteriaTagNames(uis.criteria, criteriaTagNames)

	var hiddenTags map[string]struct{}
	var tagFilter logical.TagFilter
	if uis.criteria != nil {
		var errFilter error
		tagFilter, errFilter = logical.BuildTagFilter(uis.criteria, entityDict, s, s, false, "")
		if errFilter != nil {
			return nil, errFilter
		}
		if tagFilter != logical.DummyFilter {
			hiddenTags = make(map[string]struct{})
			for tagName := range criteriaTagNames {
				if _, isEntity := entityDict[tagName]; isEntity {
					continue
				}
				added, errAdd := projBuilder.AddTagFromSchema(s, tagName)
				if errAdd != nil {
					return nil, errAdd
				}
				if added {
					hiddenTags[tagName] = struct{}{}
				}
			}
		}
	}

	ctx.projectionTags = projBuilder.Model()
	if logicalTags := projBuilder.LogicalTags(); len(logicalTags) > 0 {
		var errProject error
		ctx.projTagsRefs, errProject = s.CreateTagRef(logicalTags...)
		if errProject != nil {
			return nil, errProject
		}
	}

	plan := uis.selectIndexScanner(ctx, uis.ec)
	if uis.criteria != nil && tagFilter != nil && tagFilter != logical.DummyFilter {
		projSchema := s
		if len(ctx.projTagsRefs) > 0 {
			if projected := s.ProjTags(ctx.projTagsRefs...); projected != nil {
				projSchema = projected
			}
		}
		plan = newTagFilter(projSchema, plan, tagFilter, hiddenTags)
	}
	return plan, err
}

func (uis *unresolvedTagFilter) selectIndexScanner(ctx *analyzeContext, ec executor.StreamExecutionContext) logical.Plan {
	return &localIndexScan{
		timeRange:         timestamp.NewInclusiveTimeRange(uis.startTime, uis.endTime),
		schema:            ctx.s,
		projectionTagRefs: ctx.projTagsRefs,
		projectionTags:    ctx.projectionTags,
		metadata:          uis.metadata,
		invertedFilter:    ctx.invertedFilter,
		skippingFilter:    ctx.skippingFilter,
		entities:          ctx.entities,
		l:                 logger.GetLogger("query", "stream", "local-index"),
		ec:                ec,
	}
}

func tagFilter(startTime, endTime time.Time, metadata *commonv1.Metadata, criteria *modelv1.Criteria,
	projection [][]*logical.Tag, ec executor.StreamExecutionContext,
) logical.UnresolvedPlan {
	return &unresolvedTagFilter{
		startTime:      startTime,
		endTime:        endTime,
		metadata:       metadata,
		criteria:       criteria,
		projectionTags: projection,
		ec:             ec,
	}
}

type analyzeContext struct {
	s              logical.Schema
	invertedFilter index.Filter
	skippingFilter index.Filter
	entities       [][]*modelv1.TagValue
	projectionTags []model.TagProjection
	projTagsRefs   [][]*logical.TagRef
}

func newAnalyzerContext(s logical.Schema) *analyzeContext {
	return &analyzeContext{
		s: s,
	}
}

var (
	_ logical.Plan              = (*tagFilterPlan)(nil)
	_ executor.StreamExecutable = (*tagFilterPlan)(nil)
)

type tagFilterPlan struct {
	s         logical.Schema
	parent    logical.Plan
	tagFilter logical.TagFilter
	hidden    map[string]struct{}
}

func (t *tagFilterPlan) Close() {
	t.parent.(executor.StreamExecutable).Close()
}

func newTagFilter(s logical.Schema, parent logical.Plan, tagFilter logical.TagFilter, hidden map[string]struct{}) logical.Plan {
	return &tagFilterPlan{
		s:         s,
		parent:    parent,
		tagFilter: tagFilter,
		hidden:    hidden,
	}
}

func (t *tagFilterPlan) Execute(ec context.Context) ([]*streamv1.Element, error) {
	var filteredElements []*streamv1.Element

	for {
		entities, err := t.parent.(executor.StreamExecutable).Execute(ec)
		if err != nil {
			return nil, err
		}
		if len(entities) == 0 {
			break
		}
		for _, e := range entities {
			ok, err := t.tagFilter.Match(logical.TagFamilies(e.TagFamilies), t.s)
			if err != nil {
				return nil, err
			}
			if ok {
				t.stripHiddenTags(e)
				filteredElements = append(filteredElements, e)
			}
		}
		if len(filteredElements) > 0 {
			break
		}
	}

	return filteredElements, nil
}

func (t *tagFilterPlan) String() string {
	return fmt.Sprintf("%s tag-filter:%s", t.parent, t.tagFilter.String())
}

func (t *tagFilterPlan) Children() []logical.Plan {
	return []logical.Plan{t.parent}
}

func (t *tagFilterPlan) Schema() logical.Schema {
	return t.s
}

func (t *tagFilterPlan) stripHiddenTags(element *streamv1.Element) {
	if len(t.hidden) == 0 || element == nil {
		return
	}
	families := element.TagFamilies[:0]
	for _, tf := range element.TagFamilies {
		if tf == nil {
			continue
		}
		tags := tf.Tags[:0]
		for _, tag := range tf.Tags {
			if tag == nil {
				continue
			}
			if _, hidden := t.hidden[tag.GetKey()]; hidden {
				continue
			}
			tags = append(tags, tag)
		}
		if len(tags) == 0 {
			continue
		}
		tf.Tags = tags
		families = append(families, tf)
	}
	element.TagFamilies = families
}

type projectionBuilder struct {
	familyOrder []string
	familyTags  map[string][]string
	tagSet      map[string]struct{}
}

func newProjectionBuilder(existing [][]*logical.Tag) *projectionBuilder {
	pb := &projectionBuilder{
		familyTags: make(map[string][]string),
		tagSet:     make(map[string]struct{}),
	}
	for _, tags := range existing {
		if len(tags) == 0 {
			continue
		}
		family := tags[0].GetFamilyName()
		pb.ensureFamily(family)
		for _, tag := range tags {
			pb.addTag(family, tag.GetTagName())
		}
	}
	return pb
}

func (pb *projectionBuilder) HasTag(tagName string) bool {
	_, ok := pb.tagSet[tagName]
	return ok
}

func (pb *projectionBuilder) addTag(family, tagName string) {
	if tagName == "" || pb.HasTag(tagName) {
		return
	}
	pb.ensureFamily(family)
	pb.familyTags[family] = append(pb.familyTags[family], tagName)
	pb.tagSet[tagName] = struct{}{}
}

func (pb *projectionBuilder) ensureFamily(family string) {
	if _, ok := pb.familyTags[family]; ok {
		return
	}
	pb.familyOrder = append(pb.familyOrder, family)
	pb.familyTags[family] = make([]string, 0)
}

func (pb *projectionBuilder) AddTagFromSchema(s logical.Schema, tagName string) (bool, error) {
	if pb.HasTag(tagName) {
		return false, nil
	}
	tagSpec := s.FindTagSpecByName(tagName)
	if tagSpec == nil {
		return false, fmt.Errorf("tag %s not defined in schema", tagName)
	}
	family, ok := familyNameFromSchema(s, tagSpec)
	if !ok {
		return false, fmt.Errorf("tag family not found for %s", tagName)
	}
	pb.addTag(family, tagName)
	return true, nil
}

func (pb *projectionBuilder) Model() []model.TagProjection {
	if len(pb.familyOrder) == 0 {
		return nil
	}
	projections := make([]model.TagProjection, 0, len(pb.familyOrder))
	for _, family := range pb.familyOrder {
		names := pb.familyTags[family]
		if len(names) == 0 {
			continue
		}
		proj := model.TagProjection{
			Family: family,
			Names:  append([]string(nil), names...),
		}
		projections = append(projections, proj)
	}
	if len(projections) == 0 {
		return nil
	}
	return projections
}

func (pb *projectionBuilder) LogicalTags() [][]*logical.Tag {
	if len(pb.familyOrder) == 0 {
		return nil
	}
	logicalTags := make([][]*logical.Tag, 0, len(pb.familyOrder))
	for _, family := range pb.familyOrder {
		names := pb.familyTags[family]
		if len(names) == 0 {
			continue
		}
		familyTags := make([]*logical.Tag, 0, len(names))
		for _, name := range names {
			familyTags = append(familyTags, logical.NewTag(family, name))
		}
		logicalTags = append(logicalTags, familyTags)
	}
	if len(logicalTags) == 0 {
		return nil
	}
	return logicalTags
}

func familyNameFromSchema(s logical.Schema, tagSpec *logical.TagSpec) (string, bool) {
	streamSchema, ok := s.(*schema)
	if !ok || streamSchema == nil || streamSchema.stream == nil {
		return "", false
	}
	tagFamilies := streamSchema.stream.GetTagFamilies()
	if tagSpec.TagFamilyIdx < 0 || tagSpec.TagFamilyIdx >= len(tagFamilies) {
		return "", false
	}
	return tagFamilies[tagSpec.TagFamilyIdx].GetName(), true
}
