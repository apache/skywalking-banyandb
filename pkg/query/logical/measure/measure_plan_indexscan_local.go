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

package measure

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// vectorizedExecutionContext is an optional capability the executor may
// expose. When set and Enabled, localIndexScan wraps the row-path
// MeasureQueryResult in a vectorized adapter instead of the legacy
// resultMIterator. The bool guard keeps the row path mechanically untouched
// when the flag is off — confirming C1 (zero regression).
type vectorizedExecutionContext interface {
	VectorizedConfig() vmeasure.VectorizedConfig
}

var _ logical.UnresolvedPlan = (*unresolvedIndexScan)(nil)

type unresolvedIndexScan struct {
	startTime        time.Time
	endTime          time.Time
	ec               executor.MeasureExecutionContext
	metadata         *commonv1.Metadata
	criteria         *modelv1.Criteria
	projectionTags   [][]*logical.Tag
	projectionFields []*logical.Field
	groupByEntity    bool
}

func (uis *unresolvedIndexScan) Analyze(s logical.Schema) (logical.Plan, error) {
	projTags := make([]model.TagProjection, len(uis.projectionTags))
	projectedTagNames := make(map[string]struct{})
	var projTagsRefs [][]*logical.TagRef
	if len(uis.projectionTags) > 0 {
		for i := range uis.projectionTags {
			for _, tag := range uis.projectionTags[i] {
				projTags[i].Family = tag.GetFamilyName()
				projTags[i].Names = append(projTags[i].Names, tag.GetTagName())
				projectedTagNames[tag.GetTagName()] = struct{}{}
			}
		}
	}
	tagRefInput := make([][]*logical.Tag, 0, len(uis.projectionTags))
	tagRefInput = append(tagRefInput, uis.projectionTags...)

	// Create tag refs for projection tags before any early returns
	if len(tagRefInput) > 0 {
		var err error
		projTagsRefs, err = s.CreateTagRef(tagRefInput...)
		if err != nil {
			return nil, err
		}
	}

	var projField []string
	var projFieldRefs []*logical.FieldRef
	if len(uis.projectionFields) > 0 {
		for i := range uis.projectionFields {
			projField = append(projField, uis.projectionFields[i].Name)
		}
		var err error
		projFieldRefs, err = s.CreateFieldRef(uis.projectionFields...)
		if err != nil {
			return nil, err
		}
	}

	tr := timestamp.NewInclusiveTimeRange(uis.startTime, uis.endTime)
	ms := s.(*schema)
	if ms.measure.IndexMode {
		query, err := inverted.BuildIndexModeQuery(uis.metadata.Name, uis.criteria, s)
		if err != nil {
			return nil, err
		}
		return &localIndexScan{
			timeRange:            tr,
			schema:               s,
			projectionTags:       projTags,
			projectionFields:     projField,
			projectionTagsRefs:   projTagsRefs,
			projectionFieldsRefs: projFieldRefs,
			metadata:             uis.metadata,
			query:                query,
			groupByEntity:        uis.groupByEntity,
			uis:                  uis,
			l:                    logger.GetLogger("query", "measure", uis.metadata.Group, uis.metadata.Name, "local-index"),
			ec:                   uis.ec,
		}, nil
	}

	entityList := s.EntityList()
	entityMap := make(map[string]int)
	entity := make([]*modelv1.TagValue, len(entityList))
	for idx, e := range entityList {
		entityMap[e] = idx
		// fill AnyEntry by default
		entity[idx] = pbv1.AnyTagValue
	}

	// Collect hidden criteria tags using shared utility
	tagFamilies := ms.measure.GetTagFamilies()
	hiddenTags, extraTags := logical.CollectHiddenCriteriaTags(
		uis.criteria,
		projectedTagNames,
		entityMap,
		s,
		func() []string {
			names := make([]string, len(tagFamilies))
			for i, tf := range tagFamilies {
				names[i] = tf.GetName()
			}
			return names
		},
	)
	if len(extraTags) > 0 {
		tagRefInput = append(tagRefInput, extraTags...)
		// Recreate tag refs with both projection tags and hidden criteria tags
		var err error
		projTagsRefs, err = s.CreateTagRef(tagRefInput...)
		if err != nil {
			return nil, err
		}
	}
	query, entities, _, err := inverted.BuildQuery(uis.criteria, s, entityMap, entity)
	if err != nil {
		return nil, err
	}

	return &localIndexScan{
		timeRange:            tr,
		schema:               s,
		projectionTags:       projTags,
		projectionFields:     projField,
		projectionTagsRefs:   projTagsRefs,
		projectionFieldsRefs: projFieldRefs,
		metadata:             uis.metadata,
		query:                query,
		entities:             entities,
		groupByEntity:        uis.groupByEntity,
		hiddenTags:           hiddenTags,
		uis:                  uis,
		l:                    logger.GetLogger("query", "measure", uis.metadata.Group, uis.metadata.Name, "local-index"),
		ec:                   uis.ec,
	}, nil
}

var (
	_ logical.Plan   = (*localIndexScan)(nil)
	_ logical.Sorter = (*localIndexScan)(nil)
)

type localIndexScan struct {
	ec                   executor.MeasureExecutionContext
	schema               logical.Schema
	query                index.Query
	uis                  *unresolvedIndexScan
	order                *logical.OrderBy
	metadata             *commonv1.Metadata
	l                    *logger.Logger
	hiddenTags           logical.HiddenTagSet
	timeRange            timestamp.TimeRange
	projectionTagsRefs   [][]*logical.TagRef
	projectionFieldsRefs []*logical.FieldRef
	entities             [][]*modelv1.TagValue
	projectionFields     []string
	projectionTags       []model.TagProjection
	groupByEntity        bool
}

func (i *localIndexScan) Sort(order *logical.OrderBy) {
	i.order = order
}

func (i *localIndexScan) Execute(ctx context.Context) (mit executor.MIterator, err error) {
	var orderBy *index.OrderBy

	if i.order != nil {
		orderBy = &index.OrderBy{
			Sort:  i.order.Sort,
			Index: i.order.Index,
		}
		if orderBy.Index == nil {
			orderBy.Type = index.OrderByTypeTime
		} else {
			orderBy.Type = index.OrderByTypeIndex
		}
	}
	if i.groupByEntity {
		if orderBy == nil {
			orderBy = &index.OrderBy{}
		}
		orderBy.Type = index.OrderByTypeSeries
	}
	ctx, stop := i.startSpan(ctx, query.GetTracer(ctx), orderBy)
	defer stop(err)
	opts := model.MeasureQueryOptions{
		Name:            i.metadata.GetName(),
		TimeRange:       &i.timeRange,
		Entities:        i.entities,
		Query:           i.query,
		Order:           orderBy,
		TagProjection:   i.projectionTags,
		FieldProjection: i.projectionFields,
	}
	result, err := i.ec.Query(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to query measure: %w", err)
	}
	if vit, vok, vErr := i.maybeVectorized(ctx, result, opts); vErr != nil {
		return nil, vErr
	} else if vok {
		return vit, nil
	}
	return &resultMIterator{
		result:           result,
		projectionTags:   i.projectionTags,
		projectionFields: i.projectionFields,
		hiddenTags:       i.hiddenTags,
	}, nil
}

// maybeVectorized returns a vectorized MIterator iff every gate is met:
//   - the executor exposes a VectorizedConfig.
//   - the config is Enabled.
//   - the underlying result is non-nil (the row path returns a typed-nil result
//     for empty queries; routing that through the vectorized scan would NPE).
//   - the logical schema exposes the *databasev1.Measure needed to type the
//     batch columns.
//
// Any negative answer falls through to the row path, so flag-off behavior is
// byte-identical to the pre-G4 codepath.
func (i *localIndexScan) maybeVectorized(ctx context.Context, result model.MeasureQueryResult,
	opts model.MeasureQueryOptions,
) (executor.MIterator, bool, error) {
	if result == nil {
		return nil, false, nil
	}
	src, ok := i.ec.(vectorizedExecutionContext)
	if !ok {
		return nil, false, nil
	}
	cfg := src.VectorizedConfig()
	if !cfg.Enabled {
		return nil, false, nil
	}
	measureSchema := i.measureSchema()
	if measureSchema == nil {
		return nil, false, nil
	}
	it, buildErr := vmeasure.NewMIterator(ctx, result, measureSchema, opts, cfg)
	if buildErr != nil {
		// NewMIterator does not take ownership of result on failure; the
		// row-path code releases on Close, so without this Release the
		// snapshots / segments behind result leak.
		result.Release()
		return nil, false, fmt.Errorf("failed to build vectorized iterator: %w", buildErr)
	}
	if i.hiddenTags.IsEmpty() {
		return it, true, nil
	}
	return &hiddenTagsMIterator{inner: it, hiddenTags: i.hiddenTags}, true, nil
}

// hiddenTagsMIterator wraps an MIterator and strips hidden criteria tags from
// each Current() result. The row path applies the same strip inside
// resultMIterator.Next; this decorator keeps the vectorized path's contract
// identical without duplicating filter logic into the vectorized package.
type hiddenTagsMIterator struct {
	inner      executor.MIterator
	hiddenTags logical.HiddenTagSet
}

func (h *hiddenTagsMIterator) Next() bool { return h.inner.Next() }

func (h *hiddenTagsMIterator) Current() []*measurev1.InternalDataPoint {
	dps := h.inner.Current()
	for _, dp := range dps {
		if dp == nil || dp.DataPoint == nil {
			continue
		}
		dp.DataPoint.TagFamilies = h.hiddenTags.StripHiddenTags(dp.DataPoint.TagFamilies)
	}
	return dps
}

func (h *hiddenTagsMIterator) Close() error { return h.inner.Close() }

// measureSchema extracts the *databasev1.Measure from the logical schema. The
// vectorized path needs it to type each projected column.
func (i *localIndexScan) measureSchema() *databasev1.Measure {
	if i.schema == nil {
		return nil
	}
	ms, ok := i.schema.(*schema)
	if !ok {
		return nil
	}
	return ms.measure
}

func (i *localIndexScan) String() string {
	return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s; projection=%s; order=%s;",
		i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(),
		i.query, logical.FormatTagRefs(", ", i.projectionTagsRefs...), i.order)
}

func (i *localIndexScan) Children() []logical.Plan {
	return []logical.Plan{}
}

func (i *localIndexScan) Schema() logical.Schema {
	schema := i.schema
	if len(i.projectionTagsRefs) > 0 {
		if projected := schema.ProjTags(i.projectionTagsRefs...); projected != nil {
			schema = projected
		}
	}
	if len(i.projectionFieldsRefs) > 0 {
		schema = schema.ProjFields(i.projectionFieldsRefs...)
	}
	return schema
}

func indexScan(startTime, endTime time.Time, metadata *commonv1.Metadata, projectionTags [][]*logical.Tag,
	projectionFields []*logical.Field, groupByEntity bool, criteria *modelv1.Criteria, ec executor.MeasureExecutionContext,
) logical.UnresolvedPlan {
	return &unresolvedIndexScan{
		startTime:        startTime,
		endTime:          endTime,
		metadata:         metadata,
		projectionTags:   projectionTags,
		projectionFields: projectionFields,
		groupByEntity:    groupByEntity,
		criteria:         criteria,
		ec:               ec,
	}
}

type resultMIterator struct {
	result           model.MeasureQueryResult
	hiddenTags       logical.HiddenTagSet
	err              error
	current          []*measurev1.InternalDataPoint
	projectionTags   []model.TagProjection
	projectionFields []string
	i                int
}

func (ei *resultMIterator) Next() bool {
	if ei.result == nil {
		return false
	}
	ei.i++
	if ei.i < len(ei.current) {
		return true
	}

	r := ei.result.Pull()
	if r == nil {
		return false
	}
	if r.Error != nil {
		ei.err = r.Error
		return false
	}
	ei.current = ei.current[:0]
	ei.i = 0
	tagFamilyMap := make(map[string]*model.TagFamily, len(r.TagFamilies))
	for idx := range r.TagFamilies {
		tagFamilyMap[r.TagFamilies[idx].Name] = &r.TagFamilies[idx]
	}
	for i := range r.Timestamps {
		dp := &measurev1.DataPoint{
			Timestamp: timestamppb.New(time.Unix(0, r.Timestamps[i])),
			Sid:       uint64(r.SID),
			Version:   r.Versions[i],
		}

		for _, proj := range ei.projectionTags {
			tagFamily := &modelv1.TagFamily{
				Name: proj.Family,
			}
			dp.TagFamilies = append(dp.TagFamilies, tagFamily)
			resultTagFamily := tagFamilyMap[proj.Family]
			for _, tagName := range proj.Names {
				var tagValue *modelv1.TagValue
				if resultTagFamily != nil {
					for _, t := range resultTagFamily.Tags {
						if t.Name == tagName {
							tagValue = t.Values[i]
							break
						}
					}
				}
				if tagValue == nil {
					tagValue = pbv1.NullTagValue
				}
				tagFamily.Tags = append(tagFamily.Tags, &modelv1.Tag{
					Key:   tagName,
					Value: tagValue,
				})
			}
		}

		// Strip hidden tags from the result
		dp.TagFamilies = ei.hiddenTags.StripHiddenTags(dp.TagFamilies)

		for _, pf := range ei.projectionFields {
			foundIdx := -1
			for idx := range r.Fields {
				if r.Fields[idx].Name == pf {
					foundIdx = idx
					break
				}
			}
			if foundIdx != -1 {
				dp.Fields = append(dp.Fields, &measurev1.DataPoint_Field{
					Name:  r.Fields[foundIdx].Name,
					Value: r.Fields[foundIdx].Values[i],
				})
			} else {
				dp.Fields = append(dp.Fields, &measurev1.DataPoint_Field{
					Name:  pf,
					Value: pbv1.NullFieldValue,
				})
			}
		}
		var shardID common.ShardID
		if len(r.ShardIDs) > i {
			shardID = r.ShardIDs[i]
		}
		ei.current = append(ei.current, &measurev1.InternalDataPoint{
			DataPoint: dp,
			ShardId:   uint32(shardID),
		})
	}

	return true
}

func (ei *resultMIterator) Current() []*measurev1.InternalDataPoint {
	return []*measurev1.InternalDataPoint{ei.current[ei.i]}
}

func (ei *resultMIterator) Close() error {
	if ei.result != nil {
		ei.result.Release()
	}
	return ei.err
}

func (i *localIndexScan) startSpan(ctx context.Context, tracer *query.Tracer, orderBy *index.OrderBy) (context.Context, func(error)) {
	if tracer == nil {
		return ctx, func(error) {}
	}

	span, ctx := tracer.StartSpan(ctx, "indexScan-%s", i.metadata)
	if orderBy != nil {
		sortName := modelv1.Sort_name[int32(orderBy.Sort)]
		switch orderBy.Type {
		case index.OrderByTypeTime:
			span.Tag("orderBy", "time-"+sortName)
		case index.OrderByTypeIndex:
			span.Tag("orderBy", fmt.Sprintf("indexRule:%s-%s", orderBy.Index.Metadata.Name, sortName))
		case index.OrderByTypeSeries:
			span.Tag("orderBy", "series")
		}
	} else {
		span.Tag("orderBy", "time-asc(default)")
	}
	span.Tag("details", i.String())

	return ctx, func(err error) {
		if err != nil {
			span.Error(err)
		}
		span.Stop()
	}
}
