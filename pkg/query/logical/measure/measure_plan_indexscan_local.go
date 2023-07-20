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
	"io"
	"time"

	"go.uber.org/multierr"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ logical.UnresolvedPlan = (*unresolvedIndexScan)(nil)

type unresolvedIndexScan struct {
	startTime        time.Time
	endTime          time.Time
	metadata         *commonv1.Metadata
	criteria         *modelv1.Criteria
	projectionTags   [][]*logical.Tag
	projectionFields []*logical.Field
	groupByEntity    bool
}

func (uis *unresolvedIndexScan) Analyze(s logical.Schema) (logical.Plan, error) {
	var projTagsRefs [][]*logical.TagRef
	if len(uis.projectionTags) > 0 {
		var err error
		projTagsRefs, err = s.CreateTagRef(uis.projectionTags...)
		if err != nil {
			return nil, err
		}
	}

	var projFieldRefs []*logical.FieldRef
	if len(uis.projectionFields) > 0 {
		var err error
		projFieldRefs, err = s.CreateFieldRef(uis.projectionFields...)
		if err != nil {
			return nil, err
		}
	}

	entityList := s.EntityList()
	entityMap := make(map[string]int)
	entity := make([]tsdb.Entry, len(entityList))
	for idx, e := range entityList {
		entityMap[e] = idx
		// fill AnyEntry by default
		entity[idx] = tsdb.AnyEntry
	}
	filter, entities, err := logical.BuildLocalFilter(uis.criteria, s, entityMap, entity)
	if err != nil {
		return nil, err
	}

	return &localIndexScan{
		timeRange:            timestamp.NewInclusiveTimeRange(uis.startTime, uis.endTime),
		schema:               s,
		projectionTagsRefs:   projTagsRefs,
		projectionFieldsRefs: projFieldRefs,
		metadata:             uis.metadata,
		filter:               filter,
		entities:             entities,
		groupByEntity:        uis.groupByEntity,
		l:                    logger.GetLogger("query", "measure", uis.metadata.Group, uis.metadata.Name, "local-index"),
	}, nil
}

var (
	_ logical.Plan          = (*localIndexScan)(nil)
	_ logical.Sorter        = (*localIndexScan)(nil)
	_ logical.VolumeLimiter = (*localIndexScan)(nil)
)

type localIndexScan struct {
	schema               logical.Schema
	filter               index.Filter
	order                *logical.OrderBy
	metadata             *commonv1.Metadata
	l                    *logger.Logger
	timeRange            timestamp.TimeRange
	projectionTagsRefs   [][]*logical.TagRef
	projectionFieldsRefs []*logical.FieldRef
	entities             []tsdb.Entity
	groupByEntity        bool
	maxDataPointsSize    int
}

func (i *localIndexScan) Limit(max int) {
	i.maxDataPointsSize = max
}

func (i *localIndexScan) Sort(order *logical.OrderBy) {
	i.order = order
}

func (i *localIndexScan) Execute(ec executor.MeasureExecutionContext) (mit executor.MIterator, err error) {
	var orderBy *tsdb.OrderBy
	if i.order.Index != nil {
		orderBy = &tsdb.OrderBy{
			Index: i.order.Index,
			Sort:  i.order.Sort,
		}
	}
	var seriesList tsdb.SeriesList
	for _, e := range i.entities {
		shards, errInternal := ec.Shards(e)
		if errInternal != nil {
			return nil, errInternal
		}
		for _, shard := range shards {
			sl, errInternal := shard.Series().Search(
				context.WithValue(
					context.Background(),
					logger.ContextKey,
					i.l,
				),
				tsdb.NewPath(e),
				i.filter,
				orderBy,
			)
			if errInternal != nil {
				return nil, errInternal
			}
			seriesList = seriesList.Merge(sl)
		}
	}
	if len(seriesList) == 0 {
		return dummyIter, nil
	}
	var builders []logical.SeekerBuilder
	if i.order.Index == nil {
		builders = append(builders, func(builder tsdb.SeekerBuilder) {
			builder.OrderByTime(i.order.Sort)
		})
	}
	// CAVEAT: the order of series list matters when sorting by an index.
	iters, closers, err := logical.ExecuteForShard(i.l, seriesList, i.timeRange, builders...)
	if err != nil {
		return nil, err
	}
	if len(closers) > 0 {
		defer func(closers []io.Closer) {
			for _, c := range closers {
				err = multierr.Append(err, c.Close())
			}
		}(closers)
	}

	if len(iters) == 0 {
		return dummyIter, nil
	}
	transformContext := transformContext{
		ec:                   ec,
		projectionTagsRefs:   i.projectionTagsRefs,
		projectionFieldsRefs: i.projectionFieldsRefs,
	}
	if i.groupByEntity {
		return newSeriesMIterator(iters, transformContext, i.maxDataPointsSize), nil
	}
	it := logical.NewItemIter(iters, i.order.Sort)
	return newIndexScanIterator(it, transformContext, i.maxDataPointsSize), nil
}

func (i *localIndexScan) String() string {
	return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s; projection=%s; order=%s; limit=%d",
		i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(),
		i.filter, logical.FormatTagRefs(", ", i.projectionTagsRefs...), i.order, i.maxDataPointsSize)
}

func (i *localIndexScan) Children() []logical.Plan {
	return []logical.Plan{}
}

func (i *localIndexScan) Schema() logical.Schema {
	if len(i.projectionTagsRefs) == 0 {
		return i.schema
	}
	return i.schema.ProjTags(i.projectionTagsRefs...).ProjFields(i.projectionFieldsRefs...)
}

func indexScan(startTime, endTime time.Time, metadata *commonv1.Metadata, projectionTags [][]*logical.Tag,
	projectionFields []*logical.Field, groupByEntity bool, criteria *modelv1.Criteria,
) logical.UnresolvedPlan {
	return &unresolvedIndexScan{
		startTime:        startTime,
		endTime:          endTime,
		metadata:         metadata,
		projectionTags:   projectionTags,
		projectionFields: projectionFields,
		groupByEntity:    groupByEntity,
		criteria:         criteria,
	}
}

var _ executor.MIterator = (*indexScanIterator)(nil)

type indexScanIterator struct {
	inner   logical.ItemIterator
	err     error
	current *measurev1.DataPoint
	context transformContext
	max     int
	num     int
}

func newIndexScanIterator(inner logical.ItemIterator, context transformContext, max int) executor.MIterator {
	return &indexScanIterator{
		inner:   inner,
		context: context,
		max:     max,
	}
}

func (ism *indexScanIterator) Next() bool {
	if !ism.inner.HasNext() || ism.err != nil || ism.num > ism.max {
		return false
	}
	nextItem := ism.inner.Next()
	var err error
	if ism.current, err = transform(nextItem, ism.context); err != nil {
		ism.err = multierr.Append(ism.err, err)
	}
	ism.num++
	return true
}

func (ism *indexScanIterator) Current() []*measurev1.DataPoint {
	if ism.current == nil {
		return nil
	}
	return []*measurev1.DataPoint{ism.current}
}

func (ism *indexScanIterator) Close() error {
	return multierr.Combine(ism.err, ism.inner.Close())
}

var _ executor.MIterator = (*seriesIterator)(nil)

type seriesIterator struct {
	err     error
	context transformContext
	inner   []tsdb.Iterator
	current []*measurev1.DataPoint
	index   int
	num     int
	max     int
}

func newSeriesMIterator(inner []tsdb.Iterator, context transformContext, max int) executor.MIterator {
	return &seriesIterator{
		inner:   inner,
		context: context,
		index:   -1,
		max:     max,
	}
}

func (ism *seriesIterator) Next() bool {
	if ism.err != nil || ism.num > ism.max {
		return false
	}
	ism.index++
	if ism.index >= len(ism.inner) {
		return false
	}
	iter := ism.inner[ism.index]
	if ism.current != nil {
		ism.current = ism.current[:0]
	}
	for iter.Next() {
		dp, err := transform(iter.Val(), ism.context)
		if err != nil {
			ism.err = err
			return false
		}
		ism.current = append(ism.current, dp)
	}
	ism.num++
	return true
}

func (ism *seriesIterator) Current() []*measurev1.DataPoint {
	return ism.current
}

func (ism *seriesIterator) Close() error {
	for _, i := range ism.inner {
		ism.err = multierr.Append(ism.err, i.Close())
	}
	return ism.err
}

type transformContext struct {
	ec                   executor.MeasureExecutionContext
	projectionTagsRefs   [][]*logical.TagRef
	projectionFieldsRefs []*logical.FieldRef
}

func transform(item tsdb.Item, ism transformContext) (*measurev1.DataPoint, error) {
	tagFamilies, err := logical.ProjectItem(ism.ec, item, ism.projectionTagsRefs)
	if err != nil {
		return nil, err
	}
	dpFields := make([]*measurev1.DataPoint_Field, 0)
	for _, f := range ism.projectionFieldsRefs {
		fieldVal, parserFieldErr := ism.ec.ParseField(f.Field.Name, item)
		if parserFieldErr != nil {
			return nil, parserFieldErr
		}
		dpFields = append(dpFields, fieldVal)
	}
	return &measurev1.DataPoint{
		Fields:      dpFields,
		TagFamilies: tagFamilies,
		Timestamp:   timestamppb.New(time.Unix(0, int64(item.Time()))),
	}, nil
}

var dummyIter = dummyMIterator{}

type dummyMIterator struct{}

func (ei dummyMIterator) Next() bool {
	return false
}

func (ei dummyMIterator) Current() []*measurev1.DataPoint {
	return nil
}

func (ei dummyMIterator) Close() error {
	return nil
}
