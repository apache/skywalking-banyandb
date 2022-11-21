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
	"fmt"
	"io"
	"time"

	"go.uber.org/multierr"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ logical.UnresolvedPlan = (*unresolvedIndexScan)(nil)

type unresolvedIndexScan struct {
	startTime         time.Time
	endTime           time.Time
	metadata          *commonv1.Metadata
	filter            index.Filter
	projectionTags    [][]*logical.Tag
	projectionFields  []*logical.Field
	entities          []tsdb.Entity
	groupByEntity     bool
	unresolvedOrderBy *logical.UnresolvedOrderBy
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

	// resolve sub-plan with the projected view of streamSchema
	orderBySubPlan, err := uis.unresolvedOrderBy.Analyze(s.ProjTags(projTagsRefs...))
	if err != nil {
		return nil, err
	}

	return &localIndexScan{
		timeRange:            timestamp.NewInclusiveTimeRange(uis.startTime, uis.endTime),
		schema:               s,
		projectionTagsRefs:   projTagsRefs,
		projectionFieldsRefs: projFieldRefs,
		metadata:             uis.metadata,
		filter:               uis.filter,
		entities:             uis.entities,
		groupByEntity:        uis.groupByEntity,
		OrderBy:              orderBySubPlan,
	}, nil
}

var _ logical.Plan = (*localIndexScan)(nil)

type localIndexScan struct {
	*logical.OrderBy
	timeRange            timestamp.TimeRange
	schema               logical.Schema
	metadata             *commonv1.Metadata
	filter               index.Filter
	projectionTagsRefs   [][]*logical.TagRef
	projectionFieldsRefs []*logical.FieldRef
	entities             []tsdb.Entity
	groupByEntity        bool
}

func (i *localIndexScan) Execute(ec executor.MeasureExecutionContext) (executor.MIterator, error) {
	var seriesList tsdb.SeriesList
	for _, e := range i.entities {
		shards, err := ec.Shards(e)
		if err != nil {
			return nil, err
		}
		for _, shard := range shards {
			sl, err := shard.Series().List(tsdb.NewPath(e))
			if err != nil {
				return nil, err
			}
			seriesList = seriesList.Merge(sl)
		}
	}
	if len(seriesList) == 0 {
		return executor.EmptyMIterator, nil
	}
	var builders []logical.SeekerBuilder
	if i.Index != nil {
		builders = append(builders, func(builder tsdb.SeekerBuilder) {
			builder.OrderByIndex(i.Index, i.Sort)
		})
	} else {
		builders = append(builders, func(builder tsdb.SeekerBuilder) {
			builder.OrderByTime(i.Sort)
		})
	}
	if i.filter != nil {
		builders = append(builders, func(b tsdb.SeekerBuilder) {
			b.Filter(i.filter)
		})
	}
	iters, closers, innerErr := logical.ExecuteForShard(seriesList, i.timeRange, builders...)
	if len(closers) > 0 {
		defer func(closers []io.Closer) {
			for _, c := range closers {
				_ = c.Close()
			}
		}(closers)
	}
	if innerErr != nil {
		return nil, innerErr
	}

	if len(iters) == 0 {
		return executor.EmptyMIterator, nil
	}
	transformContext := transformContext{
		ec:                   ec,
		projectionTagsRefs:   i.projectionTagsRefs,
		projectionFieldsRefs: i.projectionFieldsRefs,
	}
	if i.groupByEntity {
		return newSeriesMIterator(iters, transformContext), nil
	}
	c := logical.CreateComparator(i.Sort)
	it := logical.NewItemIter(iters, c)
	return newIndexScanIterator(it, transformContext), nil
}

func (i *localIndexScan) String() string {
	return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s; projection=%s",
		i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(),
		i.filter, logical.FormatTagRefs(", ", i.projectionTagsRefs...))
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

func IndexScan(startTime, endTime time.Time, metadata *commonv1.Metadata, filter index.Filter, entities []tsdb.Entity,
	projectionTags [][]*logical.Tag, projectionFields []*logical.Field, groupByEntity bool, unresolvedOrderBy *logical.UnresolvedOrderBy,
) logical.UnresolvedPlan {
	return &unresolvedIndexScan{
		startTime:         startTime,
		endTime:           endTime,
		metadata:          metadata,
		filter:            filter,
		projectionTags:    projectionTags,
		projectionFields:  projectionFields,
		entities:          entities,
		groupByEntity:     groupByEntity,
		unresolvedOrderBy: unresolvedOrderBy,
	}
}

var _ executor.MIterator = (*indexScanIterator)(nil)

type indexScanIterator struct {
	context transformContext
	inner   logical.ItemIterator

	current *measurev1.DataPoint
	err     error
}

func newIndexScanIterator(inner logical.ItemIterator, context transformContext) executor.MIterator {
	return &indexScanIterator{
		inner:   inner,
		context: context,
	}
}

func (ism *indexScanIterator) Next() bool {
	if !ism.inner.HasNext() || ism.err != nil {
		return false
	}
	nextItem := ism.inner.Next()
	var err error
	if ism.current, err = transform(nextItem, ism.context); err != nil {
		ism.err = multierr.Append(ism.err, err)
	}
	return true
}

func (ism *indexScanIterator) Current() []*measurev1.DataPoint {
	if ism.current == nil {
		return nil
	}
	return []*measurev1.DataPoint{ism.current}
}

func (ism *indexScanIterator) Close() error {
	return ism.err
}

var _ executor.MIterator = (*seriesIterator)(nil)

type seriesIterator struct {
	inner   []tsdb.Iterator
	context transformContext

	index   int
	current []*measurev1.DataPoint
	err     error
}

func newSeriesMIterator(inner []tsdb.Iterator, context transformContext) executor.MIterator {
	return &seriesIterator{
		inner:   inner,
		context: context,
		index:   -1,
	}
}

func (ism *seriesIterator) Next() bool {
	if ism.err != nil {
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
