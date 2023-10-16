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

// Package measure implements execution operations for querying measure data.
package measure

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ logical.UnresolvedPlan = (*unresolvedLocalScan)(nil)

type unresolvedLocalScan struct {
	startTime        time.Time
	endTime          time.Time
	metadata         *commonv1.Metadata
	conditions       []*modelv1.Condition
	projectionTags   [][]*logical.Tag
	projectionFields []*logical.Field
	sort             modelv1.Sort
}

func (uls *unresolvedLocalScan) Analyze(s logical.Schema) (logical.Plan, error) {
	var projTagsRefs [][]*logical.TagRef
	if len(uls.projectionTags) > 0 {
		var err error
		projTagsRefs, err = s.CreateTagRef(uls.projectionTags...)
		if err != nil {
			return nil, err
		}
	}

	var projFieldRefs []*logical.FieldRef
	if len(uls.projectionFields) > 0 {
		var err error
		projFieldRefs, err = s.CreateFieldRef(uls.projectionFields...)
		if err != nil {
			return nil, err
		}
	}

	entity, err := uls.locateEntity(s.EntityList())
	if err != nil {
		return nil, err
	}

	return &localScan{
		timeRange:            timestamp.NewInclusiveTimeRange(uls.startTime, uls.endTime),
		schema:               s,
		projectionTagsRefs:   projTagsRefs,
		projectionFieldsRefs: projFieldRefs,
		metadata:             uls.metadata,
		entity:               entity,
		l:                    logger.GetLogger("topn", "measure", uls.metadata.Group, uls.metadata.Name, "local-index"),
	}, nil
}

func (uls *unresolvedLocalScan) locateEntity(entityList []string) (tsdb.Entity, error) {
	entityMap := make(map[string]int)
	entity := make([]tsdb.Entry, 1+1+len(entityList))
	// sortDirection
	entity[0] = convert.Int64ToBytes(int64(uls.sort.Number()))
	// rankNumber
	entity[1] = tsdb.AnyEntry
	for idx, tagName := range entityList {
		entityMap[tagName] = idx + 2
		// allow to make fuzzy search with partial conditions
		entity[idx+2] = tsdb.AnyEntry
	}
	for _, pairQuery := range uls.conditions {
		if pairQuery.GetOp() != modelv1.Condition_BINARY_OP_EQ {
			return nil, errors.Errorf("tag belongs to the entity only supports EQ operation in condition(%v)", pairQuery)
		}
		if entityIdx, ok := entityMap[pairQuery.GetName()]; ok {
			switch v := pairQuery.GetValue().GetValue().(type) {
			case *modelv1.TagValue_Str:
				entity[entityIdx] = []byte(v.Str.GetValue())
			case *modelv1.TagValue_Int:
				entity[entityIdx] = convert.Int64ToBytes(v.Int.GetValue())
			default:
				return nil, errors.New("unsupported condition tag type for entity")
			}
			continue
		}
		return nil, errors.New("only groupBy tag name is supported")
	}

	return entity, nil
}

func local(startTime, endTime time.Time, metadata *commonv1.Metadata, projectionTags [][]*logical.Tag,
	projectionFields []*logical.Field, conditions []*modelv1.Condition, sort modelv1.Sort,
) logical.UnresolvedPlan {
	return &unresolvedLocalScan{
		startTime:        startTime,
		endTime:          endTime,
		metadata:         metadata,
		projectionTags:   projectionTags,
		projectionFields: projectionFields,
		conditions:       conditions,
		sort:             sort,
	}
}

var _ logical.Plan = (*localScan)(nil)

type localScan struct {
	schema               logical.Schema
	metadata             *commonv1.Metadata
	l                    *logger.Logger
	timeRange            timestamp.TimeRange
	projectionTagsRefs   [][]*logical.TagRef
	projectionFieldsRefs []*logical.FieldRef
	entity               tsdb.Entity
	sort                 modelv1.Sort
}

func (i *localScan) Execute(ctx context.Context) (mit executor.MIterator, err error) {
	var seriesList tsdb.SeriesList
	ec := executor.FromMeasureExecutionContext(ctx)
	shards, err := ec.(measure.Measure).CompanionShards(i.metadata)
	if err != nil {
		return nil, err
	}
	for _, shard := range shards {
		sl, errInternal := shard.Series().List(context.WithValue(
			ctx,
			logger.ContextKey,
			i.l,
		), tsdb.NewPath(i.entity))
		if errInternal != nil {
			return nil, errInternal
		}
		seriesList = seriesList.Merge(sl)
	}
	if len(seriesList) == 0 {
		return dummyIter, nil
	}
	var builders []logical.SeekerBuilder
	builders = append(builders, func(builder tsdb.SeekerBuilder) {
		builder.OrderByTime(i.sort)
	})
	iters, closers, err := logical.ExecuteForShard(ctx, i.l, seriesList, i.timeRange, builders...)
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
	tc := transformContext{
		ec:                   ec,
		projectionTagsRefs:   i.projectionTagsRefs,
		projectionFieldsRefs: i.projectionFieldsRefs,
	}
	it := logical.NewItemIter(iters, i.sort)

	return newLocalScanIterator(it, tc), nil
}

func (i *localScan) String() string {
	return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s}; projection=%s; sort=%s;",
		i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(),
		logical.FormatTagRefs(", ", i.projectionTagsRefs...), i.sort)
}

func (i *localScan) Children() []logical.Plan {
	return []logical.Plan{}
}

func (i *localScan) Schema() logical.Schema {
	if len(i.projectionTagsRefs) == 0 {
		return i.schema
	}
	return i.schema.ProjTags(i.projectionTagsRefs...).ProjFields(i.projectionFieldsRefs...)
}

type localScanIterator struct {
	inner   sort.Iterator[tsdb.Item]
	err     error
	current *measurev1.DataPoint
	context transformContext
	num     int
}

func (lst *localScanIterator) Next() bool {
	if !lst.inner.Next() || lst.err != nil {
		return false
	}
	nextItem := lst.inner.Val()
	var err error
	if lst.current, err = transform(nextItem, lst.context); err != nil {
		lst.err = multierr.Append(lst.err, err)
	}
	lst.num++
	return true
}

func (lst *localScanIterator) Current() []*measurev1.DataPoint {
	if lst.current == nil {
		return nil
	}
	return []*measurev1.DataPoint{lst.current}
}

func (lst *localScanIterator) Close() error {
	return multierr.Combine(lst.err, lst.inner.Close())
}

func newLocalScanIterator(inner sort.Iterator[tsdb.Item], context transformContext) executor.MIterator {
	return &localScanIterator{
		inner:   inner,
		context: context,
	}
}
