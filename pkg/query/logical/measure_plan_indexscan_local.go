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

package logical

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ UnresolvedPlan = (*unresolvedMeasureIndexScan)(nil)

type unresolvedMeasureIndexScan struct {
	startTime        time.Time
	endTime          time.Time
	metadata         *commonv1.Metadata
	conditions       []Expr
	projectionTags   [][]*Tag
	projectionFields []*Field
	entity           tsdb.Entity
	groupByEntity    bool
}

func (uis *unresolvedMeasureIndexScan) Analyze(s Schema) (Plan, error) {
	localConditionMap := make(map[*databasev1.IndexRule][]Expr)
	for _, cond := range uis.conditions {
		if resolvable, ok := cond.(ResolvableExpr); ok {
			err := resolvable.Resolve(s)
			if err != nil {
				return nil, err
			}

			if bCond, ok := cond.(*binaryExpr); ok {
				tag := bCond.l.(*TagRef).tag
				if defined, indexObj := s.IndexDefined(tag); defined {
					if indexObj.GetLocation() == databasev1.IndexRule_LOCATION_SERIES {
						if v, exist := localConditionMap[indexObj]; exist {
							v = append(v, cond)
							localConditionMap[indexObj] = v
						} else {
							localConditionMap[indexObj] = []Expr{cond}
						}
					}
				} else {
					return nil, errors.Wrap(ErrIndexNotDefined, tag.GetCompoundName())
				}
			}
		}
	}

	var projTagsRefs [][]*TagRef
	if len(uis.projectionTags) > 0 {
		var err error
		projTagsRefs, err = s.CreateTagRef(uis.projectionTags...)
		if err != nil {
			return nil, err
		}
	}

	var projFieldRefs []*FieldRef
	if len(uis.projectionFields) > 0 {
		var err error
		projFieldRefs, err = s.CreateFieldRef(uis.projectionFields...)
		if err != nil {
			return nil, err
		}
	}

	return &localMeasureIndexScan{
		timeRange:            timestamp.NewInclusiveTimeRange(uis.startTime, uis.endTime),
		schema:               s,
		projectionTagsRefs:   projTagsRefs,
		projectionFieldsRefs: projFieldRefs,
		metadata:             uis.metadata,
		conditionMap:         localConditionMap,
		entity:               uis.entity,
		groupByEntity:        uis.groupByEntity,
	}, nil
}

var _ Plan = (*localMeasureIndexScan)(nil)

type localMeasureIndexScan struct {
	timeRange            timestamp.TimeRange
	schema               Schema
	metadata             *commonv1.Metadata
	conditionMap         map[*databasev1.IndexRule][]Expr
	projectionTagsRefs   [][]*TagRef
	projectionFieldsRefs []*FieldRef
	entity               tsdb.Entity
	groupByEntity        bool
}

func (i *localMeasureIndexScan) Execute(ec executor.MeasureExecutionContext) (executor.MIterator, error) {
	shards, err := ec.Shards(i.entity)
	if err != nil {
		return nil, err
	}
	var iters []tsdb.Iterator
	for _, shard := range shards {
		itersInShard, innerErr := i.executeInShard(shard)
		if innerErr != nil {
			return nil, innerErr
		}
		if itersInShard == nil {
			continue
		}
		iters = append(iters, itersInShard...)
	}

	if len(iters) == 0 {
		return executor.EmptyMIterator, nil
	}
	transformContext := transformContext{
		ec:                   ec,
		projectionTagsRefs:   i.projectionTagsRefs,
		projectionFieldsRefs: i.projectionFieldsRefs,
	}
	if len(iters) == 1 || i.groupByEntity {
		return newSeriesMIterator(iters, transformContext), nil
	}
	c := createComparator(modelv1.Sort_SORT_DESC)
	it := NewItemIter(iters, c)
	return newIndexScanMIterator(it, transformContext), nil
}

func (i *localMeasureIndexScan) executeInShard(shard tsdb.Shard) ([]tsdb.Iterator, error) {
	seriesList, err := shard.Series().List(tsdb.NewPath(i.entity))
	if err != nil {
		return nil, err
	}

	if len(seriesList) == 0 {
		return nil, nil
	}

	var builders []seekerBuilder

	builders = append(builders, func(builder tsdb.SeekerBuilder) {
		builder.OrderByTime(modelv1.Sort_SORT_DESC)
	})

	if i.conditionMap != nil && len(i.conditionMap) > 0 {
		builders = append(builders, func(b tsdb.SeekerBuilder) {
			for idxRule, exprs := range i.conditionMap {
				b.Filter(idxRule, exprToCondition(exprs))
			}
		})
	}

	return executeForShard(seriesList, i.timeRange, builders...)
}

func (i *localMeasureIndexScan) String() string {
	exprStr := make([]string, 0, len(i.conditionMap))
	for _, conditions := range i.conditionMap {
		var conditionStr []string
		for _, cond := range conditions {
			conditionStr = append(conditionStr, cond.String())
		}
		exprStr = append(exprStr, fmt.Sprintf("(%s)", strings.Join(conditionStr, " AND ")))
	}
	if len(i.projectionTagsRefs) == 0 {
		return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s; projection=None",
			i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(), strings.Join(exprStr, " AND "))
	}
	return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s; projection=%s",
		i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(),
		strings.Join(exprStr, " AND "), formatTagRefs(", ", i.projectionTagsRefs...))
}

func (i *localMeasureIndexScan) Type() PlanType {
	return PlanLocalIndexScan
}

func (i *localMeasureIndexScan) Children() []Plan {
	return []Plan{}
}

func (i *localMeasureIndexScan) Schema() Schema {
	if len(i.projectionTagsRefs) == 0 {
		return i.schema
	}
	return i.schema.ProjTags(i.projectionTagsRefs...).ProjFields(i.projectionFieldsRefs...)
}

func (i *localMeasureIndexScan) Equal(plan Plan) bool {
	if plan.Type() != PlanLocalIndexScan {
		return false
	}
	other := plan.(*localMeasureIndexScan)
	return i.metadata.GetGroup() == other.metadata.GetGroup() &&
		i.metadata.GetName() == other.metadata.GetName() &&
		i.timeRange.Start.UnixNano() == other.timeRange.Start.UnixNano() &&
		i.timeRange.End.UnixNano() == other.timeRange.End.UnixNano() &&
		len(i.entity) == len(other.entity) &&
		bytes.Equal(i.entity.Marshal(), other.entity.Marshal()) &&
		cmp.Equal(i.projectionTagsRefs, other.projectionTagsRefs) &&
		cmp.Equal(i.projectionFieldsRefs, other.projectionFieldsRefs) &&
		cmp.Equal(i.schema, other.schema) &&
		i.groupByEntity == other.groupByEntity &&
		cmp.Equal(i.conditionMap, other.conditionMap)
}

func MeasureIndexScan(startTime, endTime time.Time, metadata *commonv1.Metadata, conditions []Expr, entity tsdb.Entity,
	projectionTags [][]*Tag, projectionFields []*Field, groupByEntity bool) UnresolvedPlan {
	return &unresolvedMeasureIndexScan{
		startTime:        startTime,
		endTime:          endTime,
		metadata:         metadata,
		conditions:       conditions,
		projectionTags:   projectionTags,
		projectionFields: projectionFields,
		entity:           entity,
		groupByEntity:    groupByEntity,
	}
}

var _ executor.MIterator = (*indexScanMIterator)(nil)

type indexScanMIterator struct {
	context transformContext
	inner   ItemIterator

	current *measurev1.DataPoint
	err     error
}

func newIndexScanMIterator(inner ItemIterator, context transformContext) executor.MIterator {
	return &indexScanMIterator{
		inner:   inner,
		context: context,
	}
}

func (ism *indexScanMIterator) Next() bool {
	if !ism.inner.HasNext() || ism.err != nil {
		return false
	}
	nextItem := ism.inner.Next()
	var err error
	ism.current, err = transform(nextItem, ism.context)
	if err != nil {
		ism.err = err
		return false
	}
	return true
}

func (ism *indexScanMIterator) Current() []*measurev1.DataPoint {
	if ism.current == nil {
		return nil
	}
	return []*measurev1.DataPoint{ism.current}
}

func (ism *indexScanMIterator) Close() error {
	return ism.err
}

var _ executor.MIterator = (*seriesMIterator)(nil)

type seriesMIterator struct {
	inner   []tsdb.Iterator
	context transformContext

	index   int
	current []*measurev1.DataPoint
	err     error
}

func newSeriesMIterator(inner []tsdb.Iterator, context transformContext) executor.MIterator {
	return &seriesMIterator{
		inner:   inner,
		context: context,
		index:   -1,
	}
}

func (ism *seriesMIterator) Next() bool {
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

func (ism *seriesMIterator) Current() []*measurev1.DataPoint {
	return ism.current
}

func (ism *seriesMIterator) Close() error {
	for _, i := range ism.inner {
		ism.err = multierr.Append(ism.err, i.Close())
	}
	return ism.err
}

type transformContext struct {
	ec                   executor.MeasureExecutionContext
	projectionTagsRefs   [][]*TagRef
	projectionFieldsRefs []*FieldRef
}

func transform(item tsdb.Item, ism transformContext) (*measurev1.DataPoint, error) {
	tagFamilies, err := projectItem(ism.ec, item, ism.projectionTagsRefs)
	if err != nil {
		return nil, err
	}
	dpFields := make([]*measurev1.DataPoint_Field, 0)
	for _, f := range ism.projectionFieldsRefs {
		fieldVal, parserFieldErr := ism.ec.ParseField(f.field.name, item)
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
