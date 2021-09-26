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
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	databasev2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v2"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/v2/executor"
)

var _ UnresolvedPlan = (*unresolvedIndexScan)(nil)

type unresolvedIndexScan struct {
	unresolvedOrderBy *UnresolvedOrderBy
	startTime         time.Time
	endTime           time.Time
	metadata          *commonv2.Metadata
	conditions        []Expr
	projectionFields  [][]*Tag
	entity            tsdb.Entity
}

func (uis *unresolvedIndexScan) Type() PlanType {
	return PlanIndexScan
}

func (uis *unresolvedIndexScan) Analyze(s Schema) (Plan, error) {
	conditionMap := make(map[*databasev2.IndexRule][]Expr)
	for _, cond := range uis.conditions {
		if resolvable, ok := cond.(ResolvableExpr); ok {
			err := resolvable.Resolve(s)
			if err != nil {
				return nil, err
			}

			if bCond, ok := cond.(*binaryExpr); ok {
				tag := bCond.l.(*FieldRef).tag
				if defined, indexObj := s.IndexDefined(tag); defined {
					if v, exist := conditionMap[indexObj]; exist {
						v = append(v, cond)
						conditionMap[indexObj] = v
					} else {
						conditionMap[indexObj] = []Expr{cond}
					}
				} else {
					return nil, errors.Wrap(ErrIndexNotDefined, tag.GetCompoundName())
				}
			}
		}
	}

	var projFieldsRefs [][]*FieldRef
	if uis.projectionFields != nil && len(uis.projectionFields) > 0 {
		var err error
		projFieldsRefs, err = s.CreateRef(uis.projectionFields...)
		if err != nil {
			return nil, err
		}
	}

	// resolve sub-plan with the projected view of schema
	orderBySubPlan, err := uis.unresolvedOrderBy.analyze(s.Proj(projFieldsRefs...))

	if err != nil {
		return nil, err
	}

	return &indexScan{
		orderBy:             orderBySubPlan,
		timeRange:           tsdb.NewTimeRange(uis.startTime, uis.endTime),
		schema:              s,
		projectionFieldRefs: projFieldsRefs,
		metadata:            uis.metadata,
		conditionMap:        conditionMap,
		entity:              uis.entity,
	}, nil
}

var _ Plan = (*indexScan)(nil)

type indexScan struct {
	*orderBy
	timeRange           tsdb.TimeRange
	schema              Schema
	metadata            *commonv2.Metadata
	conditionMap        map[*databasev2.IndexRule][]Expr
	projectionFieldRefs [][]*FieldRef
	entity              tsdb.Entity
}

func (i *indexScan) Execute(ec executor.ExecutionContext) ([]*streamv2.Element, error) {
	shards, err := ec.Shards(i.entity)
	if err != nil {
		return nil, err
	}
	var iters []tsdb.Iterator
	for _, shard := range shards {
		itersInShard, err := i.executeInShard(shard)
		if err != nil {
			return nil, err
		}
		iters = append(iters, itersInShard...)
	}

	var c comparator
	if i.index == nil {
		c = createTimestampComparator(i.sort)
	} else {
		c = createComparator(i.sort)
	}

	var elems []*streamv2.Element
	it := NewItemIter(iters, c)
	for it.HasNext() {
		nextItem := it.Next()
		tagFamilies, innerErr := projectItem(ec, nextItem, i.projectionFieldRefs)
		if innerErr != nil {
			return nil, innerErr
		}
		elementID, innerErr := ec.ParseElementID(nextItem)
		if innerErr != nil {
			return nil, innerErr
		}
		elems = append(elems, &streamv2.Element{
			ElementId:   elementID,
			Timestamp:   timestamppb.New(time.Unix(0, int64(nextItem.Time()))),
			TagFamilies: tagFamilies,
		})
	}
	return elems, nil
}

func (i *indexScan) executeInShard(shard tsdb.Shard) ([]tsdb.Iterator, error) {
	seriesList, err := shard.Series().List(tsdb.NewPath(i.entity))
	if err != nil {
		return nil, err
	}

	var builders []seekerBuilder

	if i.index != nil {
		builders = append(builders, func(builder tsdb.SeekerBuilder) {
			builder.OrderByIndex(i.index, i.sort)
		})
	} else {
		builders = append(builders, func(builder tsdb.SeekerBuilder) {
			builder.OrderByTime(i.sort)
		})
	}

	if i.conditionMap != nil && len(i.conditionMap) > 0 {
		builders = append(builders, func(b tsdb.SeekerBuilder) {
			for idxRule, exprs := range i.conditionMap {
				b.Filter(idxRule, exprToCondition(exprs))
			}
		})
	}

	return executeForShard(seriesList, i.timeRange, builders...)
}

func (i *indexScan) String() string {
	exprStr := make([]string, 0, len(i.conditionMap))
	for _, conditions := range i.conditionMap {
		var conditionStr []string
		for _, cond := range conditions {
			conditionStr = append(conditionStr, cond.String())
		}
		exprStr = append(exprStr, fmt.Sprintf("(%s)", strings.Join(conditionStr, " AND ")))
	}
	if len(i.projectionFieldRefs) == 0 {
		return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s; projection=None",
			i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(), strings.Join(exprStr, " AND "))
	}
	return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s; projection=%s",
		i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(),
		strings.Join(exprStr, " AND "), formatExpr(", ", i.projectionFieldRefs...))
}

func (i *indexScan) Type() PlanType {
	return PlanIndexScan
}

func (i *indexScan) Children() []Plan {
	return []Plan{}
}

func (i *indexScan) Schema() Schema {
	if i.projectionFieldRefs == nil || len(i.projectionFieldRefs) == 0 {
		return i.schema
	}
	return i.schema.Proj(i.projectionFieldRefs...)
}

func (i *indexScan) Equal(plan Plan) bool {
	if plan.Type() != PlanIndexScan {
		return false
	}
	other := plan.(*indexScan)
	return i.metadata.GetGroup() == other.metadata.GetGroup() &&
		i.metadata.GetName() == other.metadata.GetName() &&
		i.timeRange.Start.UnixNano() == other.timeRange.Start.UnixNano() &&
		i.timeRange.End.UnixNano() == other.timeRange.End.UnixNano() &&
		bytes.Equal(i.entity.Marshal(), other.entity.Marshal()) &&
		cmp.Equal(i.projectionFieldRefs, other.projectionFieldRefs) &&
		cmp.Equal(i.schema, other.schema) &&
		cmp.Equal(i.conditionMap, other.conditionMap) &&
		cmp.Equal(i.orderBy, other.orderBy)
}

func IndexScan(startTime, endTime time.Time, metadata *commonv2.Metadata, conditions []Expr, entity tsdb.Entity,
	orderBy *UnresolvedOrderBy, projection ...[]*Tag) UnresolvedPlan {
	return &unresolvedIndexScan{
		unresolvedOrderBy: orderBy,
		startTime:         startTime,
		endTime:           endTime,
		metadata:          metadata,
		conditions:        conditions,
		projectionFields:  projection,
		entity:            entity,
	}
}

func exprToCondition(exprs []Expr) tsdb.Condition {
	cond := make(map[string][]index.ConditionValue)
	for _, expr := range exprs {
		bExpr := expr.(*binaryExpr)
		l := bExpr.l.(*FieldRef)
		r := bExpr.r.(LiteralExpr)
		if existingList, ok := cond[l.tag.GetTagName()]; ok {
			existingList = append(existingList, index.ConditionValue{
				Values: r.Bytes(),
				Op:     bExpr.op,
			})
			cond[l.tag.GetTagName()] = existingList
		} else {
			cond[l.tag.GetTagName()] = []index.ConditionValue{
				{
					Values: r.Bytes(),
					Op:     bExpr.op,
				},
			}
		}
	}
	return cond
}
