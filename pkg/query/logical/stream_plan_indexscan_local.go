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
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ Plan = (*localIndexScan)(nil)

type localIndexScan struct {
	*orderBy
	timeRange         timestamp.TimeRange
	schema            Schema
	metadata          *commonv1.Metadata
	conditionMap      map[*databasev1.IndexRule][]Expr
	projectionTagRefs [][]*TagRef
	entity            tsdb.Entity
}

func (i *localIndexScan) Execute(ec executor.StreamExecutionContext) ([]*streamv1.Element, error) {
	shards, err := ec.Shards(i.entity)
	if err != nil {
		return nil, err
	}
	var iters []tsdb.ItemIterator
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

	var elems []*streamv1.Element

	if len(iters) == 0 {
		return elems, nil
	}

	c := createComparator(i.sort)
	it := NewItemIter(iters, c)
	for it.HasNext() {
		nextItem := it.Next()
		tagFamilies, innerErr := projectItem(ec, nextItem, i.projectionTagRefs)
		if innerErr != nil {
			return nil, innerErr
		}
		elementID, innerErr := ec.ParseElementID(nextItem)
		if innerErr != nil {
			return nil, innerErr
		}
		elems = append(elems, &streamv1.Element{
			ElementId:   elementID,
			Timestamp:   timestamppb.New(time.Unix(0, int64(nextItem.Time()))),
			TagFamilies: tagFamilies,
		})
	}
	return elems, nil
}

func (i *localIndexScan) executeInShard(shard tsdb.Shard) ([]tsdb.ItemIterator, error) {
	seriesList, err := shard.Series().List(tsdb.NewPath(i.entity))
	if err != nil {
		return nil, err
	}

	if len(seriesList) == 0 {
		return nil, nil
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
				b.Filter(idxRule, ExprToCondition(exprs))
			}
		})
	}

	return executeForShard(seriesList, i.timeRange, builders...)
}

func (i *localIndexScan) String() string {
	exprStr := make([]string, 0, len(i.conditionMap))
	for _, conditions := range i.conditionMap {
		var conditionStr []string
		for _, cond := range conditions {
			conditionStr = append(conditionStr, cond.String())
		}
		exprStr = append(exprStr, fmt.Sprintf("(%s)", strings.Join(conditionStr, " AND ")))
	}
	if len(i.projectionTagRefs) == 0 {
		return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s; projection=None",
			i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(), strings.Join(exprStr, " AND "))
	}
	return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s; projection=%s",
		i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(),
		strings.Join(exprStr, " AND "), formatTagRefs(", ", i.projectionTagRefs...))
}

func (i *localIndexScan) Type() PlanType {
	return PlanLocalIndexScan
}

func (i *localIndexScan) Children() []Plan {
	return []Plan{}
}

func (i *localIndexScan) Schema() Schema {
	if i.projectionTagRefs == nil || len(i.projectionTagRefs) == 0 {
		return i.schema
	}
	return i.schema.ProjTags(i.projectionTagRefs...)
}

func (i *localIndexScan) Equal(plan Plan) bool {
	if plan.Type() != PlanLocalIndexScan {
		return false
	}
	other := plan.(*localIndexScan)
	return i.metadata.GetGroup() == other.metadata.GetGroup() &&
		i.metadata.GetName() == other.metadata.GetName() &&
		i.timeRange.Start.UnixNano() == other.timeRange.Start.UnixNano() &&
		i.timeRange.End.UnixNano() == other.timeRange.End.UnixNano() &&
		len(i.entity) == len(other.entity) &&
		bytes.Equal(i.entity.Marshal(), other.entity.Marshal()) &&
		cmp.Equal(i.projectionTagRefs, other.projectionTagRefs) &&
		cmp.Equal(i.schema, other.schema) &&
		cmp.Equal(i.conditionMap, other.conditionMap) &&
		cmp.Equal(i.orderBy, other.orderBy)
}

func ExprToCondition(exprs []Expr) tsdb.Condition {
	cond := make(map[string][]index.ConditionValue)
	for _, expr := range exprs {
		bExpr := expr.(*binaryExpr)
		l := bExpr.l.(*TagRef)
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
