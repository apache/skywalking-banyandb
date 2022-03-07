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
}

func (i *localMeasureIndexScan) Execute(ec executor.MeasureExecutionContext) ([]*measurev1.DataPoint, error) {
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

	var dataPoints []*measurev1.DataPoint

	if len(iters) == 0 {
		return dataPoints, nil
	}

	c := createComparator(modelv1.Sort_SORT_DESC)
	it := NewItemIter(iters, c)
	for it.HasNext() {
		nextItem := it.Next()
		tagFamilies, innerErr := projectItem(ec, nextItem, i.projectionTagsRefs)
		if innerErr != nil {
			return nil, innerErr
		}
		var dpFields []*measurev1.DataPoint_Field
		for _, f := range i.projectionFieldsRefs {
			fieldVal, parserFieldErr := ec.ParseField(f.field.name, nextItem)
			if parserFieldErr != nil {
				return nil, parserFieldErr
			}
			dpFields = append(dpFields, fieldVal)
		}
		dataPoints = append(dataPoints, &measurev1.DataPoint{
			Fields:      dpFields,
			TagFamilies: tagFamilies,
			Timestamp:   timestamppb.New(time.Unix(0, int64(nextItem.Time()))),
		})
	}
	return dataPoints, nil
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

	// TODO: shall we support other sort options?
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
	return i.schema.ProjTags(i.projectionTagsRefs...)
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
		cmp.Equal(i.conditionMap, other.conditionMap)
}

func MeasureIndexScan(startTime, endTime time.Time, metadata *commonv1.Metadata, conditions []Expr, entity tsdb.Entity,
	projectionTags [][]*Tag, projectionFields []*Field) UnresolvedPlan {
	return &unresolvedMeasureIndexScan{
		startTime:        startTime,
		endTime:          endTime,
		metadata:         metadata,
		conditions:       conditions,
		projectionTags:   projectionTags,
		projectionFields: projectionFields,
		entity:           entity,
	}
}
