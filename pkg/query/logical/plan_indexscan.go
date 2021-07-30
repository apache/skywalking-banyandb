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
	"fmt"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	apiv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/banyand/index"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/pkg/posting"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
)

var _ UnresolvedPlan = (*unresolvedIndexScan)(nil)

type unresolvedIndexScan struct {
	startTime        int64
	endTime          int64
	traceMetadata    *common.Metadata
	conditions       []Expr
	projectionFields []string
	traceState       series.TraceState
}

func (uis *unresolvedIndexScan) Type() PlanType {
	return PlanIndexScan
}

func (uis *unresolvedIndexScan) Analyze(s Schema) (Plan, error) {
	conditionMap := make(map[*apiv1.IndexObject][]Expr)
	for _, cond := range uis.conditions {
		if resolvable, ok := cond.(ResolvableExpr); ok {
			err := resolvable.Resolve(s)
			if err != nil {
				return nil, err
			}

			if bCond, ok := cond.(*binaryExpr); ok {
				fieldName := bCond.l.(*FieldRef).name
				if defined, indexObj := s.IndexDefined(fieldName); defined {
					if v, exist := conditionMap[indexObj]; exist {
						v = append(v, cond)
						conditionMap[indexObj] = v
					} else {
						conditionMap[indexObj] = []Expr{cond}
					}
				} else {
					return nil, errors.Wrap(ErrIndexNotDefined, fieldName)
				}
			}
		}
	}

	var projFieldsRefs []*FieldRef
	if uis.projectionFields != nil && len(uis.projectionFields) > 0 {
		var err error
		projFieldsRefs, err = s.CreateRef(uis.projectionFields...)
		if err != nil {
			return nil, err
		}
	}

	return &indexScan{
		startTime:           uis.startTime,
		endTime:             uis.endTime,
		schema:              s,
		projectionFields:    uis.projectionFields,
		projectionFieldRefs: projFieldsRefs,
		traceMetadata:       uis.traceMetadata,
		conditionMap:        conditionMap,
		traceState:          uis.traceState,
	}, nil
}

var _ Plan = (*indexScan)(nil)

type indexScan struct {
	startTime           int64
	endTime             int64
	schema              Schema
	traceMetadata       *common.Metadata
	conditionMap        map[*apiv1.IndexObject][]Expr
	projectionFields    []string
	projectionFieldRefs []*FieldRef
	traceState          series.TraceState
}

func (i *indexScan) Execute(ec executor.ExecutionContext) ([]data.Entity, error) {
	dataEntities := make([]data.Entity, 0)

	// iterate over shards
	for shardID := uint32(0); shardID < i.schema.ShardNumber(); shardID++ {
		var chunkSet posting.List

		// first collect all chunkIDs via indexes
		for idxObj, exprs := range i.conditionMap {
			// 1) traceSeries Metadata -> IndexObject
			// 2) IndexObject -> Fields
			chunks, err := ec.Search(*i.traceMetadata, uint(shardID), uint64(i.startTime), uint64(i.endTime), idxObj.GetName(), convertToConditions(exprs))
			if err != nil {
				return nil, err
			}
			if chunkSet == nil {
				// chunkSet is nil before the first assignment
				chunkSet = chunks
			} else {
				// afterwards, it must not be nil
				_ = chunkSet.Intersect(chunks)
				// stop loop if chunkSet is empty
				if chunkSet.Len() == 0 {
					continue
				}
			}
		}

		if chunkSet == nil || chunkSet.Len() == 0 {
			continue
		}

		// fetch entities with chunkIDs
		entitiesFromSingleShard, err := ec.FetchEntity(*i.traceMetadata, uint(shardID), chunkSet, series.ScanOptions{
			Projection: i.projectionFields,
			State:      i.traceState,
		})

		if err != nil {
			return nil, err
		}

		// merge results
		dataEntities = append(dataEntities, entitiesFromSingleShard...)
	}

	return dataEntities, nil
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
			i.startTime, i.endTime, i.traceMetadata.Spec.GetGroup(), i.traceMetadata.Spec.GetName(), strings.Join(exprStr, " AND "))
	}
	return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s; projection=%s",
		i.startTime, i.endTime, i.traceMetadata.Spec.GetGroup(), i.traceMetadata.Spec.GetName(), strings.Join(exprStr, " AND "),
		formatExpr(", ", i.projectionFieldRefs...))
}

func (i *indexScan) Type() PlanType {
	return PlanIndexScan
}

func (i *indexScan) Children() []Plan {
	return []Plan{}
}

func (i *indexScan) Schema() Schema {
	// TODO: consider TraceState?
	if i.projectionFieldRefs == nil || len(i.projectionFieldRefs) == 0 {
		return i.schema
	}
	return i.schema.Map(i.projectionFieldRefs...)
}

func (i *indexScan) Equal(plan Plan) bool {
	if plan.Type() != PlanIndexScan {
		return false
	}
	other := plan.(*indexScan)
	return i.startTime == other.startTime &&
		i.endTime == other.endTime &&
		i.traceState != other.traceState &&
		cmp.Equal(i.projectionFieldRefs, other.projectionFieldRefs) &&
		cmp.Equal(i.schema, other.schema) &&
		cmp.Equal(i.traceMetadata, other.traceMetadata) &&
		cmp.Equal(i.conditionMap, other.conditionMap)
}

func IndexScan(startTime, endTime int64, traceMetadata *common.Metadata, conditions []Expr, traceState series.TraceState, projection ...string) UnresolvedPlan {
	return &unresolvedIndexScan{
		startTime:        startTime,
		endTime:          endTime,
		traceMetadata:    traceMetadata,
		conditions:       conditions,
		traceState:       traceState,
		projectionFields: projection,
	}
}

func convertToConditions(exprs []Expr) []index.Condition {
	var conditions []index.Condition
	for _, expr := range exprs {
		if bExpr, ok := expr.(*binaryExpr); ok {
			conditions = append(conditions, index.Condition{
				Key:    bExpr.l.(*FieldRef).name,
				Op:     bExpr.op,
				Values: bExpr.r.(LiteralExpr).Bytes(),
			})
		}
	}
	return conditions
}
