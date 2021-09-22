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
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"

	commonv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/query/v2/executor"
)

var _ Plan = (*tableScan)(nil)
var _ UnresolvedPlan = (*unresolvedTableScan)(nil)

type unresolvedTableScan struct {
	*unresolvedOrderBy
	startTime        time.Time
	endTime          time.Time
	projectionFields [][]*Tag
	metadata         *commonv2.Metadata
	entity           tsdb.Entity
}

func (u *unresolvedTableScan) Type() PlanType {
	return PlanTableScan
}

func (u *unresolvedTableScan) Analyze(schema Schema) (Plan, error) {
	if schema == nil {
		return nil, errors.Wrap(ErrInvalidSchema, "nil")
	}

	if u.projectionFields == nil || len(u.projectionFields) == 0 {
		// directly resolve sub-plan if no projection exists
		orderBySubPlan, err := u.unresolvedOrderBy.Analyze(schema)

		if err != nil {
			return nil, err
		}

		return &tableScan{
			timeRange: tsdb.NewTimeRange(u.startTime, u.endTime),
			schema:    schema,
			metadata:  u.metadata,
			entity:    u.entity,
			orderBy:   orderBySubPlan,
		}, nil
	}

	fieldRefs, err := schema.CreateRef(u.projectionFields...)
	if err != nil {
		return nil, err
	}

	// resolve sub-plan with the projected view of schema
	orderBySubPlan, err := u.unresolvedOrderBy.Analyze(schema.Proj(fieldRefs...))

	if err != nil {
		return nil, err
	}

	return &tableScan{
		orderBy:             orderBySubPlan,
		timeRange:           tsdb.NewTimeRange(u.startTime, u.endTime),
		projectionFieldRefs: fieldRefs,
		schema:              schema,
		metadata:            u.metadata,
		entity:              u.entity,
	}, nil
}

type tableScan struct {
	// use orderBy as a sub-plan
	*orderBy
	timeRange           tsdb.TimeRange
	projectionFieldRefs [][]*FieldRef
	schema              Schema
	metadata            *commonv2.Metadata
	entity              tsdb.Entity
}

func (s *tableScan) Execute(ec executor.ExecutionContext) ([]*streamv2.Element, error) {
	var elements [][]*streamv2.Element
	shards, err := ec.Shards(s.entity)
	if err != nil {
		return nil, err
	}
	for _, shard := range shards {
		elementsInShard, err := s.executeForShard(ec, shard)
		if err != nil {
			return nil, err
		}
		elements = append(elements, elementsInShard...)
	}
	var c comparator
	if s.index == nil {
		c = createTimestampComparator(s.sort)
	} else {
		c = createMultiTagsComparator(s.fieldRefs, s.sort)
	}
	return mergeSort(elements, c), nil
}

func (s *tableScan) executeForShard(ec executor.ExecutionContext, shard tsdb.Shard) ([][]*streamv2.Element, error) {
	seriesList, err := shard.Series().List(tsdb.NewPath(s.entity))
	if err != nil {
		return nil, err
	}
	var orderByFilter seekerBuilder
	if s.index != nil {
		orderByFilter = func(builder tsdb.SeekerBuilder) {
			builder.OrderByIndex(s.index, s.sort)
		}
	} else {
		orderByFilter = func(builder tsdb.SeekerBuilder) {
			builder.OrderByTime(s.sort)
		}
	}

	return executeForShard(ec, seriesList, s.timeRange, s.projectionFieldRefs, orderByFilter)
}

func (s *tableScan) Equal(plan Plan) bool {
	if plan.Type() != PlanTableScan {
		return false
	}
	other := plan.(*tableScan)
	return s.timeRange.Start.UnixNano() == other.timeRange.Start.UnixNano() &&
		s.timeRange.End.UnixNano() == other.timeRange.End.UnixNano() &&
		s.metadata.GetGroup() == other.metadata.GetGroup() &&
		s.metadata.GetName() == other.metadata.GetName() &&
		cmp.Equal(s.projectionFieldRefs, other.projectionFieldRefs) &&
		cmp.Equal(s.schema, other.schema) &&
		cmp.Equal(s.orderBy, other.orderBy) &&
		bytes.Equal(s.entity.Marshal(), other.entity.Marshal())
}

func (s *tableScan) Schema() Schema {
	if s.projectionFieldRefs == nil || len(s.projectionFieldRefs) == 0 {
		return s.schema
	}
	return s.schema.Proj(s.projectionFieldRefs...)
}

func (s *tableScan) String() string {
	if len(s.projectionFieldRefs) == 0 {
		return fmt.Sprintf("TableScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s}; entity=%v; projection=None",
			s.timeRange.Start.Unix(), s.timeRange.End.Unix(), s.metadata.GetGroup(), s.metadata.GetName(), s.entity)
	}
	return fmt.Sprintf("TableScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s}; entity=%v; projection=%s",
		s.timeRange.Start.Unix(), s.timeRange.End.Unix(), s.metadata.GetGroup(), s.metadata.GetName(),
		s.entity, formatExpr(", ", s.projectionFieldRefs...))
}

func (s *tableScan) Children() []Plan {
	return []Plan{}
}

func (s *tableScan) Type() PlanType {
	return PlanTableScan
}

func TableScan(startTime, endTime time.Time, metadata *commonv2.Metadata, entity tsdb.Entity, orderBy *unresolvedOrderBy,
	projection ...[]*Tag) UnresolvedPlan {
	return &unresolvedTableScan{
		unresolvedOrderBy: orderBy,
		startTime:         startTime,
		endTime:           endTime,
		projectionFields:  projection,
		metadata:          metadata,
		entity:            entity,
	}
}
