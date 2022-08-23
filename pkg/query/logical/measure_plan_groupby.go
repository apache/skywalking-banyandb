// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"math"

	"github.com/cespare/xxhash"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
)

var (
	_ UnresolvedPlan = (*unresolvedGroup)(nil)
	_ Plan           = (*groupBy)(nil)
)

type unresolvedGroup struct {
	unresolvedInput UnresolvedPlan
	// groupBy should be a subset of tag projection
	groupBy       [][]*Tag
	groupByEntity bool
}

func GroupBy(input UnresolvedPlan, groupBy [][]*Tag, groupByEntity bool) UnresolvedPlan {
	return &unresolvedGroup{
		unresolvedInput: input,
		groupBy:         groupBy,
		groupByEntity:   groupByEntity,
	}
}

func (gba *unresolvedGroup) Analyze(measureSchema Schema) (Plan, error) {
	prevPlan, err := gba.unresolvedInput.Analyze(measureSchema)
	if err != nil {
		return nil, err
	}
	// check validity of groupBy tags
	groupByTagRefs, err := prevPlan.Schema().CreateTagRef(gba.groupBy...)
	if err != nil {
		return nil, err
	}
	return &groupBy{
		parent: &parent{
			unresolvedInput: gba.unresolvedInput,
			input:           prevPlan,
		},
		schema:          measureSchema,
		groupByTagsRefs: groupByTagRefs,
		groupByEntity:   gba.groupByEntity,
	}, nil
}

type groupBy struct {
	*parent
	schema          Schema
	groupByTagsRefs [][]*TagRef
	groupByEntity   bool
}

func (g *groupBy) String() string {
	var method string
	if g.groupByEntity {
		method = "sort"
	} else {
		method = "hash"
	}
	return fmt.Sprintf("GroupBy: groupBy=%s, method=%s",
		formatTagRefs(", ", g.groupByTagsRefs...), method)
}

func (g *groupBy) Type() PlanType {
	return PlanGroupBy
}

func (g *groupBy) Equal(plan Plan) bool {
	if plan.Type() != PlanGroupBy {
		return false
	}
	other := plan.(*groupBy)
	if cmp.Equal(g.groupByTagsRefs, other.groupByTagsRefs) &&
		g.groupByEntity == other.groupByEntity {
		return g.parent.input.Equal(other.parent.input)
	}

	return false
}

func (g *groupBy) Children() []Plan {
	return []Plan{g.input}
}

func (g *groupBy) Schema() Schema {
	return g.schema.ProjTags(g.groupByTagsRefs...)
}

func (g *groupBy) Execute(ec executor.MeasureExecutionContext) (executor.MIterator, error) {
	if g.groupByEntity {
		return g.sort(ec)
	}
	return g.hash(ec)
}

func (g *groupBy) sort(ec executor.MeasureExecutionContext) (executor.MIterator, error) {
	iter, err := g.parent.input.(executor.MeasureExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}
	return newGroupSortMIterator(iter, g.groupByTagsRefs), nil
}

func (g *groupBy) hash(ec executor.MeasureExecutionContext) (executor.MIterator, error) {
	iter, err := g.parent.input.(executor.MeasureExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}
	groupMap := make(map[uint64][]*measurev1.DataPoint)
	groupLst := make([]uint64, 0)
	for iter.Next() {
		dataPoints := iter.Current()
		for _, dp := range dataPoints {
			key, innerErr := formatGroupByKey(dp, g.groupByTagsRefs)
			if innerErr != nil {
				return nil, innerErr
			}
			group, ok := groupMap[key]
			if !ok {
				group = make([]*measurev1.DataPoint, 0)
				groupLst = append(groupLst, key)
			}
			if group == nil {
				return nil, errors.New("aggregation op does not exist")
			}
			group = append(group, dp)
			groupMap[key] = group
		}
	}
	if err = iter.Close(); err != nil {
		return nil, err
	}
	return newGroupMIterator(groupMap, groupLst), nil
}

func formatGroupByKey(point *measurev1.DataPoint, groupByTagsRefs [][]*TagRef) (uint64, error) {
	hash := xxhash.New()
	for _, tagFamilyRef := range groupByTagsRefs {
		for _, tagRef := range tagFamilyRef {
			tag := point.GetTagFamilies()[tagRef.Spec.TagFamilyIdx].GetTags()[tagRef.Spec.TagIdx]
			switch v := tag.GetValue().GetValue().(type) {
			case *modelv1.TagValue_Str:
				_, innerErr := hash.Write([]byte(v.Str.GetValue()))
				if innerErr != nil {
					return 0, innerErr
				}
			case *modelv1.TagValue_Int:
				_, innerErr := hash.Write(convert.Int64ToBytes(v.Int.GetValue()))
				if innerErr != nil {
					return 0, innerErr
				}
			case *modelv1.TagValue_IntArray, *modelv1.TagValue_StrArray, *modelv1.TagValue_BinaryData:
				return 0, errors.New("group-by on array/binary tag is not supported")
			}
		}
	}
	return hash.Sum64(), nil
}

type groupMIterator struct {
	groupMap map[uint64][]*measurev1.DataPoint
	groupLst []uint64
	index    int
}

func newGroupMIterator(groupedMap map[uint64][]*measurev1.DataPoint, groupLst []uint64) executor.MIterator {
	return &groupMIterator{
		groupMap: groupedMap,
		groupLst: groupLst,
		index:    -1,
	}
}

func (gmi *groupMIterator) Next() bool {
	if gmi.index >= (len(gmi.groupLst) - 1) {
		return false
	}
	gmi.index++
	return true
}

func (gmi *groupMIterator) Current() []*measurev1.DataPoint {
	key := gmi.groupLst[gmi.index]
	return gmi.groupMap[key]
}

func (gmi *groupMIterator) Close() error {
	gmi.index = math.MaxInt
	return nil
}

type groupSortMIterator struct {
	groupByTagsRefs [][]*TagRef
	iter            executor.MIterator
	index           int

	current []*measurev1.DataPoint
	cdp     *measurev1.DataPoint
	key     uint64
	closed  bool
	err     error
}

func newGroupSortMIterator(iter executor.MIterator, groupByTagsRefs [][]*TagRef) executor.MIterator {
	return &groupSortMIterator{
		groupByTagsRefs: groupByTagsRefs,
		iter:            iter,
		index:           -1,
	}
}

func (gmi *groupSortMIterator) Next() bool {
	if gmi.closed {
		return false
	}
	if gmi.current != nil {
		gmi.current = gmi.current[:0]
	}
	if gmi.cdp != nil {
		gmi.current = append(gmi.current, gmi.cdp)
	}
	for {
		dp, ok := gmi.nextDP()
		if !ok {
			gmi.closed = true
			return len(gmi.current) > 0
		}
		k, err := formatGroupByKey(dp, gmi.groupByTagsRefs)
		if err != nil {
			gmi.closed = true
			gmi.err = err
			return false
		}
		if gmi.key == 0 {
			gmi.key = k
		}
		if gmi.key != k {
			gmi.cdp = dp
			gmi.key = k
			return true
		}
		gmi.current = append(gmi.current, dp)
	}
}

func (gmi *groupSortMIterator) Current() []*measurev1.DataPoint {
	return gmi.current
}

func (gmi *groupSortMIterator) Close() error {
	gmi.closed = true
	return gmi.err
}

func (gmi *groupSortMIterator) nextDP() (*measurev1.DataPoint, bool) {
	if gmi.index < 0 {
		if ok := gmi.iter.Next(); !ok {
			return nil, false
		}
		gmi.index = 0
	} else {
		gmi.index++
	}
	current := gmi.iter.Current()
	if len(current) < 1 || gmi.index >= len(current) {
		gmi.index = -1
		return gmi.nextDP()
	}
	return current[gmi.index], true
}
