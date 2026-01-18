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

package measure

import (
	"context"
	"fmt"
	"math"

	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

var (
	_ logical.UnresolvedPlan = (*unresolvedGroup)(nil)
	_ logical.Plan           = (*groupBy)(nil)
)

type unresolvedGroup struct {
	unresolvedInput logical.UnresolvedPlan
	// groupBy should be a subset of tag projection
	groupBy       [][]*logical.Tag
	groupByEntity bool
}

func newUnresolvedGroupBy(input logical.UnresolvedPlan, groupBy [][]*logical.Tag, groupByEntity bool) logical.UnresolvedPlan {
	return &unresolvedGroup{
		unresolvedInput: input,
		groupBy:         groupBy,
		groupByEntity:   groupByEntity,
	}
}

func (gba *unresolvedGroup) Analyze(measureSchema logical.Schema) (logical.Plan, error) {
	prevPlan, err := gba.unresolvedInput.Analyze(measureSchema)
	if err != nil {
		return nil, err
	}
	// check validity of groupBy tags
	schema := prevPlan.Schema()
	groupByTagRefs, err := schema.CreateTagRef(gba.groupBy...)
	if err != nil {
		return nil, err
	}
	if len(groupByTagRefs) == 0 {
		return nil, errors.Wrap(logical.ErrTagNotDefined, "groupBy schema")
	}
	return &groupBy{
		Parent: &logical.Parent{
			UnresolvedInput: gba.unresolvedInput,
			Input:           prevPlan,
		},
		schema:          schema,
		groupByTagsRefs: groupByTagRefs,
		groupByEntity:   gba.groupByEntity,
	}, nil
}

type groupBy struct {
	*logical.Parent
	schema          logical.Schema
	groupByTagsRefs [][]*logical.TagRef
	groupByEntity   bool
}

func (g *groupBy) String() string {
	var method string
	if g.groupByEntity {
		method = "sort"
	} else {
		method = "hash"
	}
	return fmt.Sprintf("%s GroupBy: groupBy=%s, method=%s",
		g.Input,
		logical.FormatTagRefs(", ", g.groupByTagsRefs...), method)
}

func (g *groupBy) Children() []logical.Plan {
	return []logical.Plan{g.Input}
}

func (g *groupBy) Schema() logical.Schema {
	return g.schema.ProjTags(g.groupByTagsRefs...)
}

func (g *groupBy) Execute(ec context.Context) (executor.MIterator, error) {
	if g.groupByEntity {
		return g.sort(ec)
	}
	return g.hash(ec)
}

func (g *groupBy) sort(ec context.Context) (executor.MIterator, error) {
	iter, err := g.Parent.Input.(executor.MeasureExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}
	return newGroupSortIterator(iter, g.groupByTagsRefs), nil
}

func (g *groupBy) hash(ec context.Context) (mit executor.MIterator, err error) {
	iter, err := g.Parent.Input.(executor.MeasureExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()

	groupMap := make(map[uint64][]*measurev1.InternalDataPoint)
	groupLst := make([]uint64, 0)
	for iter.Next() {
		dataPoints := iter.Current()
		for _, idp := range dataPoints {
			key, innerErr := formatGroupByKey(idp.GetDataPoint(), g.groupByTagsRefs)
			if innerErr != nil {
				return nil, innerErr
			}
			group, ok := groupMap[key]
			if !ok {
				group = make([]*measurev1.InternalDataPoint, 0)
				groupLst = append(groupLst, key)
			}
			if group == nil {
				return nil, errors.New("aggregation op does not exist")
			}
			group = append(group, idp)
			groupMap[key] = group
		}
	}
	return newGroupIterator(groupMap, groupLst), nil
}

func formatGroupByKey(point *measurev1.DataPoint, groupByTagsRefs [][]*logical.TagRef) (uint64, error) {
	hash := xxhash.New()
	for _, tagFamilyRef := range groupByTagsRefs {
		for _, tagRef := range tagFamilyRef {
			if tagRef.Spec.TagFamilyIdx >= len(point.GetTagFamilies()) {
				return 0, errors.New("tag family index out of range")
			}
			if tagRef.Spec.TagIdx >= len(point.GetTagFamilies()[tagRef.Spec.TagFamilyIdx].GetTags()) {
				return 0, errors.New("tag index out of range")
			}
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

type groupIterator struct {
	groupMap map[uint64][]*measurev1.InternalDataPoint
	groupLst []uint64
	index    int
}

func newGroupIterator(groupedMap map[uint64][]*measurev1.InternalDataPoint, groupLst []uint64) executor.MIterator {
	return &groupIterator{
		groupMap: groupedMap,
		groupLst: groupLst,
		index:    -1,
	}
}

func (gmi *groupIterator) Next() bool {
	if gmi.index >= (len(gmi.groupLst) - 1) {
		return false
	}
	gmi.index++
	return true
}

func (gmi *groupIterator) Current() []*measurev1.InternalDataPoint {
	key := gmi.groupLst[gmi.index]
	return gmi.groupMap[key]
}

func (gmi *groupIterator) Close() error {
	gmi.index = math.MaxInt
	return nil
}

type groupSortIterator struct {
	iter            executor.MIterator
	err             error
	cdp             *measurev1.InternalDataPoint
	groupByTagsRefs [][]*logical.TagRef
	current         []*measurev1.InternalDataPoint
	index           int
	key             uint64
	closed          bool
}

func newGroupSortIterator(iter executor.MIterator, groupByTagsRefs [][]*logical.TagRef) executor.MIterator {
	return &groupSortIterator{
		groupByTagsRefs: groupByTagsRefs,
		iter:            iter,
		index:           -1,
	}
}

func (gmi *groupSortIterator) Next() bool {
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
		idp, ok := gmi.nextDP()
		if !ok {
			gmi.closed = true
			return len(gmi.current) > 0
		}
		k, err := formatGroupByKey(idp.GetDataPoint(), gmi.groupByTagsRefs)
		if err != nil {
			gmi.closed = true
			gmi.err = err
			return false
		}
		if gmi.key == 0 {
			gmi.key = k
		}
		if gmi.key != k {
			gmi.cdp = idp
			gmi.key = k
			return true
		}
		gmi.current = append(gmi.current, idp)
	}
}

func (gmi *groupSortIterator) Current() []*measurev1.InternalDataPoint {
	return gmi.current
}

func (gmi *groupSortIterator) Close() error {
	gmi.closed = true
	return multierr.Combine(gmi.err, gmi.iter.Close())
}

func (gmi *groupSortIterator) nextDP() (*measurev1.InternalDataPoint, bool) {
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
