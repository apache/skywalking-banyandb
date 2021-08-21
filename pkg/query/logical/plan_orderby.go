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
	"sort"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
)

var _ Plan = (*orderBy)(nil)
var _ UnresolvedPlan = (*unresolvedOrderBy)(nil)

type unresolvedOrderBy struct {
	input         UnresolvedPlan
	sort          modelv1.QueryOrder_Sort
	targetLiteral string
}

func (u *unresolvedOrderBy) Type() PlanType {
	return PlanOrderBy
}

func (u *unresolvedOrderBy) Analyze(s Schema) (Plan, error) {
	plan, err := u.input.Analyze(s)
	if err != nil {
		return nil, err
	}
	parentSchema := plan.Schema()
	ref, err := parentSchema.CreateRef(u.targetLiteral)
	if err != nil {
		return nil, err
	}

	return &orderBy{
		input:     plan,
		sort:      u.sort,
		targetRef: ref[0],
	}, nil
}

type orderBy struct {
	input     Plan
	sort      modelv1.QueryOrder_Sort
	targetRef *FieldRef
}

func (o *orderBy) Execute(ec executor.ExecutionContext) ([]data.Entity, error) {
	entities, err := o.input.Execute(ec)
	if err != nil {
		return nil, err
	}

	if len(entities) <= 1 {
		return entities, nil
	}

	sort.Slice(entities, sortMethod(entities, o.targetRef.Spec.Idx, o.sort))

	return entities, nil
}

func (o *orderBy) Equal(plan Plan) bool {
	if plan.Type() != PlanOrderBy {
		return false
	}
	other := plan.(*orderBy)
	if o.sort == other.sort && o.targetRef.Equal(other.targetRef) {
		return o.input.Equal(other.input)
	}
	return false
}

func (o *orderBy) Schema() Schema {
	return o.input.Schema()
}

func (o *orderBy) String() string {
	return fmt.Sprintf("OrderBy: %s, sort=%s", o.targetRef.String(), o.sort.String())
}

func (o *orderBy) Children() []Plan {
	return []Plan{o.input}
}

func (o *orderBy) Type() PlanType {
	return PlanOrderBy
}

func OrderBy(input UnresolvedPlan, targetField string, sort modelv1.QueryOrder_Sort) UnresolvedPlan {
	return &unresolvedOrderBy{
		input:         input,
		sort:          sort,
		targetLiteral: targetField,
	}
}

func getFieldRaw(typedPair *modelv1.TypedPair) ([]byte, error) {
	switch v := typedPair.GetTyped().(type) {
	case *modelv1.TypedPair_StrPair:
		return []byte(v.StrPair.GetValue()), nil
	case *modelv1.TypedPair_IntPair:
		return convert.Int64ToBytes(v.IntPair.GetValue()), nil
	default:
		return nil, errors.New("unsupported data types")
	}
}

// Sorted is used to test whether the given entities are sorted by the sortDirection
// The given entities MUST satisfy both the positive check and the negative check for the reversed direction
func Sorted(entities []data.Entity, fieldIdx int, sortDirection modelv1.QueryOrder_Sort) bool {
	if modelv1.QueryOrder_SORT_UNSPECIFIED == sortDirection {
		return true
	}
	return sort.SliceIsSorted(entities, sortMethod(entities, fieldIdx, sortDirection)) &&
		!sort.SliceIsSorted(entities, sortMethod(entities, fieldIdx, reverseSortDirection(sortDirection)))
}

func reverseSortDirection(sort modelv1.QueryOrder_Sort) modelv1.QueryOrder_Sort {
	if sort == modelv1.QueryOrder_SORT_DESC {
		return modelv1.QueryOrder_SORT_ASC
	}
	return modelv1.QueryOrder_SORT_DESC
}

func sortMethod(entities []data.Entity, fieldIdx int, sortDirection modelv1.QueryOrder_Sort) func(i, j int) bool {
	return func(i, j int) bool {
		iPair := entities[i].GetFields()[fieldIdx]
		jPair := entities[j].GetFields()[fieldIdx]
		lField, _ := getFieldRaw(iPair)
		rField, _ := getFieldRaw(jPair)
		comp := bytes.Compare(lField, rField)
		if sortDirection == modelv1.QueryOrder_SORT_ASC {
			return comp == -1
		}
		return comp == 1
	}
}
