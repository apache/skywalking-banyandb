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

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/data"
	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/executor"
)

var _ Plan = (*orderBy)(nil)
var _ UnresolvedPlan = (*unresolvedOrderBy)(nil)

type unresolvedOrderBy struct {
	input         UnresolvedPlan
	sort          apiv1.Sort
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
	sort      apiv1.Sort
	targetRef *fieldRef
}

func (o *orderBy) Execute(ec executor.ExecutionContext) ([]data.Entity, error) {
	entities, err := o.input.Execute(ec)
	if err != nil {
		return nil, err
	}

	if len(entities) <= 1 {
		return entities, nil
	}

	sort.Slice(entities, sortMethod(entities, o.targetRef.spec.idx, o.sort))

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
	return fmt.Sprintf("OrderBy: %s, sort=%s", o.targetRef.String(), apiv1.EnumNamesSort[o.sort])
}

func (o *orderBy) Children() []Plan {
	return []Plan{o.input}
}

func (o *orderBy) Type() PlanType {
	return PlanOrderBy
}

func OrderBy(input UnresolvedPlan, targetField string, sort apiv1.Sort) UnresolvedPlan {
	return &unresolvedOrderBy{
		input:         input,
		sort:          sort,
		targetLiteral: targetField,
	}
}

func getFieldRaw(pair *apiv1.Pair) ([]byte, error) {
	unionPair := new(flatbuffers.Table)
	if ok := pair.Pair(unionPair); !ok {
		return nil, errors.New("cannot read from pair")
	}
	if pair.PairType() == apiv1.TypedPairStrPair {
		unionStrPairQuery := new(apiv1.StrPair)
		unionStrPairQuery.Init(unionPair.Bytes, unionPair.Pos)
		return unionStrPairQuery.Values(0), nil
	} else if pair.PairType() == apiv1.TypedPairIntPair {
		unionIntPairQuery := new(apiv1.IntPair)
		unionIntPairQuery.Init(unionPair.Bytes, unionPair.Pos)
		return convert.Int64ToBytes(unionIntPairQuery.Values(0)), nil
	} else {
		return nil, errors.New("unsupported data types")
	}
}

func Sorted(entities []data.Entity, fieldIdx int, sortDirection apiv1.Sort) bool {
	return sort.SliceIsSorted(entities, sortMethod(entities, fieldIdx, sortDirection))
}

func sortMethod(entities []data.Entity, fieldIdx int, sortDirection apiv1.Sort) func(i, j int) bool {
	return func(i, j int) bool {
		var iPair, jPair apiv1.Pair
		entities[i].Fields(&iPair, fieldIdx)
		entities[j].Fields(&jPair, fieldIdx)
		lField, _ := getFieldRaw(&iPair)
		rField, _ := getFieldRaw(&jPair)
		comp := bytes.Compare(lField, rField)
		if sortDirection == apiv1.SortASC {
			return comp == -1
		} else {
			return comp == 1
		}
	}
}
