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
//
package logical

import (
	"github.com/pkg/errors"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
)

var (
	_ UnresolvedPlan = (*unresolvedGroup)(nil)
	_ Plan           = (*groupBy)(nil)
)

type unresolvedTop struct {
	unresolvedInput UnresolvedPlan
	top             *measurev1.QueryRequest_Top
}

func Top(input UnresolvedPlan, top *measurev1.QueryRequest_Top) UnresolvedPlan {
	return &unresolvedTop{
		unresolvedInput: input,
		top:             top,
	}
}

func (gba *unresolvedTop) Analyze(measureSchema Schema) (Plan, error) {
	prevPlan, err := gba.unresolvedInput.Analyze(measureSchema)
	if err != nil {
		return nil, err
	}
	fieldRefs, err := prevPlan.Schema().CreateFieldRef(NewField(gba.top.FieldName))
	if err != nil {
		return nil, err
	}
	if len(fieldRefs) == 0 {
		return nil, errors.Wrap(ErrFieldNotDefined, "top schema")
	}
	reverted := false
	if gba.top.FieldValueSort == modelv1.Sort_SORT_ASC {
		reverted = true
	}
	return &top{
		parent: &parent{
			unresolvedInput: gba.unresolvedInput,
			input:           prevPlan,
		},
		topNStream: NewTopQueue(int(gba.top.Number), reverted),
		fieldRef:   fieldRefs[0],
	}, nil
}

type top struct {
	*parent
	topNStream *TopQueue
	fieldRef   *FieldRef
}

func (g *top) String() string {
	return g.topNStream.String()
}

func (g *top) Type() PlanType {
	return PlanTop
}

func (g *top) Equal(plan Plan) bool {
	if plan.Type() != PlanTop {
		return false
	}
	other := plan.(*top)
	return g.topNStream.Equal(other.topNStream)
}

func (g *top) Children() []Plan {
	return []Plan{g.input}
}

func (g *top) Schema() Schema {
	return g.input.Schema()
}

func (g *top) Execute(ec executor.MeasureExecutionContext) (executor.MIterator, error) {
	iter, err := g.parent.input.(executor.MeasureExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}
	g.topNStream.Purge()
	for iter.Next() {
		dpp := iter.Current()
		for _, dp := range dpp {
			value := dp.GetFields()[g.fieldRef.Spec.FieldIdx].
				GetValue().
				GetInt().
				GetValue()
			g.topNStream.Insert(NewTopElement(dp, value))
		}
	}
	return newTopMIterator(g.topNStream.Elements()), nil
}

type topMIterator struct {
	elements []TopElement
	current  []*measurev1.DataPoint
}

func newTopMIterator(elements []TopElement) executor.MIterator {
	return &topMIterator{
		elements: elements,
	}
}

func (ami *topMIterator) Next() bool {
	if ami.current != nil {
		return false
	}
	ami.current = make([]*measurev1.DataPoint, 0, len(ami.elements))
	for _, tn := range ami.elements {
		ami.current = append(ami.current, tn.dp)
	}
	return true
}

func (ami *topMIterator) Current() []*measurev1.DataPoint {
	return ami.current
}

func (ami *topMIterator) Close() error {
	return nil
}
