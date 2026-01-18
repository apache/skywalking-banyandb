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

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

var (
	_ logical.UnresolvedPlan = (*unresolvedGroup)(nil)
	_ logical.Plan           = (*groupBy)(nil)
)

type unresolvedTop struct {
	unresolvedInput logical.UnresolvedPlan
	top             *measurev1.QueryRequest_Top
}

func top(input logical.UnresolvedPlan, top *measurev1.QueryRequest_Top) logical.UnresolvedPlan {
	return &unresolvedTop{
		unresolvedInput: input,
		top:             top,
	}
}

func (gba *unresolvedTop) Analyze(measureSchema logical.Schema) (logical.Plan, error) {
	prevPlan, err := gba.unresolvedInput.Analyze(measureSchema)
	if err != nil {
		return nil, err
	}
	fieldRefs, err := prevPlan.Schema().CreateFieldRef(logical.NewField(gba.top.FieldName))
	if err != nil {
		return nil, err
	}
	if len(fieldRefs) == 0 {
		return nil, errors.Wrap(errFieldNotDefined, "top schema")
	}
	reverted := false
	if gba.top.FieldValueSort == modelv1.Sort_SORT_ASC {
		reverted = true
	}
	return &topOp{
		Parent: &logical.Parent{
			UnresolvedInput: gba.unresolvedInput,
			Input:           prevPlan,
		},
		topNStream: NewTopQueue(int(gba.top.Number), reverted),
		fieldRef:   fieldRefs[0],
	}, nil
}

type topOp struct {
	*logical.Parent
	topNStream *TopQueue
	fieldRef   *logical.FieldRef
}

func (g *topOp) String() string {
	return fmt.Sprintf("%s top %s", g.Input, g.topNStream.String())
}

func (g *topOp) Children() []logical.Plan {
	return []logical.Plan{g.Input}
}

func (g *topOp) Schema() logical.Schema {
	return g.Input.Schema()
}

func (g *topOp) Execute(ec context.Context) (mit executor.MIterator, err error) {
	iter, err := g.Parent.Input.(executor.MeasureExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()
	g.topNStream.Purge()
	for iter.Next() {
		dpp := iter.Current()
		for _, idp := range dpp {
			value := idp.GetDataPoint().GetFields()[g.fieldRef.Spec.FieldIdx].
				GetValue().
				GetInt().
				GetValue()
			g.topNStream.Insert(NewTopElement(idp, value))
		}
	}
	return newTopIterator(g.topNStream.Elements()), nil
}

type topIterator struct {
	elements []TopElement
	index    int
}

func newTopIterator(elements []TopElement) executor.MIterator {
	return &topIterator{
		elements: elements,
		index:    -1,
	}
}

func (ami *topIterator) Next() bool {
	ami.index++
	return ami.index < len(ami.elements)
}

func (ami *topIterator) Current() []*measurev1.InternalDataPoint {
	return []*measurev1.InternalDataPoint{ami.elements[ami.index].idp}
}

func (ami *topIterator) Close() error {
	return nil
}
