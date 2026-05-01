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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

var (
	_ logical.UnresolvedPlan = (*unresolvedGroup)(nil)
	_ logical.Plan           = (*topOp[int64])(nil)
	_ logical.Plan           = (*topOp[float64])(nil)
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
	fieldRef := fieldRefs[0]
	switch fieldRef.Spec.Spec.FieldType {
	case databasev1.FieldType_FIELD_TYPE_INT:
		return newTopOp[int64](gba, prevPlan, fieldRef, reverted), nil
	case databasev1.FieldType_FIELD_TYPE_FLOAT:
		return newTopOp[float64](gba, prevPlan, fieldRef, reverted), nil
	default:
		return nil, errors.WithMessagef(errUnsupportedAggregationField,
			"top: field %s has unsupported type %s", fieldRef.Spec.Spec.GetName(), fieldRef.Spec.Spec.GetFieldType().String())
	}
}

func newTopOp[K measure.TopSortKey](gba *unresolvedTop, prevPlan logical.Plan, fieldRef *logical.FieldRef, reverted bool) *topOp[K] {
	return &topOp[K]{
		Parent: &logical.Parent{
			UnresolvedInput: gba.unresolvedInput,
			Input:           prevPlan,
		},
		topNStream: NewTopQueue[K](int(gba.top.Number), reverted),
		fieldRef:   fieldRef,
	}
}

type topOp[K measure.TopSortKey] struct {
	*logical.Parent
	topNStream *TopQueue[K]
	fieldRef   *logical.FieldRef
}

func (g *topOp[K]) String() string {
	return fmt.Sprintf("%s top %s", g.Input, g.topNStream.String())
}

func (g *topOp[K]) Children() []logical.Plan {
	return []logical.Plan{g.Input}
}

func (g *topOp[K]) Schema() logical.Schema {
	return g.Input.Schema()
}

func (g *topOp[K]) Execute(ec context.Context) (mit executor.MIterator, err error) {
	iter, err := g.Parent.Input.(executor.MeasureExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()
	g.topNStream.Purge()
	var zero K
	var extractValue func(idp *measurev1.InternalDataPoint) K
	switch any(zero).(type) {
	case float64:
		extractValue = func(idp *measurev1.InternalDataPoint) K {
			return K(idp.GetDataPoint().GetFields()[g.fieldRef.Spec.FieldIdx].
				GetValue().GetFloat().GetValue())
		}
	default:
		extractValue = func(idp *measurev1.InternalDataPoint) K {
			return K(idp.GetDataPoint().GetFields()[g.fieldRef.Spec.FieldIdx].
				GetValue().GetInt().GetValue())
		}
	}
	for iter.Next() {
		dpp := iter.Current()
		for _, idp := range dpp {
			g.topNStream.Insert(NewTopElement[K](idp, extractValue(idp)))
		}
	}
	return newTopIterator[K](g.topNStream.Elements()), nil
}

type topIterator[K measure.TopSortKey] struct {
	elements []TopElement[K]
	index    int
}

func newTopIterator[K measure.TopSortKey](elements []TopElement[K]) executor.MIterator {
	return &topIterator[K]{
		elements: elements,
		index:    -1,
	}
}

func (ami *topIterator[K]) Next() bool {
	ami.index++
	return ami.index < len(ami.elements)
}

func (ami *topIterator[K]) Current() []*measurev1.InternalDataPoint {
	return []*measurev1.InternalDataPoint{ami.elements[ami.index].idp}
}

func (ami *topIterator[K]) Close() error {
	return nil
}
