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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package bydbql

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

const maxMeasureEntityTuples = 512

var errEmptyMeasureEntities = errors.New("empty measure entity selection")

type measureEntityValue struct {
	text    string
	number  int64
	integer bool
}

func (m *MeasureTemplate) bindMeasureEntities(request *measurev1.QueryRequest, plan *ql.RawPlan) error {
	if plan.Empty || len(m.nativeEntities) == 0 {
		return nil
	}
	normalized, err := normalizeMeasureEntities(request.Criteria, m.nativeEntities)
	if errors.Is(err, errEmptyMeasureEntities) {
		plan.Empty, plan.NativeAggregate = true, nil
		return nil
	}
	if err != nil {
		return err
	}
	if normalized == request.Criteria {
		return nil
	}
	request.Criteria = normalized
	if plan.NativeAggregate != nil {
		for index := range plan.NativeAggregate.Leaves {
			plan.NativeAggregate.Leaves[index].Request.Criteria = proto.Clone(normalized).(*modelv1.Criteria)
		}
	}
	return nil
}

// normalizeMeasureEntities lowers an entity conjunction into alternatives that
// the existing native entity parser can merge without dropping IN values.
func normalizeMeasureEntities(criteria *modelv1.Criteria, entities []vectorized.ColumnDef) (*modelv1.Criteria, error) {
	if criteria == nil || len(entities) == 0 {
		return criteria, nil
	}
	names := make(map[string]bool, len(entities))
	for _, entity := range entities {
		names[entity.Name] = true
	}
	var terms []*modelv1.Criteria
	flattenMeasureEntityAnd(criteria, &terms)
	sets := make(map[string][]measureEntityValue)
	var remaining []*modelv1.Criteria
	constraints := 0
	for _, term := range terms {
		condition := term.GetCondition()
		values, exact := measureEntityValues(condition)
		if condition == nil || !names[condition.Name] || !exact {
			remaining = append(remaining, term)
			continue
		}
		constraints++
		values = distinctMeasureEntityValues(values)
		if previous, found := sets[condition.Name]; found {
			values = intersectMeasureEntityValues(previous, values)
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("%w: constraints for %q have no common value", errEmptyMeasureEntities, condition.Name)
		}
		sets[condition.Name] = values
	}
	multiple := false
	for _, values := range sets {
		multiple = multiple || len(values) > 1
	}
	if constraints == len(sets) && (!multiple || len(sets) < 2) {
		return criteria, nil
	}
	for _, term := range remaining {
		if term.GetLe() != nil && containsMeasureEntity(term, names) {
			return nil, fmt.Errorf("entity IN conjunction cannot be combined with an entity OR predicate")
		}
	}
	if len(sets) == 1 {
		for name, values := range sets {
			return combineMeasureEntityTerms(modelv1.LogicalExpression_LOGICAL_OP_AND,
				append([]*modelv1.Criteria{measureEntityCondition(name, values)}, remaining...)), nil
		}
	}
	tupleCount := 1
	for _, values := range sets {
		if len(values) > maxMeasureEntityTuples/tupleCount {
			return nil, fmt.Errorf("entity conjunction expands beyond %d tuples", maxMeasureEntityTuples)
		}
		tupleCount *= len(values)
	}
	tuples := []*modelv1.Criteria{nil}
	for _, entity := range entities {
		values, constrained := sets[entity.Name]
		if !constrained {
			continue
		}
		next := make([]*modelv1.Criteria, 0, len(tuples)*len(values))
		for _, tuple := range tuples {
			for _, value := range values {
				next = append(next, combineMeasureCriteria(tuple, measureEntityCondition(entity.Name, []measureEntityValue{value}), false))
			}
		}
		tuples = next
	}
	assignments := combineMeasureEntityTerms(modelv1.LogicalExpression_LOGICAL_OP_OR, tuples)
	return combineMeasureEntityTerms(modelv1.LogicalExpression_LOGICAL_OP_AND, append([]*modelv1.Criteria{assignments}, remaining...)), nil
}

func flattenMeasureEntityAnd(criteria *modelv1.Criteria, terms *[]*modelv1.Criteria) {
	if expression := criteria.GetLe(); expression != nil && expression.Op == modelv1.LogicalExpression_LOGICAL_OP_AND {
		flattenMeasureEntityAnd(expression.Left, terms)
		flattenMeasureEntityAnd(expression.Right, terms)
	} else if criteria != nil {
		*terms = append(*terms, criteria)
	}
}

func measureEntityValues(condition *modelv1.Condition) ([]measureEntityValue, bool) {
	if condition == nil || condition.Value == nil {
		return nil, false
	}
	switch condition.Op {
	case modelv1.Condition_BINARY_OP_EQ:
		switch value := condition.Value.Value.(type) {
		case *modelv1.TagValue_Str:
			if value.Str != nil {
				return []measureEntityValue{{text: value.Str.Value}}, true
			}
		case *modelv1.TagValue_Int:
			if value.Int != nil {
				return []measureEntityValue{{number: value.Int.Value, integer: true}}, true
			}
		default:
		}
	case modelv1.Condition_BINARY_OP_IN:
		switch value := condition.Value.Value.(type) {
		case *modelv1.TagValue_StrArray:
			if value.StrArray != nil {
				values := make([]measureEntityValue, 0, len(value.StrArray.Value))
				for _, text := range value.StrArray.Value {
					values = append(values, measureEntityValue{text: text})
				}
				return values, true
			}
		case *modelv1.TagValue_IntArray:
			if value.IntArray != nil {
				values := make([]measureEntityValue, 0, len(value.IntArray.Value))
				for _, number := range value.IntArray.Value {
					values = append(values, measureEntityValue{number: number, integer: true})
				}
				return values, true
			}
		default:
		}
	default:
	}
	return nil, false
}

func distinctMeasureEntityValues(values []measureEntityValue) []measureEntityValue {
	seen := make(map[measureEntityValue]bool, len(values))
	result := values[:0]
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}

func intersectMeasureEntityValues(left, right []measureEntityValue) []measureEntityValue {
	wanted := make(map[measureEntityValue]bool, len(right))
	for _, value := range right {
		wanted[value] = true
	}
	result := left[:0]
	for _, value := range left {
		if wanted[value] {
			result = append(result, value)
		}
	}
	return result
}

func measureEntityCondition(name string, values []measureEntityValue) *modelv1.Criteria {
	condition := &modelv1.Condition{Name: name, Op: modelv1.Condition_BINARY_OP_EQ, Value: &modelv1.TagValue{}}
	if len(values) == 1 {
		if values[0].integer {
			condition.Value.Value = &modelv1.TagValue_Int{Int: &modelv1.Int{Value: values[0].number}}
		} else {
			condition.Value.Value = &modelv1.TagValue_Str{Str: &modelv1.Str{Value: values[0].text}}
		}
	} else {
		condition.Op = modelv1.Condition_BINARY_OP_IN
		if values[0].integer {
			numbers := make([]int64, 0, len(values))
			for _, value := range values {
				numbers = append(numbers, value.number)
			}
			condition.Value.Value = &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: numbers}}
		} else {
			texts := make([]string, 0, len(values))
			for _, value := range values {
				texts = append(texts, value.text)
			}
			condition.Value.Value = &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: texts}}
		}
	}
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: condition}}
}

func combineMeasureEntityTerms(op modelv1.LogicalExpression_LogicalOp, terms []*modelv1.Criteria) *modelv1.Criteria {
	if len(terms) == 0 {
		return nil
	}
	if len(terms) == 1 {
		return terms[0]
	}
	middle := len(terms) / 2
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{
		Op: op, Left: combineMeasureEntityTerms(op, terms[:middle]), Right: combineMeasureEntityTerms(op, terms[middle:]),
	}}}
}

func containsMeasureEntity(criteria *modelv1.Criteria, names map[string]bool) bool {
	if condition := criteria.GetCondition(); condition != nil {
		return names[condition.Name]
	}
	if expression := criteria.GetLe(); expression != nil {
		return containsMeasureEntity(expression.Left, names) || containsMeasureEntity(expression.Right, names)
	}
	return false
}
