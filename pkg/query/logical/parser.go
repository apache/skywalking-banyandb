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
	"github.com/pkg/errors"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// ParseExprOrEntity parses the condition and returns the literal expression or the entities.
func ParseExprOrEntity(entityDict map[string]int, entity []*modelv1.TagValue, cond *modelv1.Condition) (LiteralExpr, [][]*modelv1.TagValue, error) {
	entityIdx, ok := entityDict[cond.Name]
	if ok && cond.Op != modelv1.Condition_BINARY_OP_EQ && cond.Op != modelv1.Condition_BINARY_OP_IN {
		ok = false
	}
	switch v := cond.Value.Value.(type) {
	case *modelv1.TagValue_Str:
		if ok {
			parsedEntity := make([]*modelv1.TagValue, len(entity))
			copy(parsedEntity, entity)
			parsedEntity[entityIdx] = cond.Value
			return nil, [][]*modelv1.TagValue{parsedEntity}, nil
		}
		return str(v.Str.GetValue()), nil, nil
	case *modelv1.TagValue_StrArray:
		if ok && cond.Op == modelv1.Condition_BINARY_OP_IN {
			entities := make([][]*modelv1.TagValue, len(v.StrArray.Value))
			for i, va := range v.StrArray.Value {
				parsedEntity := make([]*modelv1.TagValue, len(entity))
				copy(parsedEntity, entity)
				parsedEntity[entityIdx] = &modelv1.TagValue{
					Value: &modelv1.TagValue_Str{
						Str: &modelv1.Str{
							Value: va,
						},
					},
				}
				entities[i] = parsedEntity
			}
			return nil, entities, nil
		}
		return newStrArrLiteral(v.StrArray.GetValue()), nil, nil
	case *modelv1.TagValue_Int:
		if ok {
			parsedEntity := make([]*modelv1.TagValue, len(entity))
			copy(parsedEntity, entity)
			parsedEntity[entityIdx] = cond.Value
			return nil, [][]*modelv1.TagValue{parsedEntity}, nil
		}
		return newInt64Literal(v.Int.GetValue()), nil, nil
	case *modelv1.TagValue_IntArray:
		if ok && cond.Op == modelv1.Condition_BINARY_OP_IN {
			entities := make([][]*modelv1.TagValue, len(v.IntArray.Value))
			for i, va := range v.IntArray.Value {
				parsedEntity := make([]*modelv1.TagValue, len(entity))
				copy(parsedEntity, entity)
				parsedEntity[entityIdx] = &modelv1.TagValue{
					Value: &modelv1.TagValue_Int{
						Int: &modelv1.Int{
							Value: va,
						},
					},
				}
				entities[i] = parsedEntity
			}
			return nil, entities, nil
		}
		return newInt64ArrLiteral(v.IntArray.GetValue()), nil, nil
	case *modelv1.TagValue_Null:
		return newNullLiteral(), nil, nil
	}
	return nil, nil, errors.WithMessagef(ErrUnsupportedConditionValue, "index filter parses %v", cond)
}

// ParseEntities merges entities based on the logical operation.
func ParseEntities(op modelv1.LogicalExpression_LogicalOp, input []*modelv1.TagValue, left, right [][]*modelv1.TagValue) [][]*modelv1.TagValue {
	count := len(input)
	result := make([]*modelv1.TagValue, count)
	anyEntity := func(entities [][]*modelv1.TagValue) bool {
		for _, entity := range entities {
			for _, entry := range entity {
				if entry != pbv1.AnyTagValue {
					return false
				}
			}
		}
		return true
	}
	leftAny := anyEntity(left)
	rightAny := anyEntity(right)

	mergedEntities := make([][]*modelv1.TagValue, 0, len(left)+len(right))

	switch op {
	case modelv1.LogicalExpression_LOGICAL_OP_AND:
		if leftAny && !rightAny {
			return right
		}
		if !leftAny && rightAny {
			return left
		}
		mergedEntities = append(mergedEntities, left...)
		mergedEntities = append(mergedEntities, right...)
		for i := 0; i < count; i++ {
			entry := pbv1.AnyTagValue
			for j := 0; j < len(mergedEntities); j++ {
				e := mergedEntities[j][i]
				if e == pbv1.AnyTagValue {
					continue
				}
				if entry == pbv1.AnyTagValue {
					entry = e
				} else if pbv1.MustCompareTagValue(entry, e) != 0 {
					return nil
				}
			}
			result[i] = entry
		}
	case modelv1.LogicalExpression_LOGICAL_OP_OR:
		if leftAny {
			return left
		}
		if rightAny {
			return right
		}
		mergedEntities = append(mergedEntities, left...)
		mergedEntities = append(mergedEntities, right...)
		for i := 0; i < count; i++ {
			entry := pbv1.AnyTagValue
			for j := 0; j < len(mergedEntities); j++ {
				e := mergedEntities[j][i]
				if entry == pbv1.AnyTagValue {
					entry = e
				} else if pbv1.MustCompareTagValue(entry, e) != 0 {
					return mergedEntities
				}
			}
			result[i] = entry
		}
	}
	return [][]*modelv1.TagValue{result}
}
