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

package bydbql

import (
	"fmt"
	"math"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type measurePattern struct {
	value      *measureExpression
	escape     string
	source     vectorized.ColumnDef
	ignoreCase bool
}

func hasMeasurePattern(expression *GrammarExpression) bool {
	if expression == nil {
		return false
	}
	if expression.Op == "like" || expression.Op == "ilike" {
		return true
	}
	for _, child := range expression.Args {
		if hasMeasurePattern(child) {
			return true
		}
	}
	return false
}

// extractPatterns removes only positive AND leaves. Pattern predicates must
// never reach the row evaluator or be dropped from an OR/NOT expression.
func (m *MeasureTemplate) extractPatterns(expression *GrammarExpression, columns *measureColumns) (*GrammarExpression, error) {
	if expression.Op == "and" && len(expression.Args) == 2 {
		left, err := m.extractPatterns(expression.Args[0], columns)
		if err != nil {
			return nil, err
		}
		right, err := m.extractPatterns(expression.Args[1], columns)
		if err != nil {
			return nil, err
		}
		if left == nil {
			return right, nil
		}
		if right == nil {
			return left, nil
		}
		return &GrammarExpression{Op: "and", Args: []*GrammarExpression{left, right}}, nil
	}
	if expression.Op == "time_compare" {
		temporal := &measurePredicate{temporal: expression.Temporal, hasTime: true}
		if m.predicate == nil {
			m.predicate = temporal
		} else {
			m.predicate = &measurePredicate{left: m.predicate, right: temporal, hasTime: true}
		}
		return nil, nil
	}
	if expression.Op != "like" && expression.Op != "ilike" {
		if hasMeasurePattern(expression) {
			return nil, fmt.Errorf("LIKE and ILIKE require positive source WHERE conjunctions; OR and NOT are unsupported")
		}
		return expression, nil
	}
	if len(m.patterns) >= model.MaxIndexedPatterns {
		return nil, fmt.Errorf("too many LIKE predicates: %w", native.ErrQueryLimit)
	}
	if len(expression.Args) != 2 || expression.Args[0].Op != "column" || expression.Args[0].Identifier == nil {
		return nil, fmt.Errorf("LIKE requires a scalar STRING source tag")
	}
	leaf := expression.Args[0]
	name, err := leaf.Identifier.ToString(leaf.TypeSpec != nil)
	if err != nil {
		return nil, err
	}
	source, err := columns.resolve(name, leaf.TypeSpec)
	if err != nil {
		return nil, err
	}
	if source.Role != vectorized.RoleTag || source.Type != vectorized.ColumnTypeString {
		return nil, fmt.Errorf("LIKE requires a scalar STRING source tag")
	}
	value := expression.Args[1]
	if !value.Param && (value.Op != "literal" || value.Type != ql.ExpressionString && value.Type != ql.ExpressionNull) {
		return nil, fmt.Errorf("LIKE pattern requires a literal string, scalar string parameter or NULL")
	}
	recipe := &measureExpression{value: ql.Expression{Op: "literal", Type: value.Type, Literal: value.Literal}, parameter: value.Param, paramIndex: value.ParamIndex}
	m.patterns = append(m.patterns, measurePattern{value: recipe, source: source, escape: expression.Literal.String, ignoreCase: expression.Op == "ilike"})
	return nil, nil
}

func (m *MeasureTemplate) bindPatterns(bound *BoundQuery, plan *ql.RawPlan) error {
	for _, recipe := range m.patterns {
		value, err := recipe.value.bind(bound)
		if err != nil {
			return err
		}
		if value.Type != ql.ExpressionString && value.Type != ql.ExpressionNull {
			return fmt.Errorf("LIKE pattern requires a scalar STRING or NULL")
		}
		pattern := model.IndexedPattern{
			TagFamily: recipe.source.TagFamily, TagName: recipe.source.Name,
			Pattern: value.Literal.String, Escape: recipe.escape, IgnoreCase: recipe.ignoreCase,
		}
		if err = pattern.Validate(); err != nil {
			return err
		}
		// Keep the source recipe for live metadata validation even when NULL
		// makes this positive WHERE conjunction select no rows.
		plan.Patterns = append(plan.Patterns, pattern)
		plan.Empty = plan.Empty || value.Literal.Null || value.Type == ql.ExpressionNull
	}
	return nil
}

func (m *MeasureTemplate) finishPatternExecution(request *measurev1.QueryRequest, plan *ql.RawPlan) (*MeasureExecution, error) {
	if len(m.patterns) > 0 && plan.Aggregate == nil && !plan.Distinct && plan.Sort != nil {
		return nil, invalidMeasure(fmt.Errorf("raw LIKE/ILIKE ordering requires time or an explicit native index rule"))
	}
	if len(plan.Patterns) > 0 && plan.Filter == nil && !plan.Residual && plan.Aggregate == nil &&
		plan.Expressions == nil && plan.Having == nil && plan.Sort == nil && !plan.Distinct && plan.Limit != nil {
		// Batch consumers receive the native sorted window before its final
		// egress page. Push only that window and retain the QL page so local
		// and distributed batches apply the offset exactly once.
		window := uint64(*plan.Limit) + uint64(plan.Offset)
		if window <= math.MaxUint32 {
			request.Limit, request.Offset = uint32(window), 0
		}
	}
	return &MeasureExecution{Request: request, Plan: plan}, nil
}
