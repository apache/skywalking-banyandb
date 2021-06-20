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

import "errors"

var (
	UnsupportedLogicalPlanErr = errors.New("unsupported logical plan")
)

type projectionPushDown struct {
}

func NewProjectionPushDown() Optimizer {
	return projectionPushDown{}
}

func (p projectionPushDown) Apply(plan Plan) (Plan, error) {
	return p.pushDown(plan, []*fieldRef{})
}

func (p projectionPushDown) pushDown(plan Plan, acc []*fieldRef) (Plan, error) {
	switch plan.Type() {
	case PlanProjection:
		acc = extractFields(plan.(*projection).fieldRefs, acc)
		input, err := p.pushDown(plan.(*projection).input, acc)
		if err != nil {
			return nil, err
		}
		return &projection{
			input:     input,
			fieldRefs: plan.(*projection).fieldRefs,
		}, nil
	case PlanOrderBy:
		acc = extractFields([]*fieldRef{plan.(*orderBy).targetRef}, acc)
		input, err := p.pushDown(plan.(*orderBy).input, acc)
		if err != nil {
			return nil, err
		}
		return &orderBy{
			input:     input,
			sort:      plan.(*orderBy).sort,
			targetRef: plan.(*orderBy).targetRef,
		}, nil
	case PlanScan:
		scanPlan := plan.(*scan)
		return &scan{
			startTime:           scanPlan.startTime,
			endTime:             scanPlan.endTime,
			projectionFieldRefs: acc,
			schema:              scanPlan.schema,
			metadata:            scanPlan.metadata,
		}, nil
	case PlanSelection:
		selectionPlan := plan.(*selection)
		acc = extractFieldsFromBinaryExpr(selectionPlan.selectExprs, acc)
		input, err := p.pushDown(selectionPlan.input, acc)
		if err != nil {
			return nil, err
		}
		return &selection{
			parent: &parent{
				input: input,
			},
			selectExprs: selectionPlan.selectExprs,
		}, nil
	case PlanLimit:
		limitPlan := plan.(*limit)
		input, err := p.pushDown(limitPlan.input, acc)
		if err != nil {
			return nil, err
		}
		return &limit{
			parent: &parent{
				input: input,
			},
			limitNum: limitPlan.limitNum,
		}, nil
	case PlanOffset:
		offsetPlan := plan.(*offset)
		input, err := p.pushDown(offsetPlan.input, acc)
		if err != nil {
			return nil, err
		}
		return &offset{
			parent: &parent{
				input: input,
			},
			offsetNum: offsetPlan.offsetNum,
		}, nil
	default:
		return nil, UnsupportedLogicalPlanErr
	}
}

func extractFieldsFromBinaryExpr(exprs []Expr, acc []*fieldRef) []*fieldRef {
	for _, expr := range exprs {
		if binary, ok := expr.(*binaryExpr); ok {
			if ref, ok := binary.l.(*fieldRef); ok {
				acc = append(acc, ref)
			}
		}
	}
	return acc
}

func extractFields(fieldRefs []*fieldRef, acc []*fieldRef) []*fieldRef {
	for _, ref := range fieldRefs {
		acc = append(acc, ref)
	}
	return acc
}
