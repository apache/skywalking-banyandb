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
	"fmt"
	"strings"

	"github.com/google/go-cmp/cmp"
)

var _ Plan = (*selection)(nil)
var _ UnresolvedPlan = (*selection)(nil)

// selection defines the field selections as a Logical Plan.
// The Expr(s) contained in the struct must be all satisfied,
// which means they are implicitly combined with logical AND.
type selection struct {
	*parent
	selectExprs []Expr
}

func (s *selection) Equal(plan Plan) bool {
	if plan.Type() != PlanSelection {
		return false
	}
	other := plan.(*selection)
	if cmp.Equal(s.selectExprs, other.selectExprs) {
		return s.input.Equal(other.input)
	}
	return false
}

func (s *selection) Analyze(schema Schema) (Plan, error) {
	var err error
	s.input, err = s.unresolvedInput.Analyze(schema)
	if err != nil {
		return nil, err
	}
	for _, expr := range s.selectExprs {
		if resolvable, ok := expr.(ResolvableExpr); ok {
			err := resolvable.Resolve(s.input)
			if err != nil {
				return nil, err
			}
		}
	}
	return s, nil
}

func (s *selection) Schema() Schema {
	return s.input.Schema()
}

func (s *selection) String() string {
	var exprStr []string
	for _, sp := range s.selectExprs {
		exprStr = append(exprStr, sp.String())
	}
	return fmt.Sprintf("Selection: [%s]", strings.Join(exprStr, " AND "))
}

func (s *selection) Children() []Plan {
	return []Plan{s.input}
}

func (s *selection) Type() PlanType {
	return PlanSelection
}

func Selection(input UnresolvedPlan, expr ...Expr) UnresolvedPlan {
	return &selection{
		parent: &parent{
			unresolvedInput: input,
		},
		selectExprs: expr,
	}
}
