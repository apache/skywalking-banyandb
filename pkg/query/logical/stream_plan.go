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

	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
)

var (
	_ Plan           = (*limit)(nil)
	_ UnresolvedPlan = (*limit)(nil)
)

type parent struct {
	unresolvedInput UnresolvedPlan
	input           Plan
}

type limit struct {
	*parent
	limitNum uint32
}

func (l *limit) Execute(ec executor.StreamExecutionContext) ([]*streamv1.Element, error) {
	entities, err := l.parent.input.(executor.StreamExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}

	if len(entities) > int(l.limitNum) {
		return entities[:l.limitNum], nil
	}

	return entities, nil
}

func (l *limit) Equal(plan Plan) bool {
	if plan.Type() != PlanLimit {
		return false
	}
	other := plan.(*limit)
	if l.limitNum == other.limitNum {
		return l.input.Equal(other.input)
	}
	return false
}

func (l *limit) Analyze(s Schema) (Plan, error) {
	var err error
	l.input, err = l.unresolvedInput.Analyze(s)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *limit) Schema() Schema {
	return l.input.Schema()
}

func (l *limit) String() string {
	return fmt.Sprintf("Limit: %d", l.limitNum)
}

func (l *limit) Children() []Plan {
	return []Plan{l.input}
}

func (l *limit) Type() PlanType {
	return PlanLimit
}

func Limit(input UnresolvedPlan, num uint32) UnresolvedPlan {
	return &limit{
		parent: &parent{
			unresolvedInput: input,
		},
		limitNum: num,
	}
}

var (
	_ Plan           = (*offset)(nil)
	_ UnresolvedPlan = (*offset)(nil)
)

type offset struct {
	*parent
	offsetNum uint32
}

func (l *offset) Execute(ec executor.StreamExecutionContext) ([]*streamv1.Element, error) {
	elements, err := l.parent.input.(executor.StreamExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}

	if len(elements) > int(l.offsetNum) {
		return elements[l.offsetNum:], nil
	}

	return []*streamv1.Element{}, nil
}

func (l *offset) Equal(plan Plan) bool {
	if plan.Type() != PlanOffset {
		return false
	}
	other := plan.(*offset)
	if l.offsetNum == other.offsetNum {
		return l.input.Equal(other.input)
	}
	return false
}

func (l *offset) Analyze(s Schema) (Plan, error) {
	var err error
	l.input, err = l.unresolvedInput.Analyze(s)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *offset) Schema() Schema {
	return l.input.Schema()
}

func (l *offset) String() string {
	return fmt.Sprintf("Offset: %d", l.offsetNum)
}

func (l *offset) Children() []Plan {
	return []Plan{l.input}
}

func (l *offset) Type() PlanType {
	return PlanOffset
}

func Offset(input UnresolvedPlan, num uint32) UnresolvedPlan {
	return &offset{
		parent: &parent{
			unresolvedInput: input,
		},
		offsetNum: num,
	}
}
