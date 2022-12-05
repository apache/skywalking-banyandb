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
// specific language governing permissions and Limitations
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

// Parent refers to a parent node in the execution tree(plan).
type Parent struct {
	UnresolvedInput UnresolvedPlan
	Input           Plan
}

type limit struct {
	*Parent
	LimitNum uint32
}

func (l *limit) Execute(ec executor.StreamExecutionContext) ([]*streamv1.Element, error) {
	entities, err := l.Parent.Input.(executor.StreamExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}

	if len(entities) > int(l.LimitNum) {
		return entities[:l.LimitNum], nil
	}

	return entities, nil
}

func (l *limit) Analyze(s Schema) (Plan, error) {
	var err error
	l.Input, err = l.UnresolvedInput.Analyze(s)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *limit) Schema() Schema {
	return l.Input.Schema()
}

func (l *limit) String() string {
	return fmt.Sprintf("%s Limit: %d", l.Input.String(), l.LimitNum)
}

func (l *limit) Children() []Plan {
	return []Plan{l.Input}
}

// NewLimit return a limitation expression.
func NewLimit(input UnresolvedPlan, num uint32) UnresolvedPlan {
	return &limit{
		Parent: &Parent{
			UnresolvedInput: input,
		},
		LimitNum: num,
	}
}

var (
	_ Plan           = (*offset)(nil)
	_ UnresolvedPlan = (*offset)(nil)
)

type offset struct {
	*Parent
	offsetNum uint32
}

func (l *offset) Execute(ec executor.StreamExecutionContext) ([]*streamv1.Element, error) {
	elements, err := l.Parent.Input.(executor.StreamExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}

	if len(elements) > int(l.offsetNum) {
		return elements[l.offsetNum:], nil
	}

	return []*streamv1.Element{}, nil
}

func (l *offset) Analyze(s Schema) (Plan, error) {
	var err error
	l.Input, err = l.UnresolvedInput.Analyze(s)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *offset) Schema() Schema {
	return l.Input.Schema()
}

func (l *offset) String() string {
	return fmt.Sprintf("%s Offset: %d", l.Input.String(), l.offsetNum)
}

func (l *offset) Children() []Plan {
	return []Plan{l.Input}
}

// NewOffset returns a offset expression.
func NewOffset(input UnresolvedPlan, num uint32) UnresolvedPlan {
	return &offset{
		Parent: &Parent{
			UnresolvedInput: input,
		},
		offsetNum: num,
	}
}
