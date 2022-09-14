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
	_ Plan           = (*Limit)(nil)
	_ UnresolvedPlan = (*Limit)(nil)
)

type Parent struct {
	UnresolvedInput UnresolvedPlan
	Input           Plan
}

type Limit struct {
	*Parent
	LimitNum uint32
}

func (l *Limit) Execute(ec executor.StreamExecutionContext) ([]*streamv1.Element, error) {
	entities, err := l.Parent.Input.(executor.StreamExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}

	if len(entities) > int(l.LimitNum) {
		return entities[:l.LimitNum], nil
	}

	return entities, nil
}

func (l *Limit) Analyze(s Schema) (Plan, error) {
	var err error
	l.Input, err = l.UnresolvedInput.Analyze(s)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Limit) Schema() Schema {
	return l.Input.Schema()
}

func (l *Limit) String() string {
	return fmt.Sprintf("%s Limit: %d", l.Input.String(), l.LimitNum)
}

func (l *Limit) Children() []Plan {
	return []Plan{l.Input}
}

func NewLimit(input UnresolvedPlan, num uint32) UnresolvedPlan {
	return &Limit{
		Parent: &Parent{
			UnresolvedInput: input,
		},
		LimitNum: num,
	}
}

var (
	_ Plan           = (*Offset)(nil)
	_ UnresolvedPlan = (*Offset)(nil)
)

type Offset struct {
	*Parent
	offsetNum uint32
}

func (l *Offset) Execute(ec executor.StreamExecutionContext) ([]*streamv1.Element, error) {
	elements, err := l.Parent.Input.(executor.StreamExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}

	if len(elements) > int(l.offsetNum) {
		return elements[l.offsetNum:], nil
	}

	return []*streamv1.Element{}, nil
}

func (l *Offset) Analyze(s Schema) (Plan, error) {
	var err error
	l.Input, err = l.UnresolvedInput.Analyze(s)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Offset) Schema() Schema {
	return l.Input.Schema()
}

func (l *Offset) String() string {
	return fmt.Sprintf("%s Offset: %d", l.Input.String(), l.offsetNum)
}

func (l *Offset) Children() []Plan {
	return []Plan{l.Input}
}

func NewOffset(input UnresolvedPlan, num uint32) UnresolvedPlan {
	return &Offset{
		Parent: &Parent{
			UnresolvedInput: input,
		},
		offsetNum: num,
	}
}
