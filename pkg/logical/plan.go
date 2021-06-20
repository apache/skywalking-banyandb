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
)

var _ Plan = (*limit)(nil)

type limit struct {
	input    Plan
	limitNum uint32
}

func (l *limit) Schema() (Schema, error) {
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

func NewLimit(input Plan, num uint32) Plan {
	return &limit{
		input:    input,
		limitNum: num,
	}
}

var _ Plan = (*offset)(nil)

type offset struct {
	input     Plan
	offsetNum uint32
}

func (l *offset) Schema() (Schema, error) {
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

func NewOffset(input Plan, num uint32) Plan {
	return &offset{
		input:     input,
		offsetNum: num,
	}
}
