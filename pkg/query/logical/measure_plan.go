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

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
)

var (
	_ Plan           = (*measureLimit)(nil)
	_ UnresolvedPlan = (*measureLimit)(nil)
)

type measureLimit struct {
	*parent
	offset uint32
	limit  uint32
}

func (l *measureLimit) Execute(ec executor.MeasureExecutionContext) (executor.MIterator, error) {
	dps, err := l.parent.input.(executor.MeasureExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}

	return newLimitIterator(dps, l.offset, l.limit), nil
}

func (l *measureLimit) Equal(plan Plan) bool {
	if plan.Type() != PlanLimit {
		return false
	}
	other := plan.(*measureLimit)
	if l.limit == other.limit && l.offset == other.offset {
		return l.input.Equal(other.input)
	}
	return false
}

func (l *measureLimit) Analyze(s Schema) (Plan, error) {
	var err error
	l.input, err = l.unresolvedInput.Analyze(s)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *measureLimit) Schema() Schema {
	return l.input.Schema()
}

func (l *measureLimit) String() string {
	return fmt.Sprintf("Limit: %d, %d", l.offset, l.limit)
}

func (l *measureLimit) Children() []Plan {
	return []Plan{l.input}
}

func (l *measureLimit) Type() PlanType {
	return PlanLimit
}

func MeasureLimit(input UnresolvedPlan, offset, limit uint32) UnresolvedPlan {
	return &measureLimit{
		parent: &parent{
			unresolvedInput: input,
		},
		offset: offset,
		limit:  limit,
	}
}

var _ executor.MIterator = (*limitIterator)(nil)

type limitIterator struct {
	inner  executor.MIterator
	index  uint32
	offset uint32
	limit  uint32
}

func newLimitIterator(inner executor.MIterator, offset, limit uint32) *limitIterator {
	return &limitIterator{
		inner:  inner,
		offset: offset,
		limit:  limit,
	}
}

func (l *limitIterator) Close() error {
	return l.inner.Close()
}

func (l *limitIterator) Current() []*measurev1.DataPoint {
	return l.inner.Current()
}

func (l *limitIterator) Next() bool {
	for ; l.index < l.offset; l.index++ {
		if !l.inner.Next() {
			return false
		}
	}
	if (l.index - l.offset) >= l.limit {
		return false
	}
	l.index++
	return l.inner.Next()
}
