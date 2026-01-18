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

package measure

import (
	"context"
	"fmt"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

var (
	_ logical.Plan           = (*limitPlan)(nil)
	_ logical.UnresolvedPlan = (*limitPlan)(nil)
)

type limitPlan struct {
	*logical.Parent
	offset uint32
	limit  uint32
}

func (l *limitPlan) Execute(ec context.Context) (executor.MIterator, error) {
	dps, err := l.Parent.Input.(executor.MeasureExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}

	return newLimitIterator(dps, l.offset, l.limit), nil
}

func (l *limitPlan) Analyze(s logical.Schema) (logical.Plan, error) {
	var err error
	l.Input, err = l.UnresolvedInput.Analyze(s)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *limitPlan) Schema() logical.Schema {
	return l.Input.Schema()
}

func (l *limitPlan) String() string {
	return fmt.Sprintf("%s Limit: %d, %d", l.Input.String(), l.offset, l.limit)
}

func (l *limitPlan) Children() []logical.Plan {
	return []logical.Plan{l.Input}
}

func limit(input logical.UnresolvedPlan, offset, limit uint32) logical.UnresolvedPlan {
	return &limitPlan{
		Parent: &logical.Parent{
			UnresolvedInput: input,
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

func (l *limitIterator) Current() []*measurev1.InternalDataPoint {
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
