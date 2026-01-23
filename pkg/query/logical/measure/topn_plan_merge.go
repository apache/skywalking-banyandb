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

// Package measure implements execution operations for querying measure data.
package measure

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

var _ logical.UnresolvedPlan = (*unresolvedTopNMerger)(nil)

type unresolvedTopNMerger struct {
	subPlans []*unresolvedLocalScan
}

// Analyze implements logical.UnresolvedPlan.
func (u *unresolvedTopNMerger) Analyze(s logical.Schema) (logical.Plan, error) {
	subPlans := make([]*localScan, 0, len(u.subPlans))
	for _, subPlan := range u.subPlans {
		analyzed, err := subPlan.Analyze(s)
		if err != nil {
			return nil, errors.Wrap(err, "failed to analyze sub plan")
		}
		ls, ok := analyzed.(*localScan)
		if !ok {
			return nil, errors.New("sub plan must be localScan")
		}
		subPlans = append(subPlans, ls)
	}

	return &topNMerger{
		subPlans: subPlans,
		schema:   s,
	}, nil
}

var _ logical.Plan = (*topNMerger)(nil)

type topNMerger struct {
	schema   logical.Schema
	subPlans []*localScan
}

func (t *topNMerger) Execute(ctx context.Context) (executor.MIterator, error) {
	iters := make([]executor.MIterator, 0, len(t.subPlans))
	for _, subPlan := range t.subPlans {
		iter, err := subPlan.Execute(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to execute sub plan")
		}
		iters = append(iters, iter)
	}

	return &topNMergingIterator{
		iters: iters,
	}, nil
}

func (t *topNMerger) String() string {
	return fmt.Sprintf("TopNMerge: %d sub plans", len(t.subPlans))
}

func (t *topNMerger) Children() []logical.Plan {
	children := make([]logical.Plan, 0, len(t.subPlans))
	for _, sp := range t.subPlans {
		children = append(children, sp)
	}
	return children
}

func (t *topNMerger) Schema() logical.Schema {
	return t.schema
}

type topNMergingIterator struct {
	iters      []executor.MIterator
	currentDps []*measurev1.InternalDataPoint
	currentIt  int
}

func (t *topNMergingIterator) Next() bool {
	// Try to get next batch from current iterator
	if t.currentIt < len(t.iters) && len(t.currentDps) > 0 {
		t.currentDps = t.currentDps[1:]
		if len(t.currentDps) > 0 {
			return true
		}
	}

	// Move to next iterator
	for t.currentIt < len(t.iters) {
		if t.iters[t.currentIt].Next() {
			t.currentDps = t.iters[t.currentIt].Current()
			if len(t.currentDps) > 0 {
				return true
			}
		} else {
			t.currentIt++
		}
	}

	return false
}

func (t *topNMergingIterator) Current() []*measurev1.InternalDataPoint {
	if len(t.currentDps) == 0 {
		return nil
	}
	return []*measurev1.InternalDataPoint{t.currentDps[0]}
}

func (t *topNMergingIterator) Close() error {
	var err error
	for _, iter := range t.iters {
		err = multierr.Append(err, iter.Close())
	}
	return err
}
