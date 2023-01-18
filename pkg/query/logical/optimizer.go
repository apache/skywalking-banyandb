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
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// OptimizeRule allows optimizing a Plan based on a rule.
type OptimizeRule interface {
	Optimize(Plan) (Plan, error)
}

// ApplyRules apply OptimizeRules to a Plan.
func ApplyRules(plan Plan, rules ...OptimizeRule) error {
	p := plan
	for _, r := range rules {
		var err error
		if p, err = r.Optimize(p); err != nil {
			return err
		}
	}
	cc := plan.Children()
	if len(cc) < 1 {
		return nil
	}
	for _, c := range cc {
		if err := ApplyRules(c, rules...); err != nil {
			return err
		}
	}
	return nil
}

// VolumeLimiter controls the volume of a Plan's output.
type VolumeLimiter interface {
	Plan
	Limit(max int)
}

// Sorter sorts a Plan's output.
type Sorter interface {
	Plan
	Sort(order *OrderBy)
}

var _ OptimizeRule = (*PushDownOrder)(nil)

// PushDownOrder pushes down the order to a Plan.
type PushDownOrder struct {
	order *modelv1.QueryOrder
}

// NewPushDownOrder returns a new PushDownOrder.
func NewPushDownOrder(order *modelv1.QueryOrder) PushDownOrder {
	return PushDownOrder{order: order}
}

// Optimize a Plan by pushing down the query order.
func (pdo PushDownOrder) Optimize(plan Plan) (Plan, error) {
	if v, ok := plan.(Sorter); ok {
		if order, err := ParseOrderBy(v.Schema(), pdo.order.GetIndexRuleName(), pdo.order.GetSort()); err == nil {
			v.Sort(order)
		} else {
			return nil, err
		}
	}
	return plan, nil
}

// PushDownMaxSize pushes down the max volume to a Plan.
type PushDownMaxSize struct {
	max int
}

// NewPushDownMaxSize returns a new PushDownMaxSize.
func NewPushDownMaxSize(max int) PushDownMaxSize {
	return PushDownMaxSize{max: max}
}

// Optimize a Plan by pushing down the max volume.
func (pde PushDownMaxSize) Optimize(plan Plan) (Plan, error) {
	if v, ok := plan.(VolumeLimiter); ok {
		v.Limit(pde.max)
	}
	return plan, nil
}
