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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package ql

import "github.com/apache/skywalking-banyandb/pkg/query/vectorized"

// CanScanSeries reports whether a bound QL plan preserves its result when the
// native source visits complete series instead of merging all timestamps.
// It proves only counts with nonfallible expressions and a complete final group
// ordering. Other aggregate functions and unproven shapes retain their ordering.
func (p *RawPlan) CanScanSeries() bool {
	if p == nil || p.Aggregate == nil || p.NativeAggregate != nil || p.Distinct || p.Aggregate.TimeBucketWidth != "" ||
		len(p.Aggregate.Calls) == 0 || !seriesScanExpressionSafe(p.Filter) {
		return false
	}
	for _, call := range p.Aggregate.Calls {
		if call.Function != AggregateCount && call.Function != AggregateCountDistinct {
			return false
		}
	}
	if !seriesScanProjectionSafe(p.Aggregate.GroupExpressions) || !seriesScanProjectionSafe(p.Expressions) ||
		p.Having != nil && !seriesScanExpressionSafe(p.Having.Predicate) {
		return false
	}
	if p.Sort != nil {
		for _, key := range p.Sort.Keys {
			if !seriesScanExpressionSafe(key.Expr) {
				return false
			}
		}
	}
	if len(p.Aggregate.GroupBy) == 0 {
		return true
	}
	return p.seriesScanGroupsOrdered()
}

func (p *RawPlan) seriesScanGroupsOrdered() bool {
	if p.Sort == nil {
		return false
	}
	// Group IDs and unsorted pagination follow first encounter. A final sort
	// must distinguish every group; ordering only an aggregate can leave ties.
	for index := range p.Aggregate.GroupBy {
		covered := false
		for _, key := range p.Sort.Keys {
			if key.Expr != nil && key.Expr.Op == "column" && p.seriesScanGroup(key.Expr.Source) == index {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

func (p *RawPlan) seriesScanGroup(column vectorized.ColumnDef) int {
	if p.Expressions != nil {
		var expression *Expression
		for _, output := range p.Expressions.Outputs {
			if sameSeriesScanColumn(output.Column, column) {
				if expression != nil {
					return -1
				}
				expression = output.Expr
			}
		}
		if expression == nil || expression.Op != "column" {
			return -1
		}
		column = expression.Source
	}
	if p.Having != nil {
		matched := -1
		for index, output := range p.Having.Outputs {
			if sameSeriesScanColumn(output.Output, column) {
				if matched >= 0 {
					return -1
				}
				matched = index
			}
		}
		if matched < 0 {
			return -1
		}
		column = p.Having.Outputs[matched].Source
	}
	group := -1
	for _, output := range p.Aggregate.Outputs {
		if sameSeriesScanColumn(output.Column, column) {
			if !output.IsGroup || group >= 0 {
				return -1
			}
			group = output.Index
		}
	}
	return group
}

func sameSeriesScanColumn(left, right vectorized.ColumnDef) bool {
	return left.Role == right.Role && left.TagFamily == right.TagFamily && left.Name == right.Name && left.Type == right.Type && left.Scale == right.Scale
}

func seriesScanProjectionSafe(plan *ExpressionPlan) bool {
	if plan != nil {
		for _, output := range plan.Outputs {
			if !seriesScanExpressionSafe(output.Expr) {
				return false
			}
		}
	}
	return true
}

func seriesScanExpressionSafe(expression *Expression) bool {
	if expression == nil {
		return true
	}
	switch expression.Op {
	case "column", "literal", "=", "!=", "<", "<=", ">", ">=", "in", "not_in", "and", "or", "not", "is_null", "is_not_null",
		"case", "simple_case", "coalesce":
	default:
		return false
	}
	for _, child := range expression.Args {
		if !seriesScanExpressionSafe(child) {
			return false
		}
	}
	return true
}
