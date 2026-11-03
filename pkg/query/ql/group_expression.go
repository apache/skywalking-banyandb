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

package ql

import (
	"context"
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func wrapGroupExpressions(ctx context.Context, source vectorized.PullOperator, plan *AggregatePlan) (vectorized.PullOperator, error) {
	keys := plan.GroupExpressions
	if source == nil || source.OutputSchema() == nil {
		if source != nil {
			_ = source.Close()
		}
		return nil, fmt.Errorf("group expressions require a source schema")
	}
	tracker := vectorized.QueryMemoryTracker(ctx, 256<<20)
	bytes := int64(len(keys.Outputs)+len(source.OutputSchema().Columns))*1024 + 2048
	if err := tracker.Reserve(bytes); err != nil {
		_ = source.Close()
		return nil, err
	}
	defer tracker.Release(bytes)
	projection := &ExpressionPlan{Outputs: append([]ExpressionOutput(nil), keys.Outputs...)}
	appendInput := func(definition vectorized.ColumnDef) {
		for _, output := range projection.Outputs {
			if output.Column.Role == definition.Role && output.Column.TagFamily == definition.TagFamily && output.Column.Name == definition.Name {
				return
			}
		}
		if definition.Role == vectorized.RoleField || definition.Role == vectorized.RoleTag {
			projection.Outputs = append(projection.Outputs, ExpressionOutput{Expr: &Expression{Op: "column", Source: definition}, Column: definition})
		}
	}
	for _, definition := range plan.GroupBy {
		appendInput(definition)
	}
	for _, call := range plan.Calls {
		appendInput(call.Source)
	}
	return wrapExpressions(ctx, source, projection, &groupExpressionMetrics, len(keys.Outputs))
}
