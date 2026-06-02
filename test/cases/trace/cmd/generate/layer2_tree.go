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

package main

import (
	"fmt"
	"strconv"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

// GenerateLayer2 produces trace criteria tree depth cases.
func GenerateLayer2() []*TestCase {
	traceDef := FindTrace("sw")
	if traceDef == nil {
		return nil
	}
	type depthConfig struct {
		suffix string
		depth  int
		rootOp modelv1.LogicalExpression_LogicalOp
	}
	configs := []depthConfig{
		{depth: 1, rootOp: modelv1.LogicalExpression_LOGICAL_OP_AND, suffix: "leaf"},
		{depth: 2, rootOp: modelv1.LogicalExpression_LOGICAL_OP_AND, suffix: "and"},
		{depth: 2, rootOp: modelv1.LogicalExpression_LOGICAL_OP_OR, suffix: "or"},
		{depth: 3, rootOp: modelv1.LogicalExpression_LOGICAL_OP_AND, suffix: "and_or"},
		{depth: 3, rootOp: modelv1.LogicalExpression_LOGICAL_OP_OR, suffix: "or_and"},
		{depth: 5, rootOp: modelv1.LogicalExpression_LOGICAL_OP_AND, suffix: "deep_and"},
		{depth: 5, rootOp: modelv1.LogicalExpression_LOGICAL_OP_OR, suffix: "deep_or"},
	}
	var cases []*TestCase
	for _, cfg := range configs {
		criteria := buildTree(traceDef, cfg.depth, cfg.rootOp, 0)
		req := &tracev1.QueryRequest{Name: traceDef.Name, Groups: []string{traceDef.Group}, TagProjection: []string{"trace_id", "span_id", "service_id", "duration"}, Criteria: criteria, OrderBy: &modelv1.QueryOrder{IndexRuleName: "timestamp", Sort: modelv1.Sort_SORT_DESC}}
		ql, qlErr := RenderQL(req)
		if qlErr != nil {
			continue
		}
		cases = append(cases, &TestCase{
			Name: fmt.Sprintf("gen_tree_depth%d_%s", cfg.depth, cfg.suffix), Trace: traceDef, Request: req, QL: ql,
			DisOrder: cfg.depth > 1, WantEmpty: (cfg.depth == 3 && cfg.rootOp == modelv1.LogicalExpression_LOGICAL_OP_AND) || (cfg.depth == 5 && cfg.rootOp == modelv1.LogicalExpression_LOGICAL_OP_OR),
		})
	}
	return cases
}

func buildTree(traceDef *Trace, depth int, op modelv1.LogicalExpression_LogicalOp, leafOffset int) *modelv1.Criteria {
	if depth <= 1 {
		return buildLeafCondition(traceDef, leafOffset)
	}
	left := buildTree(traceDef, depth-1, alternateOp(op), leafOffset)
	right := buildTree(traceDef, depth-1, alternateOp(op), leafOffset+1<<(depth-2))
	if left == nil || right == nil {
		return left
	}
	return BuildLogicalExpr(op, left, right)
}

func buildLeafCondition(traceDef *Trace, offset int) *modelv1.Criteria {
	tagNames := []string{"service_id", "service_instance_id", "state"}
	tagName := tagNames[offset%len(tagNames)]
	knownValues := traceDef.KnownValues(traceDef.Group, tagName)
	valueText := knownValues[offset%len(knownValues)]
	if tagName == "state" {
		stateValue, parseErr := strconv.ParseInt(valueText, 10, 64)
		if parseErr != nil {
			stateValue = 1
		}
		return BuildCriteriaFromCondition(BuildCondition(tagName, modelv1.Condition_BINARY_OP_EQ, TagValueInt(stateValue)))
	}
	return BuildCriteriaFromCondition(BuildCondition(tagName, modelv1.Condition_BINARY_OP_EQ, TagValueStr(valueText)))
}

func alternateOp(op modelv1.LogicalExpression_LogicalOp) modelv1.LogicalExpression_LogicalOp {
	if op == modelv1.LogicalExpression_LOGICAL_OP_AND {
		return modelv1.LogicalExpression_LOGICAL_OP_OR
	}
	return modelv1.LogicalExpression_LOGICAL_OP_AND
}
