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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
)

// treeConfig describes one AND/OR criteria tree shape to generate.
type treeConfig struct {
	suffix    string
	depth     int
	rootOp    modelv1.LogicalExpression_LogicalOp
	wantEmpty bool
	disOrder  bool
}

// GenerateLayer2 produces stream criteria tree depth cases.
func GenerateLayer2() []TestCase {
	streamDef := FindStream("sw")
	if streamDef == nil {
		return nil
	}
	configs := []treeConfig{
		{depth: 1, rootOp: modelv1.LogicalExpression_LOGICAL_OP_AND, suffix: "leaf", disOrder: false},
		{depth: 2, rootOp: modelv1.LogicalExpression_LOGICAL_OP_AND, suffix: "and", disOrder: false},
		{depth: 2, rootOp: modelv1.LogicalExpression_LOGICAL_OP_OR, suffix: "or", disOrder: true},
		{depth: 3, rootOp: modelv1.LogicalExpression_LOGICAL_OP_AND, suffix: "and_or", disOrder: true},
		{depth: 3, rootOp: modelv1.LogicalExpression_LOGICAL_OP_OR, suffix: "or_and", disOrder: true},
		{depth: 5, rootOp: modelv1.LogicalExpression_LOGICAL_OP_AND, suffix: "deep_and", disOrder: true},
		{depth: 5, rootOp: modelv1.LogicalExpression_LOGICAL_OP_OR, suffix: "deep_or", disOrder: true},
	}
	var cases []TestCase
	for _, cfg := range configs {
		criteria := buildTree(cfg.depth, cfg.rootOp, 0)
		if criteria == nil {
			continue
		}
		req := buildTreeRequest(streamDef, criteria)
		testCase := TestCase{
			Name:      fmt.Sprintf("gen_tree_depth%d_%s", cfg.depth, cfg.suffix),
			Request:   req,
			WantEmpty: cfg.wantEmpty,
			DisOrder:  cfg.disOrder,
		}
		testCase.QL = RenderQL(&testCase)
		cases = append(cases, testCase)
	}
	cases = append(cases, generateContradictoryCases(streamDef)...)
	return cases
}

func buildTreeRequest(streamDef *Stream, criteria *modelv1.Criteria) *streamv1.QueryRequest {
	return &streamv1.QueryRequest{
		Name:       streamDef.Name,
		Groups:     []string{streamDef.DefaultGroup},
		Projection: leafProjection(),
		Criteria:   criteria,
		OrderBy:    &modelv1.QueryOrder{IndexRuleName: "duration", Sort: modelv1.Sort_SORT_DESC},
	}
}

// buildTree recursively builds a balanced AND/OR tree, alternating the operator
// at each level and rotating leaf conditions over the seed tags by offset.
func buildTree(depth int, op modelv1.LogicalExpression_LogicalOp, leafOffset int) *modelv1.Criteria {
	if depth <= 1 {
		return buildLeafCondition(leafOffset)
	}
	left := buildTree(depth-1, alternateOp(op), leafOffset)
	right := buildTree(depth-1, alternateOp(op), leafOffset+1<<(depth-2))
	if left == nil || right == nil {
		return left
	}
	return BuildLogicalExpr(op, left, right)
}

// buildLeafCondition rotates over deterministic seed-backed predicates so trees
// are varied yet reproducible. All chosen values exist in the mirrored seed rows.
func buildLeafCondition(offset int) *modelv1.Criteria {
	switch offset % 4 {
	case 0:
		return BuildCriteriaFromCondition(BuildCondition("service_id", modelv1.Condition_BINARY_OP_EQ, TagValueStr("webapp_id")))
	case 1:
		return BuildCriteriaFromCondition(BuildCondition("state", modelv1.Condition_BINARY_OP_EQ, TagValueInt(0)))
	case 2:
		return BuildCriteriaFromCondition(BuildCondition("duration", modelv1.Condition_BINARY_OP_LE, TagValueInt(1000)))
	default:
		return BuildCriteriaFromCondition(BuildCondition("duration", modelv1.Condition_BINARY_OP_GE, TagValueInt(30)))
	}
}

func alternateOp(op modelv1.LogicalExpression_LogicalOp) modelv1.LogicalExpression_LogicalOp {
	if op == modelv1.LogicalExpression_LOGICAL_OP_AND {
		return modelv1.LogicalExpression_LOGICAL_OP_OR
	}
	return modelv1.LogicalExpression_LOGICAL_OP_AND
}

// generateContradictoryCases builds AND trees whose leaves cannot both hold,
// so the result must be empty.
func generateContradictoryCases(streamDef *Stream) []TestCase {
	criteria := BuildLogicalExpr(
		modelv1.LogicalExpression_LOGICAL_OP_AND,
		BuildCriteriaFromCondition(BuildCondition("duration", modelv1.Condition_BINARY_OP_GT, TagValueInt(500))),
		BuildCriteriaFromCondition(BuildCondition("duration", modelv1.Condition_BINARY_OP_LT, TagValueInt(500))),
	)
	req := buildTreeRequest(streamDef, criteria)
	testCase := TestCase{
		Name:      "gen_tree_depth2_contradict_and",
		Request:   req,
		WantEmpty: true,
		DisOrder:  false,
	}
	testCase.QL = RenderQL(&testCase)
	return []TestCase{testCase}
}
