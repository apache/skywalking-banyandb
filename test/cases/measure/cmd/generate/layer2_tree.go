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

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// GenerateLayer2 produces test cases for criteria tree depth.
// Tests depths 1, 2, 3, 5 with AND/OR combinations.
func GenerateLayer2() []*TestCase {
	m := FindMeasure("service_cpm_minute")
	if m == nil {
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
		criteria := buildTree(cfg.depth, cfg.rootOp, 0)
		if criteria == nil {
			continue
		}
		req := &measurev1.QueryRequest{
			Name:            m.Name,
			Groups:          []string{m.Group},
			Criteria:        criteria,
			TagProjection:   BuildTagProjection(m, []string{"id", "entity_id"}),
			FieldProjection: BuildFieldProjection([]string{"total", "value"}),
		}
		ql, qlErr := RenderQL(req)
		if qlErr != nil {
			continue
		}

		// Determine DisOrder: trees with OR at any level may produce non-deterministic order
		disOrder := cfg.depth > 1

		cases = append(cases, &TestCase{
			Name:     fmt.Sprintf("gen_tree_depth%d_%s", cfg.depth, cfg.suffix),
			Measure:  m,
			Request:  req,
			QL:       ql,
			DisOrder: disOrder,
			WantErr:  cfg.depth == 5 && cfg.rootOp == modelv1.LogicalExpression_LOGICAL_OP_OR, // server rejects deep OR trees
			Duration: "25 * time.Minute",
		})
	}

	return cases
}

// buildTree recursively builds a balanced criteria tree of the given depth.
// Leaves use EQ conditions with values from seed data, rotated across leaves.
func buildTree(depth int, op modelv1.LogicalExpression_LogicalOp, leafOffset int) *modelv1.Criteria {
	if depth <= 1 {
		return buildLeafCondition(leafOffset)
	}

	// Build left and right subtrees
	left := buildTree(depth-1, alternateOp(op), leafOffset)
	right := buildTree(depth-1, alternateOp(op), leafOffset+1<<(depth-2))
	if left == nil || right == nil {
		return left
	}

	return BuildLogicalExpr(op, left, right)
}

func buildLeafCondition(offset int) *modelv1.Criteria {
	// Rotate through available tag values from seed data
	tagNames := []string{"id", "entity_id"}
	tagValues := [][]string{
		{"svc1", "svc2", "svc3"},
		{"entity_1", "entity_2", "entity_3", "entity_4", "entity_5", "entity_6"},
	}

	tagIdx := offset % len(tagNames)
	valIdx := offset % len(tagValues[tagIdx])

	cond := BuildCondition(
		tagNames[tagIdx],
		modelv1.Condition_BINARY_OP_EQ,
		TagValueStr(tagValues[tagIdx][valIdx]),
	)
	return BuildCriteriaFromCondition(cond)
}

func alternateOp(op modelv1.LogicalExpression_LogicalOp) modelv1.LogicalExpression_LogicalOp {
	if op == modelv1.LogicalExpression_LOGICAL_OP_AND {
		return modelv1.LogicalExpression_LOGICAL_OP_OR
	}
	return modelv1.LogicalExpression_LOGICAL_OP_AND
}
