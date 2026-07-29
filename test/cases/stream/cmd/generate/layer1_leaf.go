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

// leafOp defines a valid BinaryOp + TagValue + stream tag combination.
type leafOp struct {
	value     *modelv1.TagValue
	tagName   string
	op        modelv1.Condition_BinaryOp
	wantEmpty bool
}

// GenerateLayer1 produces stream criteria leaf test cases.
func GenerateLayer1() []TestCase {
	streamDef := FindStream("sw")
	if streamDef == nil {
		return nil
	}
	ops := defineLeafOps()
	var cases []TestCase
	for _, item := range ops {
		req := buildLeafRequest(streamDef, item.tagName, item.op, item.value)
		testCase := TestCase{
			Name:      leafCaseName(streamDef, item.tagName, item.op),
			Request:   req,
			WantEmpty: item.wantEmpty,
		}
		testCase.QL = RenderQL(&testCase)
		cases = append(cases, testCase)
	}
	cases = append(cases, generateLeafErrorCases(streamDef)...)
	return cases
}

// defineLeafOps lists one leaf predicate per BinaryOp x representative tag/type.
// Filter values are chosen from the mirrored seed rows in seed.go: service_id is
// always "webapp_id", state is 0 or 1, duration is in {30,60,300,500,1000},
// extended_tags carries {"a","b","c"} and db.instance holds jdbc URLs with "mysql".
func defineLeafOps() []leafOp {
	return []leafOp{
		// duration (INT) across the six comparison operators.
		{op: modelv1.Condition_BINARY_OP_EQ, tagName: "duration", value: TagValueInt(500)},
		{op: modelv1.Condition_BINARY_OP_NE, tagName: "duration", value: TagValueInt(500)},
		{op: modelv1.Condition_BINARY_OP_LT, tagName: "duration", value: TagValueInt(1000)},
		{op: modelv1.Condition_BINARY_OP_GT, tagName: "duration", value: TagValueInt(200)},
		{op: modelv1.Condition_BINARY_OP_LE, tagName: "duration", value: TagValueInt(1000)},
		{op: modelv1.Condition_BINARY_OP_GE, tagName: "duration", value: TagValueInt(200)},
		// state (INT) across the six comparison operators.
		{op: modelv1.Condition_BINARY_OP_EQ, tagName: "state", value: TagValueInt(1)},
		{op: modelv1.Condition_BINARY_OP_NE, tagName: "state", value: TagValueInt(0)},
		{op: modelv1.Condition_BINARY_OP_LT, tagName: "state", value: TagValueInt(1)},
		{op: modelv1.Condition_BINARY_OP_GT, tagName: "state", value: TagValueInt(0)},
		{op: modelv1.Condition_BINARY_OP_LE, tagName: "state", value: TagValueInt(1)},
		{op: modelv1.Condition_BINARY_OP_GE, tagName: "state", value: TagValueInt(0)},
		// service_id (STR) equality. Only "webapp_id" exists, so NE is empty.
		{op: modelv1.Condition_BINARY_OP_EQ, tagName: "service_id", value: TagValueStr("webapp_id")},
		{op: modelv1.Condition_BINARY_OP_NE, tagName: "service_id", value: TagValueStr("webapp_id"), wantEmpty: true},
		// service_id (STR set) membership.
		{op: modelv1.Condition_BINARY_OP_IN, tagName: "service_id", value: TagValueStrArray([]string{"webapp_id", "missing_id"})},
		{op: modelv1.Condition_BINARY_OP_NOT_IN, tagName: "service_id", value: TagValueStrArray([]string{"webapp_id"}), wantEmpty: true},
		// state (INT set) membership.
		{op: modelv1.Condition_BINARY_OP_IN, tagName: "state", value: TagValueIntArray([]int64{0, 1})},
		{op: modelv1.Condition_BINARY_OP_NOT_IN, tagName: "state", value: TagValueIntArray([]int64{0, 1}), wantEmpty: true},
		// extended_tags (STRING_ARRAY) HAVING / NOT_HAVING.
		{op: modelv1.Condition_BINARY_OP_HAVING, tagName: "extended_tags", value: TagValueStrArray([]string{"c"})},
		{op: modelv1.Condition_BINARY_OP_NOT_HAVING, tagName: "extended_tags", value: TagValueStrArray([]string{"c"})},
		// db.instance (analyzer=url) full-text MATCH success.
		{op: modelv1.Condition_BINARY_OP_MATCH, tagName: "db.instance", value: TagValueStr("mysql")},
	}
}

func buildLeafRequest(streamDef *Stream, tagName string, op modelv1.Condition_BinaryOp, value *modelv1.TagValue) *streamv1.QueryRequest {
	return &streamv1.QueryRequest{
		Name:       streamDef.Name,
		Groups:     []string{streamDef.DefaultGroup},
		Projection: leafProjection(),
		Criteria:   BuildCriteriaFromCondition(BuildCondition(tagName, op, value)),
	}
}

func leafProjection() *modelv1.TagProjection {
	return &modelv1.TagProjection{
		TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: familySearchable, Tags: []string{"trace_id", "service_id", "state", "duration"}},
			{Name: familyData, Tags: []string{"data_binary"}},
		},
	}
}

func generateLeafErrorCases(streamDef *Stream) []TestCase {
	errReq := &streamv1.QueryRequest{
		Name:       streamDef.Name,
		Groups:     []string{streamDef.DefaultGroup},
		Projection: leafProjection(),
		Criteria:   BuildCriteriaFromCondition(BuildCondition("trace_id", modelv1.Condition_BINARY_OP_MATCH, TagValueStr("1"))),
	}
	errCase := TestCase{Name: "gen_err_match_trace_id", Request: errReq, WantErr: true}
	errCase.QL = RenderQL(&errCase)
	return []TestCase{errCase}
}

// leafCaseName names a leaf case. Entity tags filtered via any operation other
// than EQ/IN are rejected by the engine (ErrUnsupportedConditionOp), so those
// pairs are emitted as gen_err_ cases (WantErr) while keeping the well-formed
// request and QL; every other pair keeps the normal gen_leaf_ name.
func leafCaseName(streamDef *Stream, tagName string, op modelv1.Condition_BinaryOp) string {
	if streamDef.IsEntityTag(tagName) && isEntityUnsupportedOp(op) {
		return fmt.Sprintf("gen_err_%s_%s", binaryOpShortName(op), tagName)
	}
	return fmt.Sprintf("gen_leaf_%s_%s", binaryOpShortName(op), tagName)
}

// isEntityUnsupportedOp reports whether an operation on an entity tag is rejected
// by the series index. Only EQ and IN are supported; every comparison and the
// negated-membership operations are unsupported.
func isEntityUnsupportedOp(op modelv1.Condition_BinaryOp) bool {
	switch op {
	case modelv1.Condition_BINARY_OP_NE,
		modelv1.Condition_BINARY_OP_NOT_IN,
		modelv1.Condition_BINARY_OP_LT,
		modelv1.Condition_BINARY_OP_GT,
		modelv1.Condition_BINARY_OP_LE,
		modelv1.Condition_BINARY_OP_GE:
		return true
	default:
		return false
	}
}

func binaryOpShortName(op modelv1.Condition_BinaryOp) string {
	switch op {
	case modelv1.Condition_BINARY_OP_EQ:
		return "eq"
	case modelv1.Condition_BINARY_OP_NE:
		return "ne"
	case modelv1.Condition_BINARY_OP_LT:
		return "lt"
	case modelv1.Condition_BINARY_OP_GT:
		return "gt"
	case modelv1.Condition_BINARY_OP_LE:
		return "le"
	case modelv1.Condition_BINARY_OP_GE:
		return "ge"
	case modelv1.Condition_BINARY_OP_IN:
		return "in"
	case modelv1.Condition_BINARY_OP_NOT_IN:
		return "not_in"
	case modelv1.Condition_BINARY_OP_MATCH:
		return "match"
	case modelv1.Condition_BINARY_OP_HAVING:
		return "having"
	case modelv1.Condition_BINARY_OP_NOT_HAVING:
		return "not_having"
	case modelv1.Condition_BINARY_OP_UNSPECIFIED:
		return "unspecified"
	default:
		return "unknown"
	}
}
