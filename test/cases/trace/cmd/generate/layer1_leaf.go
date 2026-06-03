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
	"strings"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

// LeafOp defines a valid BinaryOp + TagValue + trace tag combination.
type LeafOp struct {
	Value   *modelv1.TagValue
	TagName string
	Op      modelv1.Condition_BinaryOp
}

// GenerateLayer1 produces trace criteria leaf test cases.
func GenerateLayer1() []*TestCase {
	traceDef := FindTrace("sw")
	if traceDef == nil {
		return nil
	}
	ops := defineLeafOps(traceDef)
	var cases []*TestCase
	for _, op := range ops {
		req := buildLeafRequest(traceDef, op)
		ql, qlErr := RenderQL(req)
		if qlErr != nil {
			continue
		}
		cases = append(cases, &TestCase{
			Name:      fmt.Sprintf("gen_leaf_%s_%s", binaryOpShortName(op.Op), op.TagName),
			Trace:     traceDef,
			Request:   req,
			QL:        ql,
			WantErr:   false,
			WantEmpty: isNullTagValue(op.Value) || (op.TagName == "duration" && op.Op == modelv1.Condition_BINARY_OP_EQ),
			DisOrder:  true,
		})
	}
	cases = append(cases, generateLeafErrorCases(traceDef)...)
	return cases
}

func defineLeafOps(traceDef *Trace) []LeafOp {
	traceIDs := traceDef.KnownValues(traceDef.Group, "trace_id")
	services := traceDef.KnownValues(traceDef.Group, "service_id")
	return []LeafOp{
		{Op: modelv1.Condition_BINARY_OP_EQ, TagName: "trace_id", Value: TagValueStr(traceIDs[0])},
		{Op: modelv1.Condition_BINARY_OP_NE, TagName: "trace_id", Value: TagValueStr(traceIDs[1])},
		{Op: modelv1.Condition_BINARY_OP_IN, TagName: "trace_id", Value: TagValueStrArray(traceIDs[:2])},
		{Op: modelv1.Condition_BINARY_OP_NOT_IN, TagName: "trace_id", Value: TagValueStrArray([]string{"missing_trace"})},
		{Op: modelv1.Condition_BINARY_OP_EQ, TagName: "service_id", Value: TagValueStr(services[0])},
		{Op: modelv1.Condition_BINARY_OP_NE, TagName: "service_id", Value: TagValueStr(services[1])},
		{Op: modelv1.Condition_BINARY_OP_IN, TagName: "service_id", Value: TagValueStrArray(services[:2])},
		{Op: modelv1.Condition_BINARY_OP_NOT_IN, TagName: "service_id", Value: TagValueStrArray([]string{"missing_service"})},
		{Op: modelv1.Condition_BINARY_OP_EQ, TagName: "state", Value: TagValueInt(1)},
		{Op: modelv1.Condition_BINARY_OP_NE, TagName: "state", Value: TagValueInt(0)},
		{Op: modelv1.Condition_BINARY_OP_LT, TagName: "state", Value: TagValueInt(1)},
		{Op: modelv1.Condition_BINARY_OP_GT, TagName: "state", Value: TagValueInt(0)},
		{Op: modelv1.Condition_BINARY_OP_LE, TagName: "state", Value: TagValueInt(1)},
		{Op: modelv1.Condition_BINARY_OP_GE, TagName: "state", Value: TagValueInt(0)},
		{Op: modelv1.Condition_BINARY_OP_LT, TagName: "duration", Value: TagValueInt(1000)},
		{Op: modelv1.Condition_BINARY_OP_GT, TagName: "duration", Value: TagValueInt(200)},
		{Op: modelv1.Condition_BINARY_OP_LE, TagName: "duration", Value: TagValueInt(1000)},
		{Op: modelv1.Condition_BINARY_OP_GE, TagName: "duration", Value: TagValueInt(200)},
		{Op: modelv1.Condition_BINARY_OP_EQ, TagName: "duration", Value: TagValueInt(500)},
		{Op: modelv1.Condition_BINARY_OP_EQ, TagName: "service_id_null", Value: TagValueNull()},
	}
}

func buildLeafRequest(traceDef *Trace, op LeafOp) *tracev1.QueryRequest {
	tagName := strings.TrimSuffix(op.TagName, "_null")
	req := &tracev1.QueryRequest{
		Name:          traceDef.Name,
		Groups:        []string{traceDef.Group},
		TagProjection: []string{"trace_id", "span_id", "service_id", "duration"},
		Criteria:      BuildCriteriaFromCondition(BuildCondition(tagName, op.Op, op.Value)),
	}
	if tagName != "trace_id" || (op.Op != modelv1.Condition_BINARY_OP_EQ && op.Op != modelv1.Condition_BINARY_OP_IN) {
		req.OrderBy = &modelv1.QueryOrder{IndexRuleName: "timestamp", Sort: modelv1.Sort_SORT_DESC}
	}
	return req
}

func generateLeafErrorCases(traceDef *Trace) []*TestCase {
	matchReq := &tracev1.QueryRequest{
		Name:          traceDef.Name,
		Groups:        []string{traceDef.Group},
		TagProjection: []string{"trace_id", "span_id"},
		Criteria:      BuildCriteriaFromCondition(BuildCondition("service_id", modelv1.Condition_BINARY_OP_MATCH, TagValueStr("webapp"))),
	}
	matchQL, _ := RenderQL(matchReq)
	return []*TestCase{{Name: "gen_err_match_sw", Trace: traceDef, Request: matchReq, QL: matchQL, WantErr: true}}
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
	default:
		return "unknown"
	}
}

func isNullTagValue(value *modelv1.TagValue) bool {
	if value == nil {
		return false
	}
	_, ok := value.GetValue().(*modelv1.TagValue_Null)
	return ok
}
