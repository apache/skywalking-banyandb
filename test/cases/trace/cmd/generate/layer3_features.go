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

// GenerateLayer3 produces pairwise trace feature combinations around lookup vs order modes.
func GenerateLayer3() []*TestCase {
	traceDef := FindTrace("sw")
	if traceDef == nil {
		return nil
	}
	params := map[string][]string{
		"mode":   {"traceid", "order"},
		"order":  {"duration_asc", "duration_desc", "timestamp_asc", "timestamp_desc", strNone},
		"filter": {strNone, "service_eq", "duration_range", "state_eq"},
		"limit":  {strNone, "2", "5"},
		"offset": {strNone, "1", "3"},
		"proj":   {"empty", "explicit"},
	}
	constraints := []ConstraintFunc{
		func(tv TestVector) bool {
			if tv["mode"] == "order" && tv["order"] == strNone {
				return false
			}
			return true
		},
		func(tv TestVector) bool {
			if tv["mode"] == "traceid" && tv["order"] != "" && tv["order"] != strNone {
				return false
			}
			return true
		},
		func(tv TestVector) bool {
			if tv["offset"] != "" && tv["offset"] != strNone && tv["limit"] == strNone {
				return false
			}
			return true
		},
	}
	vectors := ensureModeCoverage(PairwiseGenerate(params, constraints))
	var cases []*TestCase
	for vecIdx, tv := range vectors {
		req := buildLayer3Request(traceDef, tv)
		if req == nil {
			continue
		}
		ql, qlErr := RenderQL(req)
		if qlErr != nil {
			continue
		}
		cases = append(cases, &TestCase{
			Name:     fmt.Sprintf("%s_%d", layer3Name(tv), vecIdx),
			Trace:    traceDef,
			Request:  req,
			QL:       ql,
			DisOrder: req.GetOrderBy() == nil, WantEmpty: tv["mode"] == "traceid" && tv["offset"] != strNone,
		})
	}
	return cases
}

func buildLayer3Request(traceDef *Trace, tv TestVector) *tracev1.QueryRequest {
	req := &tracev1.QueryRequest{Name: traceDef.Name, Groups: []string{traceDef.Group}}
	if tv["proj"] == "explicit" {
		req.TagProjection = []string{"trace_id", "span_id", "service_id", "duration"}
	}
	if tv["mode"] == "traceid" {
		req.Criteria = BuildCriteriaFromCondition(BuildCondition("trace_id", modelv1.Condition_BINARY_OP_EQ, TagValueStr("trace_001")))
		applyLimitOffset(req, tv)
		return req
	}
	orderRule, sortDir := parseOrder(tv["order"])
	if orderRule == "" {
		return nil
	}
	req.OrderBy = &modelv1.QueryOrder{IndexRuleName: orderRule, Sort: sortDir}
	req.Criteria = layer3Criteria(tv["filter"])
	applyLimitOffset(req, tv)
	return req
}

func parseOrder(order string) (string, modelv1.Sort) {
	if strings.HasSuffix(order, "_asc") {
		return strings.TrimSuffix(order, "_asc"), modelv1.Sort_SORT_ASC
	}
	if strings.HasSuffix(order, "_desc") {
		return strings.TrimSuffix(order, "_desc"), modelv1.Sort_SORT_DESC
	}
	return "", modelv1.Sort_SORT_UNSPECIFIED
}

func layer3Criteria(filter string) *modelv1.Criteria {
	switch filter {
	case "service_eq":
		return BuildCriteriaFromCondition(BuildCondition("service_id", modelv1.Condition_BINARY_OP_EQ, TagValueStr("webapp_service")))
	case "duration_range":
		return BuildLogicalExpr(modelv1.LogicalExpression_LOGICAL_OP_AND,
			BuildCriteriaFromCondition(BuildCondition("duration", modelv1.Condition_BINARY_OP_GE, TagValueInt(200))),
			BuildCriteriaFromCondition(BuildCondition("duration", modelv1.Condition_BINARY_OP_LE, TagValueInt(1000))))
	case "state_eq":
		return BuildCriteriaFromCondition(BuildCondition("state", modelv1.Condition_BINARY_OP_EQ, TagValueInt(1)))
	default:
		return nil
	}
}

func applyLimitOffset(req *tracev1.QueryRequest, tv TestVector) {
	if tv["limit"] == "2" {
		req.Limit = 2
	} else if tv["limit"] == "5" {
		req.Limit = 5
	}
	if tv["offset"] == "1" {
		req.Offset = 1
	} else if tv["offset"] == "3" {
		req.Offset = 3
	}
}

func layer3Name(tv TestVector) string {
	parts := []string{"gen_feat", tv["mode"]}
	if tv["order"] != strNone {
		parts = append(parts, tv["order"])
	}
	if tv["filter"] != strNone {
		parts = append(parts, "filter_"+tv["filter"])
	}
	if tv["limit"] != strNone {
		parts = append(parts, "limit"+tv["limit"])
	}
	if tv["offset"] != strNone {
		parts = append(parts, "offset"+tv["offset"])
	}
	if tv["proj"] == "explicit" {
		parts = append(parts, "proj_explicit")
	}
	return strings.Join(parts, "_")
}

func ensureModeCoverage(vectors []TestVector) []TestVector {
	required := []TestVector{
		{"mode": "traceid", "order": strNone, "filter": strNone, "limit": strNone, "offset": strNone, "proj": "explicit"},
		{"mode": "traceid", "order": strNone, "filter": strNone, "limit": "2", "offset": strNone, "proj": "empty"},
		{"mode": "order", "order": "duration_asc", "filter": strNone, "limit": "5", "offset": strNone, "proj": "explicit"},
		{"mode": "order", "order": "duration_desc", "filter": "service_eq", "limit": "2", "offset": strNone, "proj": "empty"},
		{"mode": "order", "order": "timestamp_asc", "filter": "state_eq", "limit": "5", "offset": "1", "proj": "explicit"},
		{"mode": "order", "order": "timestamp_desc", "filter": "duration_range", "limit": "2", "offset": strNone, "proj": "empty"},
	}
	for _, requiredVector := range required {
		if hasVector(vectors, requiredVector) {
			continue
		}
		vectors = append(vectors, requiredVector)
	}
	return vectors
}

func hasVector(vectors []TestVector, required TestVector) bool {
	for _, vector := range vectors {
		matched := true
		for key, value := range required {
			if vector[key] != value {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}
