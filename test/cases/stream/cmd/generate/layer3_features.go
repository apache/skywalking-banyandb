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
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
)

const strNone = "none"

// GenerateLayer3 produces pairwise stream feature combinations across order,
// filter, limit, offset, projection, and group-fan-out dimensions.
func GenerateLayer3() []TestCase {
	streamDef := FindStream("sw")
	if streamDef == nil {
		return nil
	}
	params := map[string][]string{
		"order_by": {strNone, "ts_asc", "ts_desc", "duration_asc", "duration_desc"},
		"limit":    {strNone, "2", "5"},
		"offset":   {strNone, "1", "3"},
		"proj":     {"explicit", "all"},
		"filter":   {strNone, "service_eq", "duration_range", "state_eq"},
		"group":    {"single", "multi"},
	}
	constraints := []ConstraintFunc{
		func(tv TestVector) bool {
			// Offset requires a limit; a bare offset has no defined semantics.
			if tv["offset"] != strNone && tv["limit"] == strNone {
				return false
			}
			return true
		},
		func(tv TestVector) bool {
			// A multi-group result is only topology-independent as a full set
			// (order handled by DisOrder). Slicing a non-total order with
			// limit/offset selects a different subset under distributed vs
			// standalone, so exclude limit/offset from multi-group vectors.
			if tv["group"] == "multi" && (tv["limit"] != strNone || tv["offset"] != strNone) {
				return false
			}
			return true
		},
	}
	vectors := ensureCoverage(PairwiseGenerate(params, constraints))
	cases := make([]TestCase, 0, len(vectors))
	for vecIdx, tv := range vectors {
		req := buildLayer3Request(streamDef, tv)
		disorder := layer3Disorder(tv)
		testCase := TestCase{
			Name:            fmt.Sprintf("%s_%d", layer3Name(tv), vecIdx),
			Request:         req,
			DisOrder:        disorder,
			IgnoreElementID: disorder,
		}
		testCase.QL = RenderQL(&testCase)
		cases = append(cases, testCase)
	}
	return cases
}

// layer3Disorder reports whether ties make ordering non-deterministic for a
// vector: multi-group fan-out and timestamp ordering both produce ties.
func layer3Disorder(tv TestVector) bool {
	if tv["group"] == "multi" {
		return true
	}
	return tv["order_by"] == "ts_asc" || tv["order_by"] == "ts_desc"
}

func buildLayer3Request(streamDef *Stream, tv TestVector) *streamv1.QueryRequest {
	req := &streamv1.QueryRequest{
		Name:       streamDef.Name,
		Groups:     layer3Groups(streamDef, tv),
		Projection: layer3Projection(tv),
		Criteria:   layer3Criteria(tv["filter"]),
		OrderBy:    layer3Order(tv["order_by"]),
	}
	applyLimitOffset(req, tv)
	return req
}

func layer3Groups(streamDef *Stream, tv TestVector) []string {
	if tv["group"] == "multi" {
		return append([]string{}, streamDef.Groups...)
	}
	return []string{streamDef.DefaultGroup}
}

func layer3Projection(tv TestVector) *modelv1.TagProjection {
	if tv["proj"] == "all" {
		return fullProjection()
	}
	return leafProjection()
}

// fullProjection projects every searchable and data tag of the sw stream.
func fullProjection() *modelv1.TagProjection {
	streamDef := FindStream("sw")
	searchable := make([]string, 0, len(streamDef.Tags))
	data := make([]string, 0, len(streamDef.Tags))
	for _, tag := range streamDef.Tags {
		switch tag.Family {
		case familySearchable:
			searchable = append(searchable, tag.Name)
		case familyData:
			data = append(data, tag.Name)
		}
	}
	return &modelv1.TagProjection{
		TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: familySearchable, Tags: searchable},
			{Name: familyData, Tags: data},
		},
	}
}

func layer3Order(order string) *modelv1.QueryOrder {
	switch order {
	case "ts_asc":
		return &modelv1.QueryOrder{Sort: modelv1.Sort_SORT_ASC}
	case "ts_desc":
		return &modelv1.QueryOrder{Sort: modelv1.Sort_SORT_DESC}
	case "duration_asc":
		return &modelv1.QueryOrder{IndexRuleName: "duration", Sort: modelv1.Sort_SORT_ASC}
	case "duration_desc":
		return &modelv1.QueryOrder{IndexRuleName: "duration", Sort: modelv1.Sort_SORT_DESC}
	default:
		return nil
	}
}

func layer3Criteria(filter string) *modelv1.Criteria {
	switch filter {
	case "service_eq":
		return BuildCriteriaFromCondition(BuildCondition("service_id", modelv1.Condition_BINARY_OP_EQ, TagValueStr("webapp_id")))
	case "duration_range":
		return BuildLogicalExpr(modelv1.LogicalExpression_LOGICAL_OP_AND,
			BuildCriteriaFromCondition(BuildCondition("duration", modelv1.Condition_BINARY_OP_GT, TagValueInt(30))),
			BuildCriteriaFromCondition(BuildCondition("duration", modelv1.Condition_BINARY_OP_LT, TagValueInt(1000))))
	case "state_eq":
		return BuildCriteriaFromCondition(BuildCondition("state", modelv1.Condition_BINARY_OP_EQ, TagValueInt(1)))
	default:
		return nil
	}
}

func applyLimitOffset(req *streamv1.QueryRequest, tv TestVector) {
	switch tv["limit"] {
	case "2":
		req.Limit = 2
	case "5":
		req.Limit = 5
	}
	switch tv["offset"] {
	case "1":
		req.Offset = 1
	case "3":
		req.Offset = 3
	}
}

func layer3Name(tv TestVector) string {
	parts := []string{"gen_feat", tv["order_by"]}
	if tv["filter"] != strNone {
		parts = append(parts, "filter_"+tv["filter"])
	} else {
		parts = append(parts, "filter_none")
	}
	if tv["limit"] != strNone {
		parts = append(parts, "limit"+tv["limit"])
	}
	if tv["offset"] != strNone {
		parts = append(parts, "offset"+tv["offset"])
	}
	parts = append(parts, "proj_"+tv["proj"], "group_"+tv["group"])
	return strings.Join(parts, "_")
}

// ensureCoverage pins one vector for each order_by value and each filter value
// so the final set is complete regardless of pairwise minimization.
func ensureCoverage(vectors []TestVector) []TestVector {
	required := []TestVector{
		{"order_by": strNone, "filter": strNone, "limit": strNone, "offset": strNone, "proj": "explicit", "group": "single"},
		{"order_by": "ts_asc", "filter": "service_eq", "limit": "2", "offset": strNone, "proj": "all", "group": "single"},
		{"order_by": "ts_desc", "filter": "state_eq", "limit": "5", "offset": "1", "proj": "explicit", "group": "single"},
		{"order_by": "duration_asc", "filter": "duration_range", "limit": "5", "offset": "3", "proj": "all", "group": "single"},
		{"order_by": "duration_desc", "filter": strNone, "limit": "2", "offset": strNone, "proj": "explicit", "group": "single"},
		{"order_by": "duration_asc", "filter": "service_eq", "limit": strNone, "offset": strNone, "proj": "explicit", "group": "single"},
		{"order_by": "duration_desc", "filter": "duration_range", "limit": strNone, "offset": strNone, "proj": "all", "group": "multi"},
		{"order_by": "ts_asc", "filter": "state_eq", "limit": strNone, "offset": strNone, "proj": "all", "group": "single"},
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
