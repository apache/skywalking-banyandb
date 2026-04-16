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

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

const strNone = "none"

// GenerateLayer3 produces test cases for query feature combinations.
// Uses pairwise testing across AggFunction, TopN, GroupBy, OrderBy, and CriteriaPresence.
func GenerateLayer3() []*TestCase {
	m := FindMeasure("service_cpm_minute")
	if m == nil {
		return nil
	}

	params := map[string][]string{
		"agg":    {"MEAN", "MAX", "MIN", "COUNT", "SUM", strNone},
		"top":    {"desc", "asc", strNone},
		"group":  {"true", "false"},
		"order":  {"asc", "desc", strNone},
		"filter": {strNone, "eq"},
	}

	constraints := []ConstraintFunc{
		// TopN requires GroupBy
		func(tv TestVector) bool {
			if tv["top"] != strNone && tv["group"] == "false" {
				return false
			}
			return true
		},
		// Agg (non-none) requires GroupBy
		func(tv TestVector) bool {
			if tv["agg"] != strNone && tv["group"] == "false" {
				return false
			}
			return true
		},
		// TopN requires Agg
		func(tv TestVector) bool {
			if tv["top"] != strNone && tv["agg"] == strNone {
				return false
			}
			return true
		},
	}

	vectors := PairwiseGenerate(params, constraints)
	// Ensure all 5 agg functions are covered by injecting explicit cases
	vectors = ensureAggCoverage(vectors)
	var cases []*TestCase

	for vecIdx, tv := range vectors {
		req := buildLayer3Request(m, tv)
		if req == nil {
			continue
		}
		ql, qlErr := RenderQL(req)
		if qlErr != nil {
			continue
		}

		disOrder := tv["order"] == strNone && tv["group"] == "true"
		nameParts := []string{}
		if tv["agg"] != strNone {
			nameParts = append(nameParts, strings.ToLower(tv["agg"]))
		}
		if tv["top"] != strNone {
			nameParts = append(nameParts, "top_"+tv["top"])
		}
		if tv["group"] == "true" {
			nameParts = append(nameParts, "group")
		}
		if tv["order"] != strNone {
			nameParts = append(nameParts, "order_"+tv["order"])
		}
		if tv["filter"] != strNone {
			nameParts = append(nameParts, "filter")
		}
		if len(nameParts) == 0 {
			nameParts = append(nameParts, "plain")
		}
		name := "gen_feat_" + strings.Join(nameParts, "_")
		name = fmt.Sprintf("%s_%d", name, vecIdx)

		cases = append(cases, &TestCase{
			Name:     name,
			Measure:  m,
			Request:  req,
			QL:       ql,
			DisOrder: disOrder,
			Duration: "25 * time.Minute",
		})
	}

	return cases
}

func buildLayer3Request(m *Measure, tv TestVector) *measurev1.QueryRequest {
	req := &measurev1.QueryRequest{
		Name:            m.Name,
		Groups:          []string{m.Group},
		TagProjection:   BuildTagProjection(m, []string{"id", "entity_id"}),
		FieldProjection: BuildFieldProjection([]string{"total", "value"}),
	}

	// Criteria
	if tv["filter"] == "eq" {
		cond := BuildCondition("id", modelv1.Condition_BINARY_OP_NE, TagValueStr("svc3"))
		req.Criteria = BuildCriteriaFromCondition(cond)
	}

	// GroupBy
	if tv["group"] == "true" {
		req.GroupBy = &measurev1.QueryRequest_GroupBy{
			TagProjection: BuildTagProjection(m, []string{"id"}),
			FieldName:     "value",
		}
	}

	// Aggregation
	if tv["agg"] != strNone {
		req.Agg = &measurev1.QueryRequest_Aggregation{
			Function:  parseAggFunction(tv["agg"]),
			FieldName: "value",
		}
	}

	// TopN
	if tv["top"] != strNone {
		sortVal := modelv1.Sort_SORT_DESC
		if tv["top"] == "asc" {
			sortVal = modelv1.Sort_SORT_ASC
		}
		req.Top = &measurev1.QueryRequest_Top{
			Number:         2,
			FieldName:      "value",
			FieldValueSort: sortVal,
		}
	}

	// Order
	if tv["order"] == "asc" {
		req.OrderBy = &modelv1.QueryOrder{Sort: modelv1.Sort_SORT_ASC}
	} else if tv["order"] == "desc" {
		req.OrderBy = &modelv1.QueryOrder{Sort: modelv1.Sort_SORT_DESC}
	}

	return req
}

func parseAggFunction(name string) modelv1.AggregationFunction {
	switch name {
	case "MEAN":
		return modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN
	case "MAX":
		return modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX
	case "MIN":
		return modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN
	case "COUNT":
		return modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT
	case "SUM":
		return modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM
	default:
		return modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED
	}
}

// ensureAggCoverage adds explicit vectors for each agg function not covered by pairwise.
func ensureAggCoverage(vectors []TestVector) []TestVector {
	covered := make(map[string]bool)
	for _, tv := range vectors {
		covered[tv["agg"]] = true
	}
	aggFunctions := []string{"MEAN", "MAX", "MIN", "COUNT", "SUM"}
	for _, aggFn := range aggFunctions {
		if covered[aggFn] {
			continue
		}
		// Add a vector with this agg function, group=true, varied other params
		vectors = append(vectors, TestVector{
			"agg":    aggFn,
			"top":    strNone,
			"group":  "true",
			"order":  "desc",
			"filter": strNone,
		})
	}
	return vectors
}
