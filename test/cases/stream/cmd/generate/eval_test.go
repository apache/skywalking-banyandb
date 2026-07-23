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
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
)

func cond(name string, op modelv1.Condition_BinaryOp, value *modelv1.TagValue) *modelv1.Criteria {
	return BuildCriteriaFromCondition(BuildCondition(name, op, value))
}

func TestExpectEmpty(t *testing.T) {
	single := []string{"default"}
	multi := []string{"default", "updated"}

	tests := []struct {
		criteria *modelv1.Criteria
		name     string
		groups   []string
		limit    uint32
		offset   uint32
		want     bool
	}{
		{
			// durations {1000,500,30,60,300}: >30 AND <1000 → {500,60,300}=3, offset 3 → 0.
			name:   "duration_range_offset_exhausts",
			groups: single,
			criteria: BuildLogicalExpr(modelv1.LogicalExpression_LOGICAL_OP_AND,
				cond("duration", modelv1.Condition_BINARY_OP_GT, TagValueInt(30)),
				cond("duration", modelv1.Condition_BINARY_OP_LT, TagValueInt(1000))),
			offset: 3,
			want:   true,
		},
		{
			name:     "service_eq_matches_all",
			groups:   single,
			criteria: cond("service_id", modelv1.Condition_BINARY_OP_EQ, TagValueStr("webapp_id")),
			want:     false,
		},
		{
			name:     "service_eq_nonexistent",
			groups:   single,
			criteria: cond("service_id", modelv1.Condition_BINARY_OP_EQ, TagValueStr("nonexistent")),
			want:     true,
		},
		{
			name:   "duration_contradiction",
			groups: single,
			criteria: BuildLogicalExpr(modelv1.LogicalExpression_LOGICAL_OP_AND,
				cond("duration", modelv1.Condition_BINARY_OP_GT, TagValueInt(500)),
				cond("duration", modelv1.Condition_BINARY_OP_LT, TagValueInt(500))),
			want: true,
		},
		{
			name:     "db_instance_match_mysql",
			groups:   single,
			criteria: cond("db.instance", modelv1.Condition_BINARY_OP_MATCH, TagValueStr("mysql")),
			want:     false,
		},
		{
			name:     "multi_group_union_nonempty",
			groups:   multi,
			criteria: cond("service_id", modelv1.Condition_BINARY_OP_EQ, TagValueStr("service_1")),
			want:     false,
		},
		{
			// NE against the only present value → empty on default group.
			name:     "service_ne_only_value",
			groups:   single,
			criteria: cond("service_id", modelv1.Condition_BINARY_OP_NE, TagValueStr("webapp_id")),
			want:     true,
		},
		{
			// extended_tags HAVING "c": rows 2,3,4 carry "c" → non-empty.
			name:     "having_contains_c",
			groups:   single,
			criteria: cond("extended_tags", modelv1.Condition_BINARY_OP_HAVING, TagValueStrArray([]string{"c"})),
			want:     false,
		},
		{
			// state IN {0,1} → all rows → non-empty.
			name:     "state_in_set",
			groups:   single,
			criteria: cond("state", modelv1.Condition_BINARY_OP_IN, TagValueIntArray([]int64{0, 1})),
			want:     false,
		},
		{
			// nil criteria + limit within row count → non-empty.
			name:   "nil_criteria_limit",
			groups: single,
			limit:  2,
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := &TestCase{
				Request: &streamv1.QueryRequest{
					Name:     "sw",
					Groups:   tt.groups,
					Criteria: tt.criteria,
					Limit:    tt.limit,
					Offset:   tt.offset,
				},
			}
			if got := expectEmpty(tc); got != tt.want {
				t.Fatalf("expectEmpty(%s) = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
