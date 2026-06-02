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
	"strings"
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

func TestRenderQLTraceProjectionOrderAndPaging(t *testing.T) {
	req := &tracev1.QueryRequest{
		Name:          "sw",
		Groups:        []string{"test-trace-group"},
		TagProjection: []string{"trace_id", "span_id"},
		OrderBy:       &modelv1.QueryOrder{IndexRuleName: "timestamp", Sort: modelv1.Sort_SORT_DESC},
		Limit:         2,
		Offset:        1,
	}
	ql, renderErr := RenderQL(req)
	if renderErr != nil {
		t.Fatalf("RenderQL failed: %v", renderErr)
	}
	for _, want := range []string{"SELECT trace_id, span_id FROM TRACE sw IN test-trace-group", "ORDER BY timestamp DESC", "LIMIT 2", "OFFSET 1"} {
		if !strings.Contains(ql, want) {
			t.Fatalf("QL %q missing %q", ql, want)
		}
	}
}

func TestRenderQLEmptyProjection(t *testing.T) {
	req := &tracev1.QueryRequest{Name: "sw", Groups: []string{"test-trace-group"}, OrderBy: &modelv1.QueryOrder{IndexRuleName: "duration", Sort: modelv1.Sort_SORT_ASC}}
	ql, renderErr := RenderQL(req)
	if renderErr != nil {
		t.Fatalf("RenderQL failed: %v", renderErr)
	}
	if !strings.Contains(ql, "SELECT () FROM TRACE sw IN test-trace-group") {
		t.Fatalf("QL %q does not render empty projection as SELECT ()", ql)
	}
	if !strings.Contains(ql, "ORDER BY duration ASC") {
		t.Fatalf("QL %q missing duration ASC order", ql)
	}
}
