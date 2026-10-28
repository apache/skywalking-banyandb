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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package qlgen

import (
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
)

// SortingExpectation records independent work before final ordered pagination.
type SortingExpectation struct {
	InputRows            uint64 `json:"input_rows"`
	OutputRows           uint64 `json:"output_rows"`
	KeyExpressions       uint64 `json:"key_expressions"`
	KeyEvaluations       uint64 `json:"key_evaluations"`
	KeyKernelRows        uint64 `json:"key_kernel_rows"`
	KeyScalarEvaluations uint64 `json:"key_scalar_evaluations"`
	K                    uint64 `json:"k,omitempty"`
	FullSort             bool   `json:"full_sort,omitempty"`
	Grouped              bool   `json:"grouped,omitempty"`
	Distinct             bool   `json:"distinct,omitempty"`
	BucketMetadata       bool   `json:"bucket_metadata,omitempty"`
}

// SortingResource declares real endpoint pressure or cancellation plus cleanup.
type SortingResource struct {
	Control     *Case `json:"control,omitempty"`
	Case        Case  `json:"case"`
	BudgetBytes int64 `json:"budget_bytes,omitempty"`
	FullSort    bool  `json:"full_sort,omitempty"`
	Cancel      bool  `json:"cancel,omitempty"`
}

// SortingScenario rebinds one statement through cached and uncached QL execution.
type SortingScenario struct {
	Name  string `json:"name"`
	Cases []Case `json:"cases"`
}

type t17Key struct {
	Expr      *t10Expr
	Direction string
	Nulls     string
}

type t17Query struct {
	Keys  []t17Key
	Limit *uint32
	t16Query
	Offset uint32
}

func renderT17(q t17Query) (string, error) {
	query, err := renderT16(q.t16Query)
	if err != nil {
		return "", err
	}
	text, directive, _ := strings.Cut(query, "\n#!params: ")
	var params []any
	if err = json.Unmarshal([]byte(directive), &params); err != nil {
		return "", err
	}
	var keys []string
	for _, key := range q.Keys {
		order := key.Expr.sql()
		if key.Direction != "" {
			order += " " + key.Direction
		}
		if key.Nulls != "" {
			order += " NULLS " + key.Nulls
		}
		keys = append(keys, order)
		if strings.Contains(key.Expr.sql(), "?") {
			params = appendT16Params(params, key.Expr, q.Params)
		}
	}
	text += " ORDER BY " + strings.Join(keys, ", ")
	if q.Limit != nil {
		text += " LIMIT ? OFFSET ?"
		params = append(params, jsonValue(int64(*q.Limit)), jsonValue(int64(q.Offset)))
	}
	encoded, err := json.Marshal(params)
	return text + "\n#!params: " + string(encoded), err
}

func validateSortingScenarios(s Suite) error {
	if len(s.SortingReadiness) != 3 || len(s.SortingScenarios) != len(requiredT17Scenarios()) || len(s.SortingRejections) != 12 || len(s.SortingResources) != 4 {
		return fmt.Errorf("T17 sorting coverage missing")
	}
	if err := validateT17ReadinessAndResources(s); err != nil {
		return err
	}
	required := map[string]bool{}
	for _, name := range requiredT17Scenarios() {
		required["t17_"+name] = true
	}
	for _, rejection := range s.SortingRejections {
		if rejection.Ticket != "T17" || !rejection.NoScan || rejection.WantCode != codes.InvalidArgument {
			return fmt.Errorf("T17 invalid ORDER BY must reject before scanning")
		}
	}
	for _, scenario := range s.SortingScenarios {
		if !required[scenario.Name] {
			return fmt.Errorf("unexpected T17 scenario %s", scenario.Name)
		}
		delete(required, scenario.Name)
		if len(scenario.Cases) != 3 {
			return fmt.Errorf("T17 scenario %s needs three bindings", scenario.Name)
		}
		statement := strings.Split(scenario.Cases[0].Query, "\n#!params:")[0]
		for _, c := range scenario.Cases {
			if c.Ticket != "T17" || !c.Ordered || c.Sorting == nil || c.Sorting.KeyExpressions == 0 ||
				c.Sorting.KeyEvaluations != c.Sorting.InputRows*c.Sorting.KeyExpressions ||
				strings.Split(c.Query, "\n#!params:")[0] != statement {
				return fmt.Errorf("invalid T17 sorting witness %s", c.Name)
			}
		}
	}
	if len(required) > 0 {
		return fmt.Errorf("T17 sorting witnesses missing: %v", required)
	}
	return nil
}

func validateT17ReadinessAndResources(s Suite) error {
	for ordinal, name := range []string{T16FixtureName, T16BulkFixtureName, T17WideFixtureName} {
		fixture := s.SortingReadiness[ordinal]
		if fixture.Name != name || len(fixture.Cases) != (len(t17Rows(name))+127)/128 {
			return fmt.Errorf("T17 fixture %s lacks complete QL readiness", name)
		}
	}
	for _, resource := range s.SortingResources {
		want := codes.ResourceExhausted
		if resource.Cancel {
			want = codes.Canceled
		}
		if resource.Cancel != (resource.Control == nil) {
			return fmt.Errorf("T17 budget control missing")
		}
		if resource.Control != nil && (resource.Control.WantCode != codes.OK || resource.Control.Sorting == nil || resource.Control.Sorting.K != 5) {
			return fmt.Errorf("invalid T17 compact budget control")
		}
		if resource.Case.WantCode != want || resource.Case.NoScan || resource.Case.Sorting == nil ||
			resource.Case.Sorting.InputRows != 1153 || resource.Cancel == (resource.BudgetBytes > 0) {
			return fmt.Errorf("invalid T17 resource witness %s", resource.Case.Name)
		}
	}
	return nil
}
