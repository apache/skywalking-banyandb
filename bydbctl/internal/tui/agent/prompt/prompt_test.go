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

package prompt

import (
	"strings"
	"testing"
)

func TestBuildInitialPrompt(t *testing.T) {
	promptText := Build(Input{
		TaskPrompt:  "Generate a query.",
		PayloadJSON: `{"goal":"top slow endpoints","candidate":""}`,
		Candidate:   "",
	})
	for _, expected := range []string{
		"Generate a query",
		"propose_query_plan",
		"describe_schema",
		"Controlled workflow",
		"validate_bydbql is parse/safety-only",
		"at most five catalog candidates",
		"Use only the provided bydbctl tools",
		"query workspace assistant",
		"probe_bydbql",
	} {
		if !strings.Contains(promptText, expected) {
			t.Fatalf("prompt does not contain %q:\n%s", expected, promptText)
		}
	}
}

func TestBuildRevisePrompt(t *testing.T) {
	promptText := Build(Input{
		TaskPrompt:  "Revise the query.",
		PayloadJSON: `{"candidate":"SELECT * FROM MEASURE x IN g TIME > '-30m' LIMIT 10"}`,
		Candidate:   "SELECT * FROM MEASURE x IN g TIME > '-30m' LIMIT 10",
	})
	if !strings.Contains(promptText, "Revise the query") {
		t.Fatalf("expected revise instructions:\n%s", promptText)
	}
	if !strings.Contains(promptText, "execution_summary.error") {
		t.Fatalf("expected revision feedback guidance:\n%s", promptText)
	}
}
