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

package agent

import (
	"strings"
	"testing"
)

func TestSanitizeExecutionErrorForProviderRedactsTransportErrors(t *testing.T) {
	sanitized := SanitizeExecutionErrorForProvider(
		"Post https://banyandb.internal:17913/api/v1/bydbql/query: dial tcp 10.0.0.1: connect: refused",
	)
	if sanitized != "BYDBQL execution failed: transport error" {
		t.Fatalf("unexpected sanitized transport error: %q", sanitized)
	}
	if strings.Contains(sanitized, "banyandb.internal") {
		t.Fatalf("transport details leaked: %q", sanitized)
	}
}

func TestSanitizeExecutionErrorForProviderKeepsSchemaMessage(t *testing.T) {
	rawError := `BYDBQL query returned HTTP 500: {"code":13,` +
		`"message":"failed to transform to native request: column searchable.service_id not found in schema","details":[]}`
	sanitized := SanitizeExecutionErrorForProvider(rawError)
	if !strings.Contains(sanitized, "searchable.service_id not found in schema") {
		t.Fatalf("expected schema error to be preserved, got %q", sanitized)
	}
}
