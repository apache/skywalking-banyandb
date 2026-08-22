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

package bydbql

import (
	"context"
	"testing"
)

func TestParserValidatorValidate(t *testing.T) {
	validator := NewParserValidator()
	report, validateErr := validator.Validate(context.Background(), "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10", nil)
	if validateErr != nil {
		t.Fatalf("Validate returned error: %v", validateErr)
	}
	if !report.Valid {
		t.Fatalf("expected valid report, got %q", report.Message)
	}
	if report.QueryType != "MEASURE" {
		t.Fatalf("unexpected query type: %s", report.QueryType)
	}
}

func TestParserValidatorReportsSyntaxError(t *testing.T) {
	validator := NewParserValidator()
	report, validateErr := validator.Validate(context.Background(), "SELECT FROM", nil)
	if validateErr != nil {
		t.Fatalf("Validate returned error: %v", validateErr)
	}
	if report.Valid {
		t.Fatal("expected invalid report")
	}
	if report.Message == "" {
		t.Fatal("expected validation message")
	}
}
