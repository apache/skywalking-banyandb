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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
)

// TagType describes a trace tag value type.
type TagType int

const (
	TagTypeString TagType = iota
	TagTypeInt
	TagTypeTimestamp
)

// Role describes how generated tests use a tag.
type Role int

const (
	RoleIdentity Role = iota
	RoleIndexed
	RolePlain
)

// TagDef describes a trace tag in positional seed order.
type TagDef struct {
	Name string
	Type TagType
	Pos  int
	Role Role
}

// SpanRow mirrors one seeded span row used for generation value selection.
type SpanRow struct {
	TagValues map[string]any
	Span      string
}

// Trace describes the trace schema and rows used by generated tests.
type Trace struct {
	Name         string
	Group        string
	Groups       []string
	Tags         []TagDef
	OrderRules   []string
	TraceIDTag   string
	SpanIDTag    string
	TimestampTag string
	Rows         []SpanRow
}

// SeedData returns trace schemas available to the generator.
func SeedData() []Trace {
	return []Trace{swTrace()}
}

// FindTrace returns a trace schema by name.
func FindTrace(name string) *Trace {
	for _, traceDef := range SeedData() {
		if traceDef.Name == name {
			return &traceDef
		}
	}
	return nil
}

// KnownValues returns distinct known values for a tag in a group.
func (t *Trace) KnownValues(group, tagName string) []string {
	if group != t.Group {
		return nil
	}
	seen := make(map[string]bool)
	var values []string
	for _, row := range t.Rows {
		value, ok := row.TagValues[tagName]
		if !ok {
			continue
		}
		valueText := fmt.Sprint(value)
		if seen[valueText] {
			continue
		}
		seen[valueText] = true
		values = append(values, valueText)
	}
	slices.Sort(values)
	return values
}

func swTrace() Trace {
	rows := []SpanRow{
		row("trace_001", int64(1), "webapp_service", "webapp_instance_1", "/home_endpoint", int64(1000), "span_001_1", "trace_001_span_1"),
		row("trace_001", int64(0), "webapp_service", "webapp_instance_2", "/product_endpoint", int64(500), "span_001_2", "trace_001_span_2"),
		row("trace_001", int64(0), "webapp_service", "webapp_instance_1", "/item_endpoint", int64(300), "span_001_3", "trace_001_span_3"),
		row("trace_002", int64(1), "webapp_service", "webapp_instance_1", "/home_endpoint", int64(800), "span_002_1", "trace_002_span_1"),
		row("trace_002", int64(0), "webapp_service", "webapp_instance_3", "/price_endpoint", int64(200), "span_002_2", "trace_002_span_2"),
		row("trace_003", int64(1), "webapp_service", "webapp_instance_2", "/product_endpoint", int64(1200), "span_003_1", "trace_003_span_1"),
		row("trace_003", int64(0), "webapp_service", "webapp_instance_1", "/home_endpoint", int64(150), "span_003_2", "trace_003_span_2"),
		row("trace_003", int64(0), "webapp_service", "webapp_instance_3", "/price_endpoint", int64(400), "span_003_3", "trace_003_span_3"),
		row("trace_004", int64(1), "webapp_service", "webapp_instance_2", "/home_endpoint", int64(600), "span_004_1", "trace_004_span_1"),
		row("trace_005", int64(1), "webapp_service", "webapp_instance_2", "/product_endpoint", int64(900), "span_005_1", "trace_005_span_1"),
		row("trace_005", int64(0), "webapp_service", "webapp_instance_1", "/home_endpoint", int64(250), "span_005_2", "trace_005_span_2"),
		row("trace_005", int64(0), "webapp_service", "webapp_instance_3", "/price_endpoint", int64(350), "span_005_3", "trace_005_span_3"),
		row("trace_005", int64(0), "webapp_service", "webapp_instance_1", "/item_endpoint", int64(180), "span_005_4", "trace_005_span_4"),
		row("trace_001", int64(1), "api_service", "api_instance_1", "/api/v1/users", int64(750), "span_001_4", "trace_001_span_4"),
		row("trace_001", int64(0), "api_service", "api_instance_2", "/api/v1/profile", int64(320), "span_001_5", "trace_001_span_5"),
		row("trace_002", int64(1), "auth_service", "auth_instance_1", "/auth/login", int64(450), "span_002_3", "trace_002_span_3"),
		row("trace_002", int64(0), "auth_service", "auth_instance_2", "/auth/validate", int64(180), "span_002_4", "trace_002_span_4"),
		row("trace_006", int64(1), "payment_service", "payment_instance_1", "/payment/process", int64(1190), "span_006_1", "trace_006_span_1"),
		row("trace_006", int64(0), "payment_service", "payment_instance_2", "/payment/verify", int64(280), "span_006_2", "trace_006_span_2"),
		row("trace_006", int64(0), "payment_service", "payment_instance_3", "/payment/refund", int64(150), "span_006_3", "trace_006_span_3"),
		row("trace_007", int64(1), "notification_service", "notification_instance_1", "/notification/send", int64(650), "span_007_1", "trace_007_span_1"),
		row("trace_007", int64(0), "notification_service", "notification_instance_2", "/notification/email", int64(420), "span_007_2", "trace_007_span_2"),
		row("trace_008", int64(1), "database_service", "database_instance_1", "/db/query", int64(890), "span_008_1", "trace_008_span_1"),
		row("trace_008", int64(0), "database_service", "database_instance_2", "/db/transaction", int64(340), "span_008_2", "trace_008_span_2"),
		row("trace_008", int64(0), "database_service", "database_instance_3", "/db/backup", int64(2100), "span_008_3", "trace_008_span_3"),
	}
	return Trace{
		Name: "sw", Group: "test-trace-group", Groups: []string{"test-trace-group"},
		Tags: []TagDef{
			{Name: "trace_id", Type: TagTypeString, Pos: 0, Role: RoleIdentity},
			{Name: "state", Type: TagTypeInt, Pos: 1, Role: RoleIndexed},
			{Name: "service_id", Type: TagTypeString, Pos: 2, Role: RoleIndexed},
			{Name: "service_instance_id", Type: TagTypeString, Pos: 3, Role: RoleIndexed},
			{Name: "endpoint_id", Type: TagTypeString, Pos: 4, Role: RolePlain},
			{Name: "duration", Type: TagTypeInt, Pos: 5, Role: RoleIndexed},
			{Name: "span_id", Type: TagTypeString, Pos: 6, Role: RoleIdentity},
			{Name: "timestamp", Type: TagTypeTimestamp, Pos: 7, Role: RoleIndexed},
		},
		OrderRules: []string{"duration", "timestamp"}, TraceIDTag: "trace_id", SpanIDTag: "span_id", TimestampTag: "timestamp", Rows: rows,
	}
}

func row(traceID string, state int64, serviceID, serviceInstanceID, endpointID string, duration int64, spanID, span string) SpanRow {
	return SpanRow{TagValues: map[string]any{
		"trace_id": traceID, "state": state, "service_id": serviceID, "service_instance_id": serviceInstanceID,
		"endpoint_id": endpointID, "duration": duration, "span_id": spanID,
	}, Span: span}
}

func verifySeedMirror(outputDir string) error {
	traceDef := swTrace()
	parsedRows, parseErr := parseRows(filepath.Join(outputDir, "testdata", "sw.json"), filepath.Join(outputDir, "testdata", "sw_mixed_traces.json"))
	if parseErr != nil {
		return parseErr
	}
	if len(parsedRows) != len(traceDef.Rows) {
		return fmt.Errorf("seed mirror row count mismatch: code=%d json=%d", len(traceDef.Rows), len(parsedRows))
	}
	for rowIdx, parsedRow := range parsedRows {
		for _, tag := range traceDef.Tags {
			if tag.Name == "timestamp" {
				continue
			}
			codeValue := fmt.Sprint(traceDef.Rows[rowIdx].TagValues[tag.Name])
			jsonValue := fmt.Sprint(parsedRow.TagValues[tag.Name])
			if codeValue != jsonValue {
				return fmt.Errorf("seed mirror mismatch row=%d tag=%s code=%s json=%s", rowIdx, tag.Name, codeValue, jsonValue)
			}
		}
		if traceDef.Rows[rowIdx].Span != parsedRow.Span {
			return fmt.Errorf("seed mirror span mismatch row=%d code=%s json=%s", rowIdx, traceDef.Rows[rowIdx].Span, parsedRow.Span)
		}
	}
	return nil
}

func parseRows(paths ...string) ([]SpanRow, error) {
	var rows []SpanRow
	for _, path := range paths {
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil, fmt.Errorf("failed to read %s: %w", path, readErr)
		}
		var rawRows []struct {
			Span string                      `json:"span"`
			Tags []map[string]map[string]any `json:"tags"`
		}
		if jsonErr := json.Unmarshal(content, &rawRows); jsonErr != nil {
			return nil, fmt.Errorf("failed to parse %s: %w", path, jsonErr)
		}
		for _, rawRow := range rawRows {
			if len(rawRow.Tags) != 7 {
				return nil, fmt.Errorf("%s row has %d tags, want 7", path, len(rawRow.Tags))
			}
			rows = append(rows, row(
				stringValue(rawRow.Tags[0]), intValue(rawRow.Tags[1]), stringValue(rawRow.Tags[2]), stringValue(rawRow.Tags[3]),
				stringValue(rawRow.Tags[4]), intValue(rawRow.Tags[5]), stringValue(rawRow.Tags[6]), rawRow.Span,
			))
		}
	}
	return rows, nil
}

func stringValue(raw map[string]map[string]any) string {
	return fmt.Sprint(raw["str"]["value"])
}

func intValue(raw map[string]map[string]any) int64 {
	value := raw["int"]["value"]
	switch typedValue := value.(type) {
	case float64:
		return int64(typedValue)
	case int64:
		return typedValue
	default:
		return 0
	}
}
