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

// TagType describes a stream tag value type.
type TagType int

const (
	// TagTypeString is a single string tag.
	TagTypeString TagType = iota
	// TagTypeInt is a single integer tag.
	TagTypeInt
	// TagTypeStringArray is a string-array tag.
	TagTypeStringArray
	// TagTypeBinary is the opaque data_binary tag.
	TagTypeBinary
)

// IndexType describes the index rule kind bound to a tag, if any.
type IndexType int

const (
	// IndexNone means the tag has no index rule.
	IndexNone IndexType = iota
	// IndexInverted maps to TYPE_INVERTED index rules.
	IndexInverted
	// IndexSkipping maps to TYPE_SKIPPING index rules.
	IndexSkipping
)

// TagDef describes a stream tag in positional seed order within its family.
type TagDef struct {
	Name     string
	Family   string
	Analyzer string
	Type     TagType
	Index    IndexType
	Pos      int
}

// ElementRow mirrors one seeded stream element row used for generation value selection.
// Values holds the searchable-family tag values in positional order; data_binary is
// written separately by the harness and is not part of the mirror.
type ElementRow struct {
	Values []any
}

// Stream describes the stream schema and rows used by generated tests.
type Stream struct {
	Name         string
	DefaultGroup string
	Groups       []string
	Tags         []TagDef
	OrderRules   []string
	EntityTags   []string
	DefaultRows  []ElementRow
	UpdatedRows  []ElementRow
}

const (
	familyData       = "data"
	familySearchable = "searchable"
	analyzerURL      = "url"
)

// SeedData returns stream schemas available to the generator.
func SeedData() []Stream {
	return []Stream{swStream()}
}

// FindStream returns a stream schema by name.
func FindStream(name string) *Stream {
	streams := SeedData()
	for idx := range streams {
		if streams[idx].Name == name {
			return &streams[idx]
		}
	}
	return nil
}

// IsEntityTag reports whether a tag participates in the stream's entity, which
// the series index filters only via EQ and IN. Any other operation on an entity
// tag is rejected by the engine with ErrUnsupportedConditionOp.
func (s *Stream) IsEntityTag(name string) bool {
	return slices.Contains(s.EntityTags, name)
}

// IndexedTags returns the names of tags that carry an index rule.
func (s *Stream) IndexedTags() []string {
	var names []string
	for _, tag := range s.Tags {
		if tag.Index != IndexNone {
			names = append(names, tag.Name)
		}
	}
	return names
}

// KnownValues returns distinct known string-rendered values for a searchable tag in the default group.
func (s *Stream) KnownValues(tagName string) []string {
	pos := -1
	for _, tag := range s.Tags {
		if tag.Family == familySearchable && tag.Name == tagName {
			pos = tag.Pos
			break
		}
	}
	if pos < 0 {
		return nil
	}
	seen := make(map[string]bool)
	var values []string
	for _, row := range s.DefaultRows {
		if pos >= len(row.Values) {
			continue
		}
		valueText := fmt.Sprint(row.Values[pos])
		if seen[valueText] {
			continue
		}
		seen[valueText] = true
		values = append(values, valueText)
	}
	slices.Sort(values)
	return values
}

func swStream() Stream {
	return Stream{
		Name:         "sw",
		DefaultGroup: "default",
		Groups:       []string{"default", "updated"},
		Tags: []TagDef{
			{Name: "data_binary", Family: familyData, Type: TagTypeBinary, Index: IndexNone, Pos: 0},
			{Name: "trace_id", Family: familySearchable, Type: TagTypeString, Index: IndexSkipping, Pos: 0},
			{Name: "state", Family: familySearchable, Type: TagTypeInt, Index: IndexNone, Pos: 1},
			{Name: "service_id", Family: familySearchable, Type: TagTypeString, Index: IndexNone, Pos: 2},
			{Name: "service_instance_id", Family: familySearchable, Type: TagTypeString, Index: IndexNone, Pos: 3},
			{Name: "endpoint_id", Family: familySearchable, Type: TagTypeString, Index: IndexSkipping, Pos: 4},
			{Name: "duration", Family: familySearchable, Type: TagTypeInt, Index: IndexInverted, Pos: 5},
			{Name: "start_time", Family: familySearchable, Type: TagTypeInt, Index: IndexNone, Pos: 6},
			{Name: "http.method", Family: familySearchable, Type: TagTypeString, Index: IndexSkipping, Pos: 7},
			{Name: "status_code", Family: familySearchable, Type: TagTypeInt, Index: IndexSkipping, Pos: 8},
			{Name: "span_id", Family: familySearchable, Type: TagTypeString, Index: IndexNone, Pos: 9},
			{Name: "db.type", Family: familySearchable, Type: TagTypeString, Index: IndexSkipping, Pos: 10},
			{Name: "db.instance", Family: familySearchable, Type: TagTypeString, Analyzer: analyzerURL, Index: IndexInverted, Pos: 11},
			{Name: "mq.queue", Family: familySearchable, Type: TagTypeString, Index: IndexSkipping, Pos: 12},
			{Name: "mq.topic", Family: familySearchable, Type: TagTypeString, Index: IndexSkipping, Pos: 13},
			{Name: "mq.broker", Family: familySearchable, Type: TagTypeString, Index: IndexSkipping, Pos: 14},
			{Name: "extended_tags", Family: familySearchable, Type: TagTypeStringArray, Index: IndexInverted, Pos: 15},
			{Name: "non_indexed_tags", Family: familySearchable, Type: TagTypeStringArray, Index: IndexNone, Pos: 16},
		},
		OrderRules: []string{"duration"},
		EntityTags: []string{"service_id", "service_instance_id", "state"},
		DefaultRows: []ElementRow{
			{Values: []any{"1", int64(1), "webapp_id", "10.0.0.1_id", "/home_id", int64(1000), int64(1622933202000000000), nil, nil, "1"}},
			{Values: []any{
				"2", int64(1), "webapp_id", "10.0.0.3_id", "/product_id", int64(500), int64(1622933202000000000), "", nil, "1",
				"mysql", "jdbc:mysql://localhost:3306/bar",
			}},
			{Values: []any{
				"3", int64(0), "webapp_id", "10.0.0.1_id", "/home_id", int64(30), int64(1622933202000000000), "GET", int64(500), "2",
				"mysql", "jdbc:mysql://test:3306/bar", nil, nil, nil,
				[]string{"c"},
				[]string{"c"},
			}},
			{Values: []any{
				"4", int64(0), "webapp_id", "10.0.0.5_id", "/price_id", int64(60), int64(1622933202000000000), "GET", int64(400), "2",
				"postgresql", "jdbc:postgresql://test:5432/bar", nil, nil, nil,
				[]string{"b", "c"},
				[]string{"b", "c"},
			}},
			{Values: []any{
				"5", int64(0), "webapp_id", "10.0.0.1_id", "/item_id", int64(300), int64(1622933202000000000), "GET", int64(500), "3",
				nil, nil, nil, nil, nil,
				[]string{"a", "b", "c"},
				[]string{"a", "b", "c"},
			}},
		},
		UpdatedRows: []ElementRow{
			{Values: []any{
				"instance_1", "trace_001", int64(1234), int64(1), "service_1", "/api/v1", "GET", "span_1",
				"mysql", "db1", "topic1", "broker1",
				[]string{"tagA", "tagB"},
				"new_value", "200",
			}},
			{Values: []any{
				"instance_2", "trace_002", int64(5678), int64(0), "service_2", "/api/v2", "POST", "span_2",
				"postgresql", "db2", "topic2", "broker2",
				[]string{"tagC"},
				"another_value", "404",
			}},
			{Values: []any{
				"instance_3", "trace_003", int64(91011), int64(1), "service_3", "/api/v3", "PUT", "span_3",
				"mongodb", "db3", "topic3", "broker3",
				[]string{"tagD", "tagE", "tagF"},
				"test_tag", "500",
			}},
		},
	}
}

// verifySeedMirror reads both the default-group testdata (sw.json) and the
// updated-group testdata (sw_updated.json) and validates the code seed matches
// the JSON fixtures row-for-row and value-for-value.
func verifySeedMirror(outputDir string) error {
	streamDef := swStream()
	defaultPath := filepath.Join(outputDir, "testdata", "sw.json")
	updatedPath := filepath.Join(outputDir, "testdata", "sw_updated.json")
	if mirrorErr := compareRows(defaultPath, streamDef.DefaultRows); mirrorErr != nil {
		return mirrorErr
	}
	return compareRows(updatedPath, streamDef.UpdatedRows)
}

func compareRows(path string, codeRows []ElementRow) error {
	parsedRows, parseErr := parseRows(path)
	if parseErr != nil {
		return parseErr
	}
	if len(parsedRows) != len(codeRows) {
		return fmt.Errorf("seed mirror row count mismatch for %s: code=%d json=%d", path, len(codeRows), len(parsedRows))
	}
	for rowIdx, parsedRow := range parsedRows {
		codeRow := codeRows[rowIdx]
		if len(parsedRow.Values) != len(codeRow.Values) {
			return fmt.Errorf("seed mirror tag count mismatch in %s row=%d: code=%d json=%d", path, rowIdx, len(codeRow.Values), len(parsedRow.Values))
		}
		for colIdx := range parsedRow.Values {
			codeValue := fmt.Sprint(codeRow.Values[colIdx])
			jsonValue := fmt.Sprint(parsedRow.Values[colIdx])
			if codeValue != jsonValue {
				return fmt.Errorf("seed mirror mismatch in %s row=%d col=%d code=%s json=%s", path, rowIdx, colIdx, codeValue, jsonValue)
			}
		}
	}
	return nil
}

func parseRows(path string) ([]ElementRow, error) {
	content, readErr := os.ReadFile(path)
	if readErr != nil {
		return nil, fmt.Errorf("failed to read %s: %w", path, readErr)
	}
	var rawRows []struct {
		Tags []map[string]json.RawMessage `json:"tags"`
	}
	if jsonErr := json.Unmarshal(content, &rawRows); jsonErr != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", path, jsonErr)
	}
	rows := make([]ElementRow, 0, len(rawRows))
	for _, rawRow := range rawRows {
		values := make([]any, 0, len(rawRow.Tags))
		for _, rawTag := range rawRow.Tags {
			parsedValue, valueErr := parseTagValue(rawTag)
			if valueErr != nil {
				return nil, fmt.Errorf("failed to parse tag in %s: %w", path, valueErr)
			}
			values = append(values, parsedValue)
		}
		rows = append(rows, ElementRow{Values: values})
	}
	return rows, nil
}

func parseTagValue(raw map[string]json.RawMessage) (any, error) {
	if payload, ok := raw["str"]; ok {
		var wrapped struct {
			Value string `json:"value"`
		}
		if err := json.Unmarshal(payload, &wrapped); err != nil {
			return nil, err
		}
		return wrapped.Value, nil
	}
	if payload, ok := raw["int"]; ok {
		var wrapped struct {
			Value int64 `json:"value"`
		}
		if err := json.Unmarshal(payload, &wrapped); err != nil {
			return nil, err
		}
		return wrapped.Value, nil
	}
	if payload, ok := raw["str_array"]; ok {
		var wrapped struct {
			Value []string `json:"value"`
		}
		if err := json.Unmarshal(payload, &wrapped); err != nil {
			return nil, err
		}
		return wrapped.Value, nil
	}
	if _, ok := raw["null"]; ok {
		return nil, nil
	}
	return nil, fmt.Errorf("unknown tag value shape")
}
