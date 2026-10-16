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

package qlgen

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// BuildT03Case independently evaluates a generated typed raw-query contract.
func BuildT03Case(name string, q T03Query, features ...string) (Case, error) {
	if name == "" || len(q.Columns) == 0 {
		return Case{}, fmt.Errorf("case name and projection are required")
	}
	var points []any
	rows := t03Rows()
	if q.Descending {
		slices.Reverse(rows)
	}
	for _, row := range rows {
		if q.LegacyBegin != nil && (row.Millis < *q.LegacyBegin || row.Millis > q.LegacyEnd) {
			continue
		}
		match, err := matchT03Time(q.Where, row.Millis*1_000_000, q.ClockMillis)
		if err != nil {
			return Case{}, err
		}
		if !match {
			continue
		}
		point, err := projectT03(row, q.Columns)
		if err != nil {
			return Case{}, err
		}
		points = append(points, point)
	}
	if uint64(q.Offset) >= uint64(len(points)) {
		points = nil
	} else {
		points = points[q.Offset:]
	}
	limit := len(points)
	if q.Limit != nil {
		limit = int(*q.Limit)
	}
	if q.Legacy && (q.Limit == nil || *q.Limit == 0) {
		limit = 100
	}
	if len(points) > limit {
		points = points[:limit]
	}
	want := map[string]any{}
	if len(points) > 0 {
		want["dataPoints"] = points
	}
	content, err := json.Marshal(want)
	if err != nil {
		return Case{}, err
	}
	text, err := renderT03(q)
	if err != nil {
		return Case{}, err
	}
	return Case{
		Name: name, Ticket: "T03", Query: text, WantJSON: content, Features: features,
		Ordered: true, Trace: q.Trace, ClockMillis: q.ClockMillis,
		NoScan: q.NoScan || q.Limit != nil && *q.Limit == 0 && !q.Legacy,
	}, nil
}

func matchT03Time(p *T03Time, nanos int64, clock *int64) (bool, error) {
	if p == nil {
		return true, nil
	}
	if p.Op == "AND" || p.Op == "OR" {
		result := p.Op == "AND"
		for _, child := range p.Children {
			value, err := matchT03Time(child, nanos, clock)
			if err != nil {
				return false, err
			}
			if p.Op == "AND" {
				result = result && value
			} else {
				result = result || value
			}
		}
		return result, nil
	}
	if p.Absolute != "" {
		// Absolute extremum nodes use a symbolic fixture base. Render verifies that
		// every fixture row lies strictly inside these independently specified bounds.
		comparison := 0
		switch p.Absolute {
		case "1677-09-21T00:12:43.146Z":
			comparison = 1
		case "2262-04-11T23:47:16.854Z":
			comparison = -1
		default:
			return false, fmt.Errorf("absolute oracle supports documented storage extrema only")
		}
		return compareResult(p.Op, comparison), nil
	}
	bound := p.Nanos
	if p.Now {
		if clock == nil {
			return false, fmt.Errorf("now requires controlled execution clock")
		}
		bound += *clock * 1_000_000
	}
	comparison := 0
	if nanos < bound {
		comparison = -1
	} else if nanos > bound {
		comparison = 1
	}
	switch p.Op {
	case "=", "!=", "<", "<=", ">", ">=":
		return compareResult(p.Op, comparison), nil
	default:
		return false, fmt.Errorf("unknown time operator %q", p.Op)
	}
}

func projectT03(row fixtureRow, columns []T03Column) (map[string]any, error) {
	point := map[string]any{"timestamp": timePlaceholder(row.Millis)}
	families := map[string][]any{}
	var familyNames []string
	var fields []any
	seen := map[string]bool{}
	for _, column := range columns {
		source, annotation, _ := strings.Cut(column.Source, "::")
		family, name, qualified := strings.Cut(source, ".")
		if !qualified {
			name = family
			family = "default"
		}
		output := column.Alias
		if output == "" {
			output = name
		}
		container := "tag:" + family
		var value any
		switch name {
		case "time":
			if column.Alias == "" {
				continue
			}
			family = "__ql"
			container = "tag:" + family
			value = map[string]any{"timestamp": timePlaceholder(row.Millis)}
		case "v", "f":
			container = "fields"
			value = jsonValue(rowValue(row, name))
		case "clash":
			switch {
			case strings.EqualFold(annotation, "FIELD"):
				container = "fields"
				value = jsonValue(row.RID * 10)
			case strings.EqualFold(annotation, "TAG"):
				value = jsonValue("tag-clash")
			default:
				return nil, fmt.Errorf("ambiguous projection")
			}
		case "location":
			family = "extra"
			container = "tag:" + family
			value = jsonValue("location-" + row.Host)
		case "clock":
			family = "__ql"
			container = "tag:" + family
			value = jsonValue("stored-clock")
		case "rid", "host", "zone":
			value = jsonValue(rowValue(row, name))
		default:
			return nil, fmt.Errorf("unknown T03 projection %q", column.Source)
		}
		key := container + ":" + output
		if seen[key] {
			return nil, fmt.Errorf("duplicate output name %s", key)
		}
		seen[key] = true
		if container == "fields" {
			fields = append(fields, map[string]any{"name": output, "value": value})
			continue
		}
		if _, exists := families[family]; !exists {
			familyNames = append(familyNames, family)
		}
		families[family] = append(families[family], map[string]any{"key": output, "value": value})
	}
	if len(fields) > 0 {
		point["fields"] = fields
	}
	if len(familyNames) > 0 {
		var tags []any
		for _, family := range familyNames {
			tags = append(tags, map[string]any{"name": family, "tags": families[family]})
		}
		point["tagFamilies"] = tags
	}
	return point, nil
}
