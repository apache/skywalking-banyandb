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
	"regexp"
	"strconv"
	"strings"
	"time"
)

var timeToken = regexp.MustCompile(`\{\{(time|nanos|unix_nanos):([^{}]+)\}\}`)

// Render replaces time offsets in queries, bound parameters, or expected JSON.
func Render(template string, base time.Time) (string, error) {
	for _, line := range strings.Split(template, "\n") {
		if strings.TrimSpace(line) == "#!fixture-domain-interior: true" {
			minimum := time.Date(1677, 9, 21, 0, 12, 43, 146_000_000, time.UTC)
			maximum := time.Date(2262, 4, 11, 23, 47, 16, 854_000_000, time.UTC)
			if !base.After(minimum) || !base.Add(129*time.Second).Before(maximum) {
				return "", fmt.Errorf("fixture base must lie strictly inside the generated storage-domain bounds")
			}
		}
		if alignment, found := strings.CutPrefix(strings.TrimSpace(line), "#!base-alignment-ms:"); found {
			width, err := strconv.ParseInt(strings.TrimSpace(alignment), 10, 64)
			if err != nil || width <= 0 || base.UnixMilli()%width != 0 {
				return "", fmt.Errorf("bucket fixture base must align to %s milliseconds", alignment)
			}
		}
	}
	var renderErr error
	rendered := timeToken.ReplaceAllStringFunc(template, func(token string) string {
		kind, value, _ := strings.Cut(strings.TrimSuffix(strings.TrimPrefix(token, "{{"), "}}"), ":")
		millis, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			renderErr = fmt.Errorf("invalid time token %q: %w", token, err)
			return token
		}
		unit := time.Millisecond
		if kind == "nanos" {
			unit = time.Nanosecond
		}
		if millis > (1<<63-1)/int64(unit) || millis < (-1<<63)/int64(unit) {
			renderErr = fmt.Errorf("time offset overflows duration: %d", millis)
			return token
		}
		timestamp := base.Add(time.Duration(millis) * unit).UTC()
		if kind == "unix_nanos" {
			return strconv.FormatInt(timestamp.UnixNano(), 10)
		}
		return timestamp.Format(time.RFC3339Nano)
	})
	if renderErr != nil {
		return "", renderErr
	}
	if strings.Contains(rendered, "{{") {
		return "", fmt.Errorf("unresolved template token in %q", rendered)
	}
	return rendered, nil
}

type queryRenderer struct {
	params []any
	bind   bool
}

func renderQuery(q Query) (string, error) {
	r := queryRenderer{bind: q.Bind}
	columns := append([]string(nil), q.Columns...)
	if len(columns) == 0 {
		return "", fmt.Errorf("projection must be explicit")
	}
	if q.Aggregate != "" {
		function := strings.ToUpper(q.Aggregate)
		if function == "COUNT_DISTINCT" {
			columns = append(columns, "COUNT(DISTINCT "+q.Target+")")
		} else {
			columns = append(columns, function+"("+q.Target+")")
		}
	}
	projection := strings.Join(columns, ", ")
	if q.Top != nil {
		projection = "TOP " + r.integer(int64(*q.Top)) + " " + q.Target + " " + direction(q.Direction) + ", " + projection
	}
	groups := FixtureGroup
	if q.ParenthesizedGroups {
		groups = "(" + groups + ")"
	}
	text := "SELECT " + projection + " FROM MEASURE " + FixtureName + " IN " + groups + " TIME "
	switch strings.ToUpper(q.TimeOp) {
	case "", "BETWEEN":
		text += "BETWEEN " + r.value(timePlaceholder(q.BeginMillis)) + " AND " + r.value(timePlaceholder(q.EndMillis))
	case "=", ">", ">=", "<", "<=":
		text += q.TimeOp + " " + r.value(timePlaceholder(q.BeginMillis))
	default:
		return "", fmt.Errorf("unsupported TIME operator %q", q.TimeOp)
	}
	if q.Where != nil {
		predicate, err := r.predicate(q.Where)
		if err != nil {
			return "", err
		}
		text += " WHERE " + predicate
	}
	groupBy := append([]string(nil), q.GroupBy...)
	if q.BucketMillis > 0 {
		groupBy = append([]string{fmt.Sprintf("TIME_BUCKET('%dms')", q.BucketMillis)}, groupBy...)
	}
	if len(groupBy) > 0 {
		text += " GROUP BY " + strings.Join(groupBy, ", ")
	}
	if q.OrderBy != "" {
		text += " ORDER BY " + q.OrderBy + " " + direction(q.Direction)
	}
	if q.Trace {
		text += " WITH QUERY_TRACE"
	}
	if q.Limit != nil {
		text += " LIMIT " + r.integer(int64(*q.Limit))
	}
	if q.Offset > 0 {
		text += " OFFSET " + r.integer(int64(q.Offset))
	}
	if q.BucketMillis > 0 {
		text += fmt.Sprintf("\n#!base-alignment-ms: %d", q.BucketMillis)
	}
	if len(r.params) > 0 {
		data, err := json.Marshal(r.params)
		if err != nil {
			return "", err
		}
		text += "\n#!params: " + string(data)
	}
	return text, nil
}

func timePlaceholder(millis int64) string { return fmt.Sprintf("{{time:%d}}", millis) }
func direction(value string) string {
	if value == "" {
		return "ASC"
	}
	return strings.ToUpper(value)
}

func (r *queryRenderer) integer(value int64) string {
	if r.bind {
		r.params = append(r.params, jsonValue(value))
		return "?"
	}
	return strconv.FormatInt(value, 10)
}

func (r *queryRenderer) value(value any) string {
	if r.bind {
		r.params = append(r.params, jsonValue(value))
		return "?"
	}
	switch typed := value.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(typed, "'", "\\'") + "'"
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	default:
		panic(fmt.Sprintf("validated literal has unsupported type %T", value))
	}
}

func (r *queryRenderer) predicate(p *Predicate) (string, error) {
	op := strings.ToUpper(p.Op)
	if op == "AND" || op == "OR" {
		if len(p.Children) < 2 {
			return "", fmt.Errorf("%s needs at least two children", op)
		}
		parts := make([]string, 0, len(p.Children))
		for _, child := range p.Children {
			if child == nil {
				return "", fmt.Errorf("nil predicate")
			}
			part, err := r.predicate(child)
			if err != nil {
				return "", err
			}
			parts = append(parts, part)
		}
		return "(" + strings.Join(parts, " "+op+" ") + ")", nil
	}
	if len(p.Values) == 0 && op != "IN" && op != "NOT IN" {
		return "", fmt.Errorf("%s predicate has no values", op)
	}
	values := make([]string, 0, len(p.Values))
	for _, value := range p.Values {
		values = append(values, r.value(value))
	}
	switch op {
	case "=", "!=", "<", "<=", ">", ">=":
		if len(values) != 1 {
			return "", fmt.Errorf("comparison needs exactly one value")
		}
		return p.Column + " " + op + " " + values[0], nil
	case "IN", "NOT IN", "HAVING", "NOT HAVING", "MATCH":
		return p.Column + " " + op + " (" + strings.Join(values, ", ") + ")", nil
	default:
		return "", fmt.Errorf("unsupported predicate %q", op)
	}
}

func jsonValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return map[string]any{"null": 0}
	case string:
		return map[string]any{"str": map[string]any{"value": typed}}
	case int:
		return map[string]any{"int": map[string]any{"value": strconv.Itoa(typed)}}
	case int64:
		return map[string]any{"int": map[string]any{"value": strconv.FormatInt(typed, 10)}}
	case float64:
		return map[string]any{"float": map[string]any{"value": typed}}
	case []string:
		if typed == nil {
			return map[string]any{"null": 0}
		}
		return map[string]any{"strArray": map[string]any{"value": typed}}
	case []int64:
		if typed == nil {
			return map[string]any{"null": 0}
		}
		values := make([]string, len(typed))
		for i, v := range typed {
			values[i] = strconv.FormatInt(v, 10)
		}
		return map[string]any{"intArray": map[string]any{"value": values}}
	default:
		panic(fmt.Sprintf("unsupported JSON value %T", value))
	}
}
