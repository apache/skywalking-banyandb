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
	"unicode"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// seedRow pairs a mirrored seed row with the searchable-tag position map of the
// group it belongs to, so a tag name resolves against the group's own schema.
type seedRow struct {
	posByName map[string]int
	values    []any
}

// defaultTagOrder is the searchable-family tag order of the "default" group's sw
// schema (pkg/test/stream/testdata/streams/sw.json), mirrored by DefaultRows.
var defaultTagOrder = []string{
	"trace_id", "state", "service_id", "service_instance_id", "endpoint_id",
	"duration", "start_time", "http.method", "status_code", "span_id",
	"db.type", "db.instance", "mq.queue", "mq.topic", "mq.broker",
	"extended_tags", "non_indexed_tags",
}

// updatedTagOrder is the searchable-family tag order of the "updated" group's sw
// schema (pkg/test/stream/testdata/streams/sw_updated.json), mirrored by
// UpdatedRows. It differs from the default group's order and tag set.
var updatedTagOrder = []string{
	"service_instance_id", "trace_id", "duration", "state", "service_id",
	"endpoint_id", "http.method", "span_id", "db.type", "db.instance",
	"mq.topic", "mq.broker", "extended_tags", "new_tag", "status_code",
}

// expectEmpty evaluates a generated case's query request against the mirrored
// seed rows for its groups and reports whether the final result set is empty.
// It replaces capture-authoritative reconciliation with a deterministic
// prediction derived from the in-code seed.
func expectEmpty(tc *TestCase) bool {
	streamDef := FindStream("sw")
	if streamDef == nil {
		return true
	}
	req := tc.Request
	rows := selectRows(streamDef, req.GetGroups())
	matched := 0
	for rowIdx := range rows {
		if matchCriteria(&rows[rowIdx], req.GetCriteria()) {
			matched++
		}
	}
	finalCount := applyOffsetLimit(matched, int(req.GetOffset()), int(req.GetLimit()))
	return finalCount == 0
}

// selectRows returns the union of mirrored seed rows for the requested groups,
// each carrying its own group's tag-position map.
func selectRows(streamDef *Stream, groups []string) []seedRow {
	defaultPos := positionMap(defaultTagOrder)
	updatedPos := positionMap(updatedTagOrder)
	var rows []seedRow
	for _, group := range groups {
		switch group {
		case "default":
			for _, row := range streamDef.DefaultRows {
				rows = append(rows, seedRow{values: row.Values, posByName: defaultPos})
			}
		case "updated":
			for _, row := range streamDef.UpdatedRows {
				rows = append(rows, seedRow{values: row.Values, posByName: updatedPos})
			}
		}
	}
	return rows
}

// positionMap builds a tag-name to positional-index map from an ordered tag list.
func positionMap(order []string) map[string]int {
	positions := make(map[string]int, len(order))
	for idx, name := range order {
		positions[name] = idx
	}
	return positions
}

// applyOffsetLimit applies OFFSET (skip) then LIMIT (cap) to a matched count.
func applyOffsetLimit(matched, offset, limit int) int {
	remaining := matched - offset
	if remaining < 0 {
		remaining = 0
	}
	if limit > 0 && remaining > limit {
		remaining = limit
	}
	return remaining
}

// rowValue returns the row's typed value for a searchable tag name, using its
// group's positional layout. A missing/short/null slot yields nil.
func rowValue(row *seedRow, tagName string) any {
	pos, ok := row.posByName[tagName]
	if !ok || pos >= len(row.values) {
		return nil
	}
	return row.values[pos]
}

// matchCriteria recursively evaluates a criteria tree against a single row.
// A nil criteria matches every row.
func matchCriteria(row *seedRow, criteria *modelv1.Criteria) bool {
	if criteria == nil {
		return true
	}
	switch exp := criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		return matchCondition(row, exp.Condition)
	case *modelv1.Criteria_Le:
		return matchLogical(row, exp.Le)
	default:
		return false
	}
}

// matchLogical combines child results with the AND/OR logical operator.
func matchLogical(row *seedRow, le *modelv1.LogicalExpression) bool {
	left := matchCriteria(row, le.GetLeft())
	right := matchCriteria(row, le.GetRight())
	if le.GetOp() == modelv1.LogicalExpression_LOGICAL_OP_OR {
		return left || right
	}
	return left && right
}

// matchCondition evaluates one leaf predicate against the row's tag value.
func matchCondition(row *seedRow, cond *modelv1.Condition) bool {
	value := rowValue(row, cond.GetName())
	switch cond.GetOp() {
	case modelv1.Condition_BINARY_OP_EQ:
		return value != nil && compareTag(value, cond.GetValue()) == 0
	case modelv1.Condition_BINARY_OP_NE:
		return value != nil && compareTag(value, cond.GetValue()) != 0
	case modelv1.Condition_BINARY_OP_LT:
		return value != nil && compareTag(value, cond.GetValue()) < 0
	case modelv1.Condition_BINARY_OP_GT:
		return value != nil && compareTag(value, cond.GetValue()) > 0
	case modelv1.Condition_BINARY_OP_LE:
		return value != nil && compareTag(value, cond.GetValue()) <= 0
	case modelv1.Condition_BINARY_OP_GE:
		return value != nil && compareTag(value, cond.GetValue()) >= 0
	case modelv1.Condition_BINARY_OP_IN:
		return value != nil && inSet(value, cond.GetValue())
	case modelv1.Condition_BINARY_OP_NOT_IN:
		return value != nil && !inSet(value, cond.GetValue())
	case modelv1.Condition_BINARY_OP_HAVING:
		return value != nil && having(value, cond.GetValue())
	case modelv1.Condition_BINARY_OP_NOT_HAVING:
		// A row that lacks the array tag (nil) does not contain the queried
		// values, so NOT_HAVING matches it (verified against captured want data:
		// NOT_HAVING('c') returns the two rows with no extended_tags).
		return !having(value, cond.GetValue())
	case modelv1.Condition_BINARY_OP_MATCH:
		return value != nil && match(value, cond.GetValue())
	case modelv1.Condition_BINARY_OP_UNSPECIFIED:
		return false
	default:
		return false
	}
}

// compareTag returns -1/0/1 comparing a seed row value against a query TagValue.
// INT tags compare numerically; STRING tags compare lexicographically. A type
// mismatch reports non-equal (1) so equality never spuriously holds.
func compareTag(rowVal any, query *modelv1.TagValue) int {
	switch q := query.GetValue().(type) {
	case *modelv1.TagValue_Int:
		rowInt, ok := rowVal.(int64)
		if !ok {
			return 1
		}
		switch {
		case rowInt < q.Int.GetValue():
			return -1
		case rowInt > q.Int.GetValue():
			return 1
		default:
			return 0
		}
	case *modelv1.TagValue_Str:
		rowStr, ok := rowVal.(string)
		if !ok {
			return 1
		}
		return strings.Compare(rowStr, q.Str.GetValue())
	default:
		return 1
	}
}

// inSet reports whether a typed row value is a member of an IN/NOT_IN operand set.
func inSet(rowVal any, query *modelv1.TagValue) bool {
	switch q := query.GetValue().(type) {
	case *modelv1.TagValue_IntArray:
		rowInt, ok := rowVal.(int64)
		if !ok {
			return false
		}
		for _, candidate := range q.IntArray.GetValue() {
			if candidate == rowInt {
				return true
			}
		}
		return false
	case *modelv1.TagValue_StrArray:
		rowStr, ok := rowVal.(string)
		if !ok {
			return false
		}
		for _, candidate := range q.StrArray.GetValue() {
			if candidate == rowStr {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// having reports whether a STRING_ARRAY row value contains ALL of the query
// operand values (the semantics of BINARY_OP_HAVING).
func having(rowVal any, query *modelv1.TagValue) bool {
	rowArr, ok := rowVal.([]string)
	if !ok {
		return false
	}
	present := make(map[string]bool, len(rowArr))
	for _, item := range rowArr {
		present[item] = true
	}
	for _, wanted := range query.GetStrArray().GetValue() {
		if !present[wanted] {
			return false
		}
	}
	return true
}

// match approximates full-text MATCH on an analyzed (analyzer=url) tag: the row
// value is tokenized on non-alphanumeric runes and lowercased; MATCH holds iff
// every lowercased query term is present as a token.
func match(rowVal any, query *modelv1.TagValue) bool {
	rowStr, ok := rowVal.(string)
	if !ok {
		return false
	}
	tokens := tokenize(rowStr)
	for _, term := range queryTerms(query) {
		if !tokens[strings.ToLower(term)] {
			return false
		}
	}
	return len(queryTerms(query)) > 0
}

// queryTerms extracts the raw MATCH query terms from a str or str-array operand.
func queryTerms(query *modelv1.TagValue) []string {
	switch q := query.GetValue().(type) {
	case *modelv1.TagValue_Str:
		return tokenizeList(q.Str.GetValue())
	case *modelv1.TagValue_StrArray:
		return q.StrArray.GetValue()
	default:
		return nil
	}
}

// tokenizeList splits a MATCH string operand into terms on non-alphanumeric runes.
func tokenizeList(text string) []string {
	fields := strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	return fields
}

// tokenize returns the lowercased alphanumeric token set of a tag value.
func tokenize(text string) map[string]bool {
	tokens := make(map[string]bool)
	for _, field := range tokenizeList(text) {
		tokens[strings.ToLower(field)] = true
	}
	return tokens
}

// predictFlags overrides each case's WantEmpty with the deterministic seed
// evaluation and keeps WantErr driven by the gen_err_ name convention.
// DisOrder and IgnoreElementID from the layers are left untouched.
func predictFlags(cases []TestCase) {
	for caseIdx := range cases {
		tc := &cases[caseIdx]
		if strings.HasPrefix(tc.Name, "gen_err_") {
			tc.WantErr = true
			tc.WantEmpty = false
			continue
		}
		tc.WantErr = false
		tc.WantEmpty = expectEmpty(tc)
	}
}
