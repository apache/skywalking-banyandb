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
	"fmt"
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	corebydbql "github.com/apache/skywalking-banyandb/pkg/bydbql"
)

// validateTraceScanBounds rejects a TRACE query that BanyanDB cannot plan a scan for.
//
// A TRACE scan needs an entry point: either an ORDER BY that names a sortable index, or an equality
// filter on the trace-ID tag. Without one, banyand/trace rejects the query as an internal error at
// execution time, which reaches the user as an opaque HTTP 500 instead of a fixable diagnostic.
func validateTraceScanBounds(query string, schema *session.SchemaSnapshot) string {
	grammar, parseErr := corebydbql.ParseQuery(query)
	if parseErr != nil || grammar == nil || grammar.Select == nil {
		return ""
	}
	if !strings.EqualFold(strings.TrimSpace(grammar.Select.From.ResourceType), session.ResourceTypeTrace.String()) {
		return ""
	}
	if grammar.Select.OrderBy != nil {
		return ""
	}
	traceIDTag := schema.TraceIDTagName()
	if traceIDTag == "" {
		return "TRACE queries need ORDER BY on a sortable index, or an equality filter on the trace ID tag; " +
			"call describe_schema to learn which tag holds the trace ID"
	}
	if selectFiltersTraceID(grammar.Select, traceIDTag) {
		return ""
	}
	if suggestion := traceOrderSuggestion(schema); suggestion != "" {
		return fmt.Sprintf("TRACE queries need ORDER BY or a %s filter; add ORDER BY %s DESC or WHERE %s = '<id>'",
			traceIDTag, suggestion, traceIDTag)
	}
	return fmt.Sprintf("TRACE queries need ORDER BY or a %s filter, and this resource has no sortable index rule; "+
		"add WHERE %s = '<id>'", traceIDTag, traceIDTag)
}

// traceOrderSuggestion names a sortable index rule the query could order by.
func traceOrderSuggestion(schema *session.SchemaSnapshot) string {
	ruleNames := sortableIndexRuleNames(schema)
	if len(ruleNames) == 0 {
		return ""
	}
	timestampTag := ""
	if schema != nil {
		timestampTag = strings.TrimSpace(schema.TimestampTag)
	}
	if timestampTag != "" {
		for _, ruleName := range ruleNames {
			if strings.EqualFold(ruleName, timestampTag) {
				return ruleName
			}
		}
	}
	return ruleNames[0]
}

// selectFiltersTraceID reports whether the WHERE clause pins the trace ID with equality or IN.
//
// The server collects trace IDs from either branch of an AND or an OR, so the whole tree is walked
// rather than only its top-level conjunction.
func selectFiltersTraceID(selectClause *corebydbql.GrammarSelectStatement, traceIDTag string) bool {
	if selectClause == nil || selectClause.Where == nil {
		return false
	}
	return orExpressionPinsTag(selectClause.Where.Expr, traceIDTag)
}

func orExpressionPinsTag(expression *corebydbql.GrammarOrExpr, tagName string) bool {
	if expression == nil {
		return false
	}
	if andExpressionPinsTag(expression.Left, tagName) {
		return true
	}
	for _, right := range expression.Right {
		if right != nil && andExpressionPinsTag(right.Right, tagName) {
			return true
		}
	}
	return false
}

func andExpressionPinsTag(expression *corebydbql.GrammarAndExpr, tagName string) bool {
	if expression == nil {
		return false
	}
	if predicatePinsTag(expression.Left, tagName) {
		return true
	}
	for _, right := range expression.Right {
		if right != nil && predicatePinsTag(right.Right, tagName) {
			return true
		}
	}
	return false
}

// predicatePinsTag reports whether one predicate constrains tagName to specific values.
func predicatePinsTag(predicate *corebydbql.GrammarPredicate, tagName string) bool {
	if predicate == nil {
		return false
	}
	if predicate.Paren != nil {
		return orExpressionPinsTag(predicate.Paren, tagName)
	}
	switch {
	case predicate.In != nil:
		return predicate.In.Not == nil && identifierMatches(predicate.In.Identifier, tagName)
	case predicate.Binary != nil:
		if predicate.Binary.Tail == nil || predicate.Binary.Tail.Compare == nil {
			return false
		}
		return predicate.Binary.Tail.Compare.Operator == "=" && identifierMatches(predicate.Binary.Identifier, tagName)
	default:
		return false
	}
}

func identifierMatches(identifierPath *corebydbql.GrammarIdentifierPath, tagName string) bool {
	if identifierPath == nil {
		return false
	}
	identifier, identifierErr := identifierPath.ToString(false)
	if identifierErr != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(identifier), tagName)
}
