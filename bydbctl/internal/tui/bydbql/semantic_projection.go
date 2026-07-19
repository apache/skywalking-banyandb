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

func validateSchemaIdentifiers(query string, schema *session.SchemaSnapshot) string {
	if schema == nil || (len(schema.Tags) == 0 && len(schema.Fields) == 0) {
		return ""
	}
	grammar, parseErr := corebydbql.ParseQuery(query)
	if parseErr != nil || grammar == nil {
		return ""
	}
	if grammar.TopN != nil {
		if grammar.TopN.Where == nil {
			return ""
		}
		for _, identifier := range andExpressionIdentifiers(grammar.TopN.Where.Expr) {
			column, found := schema.ExactColumn(identifier)
			if !found || !containsSchemaIdentifier(schema.EntityTags, column.Name) {
				return fmt.Sprintf("TOPN filter identifier %q is not an entity tag: %s", identifier, strings.Join(schema.EntityTags, ", "))
			}
		}
		return ""
	}
	if grammar.Select == nil {
		return ""
	}
	knownIdentifiers := buildKnownIdentifiers(schema)
	for _, identifier := range selectIdentifiers(grammar.Select) {
		if !knownIdentifiers[identifier] {
			return fmt.Sprintf("identifier %q is not in schema tags or fields: %s", identifier, strings.Join(schemaIdentifierList(schema), ", "))
		}
	}
	for _, identifier := range groupByIdentifiers(grammar.Select) {
		if !knownIdentifiers[identifier] {
			return fmt.Sprintf("GROUP BY identifier %q is not in schema tags: %s", identifier, strings.Join(schema.Tags, ", "))
		}
	}
	if grammar.Select.Where != nil {
		for _, identifier := range orExpressionIdentifiers(grammar.Select.Where.Expr) {
			if schema.Type == session.ResourceTypeProperty && identifier == "ID" {
				continue
			}
			column, found := schema.ExactColumn(identifier)
			if !found {
				return fmt.Sprintf("filter identifier %q is not in schema tags: %s", identifier, strings.Join(schema.Tags, ", "))
			}
			if column.Kind != session.SchemaColumnTag && column.Kind != session.SchemaColumnEntityTag {
				return fmt.Sprintf("filter identifier %q must be a tag", identifier)
			}
		}
	}
	return ""
}

func orExpressionIdentifiers(expression *corebydbql.GrammarOrExpr) []string {
	if expression == nil {
		return nil
	}
	identifiers := andExpressionIdentifiers(expression.Left)
	for _, right := range expression.Right {
		if right != nil {
			identifiers = append(identifiers, andExpressionIdentifiers(right.Right)...)
		}
	}
	return identifiers
}

func andExpressionIdentifiers(expression *corebydbql.GrammarAndExpr) []string {
	if expression == nil {
		return nil
	}
	identifiers := predicateIdentifiers(expression.Left)
	for _, right := range expression.Right {
		if right != nil {
			identifiers = append(identifiers, predicateIdentifiers(right.Right)...)
		}
	}
	return identifiers
}

func predicateIdentifiers(predicate *corebydbql.GrammarPredicate) []string {
	if predicate == nil {
		return nil
	}
	if predicate.Paren != nil {
		return orExpressionIdentifiers(predicate.Paren)
	}
	var identifierPath *corebydbql.GrammarIdentifierPath
	switch {
	case predicate.Binary != nil:
		identifierPath = predicate.Binary.Identifier
	case predicate.In != nil:
		identifierPath = predicate.In.Identifier
	case predicate.Having != nil:
		identifierPath = predicate.Having.Identifier
	}
	if identifierPath == nil {
		return nil
	}
	identifier, identifierErr := identifierPath.ToString(false)
	if identifierErr != nil {
		return nil
	}
	return []string{identifier}
}

func containsSchemaIdentifier(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func buildKnownIdentifiers(schema *session.SchemaSnapshot) map[string]bool {
	knownIdentifiers := make(map[string]bool, len(schema.Tags)+len(schema.Fields)+len(schema.Columns))
	suffixCounts := make(map[string]int, len(schema.Columns))
	for _, tagName := range schema.Tags {
		addKnownIdentifier(knownIdentifiers, tagName)
	}
	for _, fieldName := range schema.Fields {
		addKnownIdentifier(knownIdentifiers, fieldName)
	}
	for _, column := range schema.Columns {
		addKnownIdentifier(knownIdentifiers, column.Name)
		suffix := identifierSuffix(column.Name)
		if suffix != "" {
			suffixCounts[suffix]++
		}
	}
	for _, column := range schema.Columns {
		suffix := identifierSuffix(column.Name)
		if suffixCounts[suffix] == 1 {
			knownIdentifiers[suffix] = true
		}
	}
	return knownIdentifiers
}

func addKnownIdentifier(identifiers map[string]bool, value string) {
	trimmedValue := strings.TrimSpace(value)
	if trimmedValue == "" {
		return
	}
	identifiers[trimmedValue] = true
}

func identifierSuffix(identifier string) string {
	trimmedIdentifier := strings.TrimSpace(identifier)
	lastDot := strings.LastIndex(trimmedIdentifier, ".")
	if lastDot < 0 {
		return ""
	}
	return strings.TrimSpace(trimmedIdentifier[lastDot+1:])
}

func schemaIdentifierList(schema *session.SchemaSnapshot) []string {
	return compactIdentifierList(append(append([]string(nil), schema.Tags...), schema.Fields...))
}

func compactIdentifierList(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	var compacted []string
	for _, value := range values {
		trimmedValue := strings.TrimSpace(value)
		if trimmedValue == "" {
			continue
		}
		if _, ok := seen[trimmedValue]; ok {
			continue
		}
		seen[trimmedValue] = struct{}{}
		compacted = append(compacted, trimmedValue)
	}
	return compacted
}

func selectIdentifiers(selectStmt *corebydbql.GrammarSelectStatement) []string {
	if selectStmt == nil || selectStmt.Projection == nil {
		return nil
	}
	projection := selectStmt.Projection
	if projection.All || projection.Empty || projection.TopN != nil {
		return nil
	}
	var identifiers []string
	for _, column := range projection.Columns {
		if column == nil {
			continue
		}
		if column.Aggregate != nil && column.Aggregate.Column != nil {
			if identifier, identifierErr := column.Aggregate.Column.ToString(false); identifierErr == nil {
				identifiers = append(identifiers, identifier)
			}
			continue
		}
		if column.Identifier != nil {
			if identifier, identifierErr := column.Identifier.ToString(column.TypeSpec != nil); identifierErr == nil {
				identifiers = append(identifiers, identifier)
			}
		}
	}
	return identifiers
}

func groupByIdentifiers(selectStmt *corebydbql.GrammarSelectStatement) []string {
	if selectStmt == nil || selectStmt.GroupBy == nil {
		return nil
	}
	var identifiers []string
	for _, column := range selectStmt.GroupBy.Columns {
		if column == nil || column.Identifier == nil {
			continue
		}
		if identifier, identifierErr := column.Identifier.ToString(column.TypeSpec != nil); identifierErr == nil {
			identifiers = append(identifiers, identifier)
		}
	}
	return identifiers
}
