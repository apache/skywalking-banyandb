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
	"fmt"
	"regexp"
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	corebydbql "github.com/apache/skywalking-banyandb/pkg/bydbql"
)

var (
	timeClausePattern  = regexp.MustCompile(`(?i)\bTIME\b`)
	limitClausePattern = regexp.MustCompile(`(?i)\bLIMIT\s+\d+\b`)
)

// SemanticValidator validates BYDBQL syntax and lightweight semantic rules.
type SemanticValidator struct {
	parser *ParserValidator
}

// NewSemanticValidator creates a validator with semantic checks.
func NewSemanticValidator() *SemanticValidator {
	return &SemanticValidator{
		parser: NewParserValidator(),
	}
}

// Validate parses a query and applies semantic checks when schema context is available.
func (validator *SemanticValidator) Validate(ctx context.Context, query string, schema *session.SchemaSnapshot) (session.ValidationReport, error) {
	report, validateErr := validator.parser.Validate(ctx, query, nil)
	if validateErr != nil || !report.Valid {
		return report, validateErr
	}
	if schema == nil {
		return report, nil
	}
	if semanticMessage := validator.semanticMessage(query, schema); semanticMessage != "" {
		report.Valid = false
		report.Message = semanticMessage
	}
	return report, nil
}

func (validator *SemanticValidator) semanticMessage(query string, schema *session.SchemaSnapshot) string {
	if resourceMessage := validateResourceIdentity(query, schema); resourceMessage != "" {
		return resourceMessage
	}
	if requiresTimeClause(query) && !timeClausePattern.MatchString(query) {
		return "TIME clause is required for MEASURE, STREAM, TRACE, and SHOW TOP queries"
	}
	if requiresLimitClause(query) && !limitClausePattern.MatchString(query) {
		return "LIMIT clause is required for SELECT queries"
	}
	sortableRules := sortableIndexRuleNames(schema)
	if orderField := extractOrderByField(query); orderField != "" && len(sortableRules) > 0 {
		if !containsIndexedField(sortableRules, orderField) {
			if suggestion := suggestIndexedField(sortableRules, orderField); suggestion != "" {
				return fmt.Sprintf("ORDER BY index rule %q is not sortable; use %q or omit ORDER BY", orderField, suggestion)
			}
			return fmt.Sprintf("ORDER BY index rule %q is not sortable; omit ORDER BY or choose one of: %s", orderField, strings.Join(sortableRules, ", "))
		}
	}
	if identifierMessage := validateSchemaIdentifiers(query, schema); identifierMessage != "" {
		return identifierMessage
	}
	return ""
}

func validateResourceIdentity(query string, schema *session.SchemaSnapshot) string {
	if schema == nil {
		return ""
	}
	grammar, parseErr := corebydbql.ParseQuery(query)
	if parseErr != nil || grammar == nil {
		return ""
	}
	var resourceType session.ResourceType
	var resourceName string
	var groups []string
	switch {
	case grammar.TopN != nil:
		resourceType = session.ResourceTypeTopN
		resourceName = grammar.TopN.From.ResourceName
		groups = grammar.TopN.From.In.Groups
	case grammar.Select != nil:
		resourceType = session.ResourceType(strings.ToUpper(grammar.Select.From.ResourceType))
		resourceName = grammar.Select.From.ResourceName
		groups = grammar.Select.From.In.Groups
	default:
		return ""
	}
	if schema.Type != "" && schema.Type != resourceType {
		return fmt.Sprintf("query resource type %s does not match schema type %s", resourceType, schema.Type)
	}
	if schema.Name != "" && schema.Name != resourceName {
		return fmt.Sprintf("query resource name %q does not match schema name %q", resourceName, schema.Name)
	}
	if len(schema.Groups) > 0 && session.SchemaKey(resourceType, resourceName, groups) != session.SchemaKey(schema.Type, schema.Name, schema.Groups) {
		return fmt.Sprintf("query groups %v do not match schema groups %v", groups, schema.Groups)
	}
	return ""
}

func requiresLimitClause(query string) bool {
	grammar, parseErr := corebydbql.ParseQuery(query)
	return parseErr == nil && grammar != nil && grammar.Select != nil
}

func requiresTimeClause(query string) bool {
	grammar, parseErr := corebydbql.ParseQuery(query)
	if parseErr != nil || grammar == nil {
		return false
	}
	if grammar.TopN != nil {
		return true
	}
	if grammar.Select == nil {
		return false
	}
	resourceType := strings.ToUpper(strings.TrimSpace(grammar.Select.From.ResourceType))
	return resourceType != session.ResourceTypeProperty.String()
}

func extractOrderByField(query string) string {
	grammar, parseErr := corebydbql.ParseQuery(query)
	if parseErr != nil || grammar == nil || grammar.Select == nil || grammar.Select.OrderBy == nil {
		return ""
	}
	orderBy := grammar.Select.OrderBy
	if orderBy.Tail.DirOnly != nil || orderBy.Tail.WithIdent == nil || orderBy.Tail.WithIdent.Identifier == nil {
		return ""
	}
	fieldName, nameErr := orderBy.Tail.WithIdent.Identifier.ToString(false)
	if nameErr != nil {
		return ""
	}
	fieldName = strings.TrimSpace(fieldName)
	if fieldName == "TIME" {
		return ""
	}
	return fieldName
}

func sortableIndexRuleNames(schema *session.SchemaSnapshot) []string {
	if schema == nil {
		return nil
	}
	if len(schema.SortableIndexes) == 0 {
		return append([]string(nil), schema.IndexedFields...)
	}
	ruleNames := make([]string, 0, len(schema.SortableIndexes))
	for _, sortableIndex := range schema.SortableIndexes {
		if ruleName := strings.TrimSpace(sortableIndex.RuleName); ruleName != "" {
			ruleNames = append(ruleNames, ruleName)
		}
	}
	return ruleNames
}

func containsIndexedField(indexedFields []string, fieldName string) bool {
	for _, indexedField := range indexedFields {
		if indexedField == fieldName {
			return true
		}
	}
	return false
}

func suggestIndexedField(indexedFields []string, fieldName string) string {
	lowerField := strings.ToLower(fieldName)
	for _, indexedField := range indexedFields {
		lowerIndexed := strings.ToLower(indexedField)
		if strings.Contains(lowerIndexed, lowerField) || strings.Contains(lowerField, lowerIndexed) {
			return indexedField
		}
	}
	return ""
}
