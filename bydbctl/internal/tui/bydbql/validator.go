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

// Package bydbql validates BYDBQL candidates for the bydbctl agent TUI workflow.
package bydbql

import (
	"context"
	"strings"
	"time"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	corebydbql "github.com/apache/skywalking-banyandb/pkg/bydbql"
)

// ParserValidator validates BYDBQL syntax with the shared parser.
type ParserValidator struct {
	now func() time.Time
}

// NewParserValidator creates a parser-backed validator.
func NewParserValidator() *ParserValidator {
	return &ParserValidator{
		now: time.Now,
	}
}

// Validate parses a query and reports whether it is syntactically valid BYDBQL.
func (pv *ParserValidator) Validate(_ context.Context, query string, _ *session.SchemaSnapshot) (session.ValidationReport, error) {
	checkedAt := pv.now()
	if strings.TrimSpace(query) == "" {
		return session.ValidationReport{
			CheckedAt: checkedAt,
			Valid:     false,
			Message:   "query is empty",
		}, nil
	}
	grammar, parseErr := corebydbql.ParseQuery(query)
	if parseErr != nil {
		return session.ValidationReport{
			CheckedAt: checkedAt,
			Valid:     false,
			Message:   parseErr.Error(),
		}, nil
	}
	return session.ValidationReport{
		CheckedAt: checkedAt,
		Valid:     true,
		Message:   "syntax validated by pkg/bydbql.ParseQuery",
		QueryType: queryType(grammar),
	}, nil
}

func queryType(grammar *corebydbql.Grammar) string {
	if grammar == nil {
		return ""
	}
	if grammar.TopN != nil {
		return session.ResourceTypeTopN.String()
	}
	if grammar.Select == nil {
		return ""
	}
	return strings.ToUpper(grammar.Select.From.ResourceType)
}
