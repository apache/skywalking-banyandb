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
	"strings"

	corebydbql "github.com/apache/skywalking-banyandb/pkg/bydbql"
)

var mutatingKeywords = []string{
	" CREATE ", " UPDATE ", " DELETE ", " DROP ", " APPLY ", " INSERT ", " ALTER ",
}

// IsReadOnly reports whether a statement is a read-only SELECT or SHOW TOP query.
func IsReadOnly(query string) bool {
	trimmedQuery := strings.TrimSpace(query)
	if trimmedQuery == "" {
		return false
	}
	if containsMutatingKeyword(trimmedQuery) {
		return false
	}
	grammar, parseErr := corebydbql.ParseQuery(trimmedQuery)
	if parseErr != nil || grammar == nil {
		return false
	}
	return grammar.Select != nil || grammar.TopN != nil
}

func containsMutatingKeyword(query string) bool {
	normalizedQuery := " " + strings.ToUpper(strings.Join(strings.Fields(query), " ")) + " "
	for _, keyword := range mutatingKeywords {
		if strings.Contains(normalizedQuery, keyword) {
			return true
		}
	}
	return false
}
