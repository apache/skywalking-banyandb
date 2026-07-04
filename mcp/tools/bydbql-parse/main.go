// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

type validateRequest struct {
	Query string `json:"query"`
}

type validateResponse struct {
	Valid      bool     `json:"valid"`
	Message    string   `json:"message"`
	QueryType  string   `json:"queryType,omitempty"`
	SyntaxOnly bool     `json:"syntaxOnly"`
	Warnings   []string `json:"warnings,omitempty"`
}

func main() {
	var req validateRequest
	decodeErr := json.NewDecoder(os.Stdin).Decode(&req)
	if decodeErr != nil {
		writeResponse(validateResponse{
			Valid:      false,
			Message:    fmt.Sprintf("failed to read validation request: %v", decodeErr),
			SyntaxOnly: true,
		})
		os.Exit(1)
	}

	writeResponse(validate(req.Query))
}

// validate performs parse-only BydbQL syntax validation and reports the
// detected query type. It never executes the query or verifies live schema
// existence, so callers can unit-test the parse contract without a database.
func validate(query string) validateResponse {
	grammar, parseErr := bydbql.ParseQuery(query)
	if parseErr != nil {
		return validateResponse{
			Valid:      false,
			Message:    fmt.Sprintf("failed to parse BydbQL: %v", parseErr),
			SyntaxOnly: true,
		}
	}

	return validateResponse{
		Valid:      true,
		Message:    "valid BydbQL syntax",
		QueryType:  queryType(grammar),
		SyntaxOnly: true,
		Warnings: []string{
			"parse-only validation does not verify group, resource, tag, field, or index-rule existence",
		},
	}
}

func queryType(grammar *bydbql.Grammar) string {
	if grammar == nil {
		return ""
	}
	if grammar.TopN != nil {
		return "TOPN"
	}
	if grammar.Select != nil && grammar.Select.From != nil {
		return grammar.Select.From.ResourceType
	}
	return ""
}

func writeResponse(resp validateResponse) {
	encodeErr := json.NewEncoder(os.Stdout).Encode(resp)
	if encodeErr != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to write validation response: %v\n", encodeErr)
		os.Exit(1)
	}
}
