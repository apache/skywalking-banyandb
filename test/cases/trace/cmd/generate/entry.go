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

// Package main generates trace query integration-test cases (input/*.ql and input/*.yml).
package main

import (
	"fmt"
	"strings"
)

const (
	strFalse = "false"
	strNone  = "none"
	strTrue  = "true"
)

// GenerateEntryCode produces g.Entry lines for trace.go.
func GenerateEntryCode(cases []*TestCase) string {
	var lines []string
	for _, tc := range cases {
		if tc.Trace == nil {
			continue
		}
		lines = append(lines, renderEntryLine(tc))
	}
	return strings.Join(lines, "\n")
}

func renderEntryLine(tc *TestCase) string {
	fields := []string{fmt.Sprintf("Input: %q", tc.Name)}
	if tc.WantEmpty {
		fields = append(fields, "WantEmpty: "+strTrue)
	}
	if tc.WantErr {
		fields = append(fields, "WantErr: "+strTrue)
	}
	if tc.DisOrder {
		fields = append(fields, "DisOrder: "+strTrue)
	}
	fields = append(fields, "Duration: 1 * time.Hour")
	return fmt.Sprintf("g.Entry(%q, helpers.Args{%s})", tc.Name, strings.Join(fields, ", "))
}

// PrintEntryCode prints all entry lines to stdout.
func PrintEntryCode(cases []*TestCase) {
	fmt.Println("\n=== Entry Code for trace.go ===")
	fmt.Println("// Paste the following lines into traceEntries:")
	fmt.Println(GenerateEntryCode(cases))
}
