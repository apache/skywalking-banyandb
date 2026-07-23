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
	"fmt"
	"strings"
)

const strTrue = "true"

// maxLineLen is the linter line-length budget (tab counts as 1 char).
const maxLineLen = 170

// GenerateEntryCode produces g.Entry lines for stream.go's streamEntries.
func GenerateEntryCode(cases []TestCase) string {
	lines := make([]string, 0, len(cases))
	for caseIdx := range cases {
		lines = append(lines, renderEntryLine(&cases[caseIdx]))
	}
	return strings.Join(lines, "\n")
}

// renderEntryLine emits a single-line entry when it fits within maxLineLen,
// or a gofumpt-aligned multi-line entry otherwise.
func renderEntryLine(tc *TestCase) string {
	singleLine := buildSingleLine(tc)
	// A leading tab counts as 1 char; the line already starts with \t.
	if len(singleLine) <= maxLineLen {
		return singleLine
	}
	return buildMultiLine(tc)
}

// buildSingleLine returns the compact one-liner (with leading tab).
func buildSingleLine(tc *TestCase) string {
	fields := buildInlineFields(tc)
	return fmt.Sprintf("\tg.Entry(%q, helpers.Args{%s}),", tc.Name, strings.Join(fields, ", "))
}

// buildInlineFields returns the Args fields as "Key: value" strings in canonical order.
func buildInlineFields(tc *TestCase) []string {
	fields := []string{fmt.Sprintf("Input: %q", tc.Name)}
	if tc.WantErr {
		fields = append(fields, "WantErr: "+strTrue)
	}
	if tc.WantEmpty {
		fields = append(fields, "WantEmpty: "+strTrue)
	}
	if tc.DisOrder {
		fields = append(fields, "DisOrder: "+strTrue)
	}
	if tc.IgnoreElementID {
		fields = append(fields, "IgnoreElementID: "+strTrue)
	}
	fields = append(fields, "Duration: 1 * time.Hour")
	return fields
}

// buildMultiLine returns a gofumpt-stable multi-line entry.
// gofumpt aligns struct-literal fields by padding each key to the length of
// the longest key present in that literal, then appending ": ".
func buildMultiLine(tc *TestCase) string {
	type kv struct{ key, val string }
	pairs := []kv{{key: "Input", val: fmt.Sprintf("%q", tc.Name)}}
	if tc.WantErr {
		pairs = append(pairs, kv{key: "WantErr", val: strTrue})
	}
	if tc.WantEmpty {
		pairs = append(pairs, kv{key: "WantEmpty", val: strTrue})
	}
	if tc.DisOrder {
		pairs = append(pairs, kv{key: "DisOrder", val: strTrue})
	}
	if tc.IgnoreElementID {
		pairs = append(pairs, kv{key: "IgnoreElementID", val: strTrue})
	}
	pairs = append(pairs, kv{key: "Duration", val: "1 * time.Hour"})

	// Compute alignment width: longest key in this literal.
	maxKey := 0
	for _, p := range pairs {
		if len(p.key) > maxKey {
			maxKey = len(p.key)
		}
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\tg.Entry(%q, helpers.Args{\n", tc.Name))
	for _, p := range pairs {
		// gofumpt aligns by padding spaces after the colon:
		// "Key:   value" where the colon+spaces total maxKey+2 chars.
		padding := strings.Repeat(" ", maxKey-len(p.key))
		sb.WriteString(fmt.Sprintf("\t\t%s:%s %s,\n", p.key, padding, p.val))
	}
	sb.WriteString("\t}),")
	return sb.String()
}

// PrintEntryCode prints all entry lines to stdout.
func PrintEntryCode(cases []TestCase) {
	fmt.Println("\n=== Entry Code for stream.go ===")
	fmt.Println("// Paste the following lines into streamEntries:")
	fmt.Println(GenerateEntryCode(cases))
}
