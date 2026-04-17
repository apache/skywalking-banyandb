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
	"os"
	"path/filepath"
	"strings"
)

// CoverageMap tracks which combinations are already tested by existing cases.
type CoverageMap struct {
	BinaryOps    map[string]bool
	AggFunctions map[string]bool
	TreeDepths   map[int]bool
	LogicalOps   map[string]bool
	SortOrders   map[string]bool
	Criteria     map[string]bool
	TestNames    map[string]bool
	TopN         bool
	GroupBy      bool
	Limit        bool
	Offset       bool
}

// AnalyzeGaps reads existing test files and builds a coverage map.
func AnalyzeGaps(inputDir string) (*CoverageMap, error) {
	cm := &CoverageMap{
		BinaryOps:    make(map[string]bool),
		AggFunctions: make(map[string]bool),
		TreeDepths:   make(map[int]bool),
		LogicalOps:   make(map[string]bool),
		SortOrders:   make(map[string]bool),
		Criteria:     make(map[string]bool),
		TestNames:    make(map[string]bool),
	}

	qlFiles, readErr := filepath.Glob(filepath.Join(inputDir, "*.ql"))
	if readErr != nil {
		return nil, fmt.Errorf("failed to read ql files: %w", readErr)
	}

	for _, qlFile := range qlFiles {
		name := strings.TrimSuffix(filepath.Base(qlFile), ".ql")
		cm.TestNames[name] = true

		content, readErr := os.ReadFile(qlFile)
		if readErr != nil {
			continue
		}
		qlText := string(content)
		analyzeQLContent(cm, qlText)
	}

	return cm, nil
}

func analyzeQLContent(cm *CoverageMap, qlText string) {
	upper := strings.ToUpper(qlText)

	// Detect operators in WHERE clause
	opPatterns := map[string]string{
		"!=": "NE", "=": "EQ", "<=": "LE", ">=": "GE",
		"<": "LT", ">": "GT",
	}
	for pattern, opName := range opPatterns {
		if strings.Contains(qlText, pattern) {
			cm.BinaryOps[opName] = true
			cm.Criteria[opName] = true
		}
	}

	if strings.Contains(upper, " NOT IN ") {
		cm.BinaryOps["NOT_IN"] = true
		cm.Criteria["NOT_IN"] = true
	}
	if strings.Contains(upper, " IN ") {
		cm.BinaryOps["IN"] = true
		cm.Criteria["IN"] = true
	}
	if strings.Contains(upper, " MATCH") {
		cm.BinaryOps["MATCH"] = true
		cm.Criteria["MATCH"] = true
	}
	if strings.Contains(upper, " HAVING ") {
		cm.BinaryOps["HAVING"] = true
		cm.Criteria["HAVING"] = true
	}
	if strings.Contains(upper, " NOT HAVING ") {
		cm.BinaryOps["NOT_HAVING"] = true
		cm.Criteria["NOT_HAVING"] = true
	}

	// Detect AND/OR
	if strings.Contains(upper, " AND ") {
		cm.LogicalOps["AND"] = true
	}
	if strings.Contains(upper, " OR ") {
		cm.LogicalOps["OR"] = true
	}

	// Detect aggregation functions
	aggNames := []string{"MEAN", "MAX", "MIN", "COUNT", "SUM"}
	for _, aggName := range aggNames {
		if strings.Contains(upper, aggName+"(") {
			cm.AggFunctions[aggName] = true
		}
	}

	// Detect sort order
	if strings.Contains(upper, "ORDER BY ASC") || strings.Contains(upper, "ORDER ASC") {
		cm.SortOrders["ASC"] = true
	}
	if strings.Contains(upper, "ORDER BY DESC") || strings.Contains(upper, "ORDER DESC") {
		cm.SortOrders["DESC"] = true
	}

	// Detect TOP N
	if strings.Contains(upper, "TOP ") {
		cm.TopN = true
	}

	// Detect GROUP BY
	if strings.Contains(upper, "GROUP BY") {
		cm.GroupBy = true
	}

	// Detect LIMIT
	if strings.Contains(upper, "LIMIT ") {
		cm.Limit = true
	}

	// Detect OFFSET
	if strings.Contains(upper, "OFFSET ") {
		cm.Offset = true
	}
}

// FilterGaps removes test cases that are already covered by existing tests.
func FilterGaps(cases []*TestCase, cm *CoverageMap) []*TestCase {
	var filtered []*TestCase
	for _, tc := range cases {
		if cm.TestNames[tc.Name] {
			continue // exact name collision
		}
		filtered = append(filtered, tc)
	}
	return filtered
}

// PrintCoverageReport prints a summary of coverage.
func PrintCoverageReport(cm *CoverageMap, generated []*TestCase) {
	fmt.Println("\n=== Coverage Report ===")

	fmt.Println("\nBinaryOps covered by existing tests:")
	for op, covered := range cm.BinaryOps {
		fmt.Printf("  %s: %v\n", op, covered)
	}

	fmt.Println("\nAggregationFunctions covered by existing tests:")
	for agg, covered := range cm.AggFunctions {
		fmt.Printf("  %s: %v\n", agg, covered)
	}

	fmt.Println("\nLogicalOps covered by existing tests:")
	for op, covered := range cm.LogicalOps {
		fmt.Printf("  %s: %v\n", op, covered)
	}

	fmt.Println("\nFeature coverage by existing tests:")
	fmt.Printf("  TopN: %v\n", cm.TopN)
	fmt.Printf("  GroupBy: %v\n", cm.GroupBy)
	fmt.Printf("  Limit: %v\n", cm.Limit)
	fmt.Printf("  Offset: %v\n", cm.Offset)

	fmt.Printf("\nExisting test names: %d\n", len(cm.TestNames))
	fmt.Printf("New test cases to generate: %d\n", len(generated))

	fmt.Println("\nNew test case names:")
	for _, tc := range generated {
		prefix := "  "
		if tc.WantErr {
			prefix = "  [ERR] "
		}
		fmt.Printf("%s%s\n", prefix, tc.Name)
	}
}
