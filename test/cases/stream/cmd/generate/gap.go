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

// CoverageMap tracks which stream query combinations are already tested.
type CoverageMap struct {
	BinaryOps  map[string]bool
	TreeDepths map[int]bool
	LogicalOps map[string]bool
	SortOrders map[string]bool
	Criteria   map[string]bool
	TestNames  map[string]bool
	OrderRules map[string]bool
	Limit      bool
	Offset     bool
}

// NewCoverageMap creates an empty coverage map.
func NewCoverageMap() *CoverageMap {
	return &CoverageMap{
		BinaryOps: make(map[string]bool), TreeDepths: make(map[int]bool), LogicalOps: make(map[string]bool), SortOrders: make(map[string]bool),
		Criteria: make(map[string]bool), TestNames: make(map[string]bool), OrderRules: make(map[string]bool),
	}
}

// AnalyzeGaps reads existing stream QL files and builds a coverage map.
func AnalyzeGaps(dataDir string) (*CoverageMap, error) {
	cm := NewCoverageMap()
	inputDir := filepath.Join(dataDir, "input")
	qlFiles, globErr := filepath.Glob(filepath.Join(inputDir, "*.ql"))
	if globErr != nil {
		return nil, fmt.Errorf("failed to glob ql files: %w", globErr)
	}
	for _, qlFile := range qlFiles {
		name := strings.TrimSuffix(filepath.Base(qlFile), ".ql")
		cm.TestNames[name] = true
		content, readErr := os.ReadFile(qlFile)
		if readErr != nil {
			continue
		}
		analyzeQLContent(cm, string(content))
	}
	return cm, nil
}

func analyzeQLContent(cm *CoverageMap, qlText string) {
	upper := strings.ToUpper(qlText)
	opPatterns := map[string]string{"!=": "NE", "=": "EQ", "<=": "LE", ">=": "GE", "<": "LT", ">": "GT"}
	for pattern, opName := range opPatterns {
		if strings.Contains(qlText, pattern) {
			cm.BinaryOps[opName] = true
			cm.Criteria[opName] = true
		}
	}
	if strings.Contains(upper, " NOT IN ") {
		cm.BinaryOps["NOT_IN"] = true
		cm.Criteria["NOT_IN"] = true
	} else if strings.Contains(upper, " IN ") {
		cm.BinaryOps["IN"] = true
		cm.Criteria["IN"] = true
	}
	if strings.Contains(upper, " NOT HAVING") {
		cm.BinaryOps["NOT_HAVING"] = true
		cm.Criteria["NOT_HAVING"] = true
	} else if strings.Contains(upper, " HAVING") {
		cm.BinaryOps["HAVING"] = true
		cm.Criteria["HAVING"] = true
	}
	if strings.Contains(upper, " MATCH") {
		cm.BinaryOps["MATCH"] = true
		cm.Criteria["MATCH"] = true
	}
	if strings.Contains(upper, " AND ") {
		cm.LogicalOps["AND"] = true
	}
	if strings.Contains(upper, " OR ") {
		cm.LogicalOps["OR"] = true
	}
	for _, rule := range []string{"duration", "time"} {
		if strings.Contains(upper, "ORDER BY "+strings.ToUpper(rule)+" ASC") {
			cm.OrderRules[rule+"_asc"] = true
			cm.SortOrders["ASC"] = true
		}
		if strings.Contains(upper, "ORDER BY "+strings.ToUpper(rule)+" DESC") {
			cm.OrderRules[rule+"_desc"] = true
			cm.SortOrders["DESC"] = true
		}
	}
	cm.Limit = cm.Limit || strings.Contains(upper, "LIMIT ")
	cm.Offset = cm.Offset || strings.Contains(upper, "OFFSET ")
}

// FilterGaps removes test cases whose names already exist as input/<name>.ql.
func FilterGaps(cases []TestCase, cm *CoverageMap) []TestCase {
	var filtered []TestCase
	for _, tc := range cases {
		if cm.TestNames[tc.Name] {
			continue
		}
		filtered = append(filtered, tc)
	}
	return filtered
}

// PrintCoverageReport prints stream coverage information.
func PrintCoverageReport(cm *CoverageMap, generated []TestCase) {
	fmt.Println("\n=== Coverage Report ===")
	fmt.Println("\nBinary ops covered by existing tests:")
	for opName, covered := range cm.BinaryOps {
		fmt.Printf("  %s: %v\n", opName, covered)
	}
	fmt.Println("\nLogical ops covered by existing tests:")
	for opName, covered := range cm.LogicalOps {
		fmt.Printf("  %s: %v\n", opName, covered)
	}
	fmt.Println("\nOrder rules covered by existing tests:")
	for ruleName, covered := range cm.OrderRules {
		fmt.Printf("  %s: %v\n", ruleName, covered)
	}
	fmt.Printf("\nFeature coverage by existing tests:\n  Limit: %v\n  Offset: %v\n", cm.Limit, cm.Offset)
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
