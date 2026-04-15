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
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: generate <generate|capture> [options]")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Subcommands:")
		fmt.Fprintln(os.Stderr, "  generate [output-dir]  Generate .ql and input/*.yaml files")
		fmt.Fprintln(os.Stderr, "  capture [output-dir] [server-addr]  Capture want/*.yaml from live server")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "generate":
		outputDir := "."
		if len(os.Args) > 2 {
			outputDir = os.Args[2]
		}
		runGenerate(outputDir)
	case "capture":
		outputDir := "."
		if len(os.Args) > 2 {
			outputDir = os.Args[2]
		}
		serverAddr := "localhost:17912"
		if len(os.Args) > 3 {
			serverAddr = os.Args[3]
		}
		runCapture(outputDir, serverAddr)
	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n", os.Args[1])
		os.Exit(1)
	}
}

func runGenerate(outputDir string) {
	inputDir := filepath.Join(outputDir, "input")

	// Step 1: Analyze existing tests for gap detection
	cm, gapErr := AnalyzeGaps(inputDir)
	if gapErr != nil {
		fmt.Fprintf(os.Stderr, "Warning: gap analysis failed: %v\n", gapErr)
		cm = &CoverageMap{
			BinaryOps:    make(map[string]bool),
			AggFunctions: make(map[string]bool),
			TreeDepths:   make(map[int]bool),
			LogicalOps:   make(map[string]bool),
			SortOrders:   make(map[string]bool),
			Criteria:     make(map[string]bool),
			TestNames:    make(map[string]bool),
		}
	}

	// Step 2: Generate all layers
	var allCases []*TestCase
	allCases = append(allCases, GenerateLayer1()...)
	allCases = append(allCases, GenerateLayer2()...)
	allCases = append(allCases, GenerateLayer3()...)

	// Step 3: Filter out already-covered cases
	filtered := FilterGaps(allCases, cm)

	// Step 4: Write files
	inputDirPath := filepath.Join(outputDir, "input")
	wantDirPath := filepath.Join(outputDir, "want")

	if mkdirErr := os.MkdirAll(inputDirPath, 0o755); mkdirErr != nil {
		fmt.Fprintf(os.Stderr, "Error creating input directory: %v\n", mkdirErr)
		os.Exit(1)
	}
	if mkdirErr := os.MkdirAll(wantDirPath, 0o755); mkdirErr != nil {
		fmt.Fprintf(os.Stderr, "Error creating want directory: %v\n", mkdirErr)
		os.Exit(1)
	}

	writtenCount := 0
	for _, tc := range filtered {
		// Write .ql file
		qlPath := filepath.Join(inputDirPath, tc.Name+".ql")
		if writeErr := os.WriteFile(qlPath, tc.QLFileContent(), 0o600); writeErr != nil {
			fmt.Fprintf(os.Stderr, "Error writing %s: %v\n", qlPath, writeErr)
			continue
		}

		// Write input YAML file
		yamlContent, yamlErr := tc.InputYAMLContent()
		if yamlErr != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling YAML for %s: %v\n", tc.Name, yamlErr)
			continue
		}
		yamlPath := filepath.Join(inputDirPath, tc.Name+".yaml")
		if writeErr := os.WriteFile(yamlPath, yamlContent, 0o600); writeErr != nil {
			fmt.Fprintf(os.Stderr, "Error writing %s: %v\n", yamlPath, writeErr)
			continue
		}

		// WantErr and WantEmpty cases don't need want files
		if !tc.WantErr && !tc.WantEmpty {
			// Write placeholder want file (to be filled by capture)
			wantPath := filepath.Join(wantDirPath, tc.Name+".yaml")
			placeholder := []byte("# Placeholder - run 'capture' subcommand to fill this file\n")
			if writeErr := os.WriteFile(wantPath, placeholder, 0o600); writeErr != nil {
				fmt.Fprintf(os.Stderr, "Error writing %s: %v\n", wantPath, writeErr)
				continue
			}
		}

		writtenCount++
	}

	// Step 5: Print reports
	PrintCoverageReport(cm, filtered)
	fmt.Printf("\nWrote %d test cases to %s\n", writtenCount, outputDir)
	PrintEntryCode(filtered)
}
