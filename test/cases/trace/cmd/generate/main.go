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
		fmt.Fprintln(os.Stderr, "Usage: generate <generate> [output-dir]")
		os.Exit(1)
	}
	if os.Args[1] != "generate" {
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n", os.Args[1])
		os.Exit(1)
	}
	outputDir := "."
	if len(os.Args) > 2 {
		outputDir = os.Args[2]
	}
	runGenerate(outputDir)
}

func runGenerate(outputDir string) {
	if verifyErr := verifySeedMirror(outputDir); verifyErr != nil {
		fmt.Fprintf(os.Stderr, "Error: seed mirror drift: %v\n", verifyErr)
		os.Exit(1)
	}
	inputDir := filepath.Join(outputDir, "input")
	cm, gapErr := AnalyzeGaps(inputDir)
	if gapErr != nil {
		fmt.Fprintf(os.Stderr, "Warning: gap analysis failed: %v\n", gapErr)
		cm = NewCoverageMap()
	}

	var allCases []*TestCase
	allCases = append(allCases, GenerateLayer1()...)
	allCases = append(allCases, GenerateLayer2()...)
	allCases = append(allCases, GenerateLayer3()...)
	filtered := FilterGaps(allCases, cm)

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
		qlPath := filepath.Join(inputDirPath, tc.Name+".ql")
		if writeErr := os.WriteFile(qlPath, tc.QLFileContent(), 0o600); writeErr != nil {
			fmt.Fprintf(os.Stderr, "Error writing %s: %v\n", qlPath, writeErr)
			continue
		}
		yamlContent, yamlErr := tc.InputYAMLContent()
		if yamlErr != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling YAML for %s: %v\n", tc.Name, yamlErr)
			continue
		}
		yamlPath := filepath.Join(inputDirPath, tc.Name+".yml")
		if writeErr := os.WriteFile(yamlPath, yamlContent, 0o600); writeErr != nil {
			fmt.Fprintf(os.Stderr, "Error writing %s: %v\n", yamlPath, writeErr)
			continue
		}
		if !tc.WantErr && !tc.WantEmpty {
			wantPath := filepath.Join(wantDirPath, tc.Name+".yml")
			placeholder := []byte(licenseHeader + "# Placeholder - run 'capture' subcommand to fill this file\n")
			if writeErr := os.WriteFile(wantPath, placeholder, 0o600); writeErr != nil {
				fmt.Fprintf(os.Stderr, "Error writing %s: %v\n", wantPath, writeErr)
				continue
			}
		}
		writtenCount++
	}
	PrintCoverageReport(cm, filtered)
	fmt.Printf("\nWrote %d test cases to %s\n", writtenCount, outputDir)
	PrintEntryCode(filtered)
}
