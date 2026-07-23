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

// Package main generates stream query integration-test cases (input/*.ql, input/*.yaml, want/*.yaml).
package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: generate <generate|entries> [data-dir]")
		os.Exit(1)
	}
	dataDir := "test/cases/stream/data"
	if len(os.Args) > 2 {
		dataDir = os.Args[2]
	}
	switch os.Args[1] {
	case "generate":
		runGenerate(dataDir)
	case "entries":
		runEntries(dataDir)
	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n", os.Args[1])
		os.Exit(1)
	}
}

// placeholderMarker identifies want files that capture left unfilled (empty-result cases).
const placeholderMarker = "# Placeholder - run 'capture' subcommand to fill this file"

// runEntries rebuilds the case set, predicts flags deterministically from the
// mirrored seed, and prints g.Entry lines. It does not touch disk.
func runEntries(dataDir string) {
	_ = dataDir
	var allCases []TestCase
	allCases = append(allCases, GenerateLayer1()...)
	allCases = append(allCases, GenerateLayer2()...)
	allCases = append(allCases, GenerateLayer3()...)
	predictFlags(allCases)
	fmt.Println(GenerateEntryCode(allCases))
}

func runGenerate(dataDir string) {
	if verifyErr := verifySeedMirror(dataDir); verifyErr != nil {
		fmt.Fprintf(os.Stderr, "Error: seed mirror drift: %v\n", verifyErr)
		os.Exit(1)
	}
	cm, gapErr := AnalyzeGaps(dataDir)
	if gapErr != nil {
		fmt.Fprintf(os.Stderr, "Warning: gap analysis failed: %v\n", gapErr)
		cm = NewCoverageMap()
	}

	var allCases []TestCase
	allCases = append(allCases, GenerateLayer1()...)
	allCases = append(allCases, GenerateLayer2()...)
	allCases = append(allCases, GenerateLayer3()...)
	filtered := FilterGaps(allCases, cm)
	predictFlags(filtered)

	inputDirPath := filepath.Join(dataDir, "input")
	wantDirPath := filepath.Join(dataDir, "want")
	if mkdirErr := os.MkdirAll(inputDirPath, 0o755); mkdirErr != nil {
		fmt.Fprintf(os.Stderr, "Error creating input directory: %v\n", mkdirErr)
		os.Exit(1)
	}
	if mkdirErr := os.MkdirAll(wantDirPath, 0o755); mkdirErr != nil {
		fmt.Fprintf(os.Stderr, "Error creating want directory: %v\n", mkdirErr)
		os.Exit(1)
	}

	writtenCount := 0
	for caseIdx := range filtered {
		tc := &filtered[caseIdx]
		if writeErr := writeCase(inputDirPath, wantDirPath, tc); writeErr != nil {
			fmt.Fprintf(os.Stderr, "Error writing %s: %v\n", tc.Name, writeErr)
			continue
		}
		writtenCount++
	}
	PrintCoverageReport(cm, filtered)
	fmt.Printf("\nWrote %d test cases to %s\n", writtenCount, dataDir)
	PrintEntryCode(filtered)
}

func writeCase(inputDirPath, wantDirPath string, tc *TestCase) error {
	qlPath := filepath.Join(inputDirPath, tc.Name+".ql")
	if writeErr := os.WriteFile(qlPath, tc.QLFileContent(), 0o600); writeErr != nil {
		return fmt.Errorf("failed to write %s: %w", qlPath, writeErr)
	}
	yamlContent, yamlErr := tc.InputYAMLContent()
	if yamlErr != nil {
		return fmt.Errorf("failed to marshal input yaml: %w", yamlErr)
	}
	yamlPath := filepath.Join(inputDirPath, tc.Name+".yaml")
	if writeErr := os.WriteFile(yamlPath, yamlContent, 0o600); writeErr != nil {
		return fmt.Errorf("failed to write %s: %w", yamlPath, writeErr)
	}
	if tc.WantErr || tc.WantEmpty {
		return nil
	}
	wantPath := filepath.Join(wantDirPath, tc.Name+".yaml")
	placeholder := []byte(licenseHeader + placeholderMarker + "\n")
	if writeErr := os.WriteFile(wantPath, placeholder, 0o600); writeErr != nil {
		return fmt.Errorf("failed to write %s: %w", wantPath, writeErr)
	}
	return nil
}
