// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package tracelabels

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestDocVocabularySectionEnumeratesAllConstants(t *testing.T) {
	constants := exportedStringConstants(t)
	docBytes, readErr := os.ReadFile(filepath.Join("..", "..", "..", "docs", "observability", "vec-query-tracing.md"))
	if readErr != nil {
		t.Fatalf("failed to read vec query tracing doc: %v", readErr)
	}
	doc := string(docBytes)
	for constantName, constantValue := range constants {
		if !strings.Contains(doc, "`"+constantName+"`") {
			t.Fatalf("doc does not list constant %s", constantName)
		}
		if !strings.Contains(doc, "`"+constantValue+"`") {
			t.Fatalf("doc does not list key %s for constant %s", constantValue, constantName)
		}
	}
}

func exportedStringConstants(t *testing.T) map[string]string {
	t.Helper()
	fileSet := token.NewFileSet()
	file, parseErr := parser.ParseFile(fileSet, "labels.go", nil, 0)
	if parseErr != nil {
		t.Fatalf("failed to parse labels.go: %v", parseErr)
	}
	constants := make(map[string]string)
	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.CONST {
			continue
		}
		for _, spec := range genDecl.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok || len(valueSpec.Values) == 0 {
				continue
			}
			for idx, name := range valueSpec.Names {
				if !name.IsExported() || idx >= len(valueSpec.Values) {
					continue
				}
				literal, ok := valueSpec.Values[idx].(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				value, unquoteErr := strconv.Unquote(literal.Value)
				if unquoteErr != nil {
					t.Fatalf("failed to unquote %s: %v", name.Name, unquoteErr)
				}
				constants[name.Name] = value
			}
		}
	}
	return constants
}
