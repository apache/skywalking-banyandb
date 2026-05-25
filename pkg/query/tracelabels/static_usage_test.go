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
	"strings"
	"testing"
)

func TestVectorizedTraceTagsUseConstants(t *testing.T) {
	repoRoot := filepath.Join("..", "..", "..")
	paths := []string{
		filepath.Join(repoRoot, "pkg", "query", "tracer.go"),
		filepath.Join(repoRoot, "pkg", "query", "vectorized", "measure"),
	}
	for _, path := range paths {
		checkTraceTagLiterals(t, path)
	}
}

func checkTraceTagLiterals(t *testing.T, path string) {
	t.Helper()
	info, statErr := os.Stat(path)
	if statErr != nil {
		t.Fatalf("failed to stat %s: %v", path, statErr)
	}
	if !info.IsDir() {
		checkTraceTagGoFile(t, path)
		return
	}
	walkErr := filepath.WalkDir(path, func(walkPath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(walkPath, ".go") {
			return nil
		}
		checkTraceTagGoFile(t, walkPath)
		return nil
	})
	if walkErr != nil {
		t.Fatalf("failed to walk %s: %v", path, walkErr)
	}
}

func checkTraceTagGoFile(t *testing.T, path string) {
	t.Helper()
	fileSet := token.NewFileSet()
	file, parseErr := parser.ParseFile(fileSet, path, nil, 0)
	if parseErr != nil {
		t.Fatalf("failed to parse %s: %v", path, parseErr)
	}
	checkTraceTagFile(t, fileSet, file)
}

func checkTraceTagFile(t *testing.T, fileSet *token.FileSet, file *ast.File) {
	t.Helper()
	ast.Inspect(file, func(node ast.Node) bool {
		callExpr, ok := node.(*ast.CallExpr)
		if !ok || len(callExpr.Args) == 0 {
			return true
		}
		selectorExpr, ok := callExpr.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if selectorExpr.Sel.Name != "Tag" && selectorExpr.Sel.Name != "Tagf" {
			return true
		}
		if literal, ok := callExpr.Args[0].(*ast.BasicLit); ok && literal.Kind == token.STRING {
			pos := fileSet.Position(literal.Pos())
			if !strings.Contains(pos.Filename, string(filepath.Separator)+"tracelabels"+string(filepath.Separator)) {
				t.Fatalf("%s uses string literal trace tag key %s; use pkg/query/tracelabels", pos, literal.Value)
			}
		}
		return true
	})
}
