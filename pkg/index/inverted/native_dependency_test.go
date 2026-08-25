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

package inverted

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	legacyReaderModulePrefix = "github.com/blugelabs/"
	nativeReaderPackagePath  = "github.com/apache/skywalking-banyandb/pkg/index/inverted/internal/nativeice"
	nativeReaderDir          = "internal/nativeice"
	readOnlyDocCountFile     = "inverted.go"
	readOnlyDocCountFunc     = "ReadOnlyDocCount"
)

// TestReadOnlyDocCountReachesNativeReader is the structural half of the
// NIDX-01A cutover. Counting documents correctly is not enough on its own: the
// point of the milestone is that the read-only path stops going through Bluge,
// and a count that is right for the wrong reason would hide that.
//
// Requirement proved here:
//
//	R6 -- ReadOnlyDocCount reaches the native ICE reader, names no Bluge symbol
//	      itself, and that native reader imports nothing from Bluge or ICE, so
//	      the read-only path no longer depends on the legacy library.
func TestReadOnlyDocCountReachesNativeReader(t *testing.T) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, readOnlyDocCountFile, nil, parser.ParseComments)
	require.NoError(t, err)

	legacyNames, nativeNames := importQualifiers(t, file)
	require.NotEmpty(t, legacyNames, "%s is expected to still import Bluge for the writable store", readOnlyDocCountFile)

	body := findFuncBody(t, file, readOnlyDocCountFunc)
	used := qualifiersUsedIn(body)

	for name := range legacyNames {
		require.NotContains(t, used, name,
			"%s must not name the legacy package %q; the read-only path is the native reader's",
			readOnlyDocCountFunc, legacyNames[name])
	}
	require.NotEmpty(t, nativeNames,
		"%s must import the native reader at %s", readOnlyDocCountFile, nativeReaderPackagePath)
	reachesNative := false
	for name := range nativeNames {
		if _, ok := used[name]; ok {
			reachesNative = true
		}
	}
	require.True(t, reachesNative, "%s must reach the native reader at %s", readOnlyDocCountFunc, nativeReaderPackagePath)

	assertNativeReaderIsLegacyFree(t)
}

// assertNativeReaderIsLegacyFree checks that no source file of the native
// reader package imports the legacy library, so the reader decodes the on-disk
// grammar itself rather than delegating back to it.
func assertNativeReaderIsLegacyFree(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(nativeReaderDir)
	require.NoError(t, err, "the native reader package must exist at %s", nativeReaderDir)
	sources := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		sources++
		path := filepath.Join(nativeReaderDir, entry.Name())
		fileSet := token.NewFileSet()
		file, parseErr := parser.ParseFile(fileSet, path, nil, parser.ImportsOnly)
		require.NoError(t, parseErr)
		for _, spec := range file.Imports {
			importPath, unquoteErr := strconv.Unquote(spec.Path.Value)
			require.NoError(t, unquoteErr)
			require.False(t, strings.HasPrefix(importPath, legacyReaderModulePrefix),
				"%s imports %s; the native reader must not depend on the legacy library", path, importPath)
		}
	}
	require.NotZero(t, sources, "the native reader package must have source files")
}

// importQualifiers returns the local names a file binds to legacy packages and
// to the native reader package, keyed by local name.
func importQualifiers(t *testing.T, file *ast.File) (legacy, native map[string]string) {
	t.Helper()
	legacy, native = map[string]string{}, map[string]string{}
	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		require.NoError(t, err)
		name := filepath.Base(importPath)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		switch {
		case strings.HasPrefix(importPath, legacyReaderModulePrefix):
			legacy[name] = importPath
		case importPath == nativeReaderPackagePath:
			native[name] = importPath
		}
	}
	return legacy, native
}

// findFuncBody returns the body of the named top-level function.
func findFuncBody(t *testing.T, file *ast.File, name string) *ast.BlockStmt {
	t.Helper()
	for _, decl := range file.Decls {
		funcDecl, ok := decl.(*ast.FuncDecl)
		if !ok || funcDecl.Recv != nil || funcDecl.Name.Name != name {
			continue
		}
		require.NotNil(t, funcDecl.Body, "%s must have a body", name)
		return funcDecl.Body
	}
	t.Fatalf("%s declares no top-level func %s", readOnlyDocCountFile, name)
	return nil
}

// qualifiersUsedIn returns the set of package-qualifier identifiers that appear
// on the left of a selector anywhere in the given code.
func qualifiersUsedIn(node ast.Node) map[string]struct{} {
	used := map[string]struct{}{}
	ast.Inspect(node, func(n ast.Node) bool {
		selector, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if ident, isIdent := selector.X.(*ast.Ident); isIdent {
			used[ident.Name] = struct{}{}
		}
		return true
	})
	return used
}
