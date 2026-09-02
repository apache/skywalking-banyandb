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
	banyanDBModulePrefix    = "github.com/apache/skywalking-banyandb/"
	nativeReaderPackagePath = banyanDBModulePrefix + "pkg/index/inverted/internal/nativeice"
	nativeReaderDir         = "internal/nativeice"
	readOnlyDocCountFile    = "inverted.go"
	readOnlyDocCountFunc    = "ReadOnlyDocCount"
)

// allowedNativeReaderExternalImports is the native reader's whole third-party
// dependency budget: the bitmap decoder its deletion masks and term postings
// are encoded with, the block codec BDB-NIDX-SPEC-001 revision 0.2 DEC-008
// fixes for the stored-field chunks of the historical corpus, and the finite
// state transducer the historical term dictionaries are serialized as.
//
// Each entry decodes one historical on-disk encoding and nothing else. None is
// a search engine, none brings a query language, collector or analyzer, and
// BDB-NIDX-SPEC-001 revision 0.2 section 21 requires that no third-party codec
// type escapes this package: the contract other packages observe stays the
// behavior of the exported functions in pkg/index/inverted.
var allowedNativeReaderExternalImports = map[string]struct{}{
	"github.com/RoaringBitmap/roaring": {},
	"github.com/blevesearch/vellum":    {},
	"github.com/klauspost/compress/s2": {},
}

// TestReadOnlyDocCountReachesNativeReader is the structural half of the
// NIDX-01A cutover. Counting documents correctly is not enough on its own: the
// point of the milestone is that the read-only path stops going through the
// retired reader, and a count that is right for the wrong reason would hide
// that.
//
// Requirement proved here:
//
//	R6 -- ReadOnlyDocCount reaches only the native ICE reader package, and that
//	      package imports only BanyanDB code, the standard library, and its
//	      explicit bitmap dependency.
func TestReadOnlyDocCountReachesNativeReader(t *testing.T) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, readOnlyDocCountFile, nil, parser.ParseComments)
	require.NoError(t, err)

	imports := importQualifiers(t, file)
	body := findFuncBody(t, file, readOnlyDocCountFunc)
	used := qualifiersUsedIn(body)

	nativeNames := importNamesForPath(imports, nativeReaderPackagePath)
	require.NotEmpty(t, nativeNames,
		"%s must import the native reader at %s", readOnlyDocCountFile, nativeReaderPackagePath)
	reachesNative := false
	for qualifier := range used {
		importPath, imported := imports[qualifier]
		if !imported {
			continue
		}
		require.Equal(t, nativeReaderPackagePath, importPath,
			"%s must reach only the native reader package, not %s", readOnlyDocCountFunc, importPath)
		if _, ok := nativeNames[qualifier]; ok {
			reachesNative = true
		}
	}
	require.True(t, reachesNative, "%s must reach the native reader at %s", readOnlyDocCountFunc, nativeReaderPackagePath)

	assertNativeReaderImportsAreAllowed(t)
}

// assertNativeReaderImportsAreAllowed checks that the native reader stays
// inside its small, explicit dependency boundary.
func assertNativeReaderImportsAreAllowed(t *testing.T) {
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
			if isStandardLibraryImport(importPath) || strings.HasPrefix(importPath, banyanDBModulePrefix) {
				continue
			}
			_, allowed := allowedNativeReaderExternalImports[importPath]
			require.True(t, allowed, "%s imports %s outside the native reader allowlist", path, importPath)
		}
	}
	require.NotZero(t, sources, "the native reader package must have source files")
}

// importQualifiers returns imported package paths keyed by their local name.
func importQualifiers(t *testing.T, file *ast.File) map[string]string {
	t.Helper()
	imports := map[string]string{}
	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		require.NoError(t, err)
		name := filepath.Base(importPath)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imports[name] = importPath
	}
	return imports
}

func importNamesForPath(imports map[string]string, wantedPath string) map[string]struct{} {
	names := map[string]struct{}{}
	for name, importPath := range imports {
		if importPath == wantedPath {
			names[name] = struct{}{}
		}
	}
	return names
}

func isStandardLibraryImport(importPath string) bool {
	firstElement := strings.SplitN(importPath, "/", 2)[0]
	return !strings.Contains(firstElement, ".")
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
