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
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// committedGenerationCounter is the whole of NIDX-01B's contract: one call from
// an index directory path to the number of documents visible in the newest
// structurally complete committed generation that directory holds, or to a
// classified error. Issue #14009 names this signature as the milestone's only
// test seam, and BDB-NIDX-SPEC-001 revision 0.2 NIDX-01 forbids exposing a
// general reader beside it.
//
// Everything the milestone adds -- generation enumeration, snapshot records,
// roaring deletion masks, segment accounting, and the newest-first walk -- sits
// behind this signature and stays private. The coder is free to move any of it;
// the coder may not move this.
type committedGenerationCounter func(path string) (int64, error)

// nidx01bBoundary binds the contract to the production symbol that satisfies
// it, so a change to ReadOnlyDocCount's signature fails to compile here rather
// than quietly redefining the milestone.
var nidx01bBoundary committedGenerationCounter = ReadOnlyDocCount

// nativeReaderSurface is every identifier the private native reader package is
// permitted to export. Methods appear as Type.Method.
//
// The list is the read-only slice NIDX-01 allows and nothing else: open a
// committed generation, report its visible document count, walk its live
// documents' stored fields, close it, and classify the two failures a caller
// must distinguish. Issue #14009 placed the document walk outside NIDX-01B;
// issue #14010 adds it and nothing more. Dictionaries, term postings, doc
// values, sort/search-after, writers and merge remain outside the milestone, so
// an entry appearing here for any of them is the milestone growing surface it
// was explicitly denied.
var nativeReaderSurface = []string{
	"ErrCorrupt",
	"ErrNoSnapshot",
	"Open",
	"Reader",
	"Reader.Close",
	"Reader.VisibleDocCount",
	"Reader.VisitLiveDocuments",
	"StoredDocument",
}

// TestNIDX01BBoundarySurface guards the boundary itself rather than any
// behavior behind it.
//
// Requirement proved here:
//
//	R6 -- the milestone is delivered entirely behind
//	      inverted.ReadOnlyDocCount. The two sentinels callers classify with
//	      remain the ones the boundary already publishes, and the private
//	      native reader exports no operation beyond opening a committed
//	      generation and counting it.
func TestNIDX01BBoundarySurface(t *testing.T) {
	tester := require.New(t)

	tester.NotNil(nidx01bBoundary, "ReadOnlyDocCount must satisfy the committed-generation counter contract")
	tester.ErrorIs(ErrCorruptIndex, ErrCorruptIndex)
	tester.ErrorIs(ErrNoCommittedIndex, ErrNoCommittedIndex)
	tester.NotErrorIs(ErrNoCommittedIndex, ErrCorruptIndex,
		"an absent committed generation and damaged committed bytes must stay separately classifiable")

	observed := exportedSurfaceOf(t, nativeReaderDir)
	tester.Equal(nativeReaderSurface, observed,
		"the native reader's exported surface changed; NIDX-01B may only extend private generation selection")
}

// exportedSurfaceOf lists the exported top-level identifiers and exported
// methods on exported types declared by the non-test Go sources in dir, sorted.
func exportedSurfaceOf(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err, "the native reader package must exist at %s", dir)
	var surface []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		fileSet := token.NewFileSet()
		file, parseErr := parser.ParseFile(fileSet, filepath.Join(dir, entry.Name()), nil, parser.SkipObjectResolution)
		require.NoError(t, parseErr)
		surface = append(surface, exportedNamesIn(file)...)
	}
	sort.Strings(surface)
	return surface
}

// exportedNamesIn lists the exported names one parsed file declares.
func exportedNamesIn(file *ast.File) []string {
	var names []string
	for _, decl := range file.Decls {
		switch declaration := decl.(type) {
		case *ast.FuncDecl:
			names = append(names, exportedFuncName(declaration)...)
		case *ast.GenDecl:
			names = append(names, exportedSpecNames(declaration)...)
		}
	}
	return names
}

// exportedFuncName names an exported function, or an exported method on an
// exported receiver type as Type.Method.
func exportedFuncName(declaration *ast.FuncDecl) []string {
	if !declaration.Name.IsExported() {
		return nil
	}
	if declaration.Recv == nil || len(declaration.Recv.List) == 0 {
		return []string{declaration.Name.Name}
	}
	receiver := declaration.Recv.List[0].Type
	if star, isPointer := receiver.(*ast.StarExpr); isPointer {
		receiver = star.X
	}
	ident, isIdent := receiver.(*ast.Ident)
	if !isIdent || !ident.IsExported() {
		return nil
	}
	return []string{ident.Name + "." + declaration.Name.Name}
}

// exportedSpecNames names the exported types, constants and variables one
// declaration group introduces.
func exportedSpecNames(declaration *ast.GenDecl) []string {
	var names []string
	for _, spec := range declaration.Specs {
		switch specification := spec.(type) {
		case *ast.TypeSpec:
			if specification.Name.IsExported() {
				names = append(names, specification.Name.Name)
			}
		case *ast.ValueSpec:
			for _, name := range specification.Names {
				if name.IsExported() {
					names = append(names, name.Name)
				}
			}
		}
	}
	return names
}
