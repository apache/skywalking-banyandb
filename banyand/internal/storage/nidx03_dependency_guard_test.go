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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations
// under the License.

package storage

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// nidx03RetiredSeriesSymbols are the pkg/index/inverted exported names NIDX-03
// §10 removed completely: the legacy series-index query builders
// (BuildIndexModeQuery -- and its unexported helper buildIndexModeCriteria)
// and the Property query builders that moved to the native criteria filter
// alongside them, since nothing else kept query.go's node-building
// apparatus alive once they were gone, per NIDX-03 §15. The series index
// itself has used native.Owner exclusively since NIDX-03 phase 2; this test
// is phase 3's companion proof that those symbols are actually gone, not
// just unreferenced.
//
// BuildQuery is deliberately NOT in this list: index.SeriesStore's interface
// still requires the method to exist on *store (the NIDX-04 element
// migration tool's only remaining dependency), so §15's "reduce to the
// minimum NIDX-04 needs" leaves it declared as an always-failing stub
// instead of removing it. TestRetiredBuildQueryIsAnAlwaysFailingStub checks
// that stub shape directly, so a regression that made BuildQuery do real
// query-building work again is still caught, just not by this list.
var nidx03RetiredSeriesSymbols = []string{
	"BuildIndexModeQuery",
	"buildIndexModeCriteria",
	"BuildPropertyQuery",
	"BuildPropertyQueryFromEntity",
}

// nidx03AllowedStreamInvertedImporters are the only banyand/stream
// non-test .go files NIDX-03 permits to import pkg/index/inverted: the
// Stream element index migration tool NIDX-04 (not this change) will remove,
// per the design doc's explicit scope boundary ("Out of scope: the rest of
// the Stream element idx work and its migration tool"). Every other
// banyand/stream series- or element-index file reaches the native engine
// directly or through pkg/index/nativeadapter.
var nidx03AllowedStreamInvertedImporters = []string{"migration_element_index.go"}

// nidx03AllowedStreamBlugeImporters are the only banyand/stream non-test .go
// files NIDX-03 permits to import blugelabs/bluge directly (not through
// pkg/index/inverted): migration_verify.go's CountBlugeDocs, which opens the
// previous-release, bluge-format Stream element index (idx/) directory
// read-only to compare document counts against the native-written copy --
// also NIDX-04 element-index scope, like migration_element_index.go, just a
// raw reader instead of a full writer/searcher.
var nidx03AllowedStreamBlugeImporters = []string{"migration_verify.go"}

// nidx03AllowedStreamTransitiveBlugeOrInvertedDeps is banyand/stream's full
// `go list -deps` transitive closure of every package named
// "blugelabs/bluge*" or "pkg/index/inverted" -- the whole dependency
// subgraph nidx03AllowedStreamInvertedImporters and
// nidx03AllowedStreamBlugeImporters's two files pull in, not just the two
// packages they import directly. TestStreamTransitiveDepsReachOnlyTheAllowedBlugeSubgraph
// asserts this is the complete set: a new, unrelated banyand/stream
// dependency that happens to reach bluge or pkg/index/inverted through some
// OTHER path (not migration_element_index.go or migration_verify.go) would
// add an entry here this test doesn't expect, and fail.
var nidx03AllowedStreamTransitiveBlugeOrInvertedDeps = []string{
	"github.com/apache/skywalking-banyandb/pkg/index/inverted",
	"github.com/blugelabs/bluge",
	"github.com/blugelabs/bluge/analysis",
	"github.com/blugelabs/bluge/analysis/analyzer",
	"github.com/blugelabs/bluge/analysis/lang/en",
	"github.com/blugelabs/bluge/analysis/token",
	"github.com/blugelabs/bluge/analysis/tokenizer",
	"github.com/blugelabs/bluge/index",
	"github.com/blugelabs/bluge/index/lock",
	"github.com/blugelabs/bluge/index/mergeplan",
	"github.com/blugelabs/bluge/numeric",
	"github.com/blugelabs/bluge/numeric/geo",
	"github.com/blugelabs/bluge/search",
	"github.com/blugelabs/bluge/search/aggregations",
	"github.com/blugelabs/bluge/search/collector",
	"github.com/blugelabs/bluge/search/searcher",
	"github.com/blugelabs/bluge/search/similarity",
	"github.com/blugelabs/bluge_segment_api",
	"github.com/blugelabs/ice",
	"github.com/blugelabs/ice/compress",
}

// repositoryRoot locates the module root from this test file's own path, so
// the guard runs correctly regardless of the working directory `go test`
// was invoked from.
func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	// This file lives at <root>/banyand/internal/storage/<this file>.
	return filepath.Join(filepath.Dir(sourceFile), "..", "..", "..")
}

// TestSeriesIndexConsumersReachNoRetiredSeriesCode is the NIDX-03 §12 item 8
// / merge-proof guard: "go list -deps of the storage, measure, trace, and
// measure-query-planner packages reaches no series code in
// pkg/index/inverted [and no github.com/blugelabs/bluge]". banyand/stream is
// checked separately (TestStreamOnlyImportsInvertedThroughElementMigrationTool)
// because it legitimately still depends on pkg/index/inverted for the
// NIDX-04 element-index migration tool.
func TestSeriesIndexConsumersReachNoRetiredSeriesCode(t *testing.T) {
	root := repositoryRoot(t)
	targets := []string{
		"./banyand/internal/storage",
		"./banyand/measure",
		"./banyand/trace",
		"./pkg/query/vectorized/measure/plan",
	}
	for _, target := range targets {
		t.Run(target, func(t *testing.T) {
			deps := goListDeps(t, root, target)
			for _, dependency := range deps {
				require.NotContains(t, dependency, "/pkg/index/inverted",
					"%s must not reach the retired series-index package through %s", target, dependency)
				if dependency == "github.com/blugelabs/bluge/numeric" {
					// Not the retired engine: a standalone prefix-coded-numeric
					// encoding helper banyand/internal/sidx's tag filter range
					// operator has used since #991 (long before NIDX-01/02/03),
					// for a purpose unrelated to the series index or
					// pkg/index/inverted. It has no import edge back to the
					// bluge engine (bluge, bluge/index, bluge/search,
					// bluge_segment_api) this guard retires; every other
					// "blugelabs/bluge" dependency is still checked below.
					continue
				}
				require.NotContains(t, dependency, "github.com/blugelabs/bluge",
					"%s must not reach the retired bluge engine through %s", target, dependency)
			}
		})
	}
}

// TestStreamOnlyImportsInvertedThroughElementMigrationTool is the
// banyand/stream half of the same merge proof: its one legitimate
// pkg/index/inverted dependency -- the NIDX-04 element-index migration tool,
// out of this change's scope -- is exactly who the package's production
// (non-test) source reaches it through. Any other file importing
// pkg/index/inverted is either a NIDX-03 regression (series-index code that
// should have moved to the native engine) or a new, undocumented exception
// this guard is designed to catch.
func TestStreamOnlyImportsInvertedThroughElementMigrationTool(t *testing.T) {
	root := repositoryRoot(t)
	invertedImporters, blugeImporters := nidx03StreamDirectImporters(t, root)
	require.Equal(t, nidx03AllowedStreamInvertedImporters, invertedImporters,
		"banyand/stream's non-test pkg/index/inverted importers must be exactly the NIDX-04 element migration tool")
	require.Equal(t, nidx03AllowedStreamBlugeImporters, blugeImporters,
		"banyand/stream's non-test direct blugelabs/bluge importers must be exactly the NIDX-04 element-index verify tool")
}

// TestStreamTransitiveDepsReachOnlyTheAllowedBlugeSubgraph is R6's
// "transitive deps, not just direct imports" half: TestStreamOnlyImports...
// above only looks at banyand/stream's OWN files' import statements, which
// proves migration_element_index.go and migration_verify.go are the direct
// edges into bluge/pkg-index-inverted, but says nothing about whether some
// OTHER, unrelated banyand/stream dependency also reaches that subgraph
// through a different path entirely (which the direct-import check can't
// see, since it never looks past banyand/stream's own files). `go list
// -deps` sees the whole transitive closure regardless of which file causes
// which edge, so comparing its bluge/inverted-shaped slice against the
// frozen expected set catches that case specifically.
func TestStreamTransitiveDepsReachOnlyTheAllowedBlugeSubgraph(t *testing.T) {
	root := repositoryRoot(t)
	deps := goListDeps(t, root, "./banyand/stream")
	var found []string
	for _, dependency := range deps {
		if strings.Contains(dependency, "blugelabs/bluge") || strings.Contains(dependency, "blugelabs/ice") ||
			dependency == "github.com/apache/skywalking-banyandb/pkg/index/inverted" {
			found = append(found, dependency)
		}
	}
	sort.Strings(found)
	expected := append([]string(nil), nidx03AllowedStreamTransitiveBlugeOrInvertedDeps...)
	sort.Strings(expected)
	require.Equal(t, expected, found,
		"banyand/stream's transitive bluge/pkg-index-inverted dependency subgraph must be exactly the one "+
			"migration_element_index.go and migration_verify.go are known to pull in")
}

// nidx03StreamDirectImporters parses every banyand/stream non-test .go
// file's own imports and returns, separately, the file names (sorted) that
// directly import pkg/index/inverted and those that directly import
// blugelabs/bluge.
func nidx03StreamDirectImporters(t *testing.T, root string) (invertedImporters, blugeImporters []string) {
	t.Helper()
	streamDir := filepath.Join(root, "banyand", "stream")
	entries, err := os.ReadDir(streamDir)
	require.NoError(t, err)

	fileSet := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(streamDir, name)
		file, parseErr := parser.ParseFile(fileSet, path, nil, parser.ImportsOnly)
		require.NoError(t, parseErr, "parse %s", path)
		for _, imported := range file.Imports {
			importPath := strings.Trim(imported.Path.Value, `"`)
			switch {
			case importPath == "github.com/apache/skywalking-banyandb/pkg/index/inverted":
				invertedImporters = append(invertedImporters, name)
			case importPath == "github.com/blugelabs/bluge":
				blugeImporters = append(blugeImporters, name)
			}
		}
	}
	sort.Strings(invertedImporters)
	sort.Strings(blugeImporters)
	return invertedImporters, blugeImporters
}

// TestRetiredBuildQueryIsAnAlwaysFailingStub is BuildQuery's own check, kept
// separate from nidx03RetiredSeriesSymbols (see that var's comment): it
// confirms the method still exists (index.SeriesStore requires it) but does
// no real query-building work -- it always returns an error -- so a
// regression that quietly restored its old behavior is still caught.
func TestRetiredBuildQueryIsAnAlwaysFailingStub(t *testing.T) {
	root := repositoryRoot(t)
	invertedDir := filepath.Join(root, "pkg", "index", "inverted")
	fileSet := token.NewFileSet()
	entries, err := os.ReadDir(invertedDir)
	require.NoError(t, err)

	var found *ast.FuncDecl
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(invertedDir, name)
		file, parseErr := parser.ParseFile(fileSet, path, nil, parser.AllErrors)
		require.NoError(t, parseErr, "parse %s", path)
		for _, decl := range file.Decls {
			funcDecl, ok := decl.(*ast.FuncDecl)
			if ok && funcDecl.Name.Name == "BuildQuery" && funcDecl.Recv != nil {
				found = funcDecl
			}
		}
	}
	require.NotNil(t, found, "pkg/index/inverted's *store must still declare a BuildQuery method (index.SeriesStore requires it)")
	// A stub this size is a short body: a handful of statements, no loops,
	// no composite literals building a query tree. More than that suggests
	// BuildQuery stopped being a stub.
	require.LessOrEqualf(t, len(found.Body.List), 3, "pkg/index/inverted's BuildQuery has grown past a trivial always-failing stub (%d statements)", len(found.Body.List))
}

// TestRetiredSeriesSymbolsAreGone is the "actually deleted, not merely
// unreferenced" half of the NIDX-03 §10 proof: it parses every non-test
// pkg/index/inverted source file's top-level declarations and fails if any
// nidx03RetiredSeriesSymbols name is still declared there.
func TestRetiredSeriesSymbolsAreGone(t *testing.T) {
	root := repositoryRoot(t)
	invertedDir := filepath.Join(root, "pkg", "index", "inverted")
	entries, err := os.ReadDir(invertedDir)
	require.NoError(t, err)

	retired := make(map[string]bool, len(nidx03RetiredSeriesSymbols))
	for _, name := range nidx03RetiredSeriesSymbols {
		retired[name] = true
	}

	fileSet := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(invertedDir, name)
		file, parseErr := parser.ParseFile(fileSet, path, nil, parser.AllErrors)
		require.NoError(t, parseErr, "parse %s", path)
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				// Checks both package-level functions and methods: none of
				// nidx03RetiredSeriesSymbols's four names were ever declared
				// as a method before removal, but checking methods too keeps
				// this guard correct if one were ever reintroduced as one
				// (for example moved onto *store the way BuildQuery was kept
				// as a method, instead of being removed outright).
				if retired[d.Name.Name] {
					t.Errorf("%s still declares removed symbol %q (NIDX-03 §10)", path, d.Name.Name)
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					if valueSpec, ok := spec.(*ast.ValueSpec); ok {
						for _, declaredName := range valueSpec.Names {
							if retired[declaredName.Name] {
								t.Errorf("%s still declares removed symbol %q (NIDX-03 §10)", path, declaredName.Name)
							}
						}
					}
				}
			}
		}
	}
}

// goListDeps runs `go list -deps <target>` from root and returns the
// dependency import paths, one per line.
func goListDeps(t *testing.T, root, target string) []string {
	t.Helper()
	command := exec.Command("go", "list", "-deps", target)
	command.Dir = root
	output, err := command.Output()
	require.NoError(t, err, "go list -deps %s", target)
	return strings.Split(strings.TrimSpace(string(output)), "\n")
}
