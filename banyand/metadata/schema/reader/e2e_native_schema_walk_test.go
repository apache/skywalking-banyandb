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

package reader

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
)

// schemaCatalogWalker is the product seam issue #14011 names: the offline
// catalog walk the migration tooling calls, narrowed to a set of schema kinds.
// The cutover happens entirely behind it -- callers keep passing kinds and
// keep receiving Doc values -- so a boundary test observes documents and
// errors here, never a dictionary, a posting list or a term.
type schemaCatalogWalker func(schemaRoot string, visit func(Doc) error, kinds ...schema.Kind) error

// nidx01dSeam binds the contract to the production symbols that satisfy it, so
// a change to either walk's signature fails to compile here rather than
// quietly redefining the milestone.
var (
	nidx01dShardSeam   schemaCatalogWalker = WalkShard
	nidx01dCatalogSeam schemaCatalogWalker = WalkDocs
)

// nativeSeamOfInverted is every identifier the schema reader may use from
// pkg/index/inverted once the cutover has happened: the two read-only walks,
// the one bounded selection they accept, the borrowed document they hand back,
// and the sentinels a caller classifies a read-only failure with.
//
// A query builder, a query type, a store constructor or a segment type
// appearing here is the schema reader building queries again, which is exactly
// what issue #14011's acceptance criteria forbid.
var nativeSeamOfInverted = map[string]struct{}{
	"ErrCorruptIndex":         {},
	"ErrInvalidSelection":     {},
	"ErrNoCommittedIndex":     {},
	"ReadOnlySelectDocuments": {},
	"ReadOnlyWalkDocuments":   {},
	"StoredDocument":          {},
	"TermSelection":           {},
}

// allowedReaderExternalImports is the schema reader's whole third-party
// dependency budget after the cutover: the protobuf runtime it decodes stored
// catalog payloads with. Everything else it needs is BanyanDB's own code or the
// standard library.
//
// The reader opens index directories through pkg/index/inverted's read-only
// entry points, so it links no search library of its own; an import outside
// this set is the schema walk reaching past the native seam.
var allowedReaderExternalImports = map[string]struct{}{
	"google.golang.org/protobuf/encoding/protojson": {},
	"google.golang.org/protobuf/proto":              {},
}

// walkedDoc is one catalog document reduced to the three declared facts that
// identify it: which property it belongs to, which kind it is, and which
// revision won. Rendering the comparison this way keeps a failure readable as
// "p3 should not be here" rather than as a wall of embedded JSON.
type walkedDoc struct {
	propID string
	kind   string
	modRev int64
}

// walkNIDX01DCatalog runs the catalog walk over the checked-in corpus for the
// given kinds and returns the documents it visited, ordered so the comparison
// does not pin the order revisions happen to be laid out in.
func walkNIDX01DCatalog(kinds ...schema.Kind) ([]walkedDoc, error) {
	var visited []walkedDoc
	err := nidx01dCatalogSeam(nidx01dRoot, func(d Doc) error {
		visited = append(visited, walkedDoc{propID: d.PropID, kind: d.KindName, modRev: d.ModRev})
		return nil
	}, kinds...)
	sort.Slice(visited, func(left, right int) bool {
		return visited[left].propID < visited[right].propID
	})
	return visited, err
}

// TestE2ESchemaWalkNativeStreamOnly walks the checked-in legacy catalog for
// stream schemas, the way the stream migration loads a backup snapshot.
//
// Requirement proved here:
//
//	R2 -- the catalog walk returns exactly the results issue #14011 declares
//	      for kinds {Stream}: only p1@2. Latest-revision selection drops p1@1,
//	      tombstone suppression drops p3 whose latest revision is a tombstone,
//	      and kind selection drops the measure and group properties. The
//	      embedded `_source` bytes survive the walk, so the migration's own
//	      loader still decodes a usable stream schema out of the same document.
//
//	R3 -- filtering precedes stored-field decode: the corpus's damaged measure
//	      revision is never decoded by a stream walk, so this load succeeds
//	      rather than failing on a document it was never asked for.
func TestE2ESchemaWalkNativeStreamOnly(t *testing.T) {
	tester := require.New(t)

	visited, err := walkNIDX01DCatalog(schema.KindStream)
	tester.NoError(err, "a stream walk must not decode the corpus's damaged measure revision")
	tester.Equal([]walkedDoc{{propID: nidx01dPropID1, kind: "stream", modRev: 2}}, visited)

	streams, err := LoadStreams(nidx01dRoot, []string{nidx01dGroup})
	tester.NoError(err)
	tester.Len(streams[nidx01dGroup], 1)
	tester.Equal("s1", streams[nidx01dGroup][0].GetMetadata().GetName(),
		"the winning revision's embedded schema payload must survive the walk byte for byte")
}

// TestE2ESchemaWalkNativeStreamAndGroup walks the checked-in legacy catalog for
// two kinds at once, the way the migration resolves stream schemas together
// with the groups that own them.
//
// Requirement proved here:
//
//	R2 -- a two-kind walk unions the two kinds' documents and returns exactly
//	      the results issue #14011 declares for {Stream, Group}: p1@2 and p4@1.
//	      Each document is visited once, p3 stays absent, and the measure
//	      properties stay absent.
func TestE2ESchemaWalkNativeStreamAndGroup(t *testing.T) {
	tester := require.New(t)

	visited, err := walkNIDX01DCatalog(schema.KindStream, schema.KindGroup)
	tester.NoError(err)
	tester.Equal([]walkedDoc{
		{propID: nidx01dPropID4, kind: "group", modRev: 1},
		{propID: nidx01dPropID1, kind: "stream", modRev: 2},
	}, visited, "a two-kind walk is the union of the two kinds, each document once")

	groups, err := LoadGroups(nidx01dRoot, []string{nidx01dGroupName})
	tester.NoError(err)
	tester.Len(groups, 1)
	tester.EqualValues(3, groups[nidx01dGroupName].GetResourceOpts().GetSegmentInterval().GetNum(),
		"the group property's embedded payload must survive the walk")
}

// TestE2ESchemaWalkNativeCorruptMeasureIsTyped walks the checked-in legacy
// catalog for the kind whose corpus revision is deliberately damaged.
//
// Requirement proved here:
//
//	R3 -- selecting the kind that owns the damaged revision decodes it, and
//	      that decode fails with the native typed corruption error rather than
//	      with a panic, a hang, a silently short result or an untyped failure a
//	      caller cannot classify. A migration that cannot read a measure schema
//	      must abort loudly instead of migrating a catalog it only partly read.
func TestE2ESchemaWalkNativeCorruptMeasureIsTyped(t *testing.T) {
	tester := require.New(t)

	visited, err := walkNIDX01DCatalog(schema.KindMeasure)
	tester.ErrorIs(err, inverted.ErrCorruptIndex,
		"a damaged stored record must reach the caller as the native typed corruption error")
	tester.NotErrorIs(err, inverted.ErrNoCommittedIndex,
		"a damaged catalog is not an absent one; callers classify the two differently")
	tester.Empty(visited, "a walk that fails must not also publish a partial catalog")

	_, loadErr := LoadMeasures(nidx01dRoot, []string{nidx01dGroup})
	tester.ErrorIs(loadErr, inverted.ErrCorruptIndex,
		"the migration's own measure loader must surface the same classified failure")
}

// TestE2ESchemaWalkNativeShardSeamMatchesCatalog walks one shard directly,
// which is the seam the catalog walk is built on.
//
// Requirement proved here:
//
//	R2 -- the per-shard walk reports every physical revision the kind
//	      selection holds, and the catalog walk's latest-revision and tombstone
//	      rules are applied above it. The corpus's single shard holds four
//	      stream revisions -- p1 twice and p3 twice -- of which the catalog
//	      walk publishes one.
func TestE2ESchemaWalkNativeShardSeamMatchesCatalog(t *testing.T) {
	tester := require.New(t)

	var revisions []walkedDoc
	tester.NoError(nidx01dShardSeam(nidx01dShardDir, func(d Doc) error {
		revisions = append(revisions, walkedDoc{propID: d.PropID, kind: d.KindName, modRev: d.ModRev})
		return nil
	}, schema.KindStream))
	sort.Slice(revisions, func(left, right int) bool {
		if revisions[left].propID != revisions[right].propID {
			return revisions[left].propID < revisions[right].propID
		}
		return revisions[left].modRev < revisions[right].modRev
	})

	tester.Equal([]walkedDoc{
		{propID: nidx01dPropID1, kind: "stream", modRev: 1},
		{propID: nidx01dPropID1, kind: "stream", modRev: 2},
		{propID: nidx01dPropID3, kind: "stream", modRev: 1},
		{propID: nidx01dPropID3, kind: "stream", modRev: 2},
	}, revisions, "the shard seam reports every stream revision the corpus declares")
}

// TestE2ESchemaWalkNativeLeavesCorpusUnchanged inventories the checked-in
// corpus before and after the walks that read it.
//
// Requirement proved here:
//
//	R4 -- the schema walk introduces no reader lock and no directory mutation.
//	      Every file's name, size, mode, modification time and content hash is
//	      unchanged afterwards, which is what lets the migration read a catalog
//	      on a live volume, and what makes rolling the schema readers back a
//	      matter of choosing a reader rather than repairing a directory.
func TestE2ESchemaWalkNativeLeavesCorpusUnchanged(t *testing.T) {
	tester := require.New(t)

	before := nidx01dInventory(t)

	_, err := walkNIDX01DCatalog(schema.KindStream, schema.KindGroup)
	tester.NoError(err)
	_, _ = walkNIDX01DCatalog(schema.KindMeasure)

	tester.Equal(before, nidx01dInventory(t),
		"reading a catalog must leave every file's bytes, mode and modification time alone")
}

// TestE2ESchemaWalkNativeReachesNativeReader is the structural half of the
// cutover. Returning the declared documents is not enough on its own: the point
// of the milestone is that schema walks stop building and executing queries
// against the retired reader, and results that are right for the wrong reason
// would hide that.
//
// Requirement proved here:
//
//	R5 -- the schema reader links no search library of its own and reaches
//	      pkg/index/inverted only through its read-only seam. Every third-party
//	      import of the package is on an explicit allowlist, and every
//	      identifier the package takes from pkg/index/inverted is one of the
//	      read-only walks, the bounded selection they accept, the borrowed
//	      document they yield, or a classification sentinel -- never a query
//	      builder, a query type or a store constructor.
func TestE2ESchemaWalkNativeReachesNativeReader(t *testing.T) {
	tester := require.New(t)

	sources := 0
	for _, file := range readerPackageSources(t) {
		sources++
		imports := importQualifiersOf(t, file.syntax, file.path)
		for _, importPath := range imports {
			if isStandardLibrary(importPath) || strings.HasPrefix(importPath, banyanDBModule) {
				continue
			}
			_, allowed := allowedReaderExternalImports[importPath]
			tester.True(allowed, "%s imports %s outside the schema reader's dependency allowlist",
				file.path, importPath)
		}
		for _, used := range selectorsOnPackage(file.syntax, imports, invertedPackage) {
			_, allowed := nativeSeamOfInverted[used]
			tester.True(allowed, "%s uses %s.%s, which is outside the read-only seam the schema walk may reach",
				file.path, filepath.Base(invertedPackage), used)
		}
	}
	tester.NotZero(sources, "the schema reader package must have source files")
}

const (
	banyanDBModule   = "github.com/apache/skywalking-banyandb/"
	invertedPackage  = banyanDBModule + "pkg/index/inverted"
	readerSourceGlob = "*.go"
)

// parsedSource is one parsed non-test source file of this package.
type parsedSource struct {
	syntax *ast.File
	path   string
}

// readerPackageSources parses every non-test Go source of this package.
func readerPackageSources(t *testing.T) []parsedSource {
	t.Helper()
	paths, err := filepath.Glob(readerSourceGlob)
	require.NoError(t, err)
	var sources []parsedSource
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		fileSet := token.NewFileSet()
		syntax, parseErr := parser.ParseFile(fileSet, path, nil, parser.SkipObjectResolution)
		require.NoError(t, parseErr)
		sources = append(sources, parsedSource{path: path, syntax: syntax})
	}
	return sources
}

// importQualifiersOf returns the file's imported package paths keyed by the
// local name they are referred to by.
func importQualifiersOf(t *testing.T, file *ast.File, path string) map[string]string {
	t.Helper()
	imports := map[string]string{}
	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		require.NoError(t, err, "%s has an unparsable import", path)
		name := filepath.Base(importPath)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imports[name] = importPath
	}
	return imports
}

// selectorsOnPackage returns every identifier the file selects from the given
// imported package, sorted and deduplicated.
func selectorsOnPackage(file *ast.File, imports map[string]string, importPath string) []string {
	used := map[string]struct{}{}
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, isIdent := selector.X.(*ast.Ident)
		if !isIdent || imports[ident.Name] != importPath {
			return true
		}
		used[selector.Sel.Name] = struct{}{}
		return true
	})
	names := make([]string, 0, len(used))
	for name := range used {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// isStandardLibrary reports whether an import path names a standard library
// package, which is the paths whose first element carries no domain.
func isStandardLibrary(importPath string) bool {
	return !strings.Contains(strings.SplitN(importPath, "/", 2)[0], ".")
}

// nidx01dInventory renders the checked-in corpus as one line per file holding
// its name, size, mode, modification time and content hash.
func nidx01dInventory(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(nidx01dShardDir)
	require.NoError(t, err)
	inventory := make([]string, 0, len(entries))
	for _, entry := range entries {
		info, infoErr := entry.Info()
		require.NoError(t, infoErr)
		payload, readErr := os.ReadFile(filepath.Join(nidx01dShardDir, entry.Name()))
		require.NoError(t, readErr)
		sum := sha256.Sum256(payload)
		inventory = append(inventory, fmt.Sprintf("%s %d %s %d %s",
			entry.Name(), info.Size(), info.Mode(), info.ModTime().UnixNano(), hex.EncodeToString(sum[:])))
	}
	sort.Strings(inventory)
	return inventory
}
