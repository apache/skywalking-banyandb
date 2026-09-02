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

package migration

import (
	"context"
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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

const (
	// nativeVisitorPackage and nativeVisitorFunc name the BanyanDB-owned
	// read-only document visitor issue #14010 introduces.
	nativeVisitorPackage = "github.com/apache/skywalking-banyandb/pkg/index/inverted"
	nativeVisitorFunc    = "ReadOnlyWalkDocuments"

	// retiredReaderEntryPoint is the call that opens a retired third-party
	// index reader directly on a source directory. This package reads sources
	// and writes the union destination; only the read side changes, so the
	// destination writer's own entry point is untouched by this assertion.
	retiredReaderEntryPoint = "OpenReader"

	// iceFooterLength is the fixed width of an ICE v3 segment footer. A segment
	// file shorter than it cannot carry a valid one.
	iceFooterLength = 60

	// unionSourceTagName is the stored tag the seeded sources carry, and
	// publishedSeriesLimit bounds a destination lookup so a stray duplicate is
	// reported rather than silently truncated away.
	unionSourceTagName   = "service"
	publishedSeriesLimit = 100
)

// TestSeriesUnionSourceReadReachesNativeVisitor is the structural half of the
// series-union cutover. Publishing the right series is not enough on its own:
// the point of the milestone is that the source read stops going through the
// retired reader, and a union that is right for the wrong reason would hide
// that.
//
// Requirement proved here:
//
//	R5 -- series union's source read reaches the BanyanDB-owned native document
//	      visitor, and no file in this package opens a retired index reader
//	      directly any more.
func TestSeriesUnionSourceReadReachesNativeVisitor(t *testing.T) {
	tester := require.New(t)

	entries, err := os.ReadDir(".")
	tester.NoError(err)
	reachesVisitor := false
	sources := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		sources++
		fileSet := token.NewFileSet()
		file, parseErr := parser.ParseFile(fileSet, name, nil, parser.ParseComments)
		tester.NoError(parseErr)
		imports := importPathsByQualifier(t, file)
		visitorQualifiers := qualifiersForImport(imports, nativeVisitorPackage)
		ast.Inspect(file, func(node ast.Node) bool {
			selector, isSelector := node.(*ast.SelectorExpr)
			if !isSelector {
				return true
			}
			tester.NotEqual(retiredReaderEntryPoint, selector.Sel.Name,
				"%s must not open a retired index reader directly at %s", name, fileSet.Position(selector.Pos()))
			qualifier, isIdent := selector.X.(*ast.Ident)
			if !isIdent || selector.Sel.Name != nativeVisitorFunc {
				return true
			}
			if _, viaVisitor := visitorQualifiers[qualifier.Name]; viaVisitor {
				reachesVisitor = true
			}
			return true
		})
	}
	tester.NotZero(sources, "the migration package must have source files")
	tester.True(reachesVisitor, "series union must read its source documents through %s.%s",
		nativeVisitorPackage, nativeVisitorFunc)
}

// importPathsByQualifier returns the imported package paths of one parsed file,
// keyed by the local name each is referred to by.
func importPathsByQualifier(t *testing.T, file *ast.File) map[string]string {
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

// qualifiersForImport returns the local names one import path is referred to by.
func qualifiersForImport(imports map[string]string, wanted string) map[string]struct{} {
	names := map[string]struct{}{}
	for name, importPath := range imports {
		if importPath == wanted {
			names[name] = struct{}{}
		}
	}
	return names
}

// unionSourceSeries is one series a legacy-created source segment index holds.
// The label is issue #14010's name for the document; a series identifier is a
// hash of the marshaled entity buffer, so the entity value, not the label, is
// what the fixture chooses.
type unionSourceSeries struct {
	label       string
	entityValue string
	tagValue    string
	timestamp   int64
	version     int64
	deleted     bool
}

// unionNativeSourceSeries is the content issue #14010 declares for the union
// case: two live series and one deleted series that must not be published.
var unionNativeSourceSeries = []unionSourceSeries{
	{label: "101", entityValue: "series-101", tagValue: "blue", timestamp: 100, version: 2},
	{label: "202", entityValue: "series-202", tagValue: "red", timestamp: 200, version: 1},
	{label: "303", entityValue: "series-303", tagValue: "gray", timestamp: 300, version: 3, deleted: true},
}

// unionSourceIdentity returns the marshaled entity buffer the store records as
// one declared series' identity.
func unionSourceIdentity(t *testing.T, declared unionSourceSeries) []byte {
	t.Helper()
	series := &pbv1.Series{Subject: "svc", EntityValues: []*modelv1.TagValue{{
		Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: declared.entityValue}},
	}}}
	require.NoError(t, series.Marshal())
	return append([]byte(nil), series.Buffer...)
}

// seedUnionSourceSidx writes the declared series into
// "<groupRoot>/<segName>/sidx" with the compatibility writer and deletes the
// ones declared deleted, so the source is a directory a released BanyanDB
// produced rather than one this test hand-assembled.
func seedUnionSourceSidx(t *testing.T, groupRoot, segName string, seriesSet []unionSourceSeries) string {
	t.Helper()
	sidxDir := filepath.Join(groupRoot, segName, sidxDirName)
	require.NoError(t, os.MkdirAll(sidxDir, storage.DirPerm))

	store, err := inverted.NewStore(inverted.StoreOpts{Path: sidxDir, BatchWaitSec: 0})
	require.NoError(t, err)
	documents := make(index.Documents, 0, len(seriesSet))
	var deletedIdentities [][]byte
	for _, declared := range seriesSet {
		identity := unionSourceIdentity(t, declared)
		tag := index.NewBytesField(index.FieldKey{TagName: unionSourceTagName}, convert.StringToBytes(declared.tagValue))
		tag.Store = true
		tag.Index = true
		tag.NoSort = true
		documents = append(documents, index.Document{
			Fields:       []index.Field{tag},
			EntityValues: identity,
			Timestamp:    declared.timestamp,
			DocID:        convert.Hash(identity),
			Version:      declared.version,
		})
		if declared.deleted {
			deletedIdentities = append(deletedIdentities, identity)
		}
	}
	require.NoError(t, store.UpdateSeriesBatch(index.Batch{Documents: documents}))
	if len(deletedIdentities) > 0 {
		require.NoError(t, store.Delete(deletedIdentities))
	}
	require.NoError(t, store.Close())
	return sidxDir
}

// openPublishedDestination opens a published union index through the retained
// series-store surface, which this milestone does not touch, so what it reports
// cannot agree with a faulty source read by construction.
func openPublishedDestination(t *testing.T, sidxDir string) index.SeriesStore {
	t.Helper()
	store, err := inverted.NewStore(inverted.StoreOpts{Path: sidxDir, BatchWaitSec: 0})
	require.NoError(t, err)
	return store
}

// publishedUnionSeries looks one identity up in a published union index and
// reports the stored tag values it holds, or reports that the index holds no
// live document under that identity. The lookup is an exact one so that the
// answer for one series can never be contaminated by another's.
func publishedUnionSeries(t *testing.T, store index.SeriesStore, identity []byte) ([]string, bool) {
	t.Helper()
	query, err := store.BuildQuery([]index.SeriesMatcher{
		{Type: index.SeriesMatcherTypeExact, Match: identity},
	}, nil, nil)
	require.NoError(t, err)
	documents, err := store.Search(context.Background(),
		[]index.FieldKey{{TagName: unionSourceTagName}}, query, publishedSeriesLimit)
	require.NoError(t, err)
	if len(documents) == 0 {
		return nil, false
	}
	require.Len(t, documents, 1, "identity %q must resolve to at most one live published document", identity)
	var values []string
	if value, stored := documents[0].Fields[unionSourceTagName]; stored && value != nil {
		values = append(values, string(value))
	}
	sort.Strings(values)
	return values, true
}

// truncateUnionSourceSegment cuts a source segment file short of the footer its
// on-disk grammar requires, which is what a segment published by a process that
// died mid-write looks like. The directory still holds a committed manifest
// naming that segment, so the damage is only visible to a reader that validates
// the segment it is about to read.
func truncateUnionSourceSegment(t *testing.T, sidxDir string) {
	t.Helper()
	segments, err := filepath.Glob(filepath.Join(sidxDir, "*.seg"))
	require.NoError(t, err)
	require.NotEmpty(t, segments, "the seeded source must hold a committed segment")
	sort.Strings(segments)
	for _, segmentPath := range segments {
		require.NoError(t, os.Truncate(segmentPath, iceFooterLength-8))
	}
}

// TestE2ESeriesUnionNativeSource walks the production situation this milestone
// exists for: a lifecycle migration unions the series indexes of several node
// replicas into one staged index before broadcasting it. The union reads every
// source through the native document visitor and re-emits the survivors through
// its retained destination writer, which this milestone does not change.
//
// Requirements proved here:
//
//	R3 -- the union reads both sources natively and publishes one copy of each
//	      live series. The second source repeats the first's series 101, and the
//	      existing first-seen dedup winner rule is unchanged, so the destination
//	      holds 101 once and 202 once; the deleted series 303 is never
//	      published.
//
//	R4 -- a source whose committed segment is damaged fails the union with the
//	      native typed corruption error and publishes no staged destination, so
//	      a migration aborts on unreadable input instead of broadcasting a
//	      partial series index. The byte-level stored-chunk failures R4 also
//	      names are proved against the visitor itself in pkg/index/inverted,
//	      where no retired reader stands in the way.
func TestE2ESeriesUnionNativeSource(t *testing.T) {
	tester := require.New(t)

	firstRoot := t.TempDir()
	secondRoot := t.TempDir()
	seedUnionSourceSidx(t, firstRoot, "seg-20260621", unionNativeSourceSeries)
	seedUnionSourceSidx(t, secondRoot, "seg-20260622", unionNativeSourceSeries[:1])

	staging := filepath.Join(t.TempDir(), "union")
	published, err := BuildGroupUnionSidx(context.Background(), []string{firstRoot, secondRoot}, staging, nil)
	tester.NoError(err)
	tester.Equal(staging, published, "a union that read live series must publish its staged index")

	publishedCount, countErr := inverted.ReadOnlyDocCount(published)
	tester.NoError(countErr)
	tester.Equal(int64(2), publishedCount,
		"the union must publish exactly the two live series, each once, even though the first is read twice")

	destination := openPublishedDestination(t, published)
	defer func() {
		tester.NoError(destination.Close())
	}()
	for _, declared := range unionNativeSourceSeries {
		identity := unionSourceIdentity(t, declared)
		values, found := publishedUnionSeries(t, destination, identity)
		if declared.deleted {
			tester.False(found, "the deleted series labeled %s must not be published", declared.label)
			continue
		}
		tester.True(found, "the series labeled %s must be published", declared.label)
		tester.Equal([]string{declared.tagValue}, values,
			"series %s must keep its declared stored tag value", declared.label)
	}

	corruptRoot := t.TempDir()
	truncateUnionSourceSegment(t, seedUnionSourceSidx(t, corruptRoot, "seg-20260621", unionNativeSourceSeries))
	corruptStaging := filepath.Join(t.TempDir(), "corrupt-union")

	corruptPublished, corruptErr := BuildGroupUnionSidx(context.Background(),
		[]string{corruptRoot}, corruptStaging, nil)
	tester.ErrorIs(corruptErr, inverted.ErrCorruptIndex,
		"a damaged source must be reported as a corrupt index, not as an opaque read failure")
	tester.Empty(corruptPublished, "a union that failed on a damaged source must publish no staged index")
}
