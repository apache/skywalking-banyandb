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

package measure

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/migration"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

const (
	// indexModeSourceReadFile is the file that holds index-mode copy's source
	// read. Issue #14010 names index-mode copy's source side as one of the two
	// production readers this milestone switches to the native visitor; the
	// destination writer and every other reader in this package stay where they
	// are, so the assertion is scoped to this file rather than to the package.
	indexModeSourceReadFile = "migration_indexmode_copy.go"

	// nativeVisitorPackage and nativeVisitorFunc name the BanyanDB-owned
	// read-only document visitor issue #14010 introduces.
	nativeVisitorPackage = "github.com/apache/skywalking-banyandb/pkg/index/inverted"
	nativeVisitorFunc    = "ReadOnlyWalkDocuments"

	// retiredReaderEntryPoint is the call that opens a retired third-party
	// index reader directly on a source directory. The whole point of the
	// milestone is that index-mode copy's source read stops making it.
	retiredReaderEntryPoint = "OpenReader"

	// iceFooterLength is the fixed width of an ICE v3 segment footer. A segment
	// file shorter than it cannot carry a valid one.
	iceFooterLength = 60

	// nativeSourceTagName is the stored, non-indexed tag the seeded sources
	// carry, and publishedSeriesLimit bounds a destination lookup so a stray
	// duplicate is reported rather than silently truncated away.
	nativeSourceTagName  = "properties"
	publishedSeriesLimit = 100
)

// TestIndexModeCopySourceReadReachesNativeVisitor is the structural half of the
// index-mode copy cutover. Copying the right documents is not enough on its
// own: the point of the milestone is that the source read stops going through
// the retired reader, and a copy that is right for the wrong reason would hide
// that.
//
// Requirement proved here:
//
//	R5 -- index-mode copy's source read reaches the BanyanDB-owned native
//	      document visitor and no longer opens a retired index reader directly.
func TestIndexModeCopySourceReadReachesNativeVisitor(t *testing.T) {
	tester := require.New(t)

	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, indexModeSourceReadFile, nil, parser.ParseComments)
	tester.NoError(err)

	imports := importPathsByQualifier(t, file)
	visitorQualifiers := qualifiersForImport(imports, nativeVisitorPackage)
	tester.NotEmpty(visitorQualifiers, "%s must import %s", indexModeSourceReadFile, nativeVisitorPackage)

	reachesVisitor := false
	ast.Inspect(file, func(node ast.Node) bool {
		selector, isSelector := node.(*ast.SelectorExpr)
		if !isSelector {
			return true
		}
		tester.NotEqual(retiredReaderEntryPoint, selector.Sel.Name,
			"%s must not open a retired index reader directly at %s",
			indexModeSourceReadFile, fileSet.Position(selector.Pos()))
		qualifier, isIdent := selector.X.(*ast.Ident)
		if !isIdent || selector.Sel.Name != nativeVisitorFunc {
			return true
		}
		if _, viaVisitor := visitorQualifiers[qualifier.Name]; viaVisitor {
			reachesVisitor = true
		}
		return true
	})
	tester.True(reachesVisitor, "%s must read its source documents through %s.%s",
		indexModeSourceReadFile, nativeVisitorPackage, nativeVisitorFunc)
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

// nativeSourceSeries is one series a legacy-created source segment index holds.
// The label is issue #14010's name for the document; a series identifier is a
// hash of the marshaled entity buffer, so the entity value, not the label, is
// what the fixture chooses.
type nativeSourceSeries struct {
	label       string
	entityValue string
	tagValue    string
	timestamp   int64
	version     int64
	deleted     bool
}

// indexModeNativeSourceSeries is the content issue #14010 declares for the
// migration source: two live series and one deleted series that must not reach
// the destination.
func indexModeNativeSourceSeries(t *testing.T) []nativeSourceSeries {
	t.Helper()
	base := day20260621Nanos(t) + int64(time.Hour)
	return []nativeSourceSeries{
		{label: "101", entityValue: "series-101", tagValue: "blue", timestamp: base, version: 2},
		{label: "202", entityValue: "series-202", tagValue: "red", timestamp: base, version: 1},
		{label: "303", entityValue: "series-303", tagValue: "gray", timestamp: base, version: 3, deleted: true},
	}
}

// seedNativeSourceSidx writes the declared series into "<root>/<segName>/sidx"
// with the compatibility writer, then deletes the ones declared deleted, so the
// source is a directory a released BanyanDB produced rather than one this test
// hand-assembled.
func seedNativeSourceSidx(t *testing.T, root, segName string, seriesSet []nativeSourceSeries) string {
	t.Helper()
	sidxDir := filepath.Join(root, segName, directCopySidxDirName)
	require.NoError(t, os.MkdirAll(sidxDir, storage.DirPerm))

	store, err := inverted.NewStore(inverted.StoreOpts{Path: sidxDir, BatchWaitSec: 0})
	require.NoError(t, err)
	documents := make(index.Documents, 0, len(seriesSet))
	var deletedIdentities [][]byte
	for _, declared := range seriesSet {
		identity := nativeSourceIdentity(t, declared)
		tag := index.NewBytesField(index.FieldKey{TagName: "properties"}, convert.StringToBytes(declared.tagValue))
		tag.Store = true
		tag.Index = false
		subject := index.NewStringField(index.FieldKey{TagName: index.IndexModeName}, "svc")
		subject.Index = true
		subject.NoSort = true
		entityTag := index.NewBytesField(index.FieldKey{TagName: index.IndexModeEntityTagPrefix + "id"},
			convert.StringToBytes(declared.entityValue))
		entityTag.Index = true
		entityTag.NoSort = true
		documents = append(documents, index.Document{
			Fields:       []index.Field{tag, subject, entityTag},
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

// nativeSourceIdentity returns the marshaled entity buffer the store records as
// one declared series' identity.
func nativeSourceIdentity(t *testing.T, declared nativeSourceSeries) []byte {
	t.Helper()
	series := &pbv1.Series{Subject: "svc", EntityValues: []*modelv1.TagValue{strTagValue(declared.entityValue)}}
	require.NoError(t, series.Marshal())
	return append([]byte(nil), series.Buffer...)
}

// observedSeries is what the independent destination oracle reports for one
// published series: its timestamp, its version and its stored tag values. It is
// read back through the retained series-store query path, which this milestone
// does not touch, so it cannot agree with a faulty source read by construction.
type observedSeries struct {
	tagValues []string
	timestamp int64
	version   int64
}

// openPublishedDestination opens a published destination sidx through the
// retained series-store surface, which this milestone does not touch, so what
// it reports cannot agree with a faulty source read by construction.
func openPublishedDestination(t *testing.T, sidxDir string) index.SeriesStore {
	t.Helper()
	store, err := inverted.NewStore(inverted.StoreOpts{Path: sidxDir, BatchWaitSec: 0})
	require.NoError(t, err)
	return store
}

// publishedSeriesFor looks one identity up in a published destination and
// reports what it holds, or reports that the destination holds no live document
// under that identity. The lookup is an exact one so that the answer for one
// series can never be contaminated by another's.
func publishedSeriesFor(t *testing.T, store index.SeriesStore, identity []byte) (observedSeries, bool) {
	t.Helper()
	query, err := store.BuildQuery([]index.SeriesMatcher{
		{Type: index.SeriesMatcherTypeExact, Match: identity},
	}, nil, nil)
	require.NoError(t, err)
	documents, err := store.Search(context.Background(),
		[]index.FieldKey{{TagName: nativeSourceTagName}}, query, publishedSeriesLimit)
	require.NoError(t, err)
	if len(documents) == 0 {
		return observedSeries{}, false
	}
	require.Len(t, documents, 1, "identity %q must resolve to at most one live destination document", identity)
	var values []string
	if value, stored := documents[0].Fields[nativeSourceTagName]; stored && value != nil {
		values = append(values, string(value))
	}
	sort.Strings(values)
	return observedSeries{
		timestamp: documents[0].Timestamp,
		version:   documents[0].Version,
		tagValues: values,
	}, true
}

// truncateSourceSegment cuts a source segment file short of the footer its
// on-disk grammar requires, which is what a segment published by a process that
// died mid-write looks like. The directory still holds a committed manifest
// naming that segment, so the damage is only visible to a reader that validates
// the segment it is about to read.
func truncateSourceSegment(t *testing.T, sidxDir string) {
	t.Helper()
	segments, err := filepath.Glob(filepath.Join(sidxDir, "*.seg"))
	require.NoError(t, err)
	require.NotEmpty(t, segments, "the seeded source must hold a committed segment")
	sort.Strings(segments)
	for _, segmentPath := range segments {
		require.NoError(t, os.Truncate(segmentPath, iceFooterLength-8))
	}
}

// TestE2EIndexModeCopyNativeSource walks the production situation this
// milestone exists for: an operator migrates an index-mode measure group whose
// segment indexes were written by a released BanyanDB. Index-mode copy reads
// those sources through the native document visitor and publishes the rebuilt
// documents through its retained destination writer, which this milestone does
// not change.
//
// Two source roots feed the same target segment, which is what a group
// migrated from replicated nodes looks like and what forces the copy down its
// rebuild path: a source that exclusively owns its target segment is byte
// copied whole and never reads a document at all.
//
// Requirements proved here:
//
//	R2 -- index-mode copy rebuilds the two live series through the native source
//	      visitor, preserving each one's declared timestamp, version, stored tag
//	      value and identity, while its destination stays written by the
//	      retained writer. The deleted series never reaches the destination.
//
//	R4 -- a source whose committed segment is damaged fails the copy with the
//	      native typed corruption error and publishes no destination, so a
//	      migration aborts on unreadable input instead of silently writing a
//	      partial target group. The byte-level stored-chunk failures R4 also
//	      names are proved against the visitor itself in
//	      pkg/index/inverted, where no retired reader stands in the way.
func TestE2EIndexModeCopyNativeSource(t *testing.T) {
	tester := require.New(t)
	declared := indexModeNativeSourceSeries(t)
	interval := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	const sourceSegName = "seg-20260621"

	firstRoot := t.TempDir()
	secondRoot := t.TempDir()
	targetRoot := filepath.Join(t.TempDir(), "target")
	seedNativeSourceSidx(t, firstRoot, sourceSegName, declared)
	seedNativeSourceSidx(t, secondRoot, sourceSegName, declared[:1])

	result, err := copyIndexModeGroup(context.Background(), migration.EntryGroupInput{
		EntryTag:        "[entry 1/1]",
		Group:           "sw_metadata",
		TargetGroupRoot: targetRoot,
		SrcRoots:        []string{firstRoot, secondRoot},
		Interval:        interval,
	}, map[uint32]indexRuleInfo{}, svcSchemas())
	tester.NoError(err)
	tester.Equal(int64(2), result.Rows,
		"the two live series survive; the repeat of the first collapses onto it and the deleted one is never read")

	targetSidx := filepath.Join(targetRoot, sourceSegName, directCopySidxDirName)
	publishedCount, countErr := inverted.ReadOnlyDocCount(targetSidx)
	tester.NoError(countErr)
	tester.Equal(int64(2), publishedCount, "the destination must hold exactly the two live series")

	destination := openPublishedDestination(t, targetSidx)
	defer func() {
		tester.NoError(destination.Close())
	}()
	for _, declaredSeries := range declared {
		identity := nativeSourceIdentity(t, declaredSeries)
		observed, found := publishedSeriesFor(t, destination, identity)
		if declaredSeries.deleted {
			tester.False(found, "the deleted series labeled %s must not reach the destination", declaredSeries.label)
			continue
		}
		tester.True(found, "the series labeled %s must reach the destination", declaredSeries.label)
		tester.Equal(declaredSeries.timestamp, observed.timestamp,
			"series %s must keep its declared timestamp", declaredSeries.label)
		tester.Equal(declaredSeries.version, observed.version,
			"series %s must keep its declared version", declaredSeries.label)
		tester.Equal([]string{declaredSeries.tagValue}, observed.tagValues,
			"series %s must keep its declared stored tag value", declaredSeries.label)
	}

	corruptRoot := t.TempDir()
	corruptTarget := filepath.Join(t.TempDir(), "corrupt-target")
	truncateSourceSegment(t, seedNativeSourceSidx(t, corruptRoot, sourceSegName, declared))

	_, corruptErr := copyIndexModeGroup(context.Background(), migration.EntryGroupInput{
		EntryTag:        "[entry 1/1]",
		Group:           "sw_metadata",
		TargetGroupRoot: corruptTarget,
		SrcRoots:        []string{corruptRoot},
		Interval:        interval,
	}, map[uint32]indexRuleInfo{}, svcSchemas())
	tester.ErrorIs(corruptErr, inverted.ErrCorruptIndex,
		"a damaged source must be reported as a corrupt index, not as an opaque read failure")
	tester.NoDirExists(filepath.Join(corruptTarget, sourceSegName, directCopySidxDirName),
		"a copy that failed on a damaged source must publish no destination sidx")
}
