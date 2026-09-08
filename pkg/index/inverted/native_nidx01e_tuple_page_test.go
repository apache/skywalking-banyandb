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
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index"
)

const (
	// iceFooterDocValueStart and iceFooterDocValueEnd bracket the doc-value
	// section root in the 60-byte ICE v3 segment footer BDB-NIDX-SPEC-001
	// revision 0.2 section 08 fixes the widths of.
	iceFooterDocValueStart = 24
	iceFooterDocValueEnd   = 32

	// nidx01eDeclaredPageSize is the page size issue #14012 declares its two
	// pages at.
	nidx01eDeclaredPageSize = 2

	// The generation sizes and page size the bounded-page requirement is
	// measured over, and the resident growth a bounded pager is allowed across
	// all of them.
	nidx01eBoundedRowCount    = 2000
	nidx01eBoundedPageSize    = 10
	nidx01eBoundedGrowthBytes = 8 << 20
)

// nidx01eSortFields are the four ascending components issue #14012 declares the
// repair order over, most significant first.
var nidx01eSortFields = [RepairSortFieldCount]string{
	nidx01eGroupField, nidx01eNameField, nidx01eEntityField, timestampField,
}

// nidx01eEncodedTimestamps are the exact doc values the corpus records for each
// declared revision. They are literals lifted from the checked-in generation,
// not a re-encoding: an eleven-byte historical prefix-coded signed int64 whose
// leading byte is the shift and whose sign bit is inverted so byte order
// matches numeric order.
var nidx01eEncodedTimestamps = map[int64][]byte{
	5:  {0x20, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05},
	7:  {0x20, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07},
	10: {0x20, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a},
	20: {0x20, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14},
}

// nidx01ePermittedReaderSurface is the ceiling on what the private native
// reader may export once this leaf lands: the NIDX-01D surface, plus opening a
// pinned generation's identifier, the bounded repair tuple page, the row that
// page yields, and the sentinel an out-of-bounds page request is classified
// with.
//
// Nothing else may appear. An entry for a collector, a sort expression, a
// comparator, a heap, a doc-value reader, a chunk decoder, a range, a prefix,
// a wildcard, an analyzer or any writer is the milestone growing the surface
// BDB-NIDX-SPEC-001 revision 0.2 NIDX-01 explicitly denied it, however
// convenient the entry is.
var nidx01ePermittedReaderSurface = map[string]struct{}{
	"ErrCorrupt":                    {},
	"ErrInvalidRepairPage":          {},
	"ErrInvalidSelection":           {},
	"ErrNoSnapshot":                 {},
	"Open":                          {},
	"Reader":                        {},
	"Reader.Close":                  {},
	"Reader.RepairTuplePage":        {},
	"Reader.SnapshotID":             {},
	"Reader.VisibleDocCount":        {},
	"Reader.VisitLiveDocuments":     {},
	"Reader.VisitSelectedDocuments": {},
	"RepairTupleRow":                {},
	"StoredDocument":                {},
}

// TestNativeRepairTuplePageReturnsTheDeclaredFirstPage asks the corpus for the
// first page issue #14012 declares.
//
// Requirement proved here:
//
//	R1 -- a page orders the pinned generation's live rows ascending by all
//	      four declared components and returns the first PageSize of them. The
//	      declared row that the generation deletes sorts between the two rows
//	      of this page, so a page built without the deletion masks would return
//	      it here rather than at the end of the order.
func TestNativeRepairTuplePageReturnsTheDeclaredFirstPage(t *testing.T) {
	tester := require.New(t)
	generation := nidx01eOpen(t, nidx01eShardDir)

	page, err := generation.RepairTuplePage(context.Background(), nidx01eRequest(nidx01eDeclaredPageSize, nil))
	tester.NoError(err)

	declared := nidx01eVisibleRows()
	tester.Equal(nidx01eDeclaredRows(declared[0], declared[1]), page)
}

// TestNativeRepairTuplePageResumesStrictlyAfterTheDeclaredCursor asks for the
// second declared page with the cursor issue #14012 declares.
//
// Requirement proved here:
//
//	R1 -- a page resumes strictly after the complete prior tuple: the row the
//	      cursor names is not repeated, and no row between it and the next in
//	      the order is skipped.
func TestNativeRepairTuplePageResumesStrictlyAfterTheDeclaredCursor(t *testing.T) {
	tester := require.New(t)
	generation := nidx01eOpen(t, nidx01eShardDir)
	declared := nidx01eVisibleRows()

	page, err := generation.RepairTuplePage(context.Background(),
		nidx01eRequest(nidx01eDeclaredPageSize, nidx01eDeclaredTuple(declared[1])))
	tester.NoError(err)

	tester.Equal(nidx01eDeclaredRows(declared[2], declared[3]), page)
}

// TestNativeRepairTuplePagePagesTheWholeGenerationExactlyOnce walks the corpus
// page by page until the order is exhausted.
//
// Requirement proved here:
//
//	R1 -- paging a generation end to end yields every visible row exactly once
//	      and in the declared order, with no duplicate across a page boundary
//	      and no row skipped at one, and the page after the last row is empty
//	      rather than an error.
func TestNativeRepairTuplePagePagesTheWholeGenerationExactlyOnce(t *testing.T) {
	tester := require.New(t)
	generation := nidx01eOpen(t, nidx01eShardDir)

	for _, pageSize := range []int{1, nidx01eDeclaredPageSize, 3, len(nidx01eRows) + 1} {
		walked, pages := nidx01eWalk(t, generation, pageSize)
		declared := nidx01eVisibleRows()
		tester.Equal(nidx01eDeclaredRows(declared...), walked,
			"page size %d must yield the declared rows once each, in order", pageSize)
		expectedPages := (len(declared)+pageSize-1)/pageSize + 1
		tester.Equal(expectedPages, pages,
			"page size %d must exhaust the order in %d calls", pageSize, expectedPages)
	}
}

// TestNativeRepairTuplePageProjectsTheRequestedStoredField reads a page whose
// rows each store two fields.
//
// Requirement proved here:
//
//	R1 -- a row carries the value of the stored field the request projects.
//	      Every corpus row also stores an unrelated payload that is recorded
//	      first, so a page that returned a row's first stored value instead of
//	      the projected one is caught here.
func TestNativeRepairTuplePageProjectsTheRequestedStoredField(t *testing.T) {
	tester := require.New(t)
	generation := nidx01eOpen(t, nidx01eShardDir)

	page, err := generation.RepairTuplePage(context.Background(), nidx01eRequest(len(nidx01eRows), nil))
	tester.NoError(err)
	tester.Len(page, len(nidx01eVisibleRows()))

	for rowIndex, row := range nidx01eVisibleRows() {
		tester.Equal(row.sha, string(page[rowIndex].Value),
			"row %d must project %s, not its stored %s", rowIndex, nidx01eSHAField, nidx01eSourceField)
	}
}

// TestNativeRepairTuplePagePinsTheDeclaredGeneration reads the identifier of
// the generation the corpus commits.
//
// Requirement proved here:
//
//	R4 -- a pinned view names the generation it reads, so a caller can record
//	      which generation a completed page sequence covered instead of asking
//	      the directory again afterwards and recording one it never read.
func TestNativeRepairTuplePagePinsTheDeclaredGeneration(t *testing.T) {
	tester := require.New(t)
	generation := nidx01eOpen(t, nidx01eShardDir)

	tester.Equal(nidx01eSnapshotID, generation.SnapshotID())
}

// TestNativeRepairTuplePageIgnoresAGenerationPublishedBetweenPages publishes a
// row that would head the second page, after the first page has been read.
//
// Requirement proved here:
//
//	R4 -- a generation published after a view is pinned does not enter a later
//	      page of that view. The published row sorts strictly between the
//	      declared cursor and the next declared row, so an unpinned pager would
//	      return it as the first row of page two.
func TestNativeRepairTuplePageIgnoresAGenerationPublishedBetweenPages(t *testing.T) {
	tester := require.New(t)
	shard := copyIndexDir(t, nidx01eShardDir)
	generation := nidx01eOpen(t, shard)
	declared := nidx01eVisibleRows()

	first, err := generation.RepairTuplePage(context.Background(), nidx01eRequest(nidx01eDeclaredPageSize, nil))
	tester.NoError(err)
	tester.Equal(nidx01eDeclaredRows(declared[0], declared[1]), first)

	published := nidx01eRow{group: "g-a", name: "n-a", entityID: "e-1", timestamp: 30, sha: "sha-published", source: "source-published"}
	nidx01ePublish(t, shard, published)

	second, err := generation.RepairTuplePage(context.Background(),
		nidx01eRequest(nidx01eDeclaredPageSize, nidx01eDeclaredTuple(declared[1])))
	tester.NoError(err)
	tester.Equal(nidx01eDeclaredRows(declared[2], declared[3]), second)

	republished := nidx01eOpen(t, shard)
	tester.NotEqual(nidx01eSnapshotID, republished.SnapshotID(),
		"the publication must have committed a newer generation, or the pin is untested")
	after, err := republished.RepairTuplePage(context.Background(), nidx01eRequest(len(nidx01eRows)+1, nil))
	tester.NoError(err)
	tester.Len(after, len(declared)+1, "a view opened after the publication must see the published row")
}

// TestNativeRepairTuplePageOrdersMissingSortValuesLast pages a generation whose
// rows do not all record a value for the least significant component.
//
// A Property revision written with no modification revision records no
// timestamp at all, so this is not a hypothetical: every Property shard holding
// such a revision reaches the repair build through this path, and refusing it
// would make the build fail on data that reads perfectly well today.
//
// Requirement proved here:
//
//	R6 -- a row that records no doc value for a sort component orders after
//	      every row that does, among the rows sharing the components before it,
//	      and reports that component as absent. A cursor carrying the absent
//	      component resumes strictly after that row, so paging still covers the
//	      generation exactly once.
func TestNativeRepairTuplePageOrdersMissingSortValuesLast(t *testing.T) {
	tester := require.New(t)
	shard := nidx01eSeed(t,
		nidx01eRow{group: "g-a", name: "n-a", entityID: "e-1", timestamp: 10, sha: "sha-present"},
		nidx01eRow{group: "g-a", name: "n-a", entityID: "e-1", sha: "sha-absent"},
		nidx01eRow{group: "g-b", name: "n-a", entityID: "e-2", timestamp: 4, sha: "sha-later-group"},
	)
	generation := nidx01eOpen(t, shard)

	page, err := generation.RepairTuplePage(context.Background(), nidx01eRequest(2, nil))
	tester.NoError(err)
	tester.Len(page, 2)
	tester.Equal("sha-present", string(page[0].Value))
	tester.Equal("sha-absent", string(page[1].Value), "a row with no timestamp doc value must order after one that has it")
	tester.Nil(page[1].SortValues[3], "an absent sort component must be reported absent, not defaulted")

	resumed, err := generation.RepairTuplePage(context.Background(), nidx01eRequest(2, page[1].SortValues))
	tester.NoError(err)
	tester.Len(resumed, 1, "resuming after the absent component must skip it and find the remaining row")
	tester.Equal("sha-later-group", string(resumed[0].Value))
}

// TestNativeRepairTuplePageRejectsRequestsOutsideItsBounds asks for pages the
// reader will not serve.
//
// Requirement proved here:
//
//	R4 -- an invalid cursor and a page-size overflow are rejected as bounded,
//	      typed failures before any doc value is read, and stay distinguishable
//	      from damaged committed bytes. No page is returned, so a caller cannot
//	      mistake a rejection for the end of the order and record a generation
//	      it never finished reading.
func TestNativeRepairTuplePageRejectsRequestsOutsideItsBounds(t *testing.T) {
	declared := nidx01eVisibleRows()
	shortCursor := nidx01eDeclaredTuple(declared[1])[:RepairSortFieldCount-1]
	longCursor := append(nidx01eDeclaredTuple(declared[1]), []byte("extra"))
	oversizeCursor := nidx01eDeclaredTuple(declared[1])
	oversizeCursor[0] = make([]byte, MaxRepairSortValueLength+1)

	cases := []struct {
		mutate func(*RepairPageRequest)
		name   string
	}{
		{name: "page size zero", mutate: func(r *RepairPageRequest) { r.PageSize = 0 }},
		{name: "page size negative", mutate: func(r *RepairPageRequest) { r.PageSize = -1 }},
		{name: "page size over the bound", mutate: func(r *RepairPageRequest) { r.PageSize = MaxRepairPageSize + 1 }},
		{name: "empty sort field", mutate: func(r *RepairPageRequest) { r.SortFields[2] = "" }},
		{name: "repeated sort field", mutate: func(r *RepairPageRequest) { r.SortFields[2] = r.SortFields[0] }},
		{name: "empty projection field", mutate: func(r *RepairPageRequest) { r.ProjectField = "" }},
		{name: "short cursor", mutate: func(r *RepairPageRequest) { r.After = shortCursor }},
		{name: "long cursor", mutate: func(r *RepairPageRequest) { r.After = longCursor }},
		{name: "oversize cursor component", mutate: func(r *RepairPageRequest) { r.After = oversizeCursor }},
	}

	generation := nidx01eOpen(t, nidx01eShardDir)
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			tester := require.New(t)
			request := nidx01eRequest(nidx01eDeclaredPageSize, nil)
			testCase.mutate(&request)

			page, err := generation.RepairTuplePage(context.Background(), request)
			tester.ErrorIs(err, ErrInvalidRepairPage)
			tester.NotErrorIs(err, ErrCorruptIndex,
				"an out-of-bounds request and damaged committed bytes must stay separately classifiable")
			tester.Empty(page)
		})
	}
}

// TestNativeRepairTuplePageStopsOnCancellation asks for a page with a context
// that is already canceled.
//
// Requirement proved here:
//
//	R4 -- cancellation stops a page and is reported as cancellation rather than
//	      as damage or as the end of the order, and no rows are returned.
func TestNativeRepairTuplePageStopsOnCancellation(t *testing.T) {
	tester := require.New(t)
	generation := nidx01eOpen(t, nidx01eShardDir)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	page, err := generation.RepairTuplePage(ctx, nidx01eRequest(nidx01eDeclaredPageSize, nil))
	tester.ErrorIs(err, context.Canceled)
	tester.Empty(page)
}

// TestNativeRepairTuplePageReportsDamagedDocValuesAsCorruption pages a copy of
// the corpus whose doc-value section root table has been overwritten.
//
// Requirement proved here:
//
//	R4 -- a doc-value section that violates the ICE v3 grammar fails the page
//	      as bounded, typed corruption. The damage is confined to the
//	      doc-value locations, so a generation whose stored records and
//	      postings are intact still fails here rather than returning a
//	      partially ordered page.
func TestNativeRepairTuplePageReportsDamagedDocValuesAsCorruption(t *testing.T) {
	tester := require.New(t)
	damaged := copyIndexDir(t, nidx01eShardDir)
	damageDocValueLocations(t, newestSegmentFile(t, damaged))

	generation := nidx01eOpen(t, damaged)
	page, err := generation.RepairTuplePage(context.Background(), nidx01eRequest(nidx01eDeclaredPageSize, nil))
	tester.ErrorIs(err, ErrCorruptIndex)
	tester.NotErrorIs(err, ErrNoCommittedIndex)
	tester.Empty(page)
}

// TestNativeRepairTuplePageClassifiesAnAbsentGeneration opens a directory that
// holds no committed generation.
//
// Requirement proved here:
//
//	R4 -- a shard that was never flushed is absent, not damaged. The repair
//	      build treats an absent generation as nothing to do, so the two
//	      failures must stay separately classifiable at the boundary.
func TestNativeRepairTuplePageClassifiesAnAbsentGeneration(t *testing.T) {
	tester := require.New(t)

	generation, err := OpenReadOnlyGeneration(t.TempDir())
	tester.ErrorIs(err, ErrNoCommittedIndex)
	tester.NotErrorIs(err, ErrCorruptIndex)
	tester.Nil(generation)
}

// TestNativeRepairTuplePageIgnoresReservedCRC32 pages a copy of the corpus
// whose reserved CRC32 slots hold bytes no writer computed.
//
// Requirement proved here:
//
//	R4 -- BDB-NIDX-SPEC-001 revision 0.2 DEC-007 preserves the historical
//	      CRC32 fields without calculating, validating or using them, so a
//	      generation carrying arbitrary values in those slots pages exactly as
//	      the untouched corpus does.
func TestNativeRepairTuplePageIgnoresReservedCRC32(t *testing.T) {
	tester := require.New(t)
	rewritten := copyIndexDir(t, nidx01eShardDir)
	fillReservedCRC32(t, rewritten, []byte{0x01, 0x23, 0x45, 0x67})

	generation := nidx01eOpen(t, rewritten)
	page, err := generation.RepairTuplePage(context.Background(), nidx01eRequest(nidx01eDeclaredPageSize, nil))
	tester.NoError(err)

	declared := nidx01eVisibleRows()
	tester.Equal(nidx01eDeclaredRows(declared[0], declared[1]), page)
}

// TestNativeRepairTuplePageLeavesTheDirectoryUnchanged inventories the corpus
// directory before and after paging it end to end, and pages a directory a live
// writer holds open.
//
// Requirement proved here:
//
//	R5 -- paging writes no bytes and takes no exclusive directory lock. Every
//	      entry's name, size, mode, modification time and content hash is
//	      unchanged afterwards, and a shard a live writer owns still serves its
//	      pinned generation, which is what lets a repair build run beside the
//	      Property writer instead of draining it.
func TestNativeRepairTuplePageLeavesTheDirectoryUnchanged(t *testing.T) {
	tester := require.New(t)
	before := dirInventory(t, nidx01eShardDir)

	generation := nidx01eOpen(t, nidx01eShardDir)
	walked, _ := nidx01eWalk(t, generation, nidx01eDeclaredPageSize)
	tester.Equal(nidx01eDeclaredRows(nidx01eVisibleRows()...), walked)
	tester.Equal(before, dirInventory(t, nidx01eShardDir),
		"a read-only page must leave every file's bytes, mode and modification time alone")

	shared := copyIndexDir(t, nidx01eShardDir)
	writer, err := NewStore(StoreOpts{Path: shared})
	tester.NoError(err)
	defer func() {
		tester.NoError(writer.Close())
	}()

	beside := nidx01eOpen(t, shared)
	page, besideErr := beside.RepairTuplePage(context.Background(), nidx01eRequest(nidx01eDeclaredPageSize, nil))
	tester.NoError(besideErr)
	declared := nidx01eVisibleRows()
	tester.Equal(nidx01eDeclaredRows(declared[0], declared[1]), page,
		"an open writer must not change what the pinned committed generation pages")
}

// TestNativeRepairTuplePageBoundsResidentStateAcrossPages pages a generation
// far larger than one page, many pages deep.
//
// Requirement proved here:
//
//	R2 -- a page holds at most the requested number of rows, and paging does
//	      not accumulate: the memory a view retains after two hundred pages of
//	      a two-thousand-row generation is not materially above what it
//	      retained before the first, so the pager is neither materializing the
//	      shard nor carrying a growing offset scan from page to page.
func TestNativeRepairTuplePageBoundsResidentStateAcrossPages(t *testing.T) {
	tester := require.New(t)
	shard := nidx01eSeed(t, nidx01eBoundedRows(nidx01eBoundedRowCount)...)
	generation := nidx01eOpen(t, shard)

	baseline := nidx01eRetainedBytes()
	var after [][]byte
	rows, pages := 0, 0
	for {
		page, err := generation.RepairTuplePage(context.Background(), nidx01eRequest(nidx01eBoundedPageSize, after))
		tester.NoError(err)
		tester.LessOrEqual(len(page), nidx01eBoundedPageSize, "a page must never exceed the size it was asked for")
		if len(page) == 0 {
			break
		}
		rows += len(page)
		pages++
		after = page[len(page)-1].SortValues
	}

	tester.Equal(nidx01eBoundedRowCount, rows)
	tester.Equal(nidx01eBoundedRowCount/nidx01eBoundedPageSize, pages)
	tester.Less(nidx01eRetainedBytes()-baseline, uint64(nidx01eBoundedGrowthBytes),
		"paging %d pages must not accumulate resident state", pages)
}

// TestNativeRepairTuplePageBoundarySurface guards the boundary itself rather
// than any behavior behind it.
//
// Requirement proved here:
//
//	R2 -- the milestone is delivered entirely behind
//	      inverted.OpenReadOnlyGeneration and the bounded page it serves. The
//	      request carries four sort field names, one projection field, one
//	      cursor and one page size and nothing else; the sentinel an
//	      out-of-bounds request is classified with stays distinct from the two
//	      the boundary already publishes; and the private native reader gains
//	      no export beyond the pinned generation's identifier, the page, and
//	      the row it yields. An entry for a collector, an arbitrary sort
//	      expression, a comparator, an offset or a relevance order is the
//	      milestone growing surface NIDX-01 explicitly denied it.
func TestNativeRepairTuplePageBoundarySurface(t *testing.T) {
	tester := require.New(t)

	tester.ErrorIs(ErrInvalidRepairPage, ErrInvalidRepairPage)
	tester.NotErrorIs(ErrInvalidRepairPage, ErrCorruptIndex)
	tester.NotErrorIs(ErrInvalidRepairPage, ErrNoCommittedIndex)
	tester.NotErrorIs(ErrInvalidRepairPage, ErrInvalidSelection)

	tester.Equal([]string{
		"SortFields:[4]string",
		"ProjectField:string",
		"After:[][]uint8",
		"PageSize:int",
	}, structShape(RepairPageRequest{}),
		"a repair page request is four sort fields, one projection, one cursor and one page size; anything more is a query language")
	tester.Equal([]string{"SortValues:[][]uint8", "Value:[]uint8"}, structShape(RepairRow{}),
		"a repair row is its encoded sort values and the projected stored value; anything more is a search hit")

	for _, exported := range exportedSurfaceOf(t) {
		_, permitted := nidx01ePermittedReaderSurface[exported]
		tester.True(permitted, "the native reader exports %s, which NIDX-01E does not admit", exported)
	}
}

// nidx01eOpen pins a directory's newest committed generation for the duration
// of the test.
func nidx01eOpen(t *testing.T, dir string) *ReadOnlyGeneration {
	t.Helper()
	generation, err := OpenReadOnlyGeneration(dir)
	require.NoError(t, err)
	require.NotNil(t, generation)
	t.Cleanup(func() {
		require.NoError(t, generation.Close())
	})
	return generation
}

// nidx01eRequest builds the repair page request the Property repair build
// issues, at the given page size and cursor.
func nidx01eRequest(pageSize int, after [][]byte) RepairPageRequest {
	return RepairPageRequest{
		SortFields:   nidx01eSortFields,
		ProjectField: nidx01eSHAField,
		After:        after,
		PageSize:     pageSize,
	}
}

// nidx01eDeclaredTuple renders the four ascending components issue #14012
// declares for one row: the group, name and entity identifier as the literal
// bytes their keyword doc values hold, and the revision as the literal encoded
// doc value the corpus records.
func nidx01eDeclaredTuple(row nidx01eRow) [][]byte {
	encoded, declared := nidx01eEncodedTimestamps[row.timestamp]
	if !declared {
		return [][]byte{[]byte(row.group), []byte(row.name), []byte(row.entityID), nil}
	}
	return [][]byte{[]byte(row.group), []byte(row.name), []byte(row.entityID), encoded}
}

// nidx01eDeclaredRows renders the rows a page must hold for the given declared
// rows, in order.
func nidx01eDeclaredRows(rows ...nidx01eRow) []RepairRow {
	declared := make([]RepairRow, 0, len(rows))
	for _, row := range rows {
		declared = append(declared, RepairRow{SortValues: nidx01eDeclaredTuple(row), Value: []byte(row.sha)})
	}
	return declared
}

// nidx01eWalk pages a generation end to end at the given page size and returns
// every row it yielded together with the number of calls it took.
func nidx01eWalk(t *testing.T, generation *ReadOnlyGeneration, pageSize int) ([]RepairRow, int) {
	t.Helper()
	var walked []RepairRow
	var after [][]byte
	for pages := 1; ; pages++ {
		page, err := generation.RepairTuplePage(context.Background(), nidx01eRequest(pageSize, after))
		require.NoError(t, err)
		if len(page) == 0 {
			return walked, pages
		}
		walked = append(walked, page...)
		after = page[len(page)-1].SortValues
		require.LessOrEqual(t, len(walked), len(nidx01eRows), "paging must terminate rather than repeat rows")
	}
}

// nidx01eSeed writes the given rows into a fresh shard directory through the
// compatibility writer and returns it.
func nidx01eSeed(t *testing.T, rows ...nidx01eRow) string {
	t.Helper()
	shard := t.TempDir()
	writer, err := NewStore(StoreOpts{Path: shard})
	require.NoError(t, err)
	require.NoError(t, writer.UpdateSeriesBatch(index.Batch{Documents: nidx01eDocumentsOf(rows)}))
	require.NoError(t, writer.Close())
	return shard
}

// nidx01ePublish commits one more row into an existing shard directory, which
// publishes a new generation over it.
func nidx01ePublish(t *testing.T, shard string, row nidx01eRow) {
	t.Helper()
	writer, err := NewStore(StoreOpts{Path: shard})
	require.NoError(t, err)
	require.NoError(t, writer.UpdateSeriesBatch(index.Batch{Documents: nidx01eDocumentsOf([]nidx01eRow{row})}))
	require.NoError(t, writer.Close())
}

// nidx01eDocumentsOf shapes rows the way the Property shard writes them.
func nidx01eDocumentsOf(rows []nidx01eRow) index.Documents {
	documents := make(index.Documents, 0, len(rows))
	for _, row := range rows {
		documents = append(documents, index.Document{
			EntityValues: []byte(nidx01eDocID(row)),
			Timestamp:    row.timestamp,
			Fields: []index.Field{
				nidx01eSortableField(nidx01eEntityField, []byte(row.entityID)),
				nidx01eSortableField(nidx01eGroupField, []byte(row.group)),
				nidx01eSortableField(nidx01eNameField, []byte(row.name)),
				nidx01eStoredField(nidx01eSourceField, []byte(row.source)),
				nidx01eStoredField(nidx01eSHAField, []byte(row.sha)),
			},
		})
	}
	return documents
}

// nidx01eBoundedRows builds a generation whose rows all differ in their least
// significant sort component, so every page boundary falls on a distinct
// cursor.
func nidx01eBoundedRows(count int) []nidx01eRow {
	rows := make([]nidx01eRow, 0, count)
	for ordinal := 1; ordinal <= count; ordinal++ {
		rows = append(rows, nidx01eRow{
			group:     "g-bounded",
			name:      "n-bounded",
			entityID:  "e-" + strconv.Itoa(ordinal),
			timestamp: int64(ordinal),
			sha:       "sha-" + strconv.Itoa(ordinal),
			source:    "source-" + strconv.Itoa(ordinal),
		})
	}
	return rows
}

// nidx01eRetainedBytes reports the heap a full collection leaves live, which is
// the memory a pager is still holding rather than the memory it has churned
// through.
func nidx01eRetainedBytes() uint64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

// damageDocValueLocations overwrites a segment's doc-value location table, the
// section the footer's doc-value root addresses, leaving its stored records,
// term dictionaries and postings intact. It is the smallest damage that is
// invisible to every operation NIDX-01 merged before this leaf and fatal to a
// page that orders by doc values.
func damageDocValueLocations(t *testing.T, segmentPath string) {
	t.Helper()
	payload, err := os.ReadFile(segmentPath)
	require.NoError(t, err)
	require.Greater(t, len(payload), iceFooterLength)
	footer := payload[len(payload)-iceFooterLength:]
	docValues := binary.BigEndian.Uint64(footer[iceFooterDocValueStart:iceFooterDocValueEnd])
	fieldsIndex := binary.BigEndian.Uint64(footer[iceFooterFieldsIndexStart:iceFooterFieldsIndexEnd])
	require.Less(t, docValues, fieldsIndex, "segment %s records no doc-value locations to damage", segmentPath)
	for offset := docValues; offset < fieldsIndex; offset++ {
		payload[offset] = 0xFF
	}
	require.NoError(t, os.WriteFile(segmentPath, payload, 0o600))
}

// structShape renders every field a struct declares as name:type, in
// declaration order, so a field added to carry a direction, an offset, a filter
// or a score fails the boundary test instead of quietly widening the milestone.
func structShape(value any) []string {
	valueType := reflect.TypeOf(value)
	shape := make([]string, 0, valueType.NumField())
	for fieldIndex := range valueType.NumField() {
		field := valueType.Field(fieldIndex)
		shape = append(shape, fmt.Sprintf("%s:%s", field.Name, field.Type))
	}
	return shape
}
