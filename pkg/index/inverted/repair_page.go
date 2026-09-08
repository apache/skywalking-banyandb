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
	"errors"
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/index/inverted/internal/nativeice"
)

// RepairSortFieldCount is the number of ascending components a repair tuple
// page orders by. It is fixed at four -- group, name, entity identifier and
// timestamp -- because BDB-NIDX-SPEC-001 revision 0.2 NIDX-01 admits exactly
// the Property repair tuple order and denies a general sort surface. The count
// is part of the type of a request, so an arity other than four is a
// compilation failure rather than a runtime rejection.
const RepairSortFieldCount = 4

const (
	// MaxRepairPageSize is the largest page a single RepairTuplePage call
	// serves. A request above it is rejected before any doc value is read, so
	// the reader's resident state stays bounded by a value it chose rather than
	// by one a caller supplied.
	MaxRepairPageSize = 1 << 16

	// MaxRepairSortValueLength is the longest encoded sort value a cursor
	// component may carry. It matches the term-length bound the reader's other
	// bounded operations hold.
	MaxRepairSortValueLength = 64 << 10
)

// ErrInvalidRepairPage reports that a repair tuple page request lies outside
// the bounds the read-only reader serves: a page size that is not positive or
// exceeds MaxRepairPageSize, an empty or repeated sort field name, an empty
// projection field name, a resume cursor whose component count is neither zero
// nor RepairSortFieldCount, or a cursor component longer than
// MaxRepairSortValueLength.
//
// It is distinct from ErrCorruptIndex: nothing on disk is damaged, the request
// itself is out of bounds, so no doc value is decoded and no page is built.
// Callers classify with errors.Is.
var ErrInvalidRepairPage = errors.New("inverted: invalid repair page request")

// RepairPageRequest describes one bounded, ordered page of a pinned
// generation's live documents.
//
// It is deliberately not a query language, and NIDX-01 denies it becoming one.
// There is no filter, no descending direction, no offset, no relevance order
// and no projection beyond the single stored field a page carries: the request
// selects every live document of the pinned generation, orders them by exactly
// RepairSortFieldCount ascending components, and returns one bounded window of
// that order.
type RepairPageRequest struct {
	// SortFields names the indexed fields whose encoded doc values order the
	// page, most significant component first. The names must be distinct and
	// non-empty.
	SortFields [RepairSortFieldCount]string

	// ProjectField names the stored field whose value each row carries. It is
	// the only stored field a page decodes.
	ProjectField string

	// After is the complete tuple to resume strictly after: a page returns only
	// rows ordering after it. It holds either no component, which starts the
	// order at its first row, or exactly RepairSortFieldCount components, which
	// are the SortValues of a row a previous page returned. Any other component
	// count is an invalid cursor.
	//
	// A nil component means the row After names records no doc value for that
	// sort field, and it compares as such: a missing component orders after
	// every present one.
	After [][]byte

	// PageSize is the largest number of rows the page holds. It must be
	// positive and at most MaxRepairPageSize.
	PageSize int
}

// RepairRow is one ordered row of a repair tuple page.
type RepairRow struct {
	// SortValues holds the row's encoded doc values, one per component of the
	// request's SortFields and in the same order, so its length is always
	// RepairSortFieldCount. A component is nil when the row records no doc
	// value for that sort field.
	//
	// The values are the bytes the generation encodes, handed back unchanged:
	// the reader neither decodes nor reinterprets them, so a caller that needs
	// a typed value decodes it with the codec its own writer used.
	SortValues [][]byte

	// Value is the row's stored ProjectField value, and is nil when the row
	// stores no value for that field. A row that stores the field more than
	// once carries the first value it records.
	Value []byte
}

// ReadOnlyGeneration is a pinned, immutable read-only view of exactly one
// committed generation of an index directory.
//
// The generation is chosen when the view is opened and fixed for its lifetime,
// so a generation committed afterwards -- by a live writer, a merge or a
// restore -- stays invisible to every operation the view serves. That is what
// lets a caller page through an order across several calls and see one
// consistent set of documents, which a sequence of independent opens could not
// promise.
//
// A view takes no exclusive directory lock and writes no bytes, so a directory
// a live writer owns can be read while it is being written, and reading leaves
// file contents, modification times and directory entries unchanged.
type ReadOnlyGeneration struct {
	reader *nativeice.Reader
	path   string
}

// OpenReadOnlyGeneration opens the index directory at path, selects its newest
// structurally complete committed generation and returns a view pinned to it.
//
// A directory holding no committed generation reports an error wrapping
// ErrNoCommittedIndex, which callers that treat a cold or unflushed index as
// empty match on. A directory whose committed bytes violate the on-disk
// grammar, and which therefore offers no generation to pin, reports an error
// wrapping ErrCorruptIndex.
func OpenReadOnlyGeneration(path string) (*ReadOnlyGeneration, error) {
	reader, openErr := nativeice.Open(path)
	if openErr != nil {
		switch {
		case errors.Is(openErr, nativeice.ErrNoSnapshot):
			return nil, fmt.Errorf("open pinned generation %q: %w", path, errors.Join(ErrNoCommittedIndex, openErr))
		case errors.Is(openErr, nativeice.ErrCorrupt):
			return nil, fmt.Errorf("open pinned generation %q: %w", path, errors.Join(ErrCorruptIndex, openErr))
		default:
			return nil, fmt.Errorf("open pinned generation %q: %w", path, openErr)
		}
	}
	return &ReadOnlyGeneration{reader: reader, path: path}, nil
}

// SnapshotID returns the identifier of the committed generation the view is
// pinned to. It identifies the same generation the directory's own snapshot
// manifests are numbered by, so a caller can record which generation a read
// covered and later decide whether the directory has moved on.
func (g *ReadOnlyGeneration) SnapshotID() uint64 {
	if g == nil || g.reader == nil {
		return 0
	}
	return g.reader.SnapshotID()
}

// RepairTuplePage returns one bounded page of the pinned generation's live
// documents, ordered ascending by the request's sort components and resuming
// strictly after its cursor.
//
// Ordering compares rows component by component, most significant first. A
// present component compares against another present one as raw bytes, exactly
// as the encoded doc values are stored; a present component orders before a
// missing one, so a row lacking a sort value sorts last among the rows sharing
// the components before it; two missing components compare equal. A row that
// records several doc values for one sort field is ordered by the smallest of
// them. Rows equal on every component are ordered by the segment and local
// document number they occupy, which is stable for a pinned generation.
//
// The page holds at most PageSize rows and returns fewer only when the order is
// exhausted, so an empty page means the cursor has reached the end. Documents
// the pinned generation's deletion masks cover are never returned. Resident
// state is bounded by the page and the comparison state its ordering needs,
// never by the number of documents scanned, so paging through a generation does
// not grow with the number of pages already served.
//
// A request outside the reader's bounds reports an error wrapping
// ErrInvalidRepairPage before any doc value is read. A doc-value section whose
// chunk table, offsets, lengths, varints or term encodings violate the ICE v3
// grammar, or that would require decoding past a configured bound, reports an
// error wrapping ErrCorruptIndex. Canceling ctx stops the page and returns
// ctx.Err(). Every failure returns no rows, so a caller never observes a
// partial page.
func (g *ReadOnlyGeneration) RepairTuplePage(ctx context.Context, request RepairPageRequest) ([]RepairRow, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	nativeRows, pageErr := g.reader.RepairTuplePage(ctx, request.SortFields, request.ProjectField, request.After, request.PageSize)
	if pageErr != nil {
		switch {
		case errors.Is(pageErr, nativeice.ErrInvalidRepairPage):
			return nil, errors.Join(ErrInvalidRepairPage, pageErr)
		case errors.Is(pageErr, nativeice.ErrCorrupt):
			return nil, errors.Join(ErrCorruptIndex, pageErr)
		default:
			return nil, pageErr
		}
	}
	rows := make([]RepairRow, len(nativeRows))
	for rowIndex, nativeRow := range nativeRows {
		rows[rowIndex] = RepairRow{SortValues: nativeRow.SortValues, Value: nativeRow.Value}
	}
	return rows, nil
}

// Close releases the segment files and mapped regions the pinned generation
// holds. A view that has been closed serves no further page.
func (g *ReadOnlyGeneration) Close() error {
	if g == nil || g.reader == nil {
		return nil
	}
	return g.reader.Close()
}
