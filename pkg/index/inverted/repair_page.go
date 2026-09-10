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

	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted/internal/nativeice"
)

// RepairSortFieldCount is the number of fixed Property repair sort components.
const RepairSortFieldCount = 4

const (
	// MaxRepairPageSize bounds the number of rows returned by a repair page.
	MaxRepairPageSize = 1 << 16
	// MaxRepairSortValueLength bounds each encoded repair sort component.
	MaxRepairSortValueLength = 64 << 10
)

// ErrInvalidRepairPage identifies invalid page sizes or resume cursors.
var ErrInvalidRepairPage = errors.New("inverted: invalid repair page request")

// RepairCursor is an opaque position in one pinned generation, including the
// document identity needed to resume across equal four-component tuples.
type RepairCursor struct {
	generation *ReadOnlyGeneration
	cursor     nativeice.RepairCursor
}

// RepairPageRequest selects a page in the fixed ascending Property repair order.
// Sorting and projection cannot be customized.
type RepairPageRequest struct {
	// After must be a cursor returned by this generation; nil starts the scan.
	After *RepairCursor
	// PageSize must be positive and no greater than MaxRepairPageSize.
	PageSize int
}

// RepairRow contains encoded (group, name, entity ID, timestamp) values and stored SHA.
type RepairRow struct {
	Cursor     *RepairCursor
	SortValues [][]byte
	Value      []byte
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

// RepairTuplePage returns a bounded page in ascending Property repair order.
// Equal tuples are ordered by segment and local document identity. Missing sort
// values and malformed sections return ErrCorruptIndex without a partial page.
func (g *ReadOnlyGeneration) RepairTuplePage(ctx context.Context, request RepairPageRequest) ([]RepairRow, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if g == nil || g.reader == nil {
		return nil, fmt.Errorf("page unopened generation: %w", ErrInvalidRepairPage)
	}
	var after *nativeice.RepairCursor
	if request.After != nil {
		if request.After.generation != g {
			return nil, fmt.Errorf("cursor belongs to another generation: %w", ErrInvalidRepairPage)
		}
		after = &request.After.cursor
	}
	nativeRows, pageErr := g.reader.RepairTuplePage(ctx, nativeice.RepairPageRequest{
		SortFields:   [RepairSortFieldCount]string{"_group", index.IndexModeName, "_entity_id", timestampField},
		ProjectField: "_sha_value", After: after, PageSize: request.PageSize,
	})
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
		cursor := nativeRow.Cursor
		cursor.SortValues = make([][]byte, len(nativeRow.SortValues))
		for valueIndex, value := range nativeRow.SortValues {
			cursor.SortValues[valueIndex] = append([]byte{}, value...)
		}
		rows[rowIndex] = RepairRow{SortValues: nativeRow.SortValues, Value: nativeRow.Value, Cursor: &RepairCursor{cursor: cursor, generation: g}}
	}
	return rows, nil
}

// Close releases the segment files and mapped regions the pinned generation
// holds. A view that has been closed serves no further page.
func (g *ReadOnlyGeneration) Close() error {
	if g == nil || g.reader == nil {
		return nil
	}
	if closeErr := g.reader.Close(); closeErr != nil {
		return fmt.Errorf("close pinned generation %q: %w", g.path, closeErr)
	}
	return nil
}
