// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package native

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

const (
	// RepairSortFieldCount is the fixed number of Property repair tuple fields.
	RepairSortFieldCount = 4
	// MaxRepairPageSize bounds one repair page.
	MaxRepairPageSize = 1 << 16
	// MaxRepairSortValueLength bounds one encoded repair tuple component.
	MaxRepairSortValueLength = 64 << 10
)

// ErrInvalidRepairPage identifies an invalid page size or continuation cursor.
var ErrInvalidRepairPage = errors.New("native: invalid repair page")

// RepairCursor is an opaque continuation position belonging to one generation.
type RepairCursor struct {
	generation *ReadOnlyGeneration
	cursor     nativeice.RepairCursor
}

// RepairPageRequest selects one bounded ascending repair page.
type RepairPageRequest struct {
	// After must be a cursor returned by this generation; nil starts the scan.
	After *RepairCursor
	// PageSize must be positive and no greater than MaxRepairPageSize.
	PageSize int
}

// RepairRow contains the fixed tuple values and projected stored value.
type RepairRow struct {
	Cursor     *RepairCursor
	SortValues [][]byte
	Value      []byte
}

// RepairTuplePage returns one bounded page ordered by Property's fixed repair
// tuple: group, name, entity ID, and timestamp. The generation remains pinned
// across pages, so a publication after the first page is invisible.
func (g *ReadOnlyGeneration) RepairTuplePage(ctx context.Context, request RepairPageRequest) ([]RepairRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
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
	nativeRows, err := g.reader.RepairTuplePage(ctx, nativeice.RepairPageRequest{
		SortFields:   [RepairSortFieldCount]string{"_group", "_im_name", "_entity_id", timestampField},
		ProjectField: "_sha_value",
		After:        after,
		PageSize:     request.PageSize,
	})
	if err != nil {
		switch {
		case errors.Is(err, nativeice.ErrInvalidRepairPage):
			return nil, errors.Join(ErrInvalidRepairPage, err)
		case errors.Is(err, nativeice.ErrCorrupt):
			return nil, errors.Join(ErrCorrupt, err)
		default:
			return nil, err
		}
	}
	rows := make([]RepairRow, len(nativeRows))
	for rowIndex, nativeRow := range nativeRows {
		sortValues := make([][]byte, len(nativeRow.SortValues))
		cursorValues := make([][]byte, len(nativeRow.SortValues))
		for valueIndex, value := range nativeRow.SortValues {
			sortValues[valueIndex] = append([]byte(nil), value...)
			cursorValues[valueIndex] = append([]byte(nil), value...)
		}
		cursor := nativeRow.Cursor
		cursor.SortValues = cursorValues
		rows[rowIndex] = RepairRow{
			SortValues: sortValues,
			Value:      append([]byte(nil), nativeRow.Value...),
			Cursor:     &RepairCursor{generation: g, cursor: cursor},
		}
	}
	return rows, nil
}
