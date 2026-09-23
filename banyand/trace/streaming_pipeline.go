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

package trace

import (
	"context"
	"fmt"

	pkgerrors "github.com/pkg/errors"
)

type traceBatch struct {
	err           error
	keys          map[string]int64
	traceIDs      map[uint64][]string
	traceIDsOrder []string // ordered list of trace IDs as they were added
	seq           int
}

type scanBatch struct {
	err       error
	cursorCh  <-chan scanCursorResult
	cursors   []*blockCursor
	snapshots []*snapshot
	traceBatch
}

type scanCursorResult struct {
	cursor *blockCursor
	err    error
}

func (t *trace) scanPartsInlineSync(ctx context.Context, parts []*part, groupedIDs [][]string, qo queryOptions) ([]*blockCursor, error) {
	if len(parts) == 0 {
		return nil, nil
	}

	recordBlock, finishSpan := startAggregatedBlockScanSpan(ctx, groupedIDs, parts)

	var (
		spanErr        error
		spanBlockBytes uint64
		cursorCount    int
	)

	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)

	tstIter := generateTstIter()
	defer releaseTstIter(tstIter)

	if finishSpan != nil {
		defer func() {
			finishSpan(cursorCount, spanBlockBytes, spanErr)
		}()
	}

	tstIter.init(bma, parts, groupedIDs)
	if initErr := tstIter.Error(); initErr != nil {
		spanErr = fmt.Errorf("cannot init tstIter: %w", initErr)
		return nil, spanErr
	}

	quota := t.pm.AvailableBytes()
	hit := 0
	cursors := make([]*blockCursor, 0)

	for tstIter.nextBlock() {
		if hit%checkDoneEvery == 0 {
			select {
			case <-ctx.Done():
				spanErr = pkgerrors.WithMessagef(ctx.Err(), "interrupt: scanned %d blocks, remained %d/%d parts to scan",
					cursorCount, len(tstIter.piPool)-tstIter.idx, len(tstIter.piPool))
				for _, cursor := range cursors {
					releaseBlockCursor(cursor)
				}
				return nil, spanErr
			default:
			}
		}
		hit++

		// Create block cursor and get size before checking quota
		bc := generateBlockCursor()
		p := tstIter.piPool[tstIter.idx]
		bc.init(p.p, p.curBlock, qo)
		blockSize := bc.bm.uncompressedSpanSizeBytes

		// Check if adding this block would exceed quota
		if quota >= 0 && spanBlockBytes+blockSize > uint64(quota) {
			releaseBlockCursor(bc)
			if cursorCount > 0 {
				// Have results, return them successfully by just closing channel
				return cursors, nil
			}
			// No results, send error
			spanErr = fmt.Errorf("block scan quota exceeded: block size %d bytes, quota is %d bytes", blockSize, quota)
			return nil, spanErr
		}

		// Quota OK, send cursor
		if recordBlock != nil {
			recordBlock(bc, blockSize)
		}
		spanBlockBytes += blockSize
		cursorCount++
		cursors = append(cursors, bc)
	}

	if iterErr := tstIter.Error(); iterErr != nil {
		spanErr = fmt.Errorf("cannot iterate tstIter: %w", iterErr)
		for _, cursor := range cursors {
			releaseBlockCursor(cursor)
		}
		return nil, spanErr
	}
	return cursors, nil
}
