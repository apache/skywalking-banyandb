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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package measure

import (
	"sync"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// scanDecodeReuse owns decoded storage for one series-ordered scan. A slot's
// reservation remains live while its storage is idle, so reusing it never
// refunds the query's monotonic admission lease. Checked-out cursors stay in
// the existing tracked pool until their storage is transferred or closed.
type scanDecodeReuse struct {
	tracker *vectorized.MemoryTracker
	first   *scanDecodeSlot
	mu      sync.Mutex
	closed  bool
}

type scanDecodeSlot struct {
	owner   *scanDecodeReuse
	next    *scanDecodeSlot
	idle    *blockCursor
	arena   []byte
	bytes   int64
	rows    uint64
	encoded uint64
}

func (r *scanDecodeReuse) acquire(cursor *blockCursor, bytes int64) (*scanDecodeSlot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errScanClosed
	}
	rows, encoded := max(cursor.bm.count, 16), max(cursor.bm.uncompressedSizeBytes, 1024)
	for slot := r.first; slot != nil; slot = slot.next {
		if slot.idle == nil || slot.bytes < bytes || slot.rows < rows || slot.encoded < encoded {
			continue
		}
		old := slot.idle
		slot.idle = nil
		cursor.timestamps, old.timestamps = old.timestamps, cursor.timestamps
		cursor.versions, old.versions = old.versions, cursor.versions
		cursor.fields, old.fields = old.fields, cursor.fields
		cursor.tagFamilies, old.tagFamilies = old.tagFamilies, cursor.tagFamilies
		cursor.columnValuesDecoder, old.columnValuesDecoder = old.columnValuesDecoder, cursor.columnValuesDecoder
		blockCursorPool.Put(old)
		return slot, nil
	}
	if err := r.tracker.Reserve(bytes); err != nil {
		return nil, err
	}
	slot := &scanDecodeSlot{owner: r, next: r.first, bytes: bytes, rows: rows, encoded: encoded}
	r.first = slot
	return slot, nil
}

// put is called only at the cursor's last reference, including borrowed batches
// released after the source closes. No decoder can reuse a pinned cursor.
func (s *scanDecodeSlot) put(cursor *blockCursor) {
	r := s.owner
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		blockCursorPool.Put(cursor)
		s.arena = nil
		r.tracker.Release(s.bytes)
		return
	}
	s.idle = cursor
}

func (r *scanDecodeReuse) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	for slot := r.first; slot != nil; slot = slot.next {
		if slot.idle != nil {
			blockCursorPool.Put(slot.idle)
			slot.idle = nil
			slot.arena = nil
			r.tracker.Release(slot.bytes)
		}
	}
	r.first = nil
}

func boundCursorFamilies(cursor *blockCursor, rows int) {
	if cap(cursor.tagFamilies) > 2*max(len(cursor.tagProjection), 1) {
		cursor.tagFamilies = nil
		return
	}
	families := cursor.tagFamilies[:cap(cursor.tagFamilies)]
	for i := range families {
		if i >= len(cursor.tagProjection) {
			families[i] = columnFamily{}
			continue
		}
		boundFamilyBacking(&families[i], len(cursor.tagProjection[i].Names), rows)
	}
}

// copyDecodedRows refills slot-owned slice backing and the byte arena. Dictionary
// decoders allocate temporary payloads; copying them into the arena ensures the
// retained values use the same prepaid storage across blocks.
func (bc *blockCursor) copyDecodedRows(source *block, start, end int) {
	slot := bc.res.reuse
	slot.arena = slot.arena[:0]
	count := len(source.tagFamilies)
	if count > cap(bc.tagFamilies) {
		bc.tagFamilies = append(bc.tagFamilies[:cap(bc.tagFamilies)], make([]columnFamily, count-cap(bc.tagFamilies))...)
	}
	bc.tagFamilies = bc.tagFamilies[:count]
	for i := range source.tagFamilies {
		copyDecodedFamily(&bc.tagFamilies[i], &source.tagFamilies[i], start, end, slot)
	}
	copyDecodedFamily(&bc.fields, &source.field, start, end, slot)
}

// A block's uncompressed size counts each stored column once. Duplicate
// projections retain the original per-cursor accounting instead of borrowing
// the unique-column payload bound for repeated copies.
func uniqueDecodeProjection(cursor *blockCursor) bool {
	for i, field := range cursor.fieldProjection {
		for _, previous := range cursor.fieldProjection[:i] {
			if previous == field {
				return false
			}
		}
	}
	for i, family := range cursor.tagProjection {
		for j, tag := range family.Names {
			for _, previous := range family.Names[:j] {
				if previous == tag {
					return false
				}
			}
			for _, previousFamily := range cursor.tagProjection[:i] {
				if previousFamily.Family != family.Family {
					continue
				}
				for _, previous := range previousFamily.Names {
					if previous == tag {
						return false
					}
				}
			}
		}
	}
	return true
}

func copyDecodedFamily(target, source *columnFamily, start, end int, slot *scanDecodeSlot) {
	target.name = source.name
	columns := target.resizeColumns(len(source.columns))
	kept := 0
	for i := range source.columns {
		current := &source.columns[i]
		if current.rowCount() == 0 {
			continue
		}
		copied := &columns[kept]
		copied.reset()
		copied.name, copied.valueType = current.name, current.valueType
		copied.copyRowsFrom(current, start, end)
		if copied.form == numericFormBytes {
			for row, value := range copied.values {
				if len(value) == 0 {
					continue
				}
				if uint64(len(value)) > slot.encoded-uint64(len(slot.arena)) {
					logger.Panicf("decoded column payload exceeds its reserved block size")
				}
				if slot.arena == nil {
					slot.arena = make([]byte, 0, int(slot.encoded))
				}
				offset := len(slot.arena)
				slot.arena = append(slot.arena, value...)
				copied.values[row] = slot.arena[offset:len(slot.arena)]
			}
		}
		kept++
	}
	target.columns = columns[:kept]
}
