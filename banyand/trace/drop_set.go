// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package trace

import (
	"bytes"
	"unsafe"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	pkgpool "github.com/apache/skywalking-banyandb/pkg/pool"
)

const maxPooledDroppedTraceIDBytes = 4 << 20

type droppedTraceIDs struct {
	ids   []string
	slots []uint64
}

var droppedTraceIDPool = pkgpool.RegisterBounded("trace-dropped-ids", maxPooledDroppedTraceIDBytes, func() *droppedTraceIDs {
	return &droppedTraceIDs{}
}, func(dropped *droppedTraceIDs) int64 {
	return int64(cap(dropped.ids))*int64(unsafe.Sizeof("")) + int64(cap(dropped.slots))*int64(unsafe.Sizeof(uint64(0)))
})

func acquireDroppedTraceIDs() *droppedTraceIDs {
	return droppedTraceIDPool.Get()
}

func recordDroppedTraceID(droppedSet **droppedTraceIDs, traceID string) {
	if *droppedSet == nil {
		*droppedSet = acquireDroppedTraceIDs()
	}
	(*droppedSet).add(traceID)
}

func releaseDroppedTraceIDs(dropped *droppedTraceIDs) {
	if dropped == nil {
		return
	}
	clear(dropped.ids)
	clear(dropped.slots)
	dropped.ids = dropped.ids[:0]
	dropped.slots = dropped.slots[:0]
	droppedTraceIDPool.Put(dropped)
}

func (dropped *droppedTraceIDs) len() int {
	if dropped == nil {
		return 0
	}
	return len(dropped.ids)
}

func (dropped *droppedTraceIDs) add(traceID string) {
	if len(dropped.slots) > 0 {
		panic("cannot add a dropped trace ID after building the lookup index")
	}
	if len(dropped.ids) > 0 {
		lastTraceID := dropped.ids[len(dropped.ids)-1]
		if traceID == lastTraceID {
			return
		}
		if traceID < lastTraceID {
			panic("dropped trace IDs must be added in ascending order")
		}
	}
	dropped.ids = append(dropped.ids, traceID)
}

func (dropped *droppedTraceIDs) keepEncoded(data []byte) bool {
	if len(data) == 0 || idFormat(data[0]) != idFormatV1 || dropped == nil || len(dropped.ids) == 0 {
		return true
	}
	dropped.buildIndex()
	traceID := data[1:]
	mask := uint64(len(dropped.slots) - 1)
	traceHash := convert.Hash(traceID)
	slotIdx := traceHash & mask
	for {
		stored := dropped.slots[slotIdx]
		if stored == 0 {
			return true
		}
		storedIdx := uint32(stored)
		if uint32(stored>>32) == uint32(traceHash>>32) && bytes.Equal(convert.StringToBytes(dropped.ids[storedIdx-1]), traceID) {
			return false
		}
		slotIdx = (slotIdx + 1) & mask
	}
}

func (dropped *droppedTraceIDs) buildIndex() {
	if len(dropped.slots) > 0 {
		return
	}
	slotCount := indexSlotCount(len(dropped.ids))
	if cap(dropped.slots) < slotCount {
		dropped.slots = make([]uint64, slotCount)
	} else {
		dropped.slots = dropped.slots[:slotCount]
		clear(dropped.slots)
	}
	mask := uint64(slotCount - 1)
	for traceIdx, traceID := range dropped.ids {
		traceHash := convert.HashStr(traceID)
		slotIdx := traceHash & mask
		for dropped.slots[slotIdx] != 0 {
			slotIdx = (slotIdx + 1) & mask
		}
		dropped.slots[slotIdx] = uint64(uint32(traceHash>>32))<<32 | uint64(uint32(traceIdx+1))
	}
}

// indexSlotCount returns the slot count buildIndex allocates for n entries:
// the smallest power of two at or above max(2, 2n).
func indexSlotCount(n int) int {
	slotCount := 2
	for slotCount < n*2 {
		slotCount *= 2
	}
	return slotCount
}

const (
	// dropSetEntryHeaderBytes prices one entry's string header (16 bytes) plus the
	// append-growth slack the ID vector carries (25%).
	dropSetEntryHeaderBytes = 20
	// dropSetEntrySlotBytes prices one entry's worst-case index-slot share: four
	// slots of eight bytes, since indexSlotCount rounds 2N up to a power of two and
	// so can leave up to 4N slots live for N entries.
	dropSetEntrySlotBytes = 32
	// allocClassGranularity and allocClassGranularityAbove are the size-class
	// strides Go's allocator uses below and above allocClassLargeThreshold.
	allocClassGranularity      = 16
	allocClassGranularityAbove = 32
	allocClassLargeThreshold   = 256
)

// allocClassBytes returns an upper bound on the heap bytes the allocator commits
// for an n-byte allocation. Go's small size classes are multiples of 16 up to 256
// bytes and of 32 above, so rounding up to that stride is at or above the real
// class for every length a trace ID occupies. It is an upper bound rather than
// the exact class because a ceiling must never under-count residency.
func allocClassBytes(n int) int64 {
	stride := allocClassGranularity
	if n > allocClassLargeThreshold {
		stride = allocClassGranularityAbove
	}
	return int64((n + stride - 1) / stride * stride)
}

// dropSetBytesPerEntry prices one dropped trace ID of idLen bytes: its header and
// append slack, its size-classed body, and its worst-case index-slot share. A
// 42-byte service-prefixed ID — spec section 1.1's measured case — prices at
// exactly 100 bytes, so deriving this from a real length generalizes the previous
// fixed constant instead of re-tuning it.
func dropSetBytesPerEntry(idLen int) int64 {
	return dropSetEntryHeaderBytes + allocClassBytes(idLen) + dropSetEntrySlotBytes
}

// dropTracker bounds one merge's droppedTraceIDs residency. The ceiling
// applies to the sampling decision, never to the pruning predicate (spec
// section 3.1): once full, every subsequent proposed drop is retained instead
// of recorded, so the underlying exact set stays complete with respect to the
// drops it actually performs.
//
// maxIDs is derived from the first recorded ID's length rather than from a fixed
// bytes/entry constant, because trace-ID length is a deployment property: a
// service-prefixed ID can be far longer than the 42 bytes spec section 1.1
// measured, and a fixed constant would then let residency overshoot the budget
// proportionally. ID length is uniform within a shard, so one observation prices
// the whole merge; record re-derives anyway if a longer ID ever appears, so the
// uniformity is self-correcting rather than assumed.
type dropTracker struct {
	exact        *droppedTraceIDs // never released early; lives to SIDX publication as today
	budget       uint64           // resolved ceiling in bytes; 0 means unlimited
	maxIDs       int              // derived on the first record; 0 means not yet derived
	sampledIDLen int              // the ID length maxIDs was derived from
	full         bool             // one-way within a merge
}

// canAccept reports whether one more dropped trace ID can be recorded without
// exceeding the tracker's budget. Once it returns false, it never returns true
// again for the lifetime of this tracker: full is one-way within a merge.
//
// A zero budget is unlimited. A non-zero budget whose maxIDs is not yet derived
// admits the entry that will derive it, so the first proposed drop of a merge is
// always recorded.
func (dt *dropTracker) canAccept() bool {
	if dt.full {
		return false
	}
	if dt.budget == 0 {
		return true
	}
	if dt.maxIDs > 0 && dt.exact.len() >= dt.maxIDs {
		dt.full = true
		return false
	}
	return true
}

// record adds a dropped trace ID to the exact set and keeps maxIDs priced
// against the longest ID seen. Re-deriving on a longer ID can only lower maxIDs,
// so the next canAccept may report full immediately — which is the point: the
// ceiling tracks real residency instead of the length it first happened to see.
func (dt *dropTracker) record(traceID string) {
	recordDroppedTraceID(&dt.exact, traceID)
	if dt.budget == 0 || len(traceID) <= dt.sampledIDLen {
		return
	}
	dt.sampledIDLen = len(traceID)
	dt.maxIDs = maxIDsForBudget(dt.budget, len(traceID))
}

// liveBytes returns the drop set's accumulation residency: the ID-vector
// headers plus the ID bodies. The lookup index is deliberately excluded so
// that liveBytes()+projectedIndexBytes() is the accumulation-phase ceiling
// predicate without double-counting slots. For residency after buildIndex has
// run, or for a tracker reusing a pooled set that still carries slot capacity,
// use residentBytes.
func (dt *dropTracker) liveBytes() int64 {
	if dt.exact == nil {
		return 0
	}
	live := int64(cap(dt.exact.ids)) * int64(unsafe.Sizeof(""))
	for _, id := range dt.exact.ids {
		live += allocClassBytes(len(id))
	}
	return live
}

// residentBytes returns every byte the drop set currently holds: liveBytes
// plus the slot capacity, which is non-zero once buildIndex has run and can
// also be non-zero on a freshly acquired set that carries capacity over from
// the reuse pool. It mirrors droppedTraceIDPool's sizing function plus the ID
// bodies that accounting omits.
func (dt *dropTracker) residentBytes() int64 {
	if dt.exact == nil {
		return 0
	}
	return dt.liveBytes() + int64(cap(dt.exact.slots))*int64(unsafe.Sizeof(uint64(0)))
}

// projectedIndexBytes returns the slot bytes an index over n entries requires:
// the next power of two at or above max(2, 2n) slots, eight bytes each. It
// mirrors buildIndex's own sizing (via indexSlotCount), so it describes the
// index's residency whether buildIndex allocates it fresh or satisfies it from
// capacity a pooled set carried in.
func projectedIndexBytes(n int) int64 {
	return int64(indexSlotCount(n)) * 8
}

// maxIDsForBudget converts a byte budget and an observed trace-ID length into the
// maximum number of dropped trace IDs a dropTracker may record before it starts
// retaining instead. A budget of zero means unlimited: production merges resolve a
// non-zero ceiling, but a filter constructed without one (tests, and any future
// caller that opts out) must not be bounded, so maxIDsForBudget(0, n) is zero.
//
// The conversion runs once per merge (or again only if a longer ID appears), so
// the enforcement hot path (dropTracker.canAccept) stays a single integer
// comparison instead of a byte computation on every proposed drop. Because
// dropSetBytesPerEntry prices an entry at or above its real residency,
// liveBytes()+projectedIndexBytes() at the point canAccept first returns false is
// at or below the budget it was derived from, for any ID length.
//
// A non-zero budget always yields a ceiling of at least one: without the clamp, a
// budget below one entry's price would divide to zero and be read back as the
// unlimited sentinel, so the tightest possible budget would disable the ceiling
// instead of enforcing it.
func maxIDsForBudget(budget uint64, idLen int) int {
	if budget == 0 {
		return 0
	}
	return max(1, int(budget/uint64(dropSetBytesPerEntry(idLen))))
}
