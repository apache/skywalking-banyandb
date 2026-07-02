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

package lifecycle

// bufPoolMinClassExp floors the size class so tiny bodies share one ~256B class
// instead of fragmenting the pool across many sub-256B classes.
const bufPoolMinClassExp = 8

// bufPoolReclaimAfter drops a size class whose free list went untouched for this
// many consecutive flushes, so a transient burst of large bodies (e.g. a cluster
// of 0.5MB rows) does not hoard memory after the burst passes.
const bufPoolReclaimAfter = 4

// classExp returns the power-of-two size class for n: the smallest k such that
// 1<<k >= n, floored at bufPoolMinClassExp. Borrow uses the requested size;
// return uses the buffer's capacity. A buffer of capacity 1<<k maps to k by both,
// so power-of-two buffers stay in a stable class across borrow/return cycles.
func classExp(n int) uint {
	k := uint(bufPoolMinClassExp)
	for (1 << k) < n {
		k++
	}
	return k
}

// bufferClass is one size class's free list plus an idle counter used for lazy
// reclamation.
type bufferClass struct {
	free [][]byte
	idle int
}

// marshalBufferPool lends reusable marshal buffers grouped by power-of-two size
// class. Callers borrow one buffer per row (sized by proto.Size), and the whole
// batch's buffers are returned together after the batch is published. Grouping by
// size class keeps each class's buffers uniformly sized so a small body never
// borrows (and pins) a large buffer; lazy reclamation drops classes idle for
// several flushes so a burst of large bodies is released rather than hoarded. The
// pool is driven by a single goroutine (the part's replay loop); it needs no
// locking.
//
// Memory is bounded two ways. Outstanding (borrowed-but-not-returned) buffers are
// capped by the sender's byte budget (it flushes at maxBytes). Each class's free
// list is capped by freeLimit so returning a batch cannot hoard more than one
// budget's worth of buffers per class; excess returns are dropped to GC. Together
// the resident pool stays within ~2x the byte budget (one budget outstanding plus
// one budget across the active classes' free lists) regardless of body sizes.
type marshalBufferPool struct {
	classes  map[uint]*bufferClass
	maxBytes int
}

func newMarshalBufferPool(maxBytes int) *marshalBufferPool {
	return &marshalBufferPool{classes: make(map[uint]*bufferClass), maxBytes: maxBytes}
}

// freeLimit is the maximum number of free buffers retained in size class k: one
// byte budget's worth (maxBytes >> k == maxBytes / 2^k), so a class's retained
// free memory never exceeds ~maxBytes. At least one buffer is kept so every class
// can still satisfy a repeat borrow.
func (p *marshalBufferPool) freeLimit(k uint) int {
	if limit := p.maxBytes >> k; limit > 0 {
		return limit
	}
	return 1
}

// borrow returns an empty (len 0) buffer whose capacity is at least size, reusing
// a free buffer from the matching size class or allocating a fresh power-of-two
// one. The returned buffer is owned by the caller until returnAll/recycle.
func (p *marshalBufferPool) borrow(size int) []byte {
	k := classExp(size)
	if c := p.classes[k]; c != nil && len(c.free) > 0 {
		c.idle = 0
		last := len(c.free) - 1
		buf := c.free[last]
		c.free[last] = nil
		c.free = c.free[:last]
		return buf[:0]
	}
	return make([]byte, 0, 1<<k)
}

// put returns one buffer to its size class (creating the class if new), resets
// that class's idle counter, and retains the buffer unless the class is already
// at its free limit (then it is dropped to GC). Returns the class so callers can
// track which classes a batch touched.
func (p *marshalBufferPool) put(buf []byte) uint {
	k := classExp(cap(buf))
	c := p.classes[k]
	if c == nil {
		c = &bufferClass{}
		p.classes[k] = c
	}
	c.idle = 0
	if len(c.free) < p.freeLimit(k) {
		c.free = append(c.free, buf)
	}
	return k
}

// returnAll returns a published batch's buffers to their size classes and runs
// one round of lazy reclamation: classes that received a buffer this batch reset
// their idle counter, the rest age and are dropped once stale. A buffer is dropped
// (left to GC) rather than retained once its class's free list reaches freeLimit,
// bounding the pool's resident memory. It must be called only after the batch's
// Publish has completed (the bodies are on the wire), so a returned buffer can be
// safely reused by the next batch.
func (p *marshalBufferPool) returnAll(lent [][]byte) {
	touched := make(map[uint]struct{}, len(lent))
	for _, buf := range lent {
		// The body was already published (returnAll runs only after the synchronous
		// Publish), so put dropping a buffer past the class limit loses no data —
		// only its backing array is left to GC.
		touched[p.put(buf)] = struct{}{}
	}
	for k, c := range p.classes {
		if _, ok := touched[k]; ok {
			c.idle = 0
			continue
		}
		c.idle++
		if c.idle >= bufPoolReclaimAfter {
			delete(p.classes, k)
		}
	}
}
