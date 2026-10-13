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

package vectorized

import "sync/atomic"

// pinnedOwners counts the references Owners sets hold right now, process-wide. It
// is one atomic add per pin and release, and lets tests and the soak prove every
// batch gave back what it borrowed.
var pinnedOwners atomic.Int64

// OutstandingOwnerPins returns the number of owner references that Owners sets
// hold and have not released. It is zero when no live batch borrows memory.
func OutstandingOwnerPins() int64 { return pinnedOwners.Load() }

// Owner is a reference-counted holder of memory that batch columns borrow
// without copying. A block cursor is the canonical Owner: its decoded value
// buffers back the string and bytes cells of every batch built from it, and it
// returns to its pool only after the last borrower has released its reference.
//
// Retain adds one reference. Release drops one; the owner reclaims its memory
// when the count reaches zero. Releasing more times than retained is a bug.
type Owner interface {
	Retain()
	Release()
}

// Owners is the set of owners a batch pins. Each member holds exactly one
// reference taken on behalf of the set, so the borrowed memory stays valid
// until the set is released.
//
// A missed Release only keeps an owner from being recycled; the garbage
// collector still frees it. Releasing early is the dangerous direction, so
// callers release only when they can prove no borrowed value is read again.
//
// The zero value is an empty set. Owners is not safe for concurrent use.
type Owners struct {
	list []Owner
}

// Len returns the number of distinct owners pinned.
func (s *Owners) Len() int { return len(s.list) }

// Contains reports whether o is pinned.
func (s *Owners) Contains(o Owner) bool {
	// Newest first: a batch being filled mostly re-pins the owner it pinned last.
	for i := len(s.list) - 1; i >= 0; i-- {
		if s.list[i] == o {
			return true
		}
	}
	return false
}

// Pin takes one reference on o on behalf of the set. Pinning an owner the set
// already holds is a no-op, so the set never holds two references to one owner.
func (s *Owners) Pin(o Owner) {
	if o == nil || s.Contains(o) {
		return
	}
	o.Retain()
	pinnedOwners.Add(1)
	s.list = append(s.list, o)
}

// PinAll takes a reference on every owner in from on behalf of s. from keeps its
// own references.
func (s *Owners) PinAll(from *Owners) {
	if from == nil {
		return
	}
	for _, o := range from.list {
		s.Pin(o)
	}
}

// TransferTo moves every owner to dst without touching reference counts, then
// empties s. An owner dst already holds is released once, because dst needs only
// one reference to it.
func (s *Owners) TransferTo(dst *Owners) {
	for i, o := range s.list {
		s.list[i] = nil
		if dst.Contains(o) {
			pinnedOwners.Add(-1)
			o.Release()
			continue
		}
		dst.list = append(dst.list, o)
	}
	s.list = s.list[:0]
}

// Release drops the reference held on every owner and empties the set. It is
// idempotent: a second call finds the set empty.
func (s *Owners) Release() {
	list := s.list
	// Empty the set before releasing so a re-entrant Release cannot see an owner twice.
	s.list = s.list[:0]
	pinnedOwners.Add(-int64(len(list)))
	for i, o := range list {
		list[i] = nil
		o.Release()
	}
}
