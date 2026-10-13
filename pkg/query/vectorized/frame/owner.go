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

package frame

import (
	"os"
	"sync/atomic"
	"unsafe"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// poisonReleasedMessages makes a decoded columnar frame decode from a private copy of
// the message and overwrite that copy with poisonByte when its last reference is
// released, so a cell read after release returns visibly wrong data. It is a test aid,
// enabled by BYDB_POISON_RELEASED_CURSOR=1 (the switch the storage cursors use) or
// SetPoisonReleasedMessages; it costs one copy of the message per decode.
var poisonReleasedMessages atomic.Bool

// poisonByte is the sentinel written over released message memory. It is not valid
// UTF-8 on its own and is unlikely to appear in test data.
const poisonByte = 0xDD

func init() {
	poisonReleasedMessages.Store(os.Getenv("BYDB_POISON_RELEASED_CURSOR") == "1")
}

// SetPoisonReleasedMessages switches message poisoning and returns the previous setting.
func SetPoisonReleasedMessages(on bool) bool { return poisonReleasedMessages.Swap(on) }

// MessageOwner pins the memory of one received columnar frame. Columns decoded from the
// frame borrow it (numeric data and string or bytes cells are views of the message), so
// a batch pins the owner through vectorized.Owners and an operator that keeps a cell
// beyond the batch's life copies it first. The Go collector keeps the bytes valid for
// as long as anything points into them; the reference count exists for the copy-on-
// retain contract and, under poisoning, to make a violation of it visible.
type MessageOwner struct {
	buf  []byte
	refs atomic.Int64
	// private is set when buf is a copy made by the decoder rather than the caller's
	// slice, so poisoning it cannot corrupt a buffer the caller still owns.
	private bool
}

// newMessageOwner adopts msg for decoding and returns the owner with one reference,
// held by the decoder. The decoder reads through the owner's buffer: msg itself when it
// is 8-byte aligned, otherwise (and always under poisoning) an aligned private copy,
// because the frame's numeric buffers are read in place as typed slices.
func newMessageOwner(msg []byte) *MessageOwner {
	o := &MessageOwner{buf: msg}
	if poisonReleasedMessages.Load() || !aligned8(msg) {
		o.buf = alignedCopy(msg)
		o.private = true
	}
	o.refs.Store(1)
	return o
}

// Bytes returns the message the decoded columns borrow.
func (o *MessageOwner) Bytes() []byte { return o.buf }

// Retain adds a reference.
func (o *MessageOwner) Retain() { o.refs.Add(1) }

// Release drops a reference. When the last one goes the message is poisoned if
// poisoning is on and the buffer is private; a release beyond the last reference panics
// under poisoning and is otherwise ignored.
func (o *MessageOwner) Release() {
	n := o.refs.Add(-1)
	if n > 0 {
		return
	}
	if n < 0 {
		o.refs.Add(1)
		if poisonReleasedMessages.Load() {
			logger.Panicf("frame message released more times than it was retained")
		}
		return
	}
	if o.private && poisonReleasedMessages.Load() {
		for i := range o.buf {
			o.buf[i] = poisonByte
		}
	}
}

func aligned8(b []byte) bool {
	return len(b) == 0 || uintptr(unsafe.Pointer(unsafe.SliceData(b)))&7 == 0
}

// alignedCopy copies msg into a fresh buffer whose first byte is 8-byte aligned.
func alignedCopy(msg []byte) []byte {
	if len(msg) == 0 {
		return nil
	}
	words := make([]uint64, (len(msg)+7)/8)
	out := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(words))), len(msg))
	copy(out, msg)
	return out
}
