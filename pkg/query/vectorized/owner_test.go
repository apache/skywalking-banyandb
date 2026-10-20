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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// countingOwner is an Owner that records its reference count.
type countingOwner struct {
	refs     int
	released int
}

func newCountingOwner() *countingOwner { return &countingOwner{refs: 1} }

func (o *countingOwner) Retain() { o.refs++ }

func (o *countingOwner) Release() {
	o.refs--
	o.released++
	if o.refs < 0 {
		panic("countingOwner released past zero")
	}
}

func TestOwnersPinIsIdempotentPerOwner(t *testing.T) {
	o := newCountingOwner()
	var s Owners
	s.Pin(o)
	s.Pin(o)
	require.Equal(t, 1, s.Len())
	require.Equal(t, 2, o.refs, "one reference for the creator, one for the set")
	s.Release()
	require.Equal(t, 1, o.refs)
	s.Release()
	require.Equal(t, 1, o.refs, "Release is idempotent")
	require.Zero(t, s.Len())
}

func TestOwnersPinAllAndTransfer(t *testing.T) {
	a, b := newCountingOwner(), newCountingOwner()
	var src, dst, third Owners
	src.Pin(a)
	src.Pin(b)

	dst.PinAll(&src)
	require.Equal(t, 3, a.refs)
	require.Equal(t, 3, b.refs)

	// Transfer into a set that already holds a: the duplicate reference is dropped.
	third.Pin(a)
	require.Equal(t, 4, a.refs)
	src.TransferTo(&third)
	require.Zero(t, src.Len())
	require.Equal(t, 2, third.Len())
	require.Equal(t, 3, a.refs, "a's duplicate reference was released")
	require.Equal(t, 3, b.refs, "b's reference moved without a count change")

	src.Release() // empty after the transfer: must not touch counts
	dst.Release()
	third.Release()
	require.Equal(t, 1, a.refs)
	require.Equal(t, 1, b.refs)
}

func TestRecordBatchResetAndPoolPutReleaseOwners(t *testing.T) {
	schema := NewBatchSchema([]ColumnDef{{Role: RoleTag, TagFamily: "f", Name: "t", Type: ColumnTypeString}})
	pool := NewBatchPool(schema, 4)
	o := newCountingOwner()

	b := pool.Get()
	b.Owners.Pin(o)
	require.Equal(t, 2, o.refs)
	b.Reset()
	require.Equal(t, 1, o.refs, "Reset releases the owners")

	b.Owners.Pin(o)
	pool.Put(b)
	require.Equal(t, 1, o.refs, "Put releases the owners")

	foreign := NewRecordBatch(NewBatchSchema([]ColumnDef{{Role: RoleTag, TagFamily: "f", Name: "u", Type: ColumnTypeInt64}}), 1)
	foreign.Owners.Pin(o)
	pool.Put(foreign)
	require.Equal(t, 1, o.refs, "a foreign batch is not pooled but still drops its owners")
}
