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

// Package roaring implements the posting list by a roaring bitmap.
package roaring

import (
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
)

var (
	// DummyPostingList is an empty list.
	DummyPostingList = NewPostingList()

	errIntersectRoaringOnly  = errors.New("Intersect only supported between roaringDocId sets")
	errUnionRoaringOnly      = errors.New("Union only supported between roaringDocId sets")
	errDifferenceRoaringOnly = errors.New("Difference only supported between roaringDocId sets")
)

var _ posting.List = (*postingsList)(nil)

// postingsList abstracts a Roaring Bitmap.
type postingsList struct {
	bitmap *roaring64.Bitmap
}

func (p *postingsList) Marshall() ([]byte, error) {
	return p.bitmap.MarshalBinary()
}

func (p *postingsList) Unmarshall(data []byte) error {
	return p.bitmap.UnmarshalBinary(data)
}

// NewPostingList returns a empty posting list.
func NewPostingList() posting.List {
	return &postingsList{
		bitmap: roaring64.New(),
	}
}

// NewPostingListWithInitialData return a posting list with initialized data.
func NewPostingListWithInitialData(data ...uint64) posting.List {
	list := NewPostingList()
	for _, d := range data {
		list.Insert(common.ItemID(d))
	}
	return list
}

// NewRange return a posting list added the integers in [start, end).
func NewRange(start, end uint64) posting.List {
	list := &postingsList{
		bitmap: roaring64.New(),
	}
	list.bitmap.AddRange(start, end)
	return list
}

func (p *postingsList) Contains(id common.ItemID) bool {
	return p.bitmap.Contains(uint64(id))
}

func (p *postingsList) IsEmpty() bool {
	return p.bitmap.IsEmpty()
}

func (p *postingsList) Max() (common.ItemID, error) {
	if p.IsEmpty() {
		return 0, posting.ErrListEmpty
	}
	return common.ItemID(p.bitmap.Maximum()), nil
}

func (p *postingsList) Len() int {
	return int(p.bitmap.GetCardinality())
}

func (p *postingsList) Iterator() posting.Iterator {
	return &roaringIterator{
		iter: p.bitmap.Iterator(),
	}
}

func (p *postingsList) Clone() posting.List {
	return &postingsList{
		bitmap: p.bitmap.Clone(),
	}
}

func (p *postingsList) Equal(other posting.List) bool {
	if p.Len() != other.Len() {
		return false
	}

	iter := p.Iterator()
	otherIter := other.Iterator()

	for iter.Next() {
		if !otherIter.Next() {
			return false
		}
		if iter.Current() != otherIter.Current() {
			return false
		}
	}

	return true
}

func (p *postingsList) Insert(id common.ItemID) {
	p.bitmap.Add(uint64(id))
}

func (p *postingsList) Intersect(other posting.List) error {
	o, ok := other.(*postingsList)
	if !ok {
		return errIntersectRoaringOnly
	}
	p.bitmap.And(o.bitmap)
	return nil
}

func (p *postingsList) Difference(other posting.List) error {
	o, ok := other.(*postingsList)
	if !ok {
		return errDifferenceRoaringOnly
	}
	p.bitmap.AndNot(o.bitmap)
	return nil
}

func (p *postingsList) Union(other posting.List) error {
	o, ok := other.(*postingsList)
	if !ok {
		return errUnionRoaringOnly
	}
	p.bitmap.Or(o.bitmap)
	return nil
}

func (p *postingsList) UnionMany(others []posting.List) error {
	for _, other := range others {
		err := p.Union(other)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *postingsList) AddIterator(iter posting.Iterator) error {
	for iter.Next() {
		p.Insert(iter.Current())
	}
	return nil
}

func (p *postingsList) AddRange(min, max common.ItemID) error {
	for i := min; i < max; i++ {
		p.bitmap.Add(uint64(i))
	}
	return nil
}

func (p *postingsList) RemoveRange(min, max common.ItemID) error {
	for i := min; i < max; i++ {
		p.bitmap.Remove(uint64(i))
	}
	return nil
}

func (p *postingsList) Reset() {
	p.bitmap.Clear()
}

func (p *postingsList) SizeInBytes() int64 {
	return int64(p.bitmap.GetSizeInBytes())
}

type roaringIterator struct {
	iter    roaring64.IntIterable64
	current common.ItemID
	closed  bool
}

func (it *roaringIterator) Current() common.ItemID {
	return it.current
}

func (it *roaringIterator) Next() bool {
	if it.closed || !it.iter.HasNext() {
		return false
	}
	v := it.iter.Next()
	it.current = common.ItemID(v)
	return true
}

func (it *roaringIterator) Close() error {
	it.closed = true
	return nil
}

func (p *postingsList) ToSlice() []common.ItemID {
	iter := p.Iterator()
	defer iter.Close()
	s := make([]common.ItemID, 0, p.Len())
	for iter.Next() {
		s = append(s, iter.Current())
	}
	return s
}
