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

package sort_test

import (
	"encoding/binary"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
)

type Int int

func (i Int) SortedField() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

// MockIterator is a mock for the Iterator interface.
type MockIterator struct {
	items []Int
	index int
}

func NewMockIterator(items []Int) *MockIterator {
	return &MockIterator{items: items, index: -1}
}

func (mi *MockIterator) Next() bool {
	mi.index++
	return mi.index < len(mi.items)
}

func (mi *MockIterator) Val() Int {
	return mi.items[mi.index]
}

func (mi *MockIterator) Close() error {
	return nil
}

func TestItemIter_Ascending(t *testing.T) {
	iters := []sort.Iterator[Int]{
		NewMockIterator([]Int{1, 3, 5}),
		NewMockIterator([]Int{2, 4, 6}),
	}

	iter := sort.NewItemIter(iters, false) // Ascending order
	for i := Int(1); i <= 6; i++ {
		if !iter.Next() {
			t.Fatalf("expected Next() to be true, got false")
		}
		if val := iter.Val(); val != i {
			t.Errorf("expected Val() to be %d, got %d", i, val)
		}
	}
	if iter.Next() {
		t.Errorf("expected Next() to be false, got true")
	}
	if err := iter.Close(); err != nil {
		t.Errorf("expected Close() to return nil, got error: %v", err)
	}
}

func TestItemIter_Descending(t *testing.T) {
	iters := []sort.Iterator[Int]{
		NewMockIterator([]Int{5, 3, 1}),
		NewMockIterator([]Int{6, 4, 2}),
	}

	iter := sort.NewItemIter(iters, true) // Descending order
	for i := Int(6); i >= 1; i-- {
		if !iter.Next() {
			t.Fatalf("expected Next() to be true, got false")
		}
		if val := iter.Val(); val != i {
			t.Errorf("expected Val() to be %d, got %d", i, val)
		}
	}
	if iter.Next() {
		t.Errorf("expected Next() to be false, got true")
	}
	if err := iter.Close(); err != nil {
		t.Errorf("expected Close() to return nil, got error: %v", err)
	}
}
