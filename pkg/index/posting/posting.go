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

// Package posting implements a posting list contains a list of document ids.
package posting

import (
	"github.com/pkg/errors"
)

// ErrListEmpty indicates the postings list is empty.
var ErrListEmpty = errors.New("postings list is empty")

// List is a collection of uint64.
type List interface {
	Contains(id uint64) bool

	IsEmpty() bool

	Max() (uint64, error)

	Len() int

	Iterator() Iterator

	Clone() List

	Equal(other List) bool

	Insert(i uint64)

	Intersect(other List) error

	Difference(other List) error

	Union(other List) error

	UnionMany(others []List) error

	AddIterator(iter Iterator) error

	AddRange(min, max uint64) error

	RemoveRange(min, max uint64) error

	Reset()

	ToSlice() []uint64

	Marshall() ([]byte, error)

	Unmarshall(data []byte) error

	SizeInBytes() int64
}

// Iterator allows iterating over a posting list.
type Iterator interface {
	Next() bool

	Current() uint64

	Close() error
}
