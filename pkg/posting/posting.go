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

package posting

import (
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
)

var ErrListEmpty = errors.New("postings list is empty")

// List is a collection of common.ChunkID.
type List interface {
	Contains(id common.ChunkID) bool

	IsEmpty() bool

	Max() (common.ChunkID, error)

	Len() int

	Iterator() Iterator

	Clone() List

	Equal(other List) bool

	Insert(i common.ChunkID)

	Intersect(other List) error

	Difference(other List) error

	Union(other List) error

	UnionMany(others []List) error

	AddIterator(iter Iterator) error

	AddRange(min, max common.ChunkID) error

	RemoveRange(min, max common.ChunkID) error

	Reset()

	ToSlice() []common.ChunkID
}

type Iterator interface {
	Next() bool

	Current() common.ChunkID

	Close() error
}
