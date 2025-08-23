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

package trace

import (
	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
)

var (
	// ENode is an empty node.
	ENode = new(emptyNode)
	bList = new(bypassList)
)

type emptyNode struct{}

func (an emptyNode) Execute(_ index.GetSearcher, _ common.SeriesID, _ *index.RangeOpts) (posting.List, posting.List, error) {
	return bList, bList, nil
}

func (an emptyNode) String() string {
	return "empty"
}

func (an emptyNode) ShouldSkip(_ index.FilterOp) (bool, error) {
	return false, nil
}

type bypassList struct{}

func (bl bypassList) Contains(_ uint64) bool {
	// all items should be fetched
	return true
}

func (bl bypassList) IsEmpty() bool {
	return false
}

func (bl bypassList) Min() (uint64, error) {
	panic("not invoked")
}

func (bl bypassList) Max() (uint64, error) {
	panic("not invoked")
}

func (bl bypassList) Len() int {
	return 0
}

func (bl bypassList) Iterator() posting.Iterator {
	panic("not invoked")
}

func (bl bypassList) Clone() posting.List {
	panic("not invoked")
}

func (bl bypassList) Equal(_ posting.List) bool {
	panic("not invoked")
}

func (bl bypassList) Insert(_ uint64) {
	panic("not invoked")
}

func (bl bypassList) Intersect(_ posting.List) error {
	panic("not invoked")
}

func (bl bypassList) Difference(_ posting.List) error {
	panic("not invoked")
}

func (bl bypassList) Union(_ posting.List) error {
	panic("not invoked")
}

func (bl bypassList) UnionMany(_ []posting.List) error {
	panic("not invoked")
}

func (bl bypassList) AddIterator(_ posting.Iterator) error {
	panic("not invoked")
}

func (bl bypassList) AddRange(_ uint64, _ uint64) error {
	panic("not invoked")
}

func (bl bypassList) RemoveRange(_ uint64, _ uint64) error {
	panic("not invoked")
}

func (bl bypassList) Reset() {
	panic("not invoked")
}

func (bl bypassList) ToSlice() []uint64 {
	panic("not invoked")
}

func (bl bypassList) Marshall() ([]byte, error) {
	panic("not invoked")
}

func (bl bypassList) Unmarshall(_ []byte) error {
	panic("not invoked")
}

func (bl bypassList) SizeInBytes() int64 {
	panic("not invoked")
}
