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

package tsdb

import (
	"bytes"
	"sort"
	"sync"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/posting"
	"github.com/apache/skywalking-banyandb/pkg/posting/roaring"
)

type termHashID uint64

type postingMap struct {
	repo  map[termHashID]*postingValue
	mutex sync.RWMutex
}

func newPostingMap() *postingMap {
	return &postingMap{
		repo: make(map[termHashID]*postingValue),
	}
}

func (p *postingMap) put(key []byte, id common.ChunkID) error {
	list := p.getOrCreate(key)
	list.Insert(id)
	return nil
}

func (p *postingMap) getOrCreate(key []byte) posting.List {
	list := p.get(key)
	if list != roaring.EmptyPostingList {
		return list
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	hashedKey := termHashID(convert.Hash(key))
	v := &postingValue{
		key:   key,
		value: roaring.NewPostingList(),
	}
	p.repo[hashedKey] = v
	return v.value
}

func (p *postingMap) get(key []byte) posting.List {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	hashedKey := termHashID(convert.Hash(key))
	v, ok := p.repo[hashedKey]
	if !ok {
		return roaring.EmptyPostingList
	}
	return v.value
}

func (p *postingMap) allValues() posting.List {
	result := roaring.NewPostingList()
	for _, value := range p.repo {
		_ = result.Union(value.value)
	}
	return result
}

func (p *postingMap) getRange(opts *RangeOpts) posting.List {
	switch bytes.Compare(opts.Upper, opts.Lower) {
	case -1:
		return roaring.EmptyPostingList
	case 0:
		if opts.IncludesUpper && opts.IncludesLower {
			return p.get(opts.Upper)
		}
		return roaring.EmptyPostingList
	}
	keys := make(Asc, 0, len(p.repo))
	for _, v := range p.repo {
		keys = append(keys, v.key)
	}
	sort.Sort(keys)
	index := sort.Search(len(keys), func(i int) bool {
		return bytes.Compare(keys[i], opts.Lower) >= 0
	})
	result := roaring.NewPostingList()
	for i := index; i < len(keys); i++ {
		k := keys[i]
		switch {
		case bytes.Equal(k, opts.Lower):
			if opts.IncludesLower {
				_ = result.Union(p.repo[termHashID(convert.Hash(k))].value)
			}
		case bytes.Compare(k, opts.Upper) > 0:
			break
		case bytes.Equal(k, opts.Upper):
			if opts.IncludesUpper {
				_ = result.Union(p.repo[termHashID(convert.Hash(k))].value)
			}
		default:
			_ = result.Union(p.repo[termHashID(convert.Hash(k))].value)
		}
	}
	return result
}

type Asc [][]byte

func (a Asc) Len() int {
	return len(a)
}

func (a Asc) Less(i, j int) bool {
	return bytes.Compare(a[i], a[j]) < 0
}

func (a Asc) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

type postingValue struct {
	key   []byte
	value posting.List
}
