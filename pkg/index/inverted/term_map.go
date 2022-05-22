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

package inverted

import (
	"sync"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
)

type termHashID uint64

type termMap struct {
	repo  map[termHashID]*index.PostingValue
	lst   []termHashID
	mutex sync.RWMutex
}

func newPostingMap() *termMap {
	return &termMap{
		repo: make(map[termHashID]*index.PostingValue),
	}
}

func (p *termMap) put(key []byte, id common.ItemID) error {
	list := p.getOrCreate(key)
	list.Insert(id)
	return nil
}

func (p *termMap) getOrCreate(key []byte) posting.List {
	list := p.get(key)
	if list != nil {
		return list
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	hashedKey := termHashID(convert.Hash(key))
	v := &index.PostingValue{
		Term:  key,
		Value: roaring.NewPostingList(),
	}
	p.repo[hashedKey] = v
	p.lst = append(p.lst, hashedKey)
	return v.Value
}

func (p *termMap) get(key []byte) posting.List {
	e := p.getEntry(key)
	if e == nil {
		return nil
	}
	return e.Value
}

func (p *termMap) getEntry(key []byte) *index.PostingValue {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	hashedKey := termHashID(convert.Hash(key))
	v, ok := p.repo[hashedKey]
	if !ok {
		return nil
	}
	return v
}

func (p *termMap) Stats() (s observability.Statistics) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	for _, pv := range p.repo {
		// 8 is the size of key
		s.MemBytes += 8
		s.MemBytes += pv.Value.SizeInBytes()
	}
	return s
}
