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
)

type fieldHashID uint64

type fieldMap struct {
	repo  map[fieldHashID]*termContainer
	lst   []fieldHashID
	mutex sync.RWMutex
}

func newFieldMap(initialSize int) *fieldMap {
	return &fieldMap{
		repo: make(map[fieldHashID]*termContainer, initialSize),
		lst:  make([]fieldHashID, 0),
	}
}

func (fm *fieldMap) createKey(field index.Field) *termContainer {
	result := &termContainer{
		key:   field.Key,
		value: newPostingMap(),
	}
	k := fieldHashID(convert.Hash(field.Key.Marshal()))
	fm.repo[k] = result
	fm.lst = append(fm.lst, k)
	return result
}

func (fm *fieldMap) get(key index.FieldKey) (*termContainer, bool) {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()
	return fm.getWithoutLock(key)
}

func (fm *fieldMap) getWithoutLock(key index.FieldKey) (*termContainer, bool) {
	v, ok := fm.repo[fieldHashID(convert.Hash(key.Marshal()))]
	return v, ok
}

func (fm *fieldMap) put(fv index.Field, id common.ItemID) error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()
	pm, ok := fm.getWithoutLock(fv.Key)
	if !ok {
		pm = fm.createKey(fv)
	}
	return pm.value.put(fv.Term, id)
}

func (fm *fieldMap) Stats() (s observability.Statistics) {
	for _, pv := range fm.repo {
		// 8 is the size of key
		s.MemBytes += 8
		s.MemBytes += pv.value.Stats().MemBytes
	}
	return s
}

type termContainer struct {
	key   index.FieldKey
	value *termMap
}
