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
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

var ErrFieldAbsent = errors.New("field doesn't exist")

type fieldHashID uint64

type fieldMap struct {
	repo map[fieldHashID]*fieldValue
}

func newFieldMap(initialSize int) *fieldMap {
	return &fieldMap{
		repo: make(map[fieldHashID]*fieldValue, initialSize),
	}
}

func (fm *fieldMap) createKey(key []byte) {
	fm.repo[fieldHashID(convert.Hash(key))] = &fieldValue{
		key:   key,
		value: newPostingMap(),
	}
}

func (fm *fieldMap) get(key []byte) (*fieldValue, bool) {
	v, ok := fm.repo[fieldHashID(convert.Hash(key))]
	return v, ok
}

func (fm *fieldMap) put(fv *Field, id common.ChunkID) error {
	pm, ok := fm.get(fv.Name)
	if !ok {
		return errors.Wrapf(ErrFieldAbsent, "filed Name:%s", fv.Name)
	}
	return pm.value.put(fv.Value, id)
}

type fieldValue struct {
	key   []byte
	value *postingMap
}
