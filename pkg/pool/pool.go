// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pool

import (
	"fmt"
	"sync"
	"sync/atomic"
)

var poolMap = sync.Map{}

func Register[T any](name string) *Synced[T] {
	p := new(Synced[T])
	if _, ok := poolMap.LoadOrStore(name, p); ok {
		panic(fmt.Sprintf("duplicated pool: %s", name))
	}
	return p
}

func AllRefsCount() map[string]int {
	result := make(map[string]int)
	poolMap.Range(func(key, value any) bool {
		result[key.(string)] = value.(Trackable).RefsCount()
		return true
	})
	return result
}

type Trackable interface {
	RefsCount() int
}

type Synced[T any] struct {
	sync.Pool
	refs atomic.Int32
}

func (p *Synced[T]) Get() T {
	v := p.Pool.Get()
	p.refs.Add(1)
	if v == nil {
		var t T
		return t
	}
	return v.(T)
}

func (p *Synced[T]) Put(v T) {
	p.Pool.Put(v)
	p.refs.Add(-1)
}

func (p *Synced[T]) RefsCount() int {
	return int(p.refs.Load())
}
