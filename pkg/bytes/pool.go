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

package bytes

import (
	"sync"
)

var defaultPool Pool

type Pool struct {
	p sync.Pool
}

func Borrow() *ByteBuf {
	return defaultPool.Borrow()
}

func (p *Pool) Borrow() *ByteBuf {
	v := p.p.Get()
	if v != nil {
		return v.(*ByteBuf)
	}
	return &ByteBuf{
		b: make([]byte, 0),
	}
}

func Return(buf *ByteBuf) {
	buf.Reset()
	defaultPool.Return(buf)
}

func (p *Pool) Return(buf *ByteBuf) {
	p.p.Put(buf)
}
