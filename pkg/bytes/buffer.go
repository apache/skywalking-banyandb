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

package bytes

import (
	"fmt"
	"sync"

	"github.com/apache/skywalking-banyandb/pkg/fs"
)

var _ fs.Writer = (*Buffer)(nil)

type Buffer struct {
	Buf []byte
}

// Close implements fs.Writer.
func (*Buffer) Close() error {
	return nil
}

// Path implements fs.Writer.
func (b *Buffer) Path() string {
	return fmt.Sprintf("mem/%p", b)
}

// Write implements fs.Writer.
func (b *Buffer) Write(bb []byte) (int, error) {
	b.Buf = append(b.Buf, bb...)
	return len(bb), nil
}

func (b *Buffer) Reset() {
	b.Buf = b.Buf[:0]
}

type BufferPool struct {
	p sync.Pool
}

func (bp *BufferPool) Get() *Buffer {
	bbv := bp.p.Get()
	if bbv == nil {
		return &Buffer{}
	}
	return bbv.(*Buffer)
}

func (bp *BufferPool) Put(b *Buffer) {
	b.Reset()
	bp.p.Put(b)
}
