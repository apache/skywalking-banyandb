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

package stream

import (
	"container/heap"
	"errors"
	"fmt"
	"io"

	"github.com/apache/skywalking-banyandb/pkg/fs"
)

type reader struct {
	r         fs.Reader
	bytesRead uint64
}

func newReader(r fs.Reader) *reader {
	return &reader{r: r}
}

func (r *reader) Path() string {
	return r.r.Path()
}

func (r *reader) Read(p []byte) (int, error) {
	n, err := r.r.Read(0, p)
	r.bytesRead += uint64(n)
	return n, err
}

type blockReader struct {
	err           error
	block         *blockPointer
	pih           partMergeIterHeap
	nextBlockNoop bool
}

func (br *blockReader) reset() {
	br.block = nil
	for i := range br.pih {
		br.pih[i] = nil
	}
	br.pih = br.pih[:0]
	br.nextBlockNoop = false
	br.err = nil
}

func (br *blockReader) init(pii []*partMergeIter) {
	br.reset()
	for _, pi := range pii {
		if pi.nextBlock() {
			br.pih = append(br.pih, pi)
			continue
		}
		if err := pi.error(); err != nil {
			br.err = fmt.Errorf("can't get the block to merge: %w", err)
			return
		}
	}
	if len(br.pih) == 0 {
		br.err = io.EOF
		return
	}
	heap.Init(&br.pih)
	br.block = &br.pih[0].block
	br.nextBlockNoop = true
}

func (br *blockReader) nextBlock() bool {
	if br.err != nil {
		return false
	}
	if br.nextBlockNoop {
		br.nextBlockNoop = false
		return true
	}

	br.err = br.next()
	if br.err != nil {
		if errors.Is(br.err, io.EOF) {
			return false
		}
		br.err = fmt.Errorf("can't get the block to merge: %w", br.err)
		return false
	}
	return true
}

func (br *blockReader) next() error {
	head := br.pih[0]
	if head.nextBlock() {
		heap.Fix(&br.pih, 0)
		br.block = &br.pih[0].block
		return nil
	}

	if err := head.error(); err != nil {
		br.block = nil
		return err
	}

	heap.Pop(&br.pih)

	if len(br.pih) == 0 {
		br.block = nil
		return io.EOF
	}

	br.block = &br.pih[0].block
	return nil
}

func (br *blockReader) error() error {
	if errors.Is(br.err, io.EOF) {
		return nil
	}
	return br.err
}