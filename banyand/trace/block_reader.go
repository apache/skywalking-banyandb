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

package trace

import (
	"container/heap"
	"errors"
	"fmt"
	"io"

	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

type seqReader struct {
	sr        fs.SeqReader
	r         fs.Reader
	bytesRead uint64
}

func (sr *seqReader) reset() {
	sr.r = nil
	if sr.sr != nil {
		fs.MustClose(sr.sr)
	}
	sr.sr = nil
	sr.bytesRead = 0
}

func (sr *seqReader) Path() string {
	return sr.r.Path()
}

func (sr *seqReader) init(r fs.Reader) {
	sr.reset()
	sr.sr = r.SequentialRead()
	sr.r = r
}

func (sr *seqReader) mustReadFull(data []byte) {
	n, err := io.ReadFull(sr.sr, data)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return
		}
		logger.Panicf("cannot read data: %v", err)
	}
	if n != len(data) {
		logger.Panicf("cannot read full data: %d/%d", n, len(data))
	}
	sr.bytesRead += uint64(n)
}

func generateSeqReader() *seqReader {
	if v := seqReaderPool.Get(); v != nil {
		return v
	}
	return &seqReader{}
}

func releaseSeqReader(sr *seqReader) {
	sr.reset()
	seqReaderPool.Put(sr)
}

var seqReaderPool = pool.Register[*seqReader]("trace-seqReader")

type seqReaders struct {
	tagMetadata map[string]*seqReader
	tags        map[string]*seqReader
	primary     seqReader
	spans        seqReader
}

func (sr *seqReaders) reset() {
	sr.primary.reset()
	sr.spans.reset()
	if sr.tagMetadata != nil {
		for k, r := range sr.tagMetadata {
			releaseSeqReader(r)
			delete(sr.tagMetadata, k)
		}
	}
	if sr.tags != nil {
		for k, r := range sr.tags {
			releaseSeqReader(r)
			delete(sr.tags, k)
		}
	}
}

func (sr *seqReaders) init(p *part) {
	sr.reset()
	sr.primary.init(p.primary)
	sr.spans.init(p.spans)
	if sr.tags == nil {
		sr.tags = make(map[string]*seqReader)
		sr.tagMetadata = make(map[string]*seqReader)
	}

	for k, r := range p.tags {
		sr.tags[k] = generateSeqReader()
		sr.tags[k].init(r)
		sr.tagMetadata[k] = generateSeqReader()
		sr.tagMetadata[k].init(p.tagMetadata[k])
	}
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
		if pi.nextBlockMetadata() {
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

func (br *blockReader) nextBlockMetadata() bool {
	if br.err != nil {
		return false
	}
	if br.nextBlockNoop {
		br.nextBlockNoop = false
		return true
	}

	br.err = br.nextMetadata()
	if br.err != nil {
		if errors.Is(br.err, io.EOF) {
			return false
		}
		br.err = fmt.Errorf("can't get the block to merge: %w", br.err)
		return false
	}
	return true
}

func (br *blockReader) nextMetadata() error {
	head := br.pih[0]
	if head.nextBlockMetadata() {
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

func (br *blockReader) loadBlockData(decoder *encoding.BytesBlockDecoder) {
	br.pih[0].mustLoadBlockData(decoder, br.block)
}

func (br *blockReader) error() error {
	if errors.Is(br.err, io.EOF) {
		return nil
	}
	return br.err
}

var blockReaderPool = pool.Register[*blockReader]("trace-blockReader")

func generateBlockReader() *blockReader {
	if v := blockReaderPool.Get(); v != nil {
		return v
	}
	return &blockReader{}
}

func releaseBlockReader(br *blockReader) {
	br.reset()
	blockReaderPool.Put(br)
}
