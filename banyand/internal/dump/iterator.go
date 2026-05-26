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

// Package dump holds the shared decoding helpers reused by the measure, stream
// and trace part parsers under banyand/dump. It contains no storage-format
// specifics: the per-format block layout lives in the sub-packages.
package dump

// Position is the (block, row) position of an iterator, or where Err() was
// raised when the iterator is in terminal state. BlockIdx is 0-based, numbered
// globally across all primary blocks. RowIdx is 0-based within the current
// block; -1 before the first successful Next().
type Position struct {
	BlockIdx int
	RowIdx   int
}

// Iterator walks all rows of a part one block at a time. The concrete Row type
// is provided by each sub-package. Not safe for concurrent use. Bytes inside a
// returned Row alias the decode buffer captured by the decode callback and are
// valid only until the next Next()/Close().
type Iterator[Row any] struct {
	decode      func(blockIdx int) ([]Row, error)
	err         error
	rowBuf      []Row
	numBlocks   int
	blockCursor int
	rowIdx      int
	closed      bool
}

// NewIterator returns an iterator that lazily decodes numBlocks blocks, calling
// decode(blockIdx) the first time each block is visited.
func NewIterator[Row any](numBlocks int, decode func(blockIdx int) ([]Row, error)) *Iterator[Row] {
	return &Iterator[Row]{numBlocks: numBlocks, decode: decode, blockCursor: -1, rowIdx: -1}
}

// NewErrIterator returns an iterator already in terminal-error state, so the
// first Next() returns false and Err() reports err.
func NewErrIterator[Row any](err error) *Iterator[Row] {
	return &Iterator[Row]{err: err, blockCursor: -1, rowIdx: -1}
}

// Next advances to the next row, returning false at end-of-part or on error.
// After it returns false, check Err(). Once Err() != nil the iterator is in a
// terminal state and stays there.
func (it *Iterator[Row]) Next() bool {
	if it.closed || it.err != nil {
		return false
	}
	for {
		if it.blockCursor >= 0 && it.blockCursor < it.numBlocks && it.rowIdx+1 < len(it.rowBuf) {
			it.rowIdx++
			return true
		}
		if it.blockCursor+1 >= it.numBlocks {
			return false
		}
		it.blockCursor++
		rows, err := it.decode(it.blockCursor)
		if err != nil {
			it.err = err
			return false
		}
		it.rowBuf = rows
		it.rowIdx = -1
	}
}

// Row returns the current row, or a zero Row before the first successful Next().
func (it *Iterator[Row]) Row() Row {
	var zero Row
	if it.rowIdx < 0 || it.rowIdx >= len(it.rowBuf) {
		return zero
	}
	return it.rowBuf[it.rowIdx]
}

// Err returns the first non-EOF error encountered during iteration.
func (it *Iterator[Row]) Err() error {
	return it.err
}

// Position returns the current row position, or where Err() was raised.
func (it *Iterator[Row]) Position() Position {
	blockIdx := it.blockCursor
	if blockIdx < 0 {
		blockIdx = 0
	}
	return Position{BlockIdx: blockIdx, RowIdx: it.rowIdx}
}

// Close releases iterator buffers. Safe to call multiple times.
func (it *Iterator[Row]) Close() error {
	it.closed = true
	it.rowBuf = nil
	return nil
}
