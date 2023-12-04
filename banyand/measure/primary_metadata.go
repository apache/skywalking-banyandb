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

package measure

import (
	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
)

type primaryBlockMetadata struct {
	seriesID     common.SeriesID
	minTimestamp int64
	maxTimestamp int64
	dataBlock
}

// reset resets ih for subsequent re-use.
func (ph *primaryBlockMetadata) reset() {
	ph.seriesID = 0
	ph.minTimestamp = 0
	ph.maxTimestamp = 0
	ph.offset = 0
	ph.size = 0
}

func (ph *primaryBlockMetadata) mustWriteBlock(data []byte, sidFirst common.SeriesID, minTimestamp, maxTimestamp int64, sw *writers) {
	ph.seriesID = sidFirst
	ph.minTimestamp = minTimestamp
	ph.maxTimestamp = maxTimestamp

	bb := longTermBufPool.Get()
	bb.Buf = zstd.Compress(bb.Buf[:0], data, 1)
	ph.offset = sw.primaryWriter.bytesWritten
	ph.size = uint64(len(bb.Buf))
	sw.primaryWriter.MustWrite(bb.Buf)
	longTermBufPool.Put(bb)
}

// marshal appends marshaled ih to dst and returns the result.
func (ph *primaryBlockMetadata) marshal(dst []byte) []byte {
	dst = ph.seriesID.AppendToBytes(dst)
	dst = encoding.Uint64ToBytes(dst, uint64(ph.minTimestamp))
	dst = encoding.Uint64ToBytes(dst, uint64(ph.maxTimestamp))
	dst = encoding.Uint64ToBytes(dst, ph.offset)
	dst = encoding.Uint64ToBytes(dst, ph.size)
	return dst
}
