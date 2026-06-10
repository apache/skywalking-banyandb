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

package dump

import "github.com/apache/skywalking-banyandb/pkg/bytes"

// ReadScratch holds reusable file-read buffers for one PartReader. The dump
// iterator decodes blocks strictly one at a time, and each read buffer is fully
// consumed by the decoder before the next read — the decoder decompresses into
// its own buffer and the decoded values alias that, not these — so a single
// grown buffer per kind is safe to reuse for the whole part. A nil *ReadScratch
// disables reuse (ReadBuf allocates a fresh buffer each call).
type ReadScratch struct {
	meta  []byte
	value []byte
	ts    []byte
}

// MetaBuf returns the reusable metadata buffer grown to length n.
func (s *ReadScratch) MetaBuf(n int) []byte { s.meta = bytes.ResizeOver(s.meta, n); return s.meta }

// ValueBuf returns the reusable value buffer grown to length n.
func (s *ReadScratch) ValueBuf(n int) []byte { s.value = bytes.ResizeOver(s.value, n); return s.value }

// TSBuf returns the reusable timestamps buffer grown to length n.
func (s *ReadScratch) TSBuf(n int) []byte { s.ts = bytes.ResizeOver(s.ts, n); return s.ts }

// ReadBuf returns a buffer of length n: a fresh allocation when s is nil
// (default), or the reused scratch buffer selected by pick otherwise.
func ReadBuf(s *ReadScratch, pick func(*ReadScratch, int) []byte, n int) []byte {
	if s == nil {
		return make([]byte, n)
	}
	return pick(s, n)
}
