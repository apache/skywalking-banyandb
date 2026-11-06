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

package dquery

import (
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/pkg/run"
)

// The function in this file is a test-only hook. An in-process integration test starts the liaison through its command
// and cannot reach the liaison's query service, so it announces the chunk caps the next service takes.

type chunkLimits struct {
	maxBytes       uint64
	maxRowsTrace   uint32
	maxRowsStream  uint32
	maxRowsMeasure uint32
}

var nextChunkLimits atomic.Pointer[chunkLimits]

// SetNextChunkLimits makes the next query service created by NewService ask data nodes for scan chunks of at most
// the given rows and bytes per model, so that a test can force every result to be split. Services created later keep the
// defaults. It is intended for tests.
func SetNextChunkLimits(maxRowsTrace, maxRowsStream, maxRowsMeasure uint32, maxBytes uint64) {
	nextChunkLimits.Store(&chunkLimits{maxRowsTrace: maxRowsTrace, maxRowsStream: maxRowsStream, maxRowsMeasure: maxRowsMeasure, maxBytes: maxBytes})
}

// applyNextChunkLimits gives svc the caps requested by SetNextChunkLimits, if any.
func applyNextChunkLimits(svc *queryService) {
	limits := nextChunkLimits.Swap(nil)
	if limits == nil {
		return
	}
	svc.maxChunkRowsTrace, svc.maxChunkRowsStream, svc.maxChunkRowsMeasure = limits.maxRowsTrace, limits.maxRowsStream, limits.maxRowsMeasure
	svc.maxChunkBytes = run.Bytes(limits.maxBytes)
}
