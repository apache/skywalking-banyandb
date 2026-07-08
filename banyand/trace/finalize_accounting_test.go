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

package trace

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// TestAccountUnsampledFlushed verifies the O(1) hot-path accounting: a no-op until the
// shard has been finalized (gen>0), then accumulates uncompressed span bytes. The
// method takes no filesystem and reads only in-memory part metadata + atomics, so by
// construction it performs zero metadata I/O on the flush path (Phase 3 acceptance).
func TestAccountUnsampledFlushed(t *testing.T) {
	tst := &tsTable{}
	flushed := map[uint64]*partWrapper{
		1: {p: &part{partMetadata: partMetadata{UncompressedSpanSizeBytes: 100}}},
		2: {p: &part{partMetadata: partMetadata{UncompressedSpanSizeBytes: 50}}},
	}

	// gen == 0 (never finalized): no accounting.
	tst.accountUnsampledFlushed(flushed)
	assert.Zero(t, tst.unsampledBytes.Load(), "gen 0 must not accumulate")

	// gen > 0: accumulate uncompressed span bytes.
	tst.finalizeGenCached.Store(1)
	tst.accountUnsampledFlushed(flushed)
	assert.Equal(t, int64(150), tst.unsampledBytes.Load())

	// Subsequent flushes keep accumulating.
	tst.accountUnsampledFlushed(map[uint64]*partWrapper{
		3: {p: &part{partMetadata: partMetadata{UncompressedSpanSizeBytes: 25}}},
	})
	assert.Equal(t, int64(175), tst.unsampledBytes.Load())

	// An empty flush set is a harmless no-op.
	tst.accountUnsampledFlushed(nil)
	assert.Equal(t, int64(175), tst.unsampledBytes.Load())
}

// TestInitTSTable_SeedsFinalizeCache verifies the cached finalize fields are seeded
// from the shard's persisted finalizeState at open (restart recovery).
func TestInitTSTable_SeedsFinalizeCache(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	dir := t.TempDir()
	require.NoError(t, writeFinalizeState(fileSystem, dir, finalizeState{FinalizeGeneration: 4, UnsampledBytes: 999}))

	tst, _ := initTSTable(fileSystem, dir, common.Position{}, logger.GetLogger("finalize-seed-test"),
		option{protector: protector.Nop{}}, nil)

	assert.Equal(t, uint64(4), tst.finalizeGenCached.Load(), "finalizeGenCached must be seeded from disk")
	assert.Equal(t, int64(999), tst.unsampledBytes.Load(), "unsampledBytes must be seeded from disk")
}
