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
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// TestSyncPartContext_SealedPartIsRemovedUntilIntroduced checks that a part a chunked sync
// session sealed, but never introduced because the session failed, leaves neither its core
// part nor its SIDX part on disk, and is not in the table's snapshot.
func TestSyncPartContext_SealedPartIsRemovedUntilIntroduced(t *testing.T) {
	tmpPath, cleanup := test.Space(require.New(t))
	defer cleanup()

	var openShardCount atomic.Int32
	db := openTestTSDBForRefTest(t, tmpPath, 1, &openShardCount)
	defer db.Close()

	const groupName = "test-group"
	callback := &syncChunkCallback{
		l:          logger.GetLogger("test"),
		schemaRepo: newTestSchemaRepo(db, groupName),
	}
	segTime := time.Date(2026, 4, 17, 0, 0, 0, 0, time.Local)
	ctx := &queue.ChunkedSyncPartContext{
		ID:           7,
		Group:        groupName,
		MinTimestamp: segTime.UnixNano(),
		MaxTimestamp: segTime.UnixNano(),
		PartType:     PartTypeCore,
	}
	handler, err := callback.CreatePartHandler(ctx)
	require.NoError(t, err)
	partCtx := handler.(*syncPartContext)
	ctx.PartType = "idx"
	require.NoError(t, partCtx.NewPartType(ctx))

	tst := partCtx.tsTable
	corePath := partPath(tst.root, partCtx.partID)
	sidxPath := filepath.Join(tst.root, sidxDirName, "idx", fmt.Sprintf("%016x", partCtx.partID))

	sealer, ok := handler.(queue.PartSealer)
	require.True(t, ok, "a trace part handler must be a queue.PartSealer")
	require.NoError(t, sealer.SealSync())
	require.NoError(t, sealer.SealSync(), "sealing a sealed part is a no-op")
	require.True(t, tst.fileSystem.IsExist(filepath.Join(corePath, metadataFilename)), "sealing writes the core part's metadata")
	require.True(t, tst.fileSystem.IsExist(sidxPath), "sealing keeps the SIDX part")
	if snp := tst.currentSnapshot(); snp != nil {
		for _, pw := range snp.parts {
			require.NotEqual(t, partCtx.partID, pw.ID(), "a sealed part must not be introduced")
		}
		snp.decRef()
	}

	require.NoError(t, partCtx.Close())
	require.False(t, tst.fileSystem.IsExist(corePath), "closing a sealed part removes the core part")
	require.False(t, tst.fileSystem.IsExist(sidxPath), "closing a sealed part removes the SIDX part")
}
