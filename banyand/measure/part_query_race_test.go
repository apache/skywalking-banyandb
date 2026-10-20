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

package measure

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
)

// TestConcurrentQueriesShareParts runs queries with different caches and shard IDs over the
// same snapshot-owned parts. A query must not write the shared parts: under -race this fails
// if the cache or shard ID is stored on a part, and the assertions fail if one query observes
// another's context.
func TestConcurrentQueriesShareParts(t *testing.T) {
	fx := newFixture(t, fxPart{{sid: 1, from: 1, to: 50, version: 1}}, fxPart{{sid: 2, from: 1, to: 50, version: 1}})
	snp := &snapshot{ref: 1}
	for _, r := range fx.parts {
		snp.parts = append(snp.parts, newPartWrapper(nil, r.p))
	}
	const queries = 16
	var wg sync.WaitGroup
	errs := make(chan error, queries)
	for g := range queries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cache := storage.NewShardCache("race-group", 0, 0)
			shardID := common.ShardID(g)
			for range 50 {
				refs, n := snp.getParts(nil, cache, shardID, 0, 1<<40)
				if n != len(snp.parts) {
					errs <- errBadPartCount
					return
				}
				ti := generateTstIter()
				ti.init(refs, fx.sids, 0, 1<<40)
				for ti.nextBlock() {
					if pi := ti.piHeap[0]; pi.shardID != shardID || pi.c != cache {
						errs <- errWrongContext
						releaseTstIter(ti)
						return
					}
				}
				releaseTstIter(ti)
			}
		}()
	}
	wg.Wait()
	close(errs)
	require.NoError(t, <-errs)
}

var (
	errBadPartCount = errors.New("unexpected part count")
	errWrongContext = errors.New("query observed another query's cache or shard ID")
)
