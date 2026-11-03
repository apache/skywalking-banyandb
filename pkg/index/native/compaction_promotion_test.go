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

package native

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOwnerCompactionSurvivesPersistencePromotion(t *testing.T) {
	cases := []struct {
		name              string
		automatic         bool
		deleteDuringMerge bool
	}{
		{name: "manual"},
		{name: "automatic", automatic: true},
		{name: "concurrent delete remains stale", deleteDuringMerge: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			path := t.TempDir()
			lease := &gatedPathLease{reached: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(lease.release) }) }
			persisted := make(chan error, 1)
			threshold := -1
			if tc.automatic {
				threshold = 2
			}
			var owner *Owner
			var prepareOnce sync.Once
			var prepareErr error
			var err error
			owner, err = NewOwner(OwnerOptions{
				Lease: lease, Path: path, CompactionThreshold: threshold,
				PrepareMergeCallback: func(mergeCtx context.Context, _ MergeDocument) (bool, error) {
					prepareOnce.Do(func() {
						// Compaction has captured the inputs. Complete the first persist now,
						// forcing its file-backed replacements into the current root before
						// compaction checks whether those inputs are still live.
						release()
						select {
						case prepareErr = <-persisted:
						case <-ctx.Done():
							prepareErr = ctx.Err()
						case <-mergeCtx.Done():
							prepareErr = mergeCtx.Err()
						}
						if prepareErr == nil && tc.deleteDuringMerge {
							prepareErr = owner.Batch(mergeCtx, Batch{Deletes: [][]byte{[]byte("series-a")}})
						}
					})
					return false, prepareErr
				},
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				cancel()
				release()
				require.NoError(t, owner.Close())
			})
			require.NoError(t, owner.Batch(ctx, Batch{
				Documents:          []Document{{Identifier: []byte("series-a")}, {Identifier: []byte("deleted")}},
				PersistentCallback: func(persistErr error) { persisted <- persistErr },
			}))
			// The third path validation is the first persist: construction and the
			// first admission ran before it. Hold that persist while admitting b.
			select {
			case <-lease.reached:
			case <-ctx.Done():
				t.Fatal("persistence did not reach its lease barrier")
			}
			require.NoError(t, owner.Batch(ctx, Batch{
				Documents: []Document{{Identifier: []byte("series-b")}},
				Deletes:   [][]byte{[]byte("deleted")},
			}))
			if !tc.automatic {
				compactErr := owner.Compact(ctx)
				if tc.deleteDuringMerge {
					require.ErrorIs(t, compactErr, ErrStaleCompaction)
				} else {
					require.NoError(t, compactErr)
				}
			}
			if !tc.deleteDuringMerge {
				require.Eventually(t, func() bool {
					owner.mu.Lock()
					defer owner.mu.Unlock()
					return len(owner.root.segments) == 1
				}, 5*time.Second, time.Millisecond, "promotion must not strand compaction")
			}
			checkDocuments := func(current *Owner) {
				view, acquireErr := current.Acquire(ctx)
				require.NoError(t, acquireErr)
				defer func() { require.NoError(t, view.Close()) }()
				for id, want := range map[string]bool{"series-a": !tc.deleteDuringMerge, "series-b": true, "deleted": false} {
					_, found, lookupErr := view.Lookup(ctx, []byte(id))
					require.NoError(t, lookupErr)
					require.Equal(t, want, found, id)
				}
			}
			checkDocuments(owner)
			require.NoError(t, owner.Close())
			reopened, reopenErr := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, CompactionThreshold: -1})
			require.NoError(t, reopenErr)
			defer func() { require.NoError(t, reopened.Close()) }()
			checkDocuments(reopened)
		})
	}
}
