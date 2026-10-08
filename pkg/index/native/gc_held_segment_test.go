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
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOwnerGarbageCollectionRefusesAChangedHeldSegment damages a segment the
// owner serves -- truncating it, replacing it with a same-sized copy, or
// replacing it with a symbolic link to a copy -- and requires garbage
// collection to refuse the newest snapshot and keep the older ones, since a
// restart could not open it either.
func TestOwnerGarbageCollectionRefusesAChangedHeldSegment(t *testing.T) {
	for _, damage := range []string{"truncated", "replaced", "symlinked"} {
		t.Run(damage, func(t *testing.T) {
			path := t.TempDir()
			owner, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path, CompactionThreshold: -1})
			require.NoError(t, err)
			defer func() { _ = owner.Close() }()
			admit := func(identifier string) {
				durable := make(chan error, 1)
				require.NoError(t, owner.Batch(context.Background(), Batch{
					Documents:          []Document{{Identifier: []byte(identifier)}},
					PersistentCallback: func(callbackErr error) { durable <- callbackErr },
				}))
				require.NoError(t, <-durable)
			}
			admit("first")
			admit("second")
			require.NoError(t, owner.CollectGarbage(context.Background()))
			admit("third")
			segments, _ := filepath.Glob(filepath.Join(path, "*.seg"))
			require.NotEmpty(t, segments)
			sort.Strings(segments)
			held := segments[0]
			data, readErr := os.ReadFile(held)
			require.NoError(t, readErr)
			copyPath := filepath.Join(path, "copy.bak")
			require.NoError(t, os.WriteFile(copyPath, data, 0o600))
			switch damage {
			case "truncated":
				require.NoError(t, os.Truncate(held, int64(len(data)-1)))
			case "replaced":
				require.NoError(t, os.Rename(copyPath, held))
			case "symlinked":
				require.NoError(t, os.Remove(held))
				require.NoError(t, os.Symlink(copyPath, held))
			}
			before, _ := filepath.Glob(filepath.Join(path, "*.snp"))
			require.Greater(t, len(before), 1, "the older snapshot must still exist before collection")
			require.Error(t, owner.CollectGarbage(context.Background()))
			after, _ := filepath.Glob(filepath.Join(path, "*.snp"))
			require.Equal(t, before, after, "a refused collection must keep every snapshot")
		})
	}
}
