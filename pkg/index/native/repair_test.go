// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package native

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

func TestReadOnlyGenerationRepairTuplePage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "property")
	owner, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path})
	require.NoError(t, err)
	callback := make(chan error, 1)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{
			repairDocument("doc-b", "b", 20),
			repairDocument("doc-a", "a", 10),
		},
		PersistentCallback: func(persistErr error) { callback <- persistErr },
	}))
	require.NoError(t, <-callback)
	require.NoError(t, owner.Close())

	generation, err := OpenReadOnlyGeneration(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, generation.Close()) }()
	rows, err := generation.RepairTuplePage(context.Background(), RepairPageRequest{PageSize: 1})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, []byte("g"), rows[0].SortValues[0])
	require.Equal(t, []byte("a"), rows[0].SortValues[1])
	require.Equal(t, []byte("a"), rows[0].SortValues[2])
	timestamp, timestampErr := nativeice.DecodePrefixCodedInt64(rows[0].SortValues[3])
	require.NoError(t, timestampErr)
	require.Equal(t, int64(10), timestamp)
	require.Equal(t, []byte("sha-a"), rows[0].Value)
	cursor := rows[0].Cursor
	rows[0].SortValues[0][0] = 'x'
	next, err := generation.RepairTuplePage(context.Background(), RepairPageRequest{PageSize: 1, After: cursor})
	require.NoError(t, err)
	require.Len(t, next, 1)
	require.Equal(t, []byte("sha-b"), next[0].Value)
	_, err = generation.RepairTuplePage(context.Background(), RepairPageRequest{PageSize: 1, After: next[0].Cursor})
	require.NoError(t, err)
}

func repairDocument(identifier, name string, timestamp int64) Document {
	return Document{
		Identifier: []byte(identifier), Timestamp: timestamp,
		Fields: []Field{
			{Name: "_group", Value: []byte("g"), Store: true, Index: true, Sort: true},
			{Name: "_im_name", Value: []byte(name), Store: true, Index: true, Sort: true},
			{Name: "_entity_id", Value: []byte(name), Store: true, Index: true, Sort: true},
			{Name: "_sha_value", Value: []byte("sha-" + name), Store: true},
		},
	}
}
