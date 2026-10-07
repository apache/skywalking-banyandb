// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
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

package db

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func TestNativeExpiredTombstoneCompactionKeepsPinnedView(t *testing.T) {
	ctx := context.Background()
	location, cleanup, err := test.NewSpace()
	require.NoError(t, err)
	defer cleanup()
	cfg := Config{
		Location: location, MetricsScopeName: "property_native_expiry",
		ExpireToDeleteDuration: time.Second,
		Repair:                 RepairConfig{Location: filepath.Join(location, "repair")},
		Index:                  IndexConfig{WaitForPersistence: true},
	}
	opened, err := OpenDB(ctx, cfg, observability.BypassRegistry, fs.NewLocalFileSystem())
	require.NoError(t, err)
	db := opened.(*database)
	property := generateProperty("expired", time.Now().UnixNano(), 1)
	propertyID := GetPropertyID(property)
	require.NoError(t, opened.Update(ctx, 0, propertyID, property))
	require.NoError(t, opened.Delete(ctx, [][]byte{propertyID}, time.Now().Add(-10*time.Second)))

	shard, err := db.loadShard(ctx, testPropertyGroup, 0)
	require.NoError(t, err)
	oldView, err := shard.nativeStore.owner.Acquire(ctx)
	require.NoError(t, err)
	oldDocument, found, err := oldView.Lookup(ctx, propertyID)
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, storedNativeField(oldDocument, deleteField))

	before, err := opened.Query(ctx, &propertyv1.QueryRequest{Groups: []string{testPropertyGroup}})
	require.NoError(t, err)
	require.Len(t, before, 1)
	require.NotZero(t, before[0].DeleteTime())

	require.NoError(t, shard.nativeStore.owner.Compact(ctx))
	after, err := opened.Query(ctx, &propertyv1.QueryRequest{Groups: []string{testPropertyGroup}})
	require.NoError(t, err)
	require.Empty(t, after)

	// Collection must not unlink data reachable by a pinned pre-compaction view.
	require.ErrorIs(t, shard.nativeStore.owner.CollectGarbage(ctx), native.ErrPersistenceBusy)
	stillReadable, found, err := oldView.Lookup(ctx, propertyID)
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, storedNativeField(stillReadable, deleteField))
	require.NoError(t, oldView.Close())
	require.NoError(t, opened.Close())

	reopened, err := OpenDB(ctx, cfg, observability.BypassRegistry, fs.NewLocalFileSystem())
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	reopenedRows, err := reopened.Query(ctx, &propertyv1.QueryRequest{Groups: []string{testPropertyGroup}})
	require.NoError(t, err)
	require.Empty(t, reopenedRows)
}

func storedNativeField(document native.Document, name string) [][]byte {
	for _, field := range document.Fields {
		if field.Name == name {
			return [][]byte{field.Value}
		}
	}
	return nil
}
