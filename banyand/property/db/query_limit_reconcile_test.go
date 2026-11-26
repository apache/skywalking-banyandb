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

package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// An unsorted limited query must count live entities, not raw hits: every revision and tombstone of an entity is a hit
// of its own, and the API layer keeps only the newest revision of an entity and drops it when that revision is a
// tombstone. Cutting the raw hits at the limit returned an empty page for LIMIT 1 when most entities were deleted.
func TestUnsortedLimitCountsLiveEntitiesNotRawHits(t *testing.T) {
	ctx := context.Background()
	location, cleanup, err := test.NewSpace()
	require.NoError(t, err)
	defer cleanup()
	db, err := OpenDB(ctx, Config{
		Location: location, MetricsScopeName: "property_limit_reconcile",
		FlushInterval: time.Second, Index: IndexConfig{WaitForPersistence: true},
	}, observability.BypassRegistry, fs.NewLocalFileSystem())
	require.NoError(t, err)
	defer db.Close()

	const total, live = 60, 5
	for i := 0; i < total; i++ {
		property := generateProperty(fmt.Sprintf("entity-%03d", i), int64(100+i), i)
		require.NoError(t, db.Update(ctx, 0, GetPropertyID(property), property))
	}
	// A second revision for the first half, so old revisions are hits too.
	for i := 0; i < total/2; i++ {
		property := generateProperty(fmt.Sprintf("entity-%03d", i), int64(1000+i), i+1000)
		require.NoError(t, db.Update(ctx, 0, GetPropertyID(property), property))
	}
	// Delete all but the last `live` entities, newest revisions included.
	var deleted [][]byte
	for i := 0; i < total-live; i++ {
		revision := int64(100 + i)
		if i < total/2 {
			revision = int64(1000 + i)
		}
		deleted = append(deleted, GetPropertyID(generateProperty(fmt.Sprintf("entity-%03d", i), revision, 0)))
	}
	require.NoError(t, db.Delete(ctx, deleted, time.Now()))

	liveRows := func(rows []QueriedProperty) int {
		count := 0
		for _, row := range rows {
			if row.DeleteTime() == 0 {
				count++
			}
		}
		return count
	}
	for _, limit := range []uint32{1, 2, live, live + 3} {
		rows, queryErr := db.Query(ctx, &propertyv1.QueryRequest{Groups: []string{testPropertyGroup}, Limit: limit})
		require.NoError(t, queryErr)
		require.Equal(t, int(min(limit, live)), liveRows(rows), "limit=%d returned the wrong number of live entities", limit)
		seen := make(map[string]struct{}, len(rows))
		for _, row := range rows {
			entity := string(row.ID())
			entity = entity[:len(entity)-len(fmt.Sprint(row.Timestamp()))-1]
			_, duplicate := seen[entity]
			require.False(t, duplicate, "limit=%d returned entity %s twice", limit, entity)
			seen[entity] = struct{}{}
		}
	}
	// No limit still returns every stored revision, as repair and internal callers rely on.
	all, err := db.Query(ctx, &propertyv1.QueryRequest{Groups: []string{testPropertyGroup}})
	require.NoError(t, err)
	require.Greater(t, len(all), total)
}
