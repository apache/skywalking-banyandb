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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations
// under the License.

package db

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// NIDX-03 §15 Property file-rollback rerun, write side. #1390 recorded this
// check once at 588cc602 (docs/design/0.12.0/native-inverted-index/
// verification/property-native-cutover/README.md), against an unreleased
// main commit; this reruns it against NIDX-03 §1's actual rollback target,
// v0.11.1 (the last published release, which runs Property on the retired
// bluge engine -- unlike 735e9ad2 or 588cc602, both already native), now
// that the legacy writer switch itself is gone (IndexConfig.NativeWriter,
// SwitchIndexWriter, shard.store -- banyand/property/db/{shard,db,
// repair_gossip}.go). Property's on-disk format is unchanged by NIDX-03
// (only its legacy branches were removed), so this is a narrower
// re-confirmation, not a new claim.
//
// TestGenerateNIDX03PropertyRollbackCorpus writes properties (including an
// update and a delete) with the current (native-only) code to
// NIDX03_PROPERTY_ROLLBACK_DIR (default /mnt/d/tmp-gao-build/
// nidx03-property-rollback; see nidx03PropertyRollbackDir), queries them
// back, and dumps the query result as JSON beside the data. v0.11.1's own
// copy of this package (staged by test/rollback/nidx03's previous-release
// harness) opens the same directory with the exported OpenDB and Query --
// unchanged in shape since before #1390 -- and is expected to match exactly.
//
// The property revision model this corpus exercises is NOT "later
// ModRevision wins": GetPropertyID (group/name/id + "/" + ModRevision) makes
// each revision its own physical document, so updating a property at a new
// ModRevision adds a second live, independently queryable row rather than
// superseding the first (see the two "rollback-update" rows -- ModRevision
// 200 and 201 -- in the captured results; nothing collapses them). Only an
// explicit Delete (a tombstone row) or two writes landing on the SAME
// ModRevision (last write in that batch wins -- see
// banyand/property/db/shard_test.go's "repair deleted version property with
// same data" case) change which row(s) a query returns.
func TestGenerateNIDX03PropertyRollbackCorpus(t *testing.T) {
	if os.Getenv("NIDX03_ROLLBACK_WRITE") == "" {
		t.Skip("one-off rollback-proof corpus generator; set NIDX03_ROLLBACK_WRITE=1 to (re)run it")
	}
	root := nidx03PropertyRollbackDir(t)
	require.NoError(t, os.RemoveAll(root))
	require.NoError(t, os.MkdirAll(root, 0o755))

	ctx := context.Background()
	database, err := OpenDB(ctx, Config{
		Location: root, MetricsScopeName: "nidx03_property_rollback", FlushInterval: time.Second,
		Index: IndexConfig{WaitForPersistence: true},
	}, observability.BypassRegistry, fs.NewLocalFileSystem())
	require.NoError(t, err)

	alive := generateProperty("rollback-alive", 100, 7)
	require.NoError(t, database.Update(ctx, 0, GetPropertyID(alive), alive))
	toUpdate := generateProperty("rollback-update", 200, 1)
	require.NoError(t, database.Update(ctx, 0, GetPropertyID(toUpdate), toUpdate))
	updated := generateProperty("rollback-update", 201, 2)
	require.NoError(t, database.Update(ctx, 0, GetPropertyID(updated), updated))
	toDelete := generateProperty("rollback-delete", 300, 1)
	require.NoError(t, database.Update(ctx, 0, GetPropertyID(toDelete), toDelete))
	require.NoError(t, database.Delete(ctx, [][]byte{GetPropertyID(toDelete)}, time.Now()))

	rows, queryErr := database.Query(ctx, &propertyv1.QueryRequest{Groups: []string{testPropertyGroup}})
	require.NoError(t, queryErr)
	require.NoError(t, database.Close())

	writeNIDX03PropertyResults(t, root, rows)
}

func nidx03PropertyRollbackDir(t *testing.T) string {
	t.Helper()
	if dir := os.Getenv("NIDX03_PROPERTY_ROLLBACK_DIR"); dir != "" {
		return dir
	}
	return "/mnt/d/tmp-gao-build/nidx03-property-rollback"
}

// cross-version diff depends on (kept identical in the 735e9ad2-ported read
// side); it is not a hot allocation path, so pointer-byte packing does not
// matter here.
//
//nolint:govet // field order is the JSON key order this proof's byte-for-byte
type nidx03PropertyRow struct {
	ID         string `json:"id"`
	Timestamp  int64  `json:"timestamp"`
	DeleteTime int64  `json:"deleteTime"`
	Source     string `json:"source"`
}

func writeNIDX03PropertyResults(t *testing.T, root string, rows []QueriedProperty) {
	t.Helper()
	results := make([]nidx03PropertyRow, 0, len(rows))
	for _, row := range rows {
		results = append(results, nidx03PropertyRow{
			ID: string(row.ID()), Timestamp: row.Timestamp(), DeleteTime: row.DeleteTime(), Source: string(row.Source()),
		})
	}
	sort.Slice(results, func(i, j int) bool { return results[i].ID < results[j].ID })
	data, err := json.MarshalIndent(results, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(root+"-results.json", data, 0o600))
}
