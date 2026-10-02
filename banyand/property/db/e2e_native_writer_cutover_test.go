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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// TestE2EPropertyNativeWriterCutoverAndRollback exercises the reversible
// native-writer cutover through the real OpenDB -> newShard -> store path
// issue #14075 (NIDX-02C) names as this leaf's single primary regression.
//
// Three requirements are proved, none of them assumed:
//
//	(1) lock-first ordering -- a second concurrent OpenDB against a Location
//	    that already holds a written shard must fail before opening (or
//	    attempting to open) any shard writer of its own, by panicking on the
//	    outer lock immediately. Before this fix, ownership was only ever
//	    discovered as a side effect of load() reaching the shard the first
//	    process already holds open, which returns a plain error rather than
//	    failing closed at the front door.
//	(2) defer-guarded release -- a startup failure that happens only after
//	    the (now first-acquired) lock is already held returns an error
//	    rather than panicking, and releases the lock: a subsequent clean
//	    OpenDB against the same Location must succeed rather than being
//	    blocked by a stale lock.
//	(3) rollback round-trips the issue's own observable sequence -- a shard
//	    written Legacy, then Native, then reopened Native, then rolled back
//	    to Legacy and written again, must read back the exact value trace
//	    [legacy-v1, native-v2, native-v2, legacy-v3] the issue's primary
//	    executable regression names. WaitForPersistence is set on every
//	    config in this phase, so every Update call already blocks on its own
//	    durable callback (shard.go's updateDocuments) before returning: no
//	    durable callback is ever still in flight when Close runs, which is
//	    what "durable callbacks drain before close" requires. This chains
//	    two already-proven facts through the real production entrypoints:
//	    TestNativeEncodedGenerationOpensInTheCompatibilityReader (native
//	    encoder output opens in the unconditional-legacy NewStore) and
//	    TestNIDX02BOutputMatchesTheNativeEncoder (the plugin's output is
//	    byte-identical to that encoder's), so this is the first test to
//	    combine both through OpenDB/newShard/NewStore.
func TestE2EPropertyNativeWriterCutoverAndRollback(t *testing.T) {
	tester := require.New(t)

	// (1) A second concurrent OpenDB against a Location that already holds a
	// written (and still open) shard must fail closed at the outer lock,
	// before it ever reaches that shard through load().
	lockDir, lockCleanup, err := test.NewSpace()
	tester.NoError(err)
	defer lockCleanup()

	lockCfg := Config{
		Location:         lockDir,
		MetricsScopeName: "property_native_cutover_lock_test",
		FlushInterval:    3 * time.Second,
		Index:            IndexConfig{NativeWriter: true},
	}
	db1, openErr := OpenDB(context.Background(), lockCfg, observability.BypassRegistry, fs.NewLocalFileSystem())
	tester.NoError(openErr)

	property := generateProperty("native-cutover-id", time.Now().UnixNano(), 42)
	tester.NoError(db1.Update(context.Background(), 0, GetPropertyID(property), property),
		"a written shard must exist and stay open, so the second OpenDB below has something to race against")

	tester.Panics(func() {
		_, _ = OpenDB(context.Background(), lockCfg, observability.BypassRegistry, fs.NewLocalFileSystem())
	}, "a second OpenDB against a live Location holding an open shard must fail at the outer lock, before load() reaches that shard")

	tester.NoError(db1.Close())

	// (2) A startup failure that happens after the lock is already held
	// (here, db.load(ctx) failing on a directory it cannot parse as a shard
	// suffix, in an otherwise-empty Location) must return an error rather
	// than panic, and must release the lock.
	failureDir, failureCleanup, err := test.NewSpace()
	tester.NoError(err)
	defer failureCleanup()

	failureCfg := Config{
		Location:         failureDir,
		MetricsScopeName: "property_native_cutover_failure_test",
		FlushInterval:    3 * time.Second,
		Index:            IndexConfig{NativeWriter: true},
	}
	groupDir := filepath.Join(failureDir, testPropertyGroup)
	tester.NoError(os.MkdirAll(filepath.Join(groupDir, "shard-bogus"), 0o755))

	_, loadErr := OpenDB(context.Background(), failureCfg, observability.BypassRegistry, fs.NewLocalFileSystem())
	tester.Error(loadErr, "a load() failure after the lock is acquired must return an error, not panic")

	tester.NoError(os.RemoveAll(filepath.Join(groupDir, "shard-bogus")))

	db2, reopenErr := OpenDB(context.Background(), failureCfg, observability.BypassRegistry, fs.NewLocalFileSystem())
	tester.NoError(reopenErr, "the failed OpenDB above must have released the lock, or this reopen fails")
	tester.NoError(db2.Close())

	// (3) The issue's own primary observable: legacy-v1 -> native-v2 ->
	// (native reopen) native-v2 -> (legacy rollback) native-v2 -> legacy-v3.
	// WaitForPersistence: true on every config below matches production's
	// real property.service.go setting, so each Update already blocked on
	// its durable callback before returning -- Close always runs with
	// nothing left to drain.
	rollbackDir, rollbackCleanup, err := test.NewSpace()
	tester.NoError(err)
	defer rollbackCleanup()

	const compatibilityProbeID = "compatibility-probe"
	ctx := context.Background()
	readTag := func(db Database) int64 {
		results := queryDB(ctx, t, db, compatibilityProbeID)
		tester.Len(results, 1, "the compatibility-probe document must be readable")
		return unmarshalProperty(t, results[0].Source()).Tags[0].Value.GetInt().Value
	}
	openWithNative := func(nativeWriter bool) Database {
		cfg := Config{
			Location:         rollbackDir,
			MetricsScopeName: "property_native_cutover_rollback_test",
			FlushInterval:    3 * time.Second,
			Index:            IndexConfig{NativeWriter: nativeWriter, WaitForPersistence: true},
		}
		d, openErr := OpenDB(ctx, cfg, observability.BypassRegistry, fs.NewLocalFileSystem())
		tester.NoError(openErr)
		return d
	}

	// The storage key a write's id names is fixed at the document's creation
	// (see banyand/property/listener.go:96, which forwards the caller's own
	// id rather than recomputing one per update): GetPropertyID folds in
	// ModRevision, so recomputing it per version would create a new document
	// each time instead of updating the one this test tracks.
	probeID := GetPropertyID(generateProperty(compatibilityProbeID, time.Now().UnixNano(), 1))

	// Seed one legacy-created shard: legacy-v1.
	legacyV1 := generateProperty(compatibilityProbeID, time.Now().UnixNano(), 1)
	legacyDB1 := openWithNative(false)
	tester.NoError(legacyDB1.Update(ctx, 0, probeID, legacyV1))
	tester.Equal(int64(1), readTag(legacyDB1), "legacy-v1 must be readable immediately after its durable write")
	tester.NoError(legacyDB1.Close())

	// Open the same database Native: existing data is readable without bulk
	// conversion or an opening rewrite, then update it to native-v2.
	nativeDB1 := openWithNative(true)
	tester.Equal(int64(1), readTag(nativeDB1), "the Native-selected OpenDB must read the Legacy-written value unchanged")
	nativeV2 := generateProperty(compatibilityProbeID, time.Now().UnixNano(), 2)
	tester.NoError(nativeDB1.Update(ctx, 0, probeID, nativeV2))
	tester.Equal(int64(2), readTag(nativeDB1), "native-v2 must be readable immediately after its durable write")
	tester.NoError(nativeDB1.Close())

	// Close and reopen Native on the same directory: native-v2 persisted.
	nativeDB2 := openWithNative(true)
	tester.Equal(int64(2), readTag(nativeDB2), "native-v2 must survive a Native close and reopen")
	tester.NoError(nativeDB2.Close())

	// Roll back: explicitly reopen the same directory with Legacy.
	legacyDB2 := openWithNative(false)
	tester.Equal(int64(2), readTag(legacyDB2), "the rollback reopen must serve the Native-written value with no data loss")
	legacyV3 := generateProperty(compatibilityProbeID, time.Now().UnixNano(), 3)
	tester.NoError(legacyDB2.Update(ctx, 0, probeID, legacyV3))
	tester.Equal(int64(3), readTag(legacyDB2), "legacy-v3 must be readable immediately after its durable write")
	tester.NoError(legacyDB2.Close())

	// Close and reopen Legacy again: legacy-v3 persisted, completing the
	// [legacy-v1, native-v2, native-v2, legacy-v3] observable trace.
	legacyDB3 := openWithNative(false)
	defer func() {
		_ = legacyDB3.Close()
	}()
	tester.Equal(int64(3), readTag(legacyDB3), "legacy-v3 must survive a Legacy close and reopen")
}
