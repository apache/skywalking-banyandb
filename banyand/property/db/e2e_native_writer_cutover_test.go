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

// TestE2EPropertyNativeWriterCutoverAndRollback exercises OpenDB's lock
// ownership and startup-failure cleanup through the real OpenDB -> newShard
// -> store path issue #14075 (NIDX-02C) names.
//
// Two requirements are proved, none of them assumed:
//
//	(1) lock-first ordering -- a second concurrent OpenDB against a Location
//	    that already holds a written shard must fail before opening (or
//	    attempting to open) any shard writer of its own, by returning an error from the
//	    outer lock immediately. Before this fix, ownership was only ever
//	    discovered as a side effect of load() reaching the shard the first
//	    process already holds open, which returns a plain error rather than
//	    failing closed at the front door.
//	(2) defer-guarded release -- a startup failure that happens only after
//	    the (now first-acquired) lock is already held returns an error
//	    rather than panicking, and releases the lock: a subsequent clean
//	    OpenDB against the same Location must succeed rather than being
//	    blocked by a stale lock.
//
// NIDX-03 removed the third requirement this test used to prove: the
// legacy<->native writer round trip (SwitchIndexWriter no longer exists --
// the binary opens every shard with the native store unconditionally, and
// rollback is a file property exercised by the dedicated rollback harness,
// not an in-process switch).
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
	}
	db1, openErr := OpenDB(context.Background(), lockCfg, observability.BypassRegistry, fs.NewLocalFileSystem())
	tester.NoError(openErr)

	property := generateProperty("native-cutover-id", time.Now().UnixNano(), 42)
	tester.NoError(db1.Update(context.Background(), 0, GetPropertyID(property), property),
		"a written shard must exist and stay open, so the second OpenDB below has something to race against")

	var contentionErr error
	tester.NotPanics(func() {
		_, contentionErr = OpenDB(context.Background(), lockCfg, observability.BypassRegistry, fs.NewLocalFileSystem())
	}, "lock contention is an expected OpenDB error, not a process-fatal panic")
	tester.Error(contentionErr, "a second OpenDB against a live Location holding an open shard must fail at the outer lock, before load() reaches that shard")

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
		Repair: RepairConfig{
			Location:           filepath.Join(failureDir, "repair"),
			BuildTreeCron:      "@every 10m",
			QuickBuildTreeTime: 10 * time.Minute,
			TreeSlotCount:      1,
			Enabled:            true,
		},
	}
	// Seed a valid shard. The malformed directory below is deliberately
	// encountered after this shard, so startup cleanup must close a partially
	// opened native writer and stop the repair scheduler before unlocking.
	seedDB, seedErr := OpenDB(context.Background(), failureCfg, observability.BypassRegistry, fs.NewLocalFileSystem())
	tester.NoError(seedErr)
	seedProperty := generateProperty("startup-cleanup", time.Now().UnixNano(), 7)
	tester.NoError(seedDB.Update(context.Background(), 0, GetPropertyID(seedProperty), seedProperty))
	tester.NoError(seedDB.Close())

	groupDir := filepath.Join(failureDir, testPropertyGroup)
	tester.NoError(os.MkdirAll(filepath.Join(groupDir, "shard-bogus"), 0o755))

	_, loadErr := OpenDB(context.Background(), failureCfg, observability.BypassRegistry, fs.NewLocalFileSystem())
	tester.Error(loadErr, "a load() failure after the lock is acquired must return an error, not panic")

	tester.NoError(os.RemoveAll(filepath.Join(groupDir, "shard-bogus")))

	db2, reopenErr := OpenDB(context.Background(), failureCfg, observability.BypassRegistry, fs.NewLocalFileSystem())
	tester.NoError(reopenErr, "the failed OpenDB above must have released the lock, or this reopen fails")
	tester.NoError(db2.Close())
}
