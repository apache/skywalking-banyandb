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
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blugelabs/bluge"

	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func openSnapshotTestDB(t *testing.T) *database {
	t.Helper()
	dataDir, dataDefer, err := test.NewSpace()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dataDefer)
	repairDir, repairDefer, err := test.NewSpace()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(repairDefer)
	dbInstance, err := OpenDB(context.Background(), Config{
		Location:               dataDir,
		MetricsScopeName:       "property_snapshot_test",
		FlushInterval:          time.Hour,
		ExpireToDeleteDuration: time.Hour,
		Repair:                 RepairConfig{Location: repairDir, BuildTreeCron: "@every 1h", TreeSlotCount: 32},
		Snapshot:               SnapshotConfig{Func: func(context.Context) (string, error) { return "", nil }},
	}, observability.BypassRegistry, fs.NewLocalFileSystem())
	if err != nil {
		t.Fatal(err)
	}
	d := dbInstance.(*database)
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func TestSnapshotShards_CopiesEveryShardWithItsDocuments(t *testing.T) {
	d := openSnapshotTestDB(t)
	sh, err := d.loadShard(context.Background(), defaultGroupName, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"1", "2", "3"} {
		p := buildProperties(propertyBuilder{id: id, version: 1})
		if err = sh.update(GetPropertyID(p), p); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for a persisted segment so the snapshot shares file names with the live shard and
	// the hard-link check below has something to compare.
	waitForPersistedSegment(t, sh.location)
	dst := filepath.Join(t.TempDir(), "session")
	if err = d.SnapshotShards(context.Background(), dst); err != nil {
		t.Fatal(err)
	}
	shardDir := filepath.Join(dst, defaultGroupName, "shard-0")
	reader, err := bluge.OpenReader(bluge.DefaultConfig(shardDir))
	if err != nil {
		t.Fatalf("the shard snapshot must be a readable index: %v", err)
	}
	count, err := reader.Count()
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("the snapshot must hold every written document, including unflushed ones: got %d, want 3", count)
	}

	entries, err := os.ReadDir(shardDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("the shard snapshot holds no files")
	}
	matched := 0
	for _, e := range entries {
		snapInfo, statErr := os.Stat(filepath.Join(shardDir, e.Name()))
		if statErr != nil {
			t.Fatal(statErr)
		}
		liveInfo, statErr := os.Stat(filepath.Join(sh.location, e.Name()))
		if statErr != nil {
			continue
		}
		matched++
		if os.SameFile(liveInfo, snapInfo) {
			t.Fatalf("%s is a hard link of the live shard; the index backup is expected to copy it", e.Name())
		}
	}
	if matched == 0 {
		t.Fatalf("no snapshot file also exists in the live shard %s, so the hard-link check proved nothing", sh.location)
	}
}

func waitForPersistedSegment(t *testing.T, dir string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if segs, _ := filepath.Glob(filepath.Join(dir, "*.seg")); len(segs) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no persisted segment in %s", dir)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSnapshotShards_ExistingDestinationIsAnError(t *testing.T) {
	d := openSnapshotTestDB(t)
	if _, err := d.loadShard(context.Background(), defaultGroupName, 0); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "session")
	if err := os.MkdirAll(filepath.Join(dst, defaultGroupName, "shard-0"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := d.SnapshotShards(context.Background(), dst); err == nil {
		t.Fatal("an existing shard snapshot directory must be reported as an error")
	}
}

func TestSnapshotShards_CanceledContext(t *testing.T) {
	d := openSnapshotTestDB(t)
	if _, err := d.loadShard(context.Background(), defaultGroupName, 0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.SnapshotShards(ctx, filepath.Join(t.TempDir(), "session")); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}
