// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. The ASF licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// This file proves database.Drop waits for an in-flight Delete/Update/
// Repair/Query to finish using the shard it is about to close, and that the
// shard's store refuses further use once closed -- against a REAL
// *native.Owner-backed nativePropertyStore, not a fake implementing a
// retired interface (the legacy index.SeriesStore the pre-NIDX-03 version of
// this file's blockingStore implemented; shard.store/index.SeriesStore are
// both gone).
//
// Two different seams make the in-flight call observably slow, because the
// write and read paths differ in how (if at all) they can block at all:
//
//   - Update (and Repair's own leading searchNative read -- see below)
//     block through a REAL, already-production seam: native.OwnerOptions.Persist
//     is a pluggable persistence function (its own doc comment: "Path enables
//     the built-in ICE snapshot publisher. An empty path keeps the owner in
//     memory-only mode unless Persist is supplied for a test seam"), and
//     nativePropertyStore.batch already waits on it when wait=true
//     (shard.go's production updateDocuments/batch call path is untouched;
//     only the test's own store construction supplies a blocking Persist).
//   - Query and Delete have no equivalent seam: native reads
//     (native.Owner.Acquire) are deliberately lock-free by design, so there
//     is no real hook that makes one observably slow. shard.go keeps one
//     minimal, unexported, documented hook for exactly this
//     (testBeforeNativeRead) -- see its doc comment on the shard struct.
//     Repair's own first step is a searchNative call (shard.go's repair()),
//     so the same read hook blocks it too; nothing about Repair's own
//     update step needs a seam.
//
// In every case, Drop not completing while the hook holds the shard "in
// use" is still enforced purely by database.mu (RWMutex): Update/Delete/
// Repair/Query all take db.mu.RLock() before touching the shard; Drop takes
// db.mu.Lock(). The hooks only make that critical section long enough to
// observe reliably; they do not implement the ordering guarantee itself.

// blockGate is testBeforeNativeRead's test double: the first call closes
// entered and then blocks until release is closed.
type blockGate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockGate() *blockGate {
	return &blockGate{entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *blockGate) hook() {
	g.once.Do(func() { close(g.entered) })
	<-g.release
}

// testRootLease is the minimal native.RootLease a memory-only (Path == "")
// *native.Owner needs: NewOwner only calls Validate, never ValidatePath,
// when Path is empty.
type testRootLease struct{}

func (testRootLease) Validate() error { return nil }

// newRealMemoryShard builds a shard backed by a REAL, memory-only
// (Path == "") *native.Owner wired through nativePropertyStore exactly as
// newNativePropertyStore would, so a read (lookup/query) that runs after
// testBeforeNativeRead's gate releases completes against a genuine owner
// instead of a nil one.
//
// It supplies a trivial, immediately-completing Persist function rather than
// leaving both Path and Persist unset. nativePropertyStore.batch always
// attaches a PersistentCallback (independent of wait), and native.Owner
// refuses any admission carrying one once Path == "" && Persist == nil,
// because that combination never starts a persistence worker to invoke it
// (see native.Owner's admitChecksLocked: queue == nil && hasCallback ->
// ErrPersistenceConfiguration). Repair's success path (no older property
// found) reaches updateDocuments/batch, so this shard needs a working
// persistence queue even though it stays memory-only.
func newRealMemoryShard(t *testing.T) *shard {
	t.Helper()
	owner, err := native.NewOwner(native.OwnerOptions{
		Lease:   testRootLease{},
		Persist: func(context.Context, *native.ReadView) error { return nil },
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = owner.Close() })
	return &shard{
		nativeStore: &nativePropertyStore{owner: owner, wait: true},
		id:          0,
		repairState: &repair{},
		l:           logger.GetLogger("db_lifetime_test"),
	}
}

// newBlockingWriteShard builds a shard with a REAL *native.Owner (memory-
// only: Path is empty, Persist is the test's own blocking function) wired
// through nativePropertyStore exactly as newNativePropertyStore would, so
// updateDocuments's production code path (shard.go) is exercised unchanged.
// release must be closed to let a persist -- and so the blocked Update --
// complete.
func newBlockingWriteShard(t *testing.T, entered, release chan struct{}) *shard {
	t.Helper()
	var once sync.Once
	owner, err := native.NewOwner(native.OwnerOptions{
		Lease: testRootLease{},
		Persist: func(context.Context, *native.ReadView) error {
			once.Do(func() { close(entered) })
			<-release
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = owner.Close() })
	return &shard{
		nativeStore: &nativePropertyStore{owner: owner, wait: true},
		id:          0,
		repairState: &repair{},
		l:           logger.GetLogger("db_lifetime_test"),
	}
}

func TestDeleteAndDropCoordinateShardLifetime(t *testing.T) {
	gate := newBlockGate()
	sd := newRealMemoryShard(t)
	sd.testBeforeNativeRead = gate.hook
	shardList := []*shard{sd}
	group := &groupShards{location: t.TempDir()}
	group.shards.Store(&shardList)
	var db database
	db.lfs = fs.NewLocalFileSystem()
	db.groups.Store("g", group)
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- db.Delete(context.Background(), [][]byte{[]byte("id")}, time.Now())
	}()
	<-gate.entered
	dropDone := make(chan error, 1)
	go func() { dropDone <- db.Drop("g") }()
	select {
	case <-dropDone:
		t.Fatal("Drop closed a shard while Delete was using it")
	case <-time.After(10 * time.Millisecond):
	}
	close(gate.release)
	require.NoError(t, <-deleteDone)
	require.NoError(t, <-dropDone)
}

func TestUpdateAndDropCoordinateShardLifetime(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	sd := newBlockingWriteShard(t, entered, release)
	shardList := []*shard{sd}
	group := &groupShards{location: t.TempDir()}
	group.shards.Store(&shardList)
	var db database
	db.lfs = fs.NewLocalFileSystem()
	db.groups.Store("g", group)
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- db.Update(context.Background(), 0, []byte("id"), &propertyv1.Property{
			Metadata: &commonv1.Metadata{Group: "g"},
			Id:       "id",
		})
	}()
	<-entered
	dropDone := make(chan error, 1)
	go func() { dropDone <- db.Drop("g") }()
	select {
	case <-dropDone:
		t.Fatal("Drop closed a shard while Update was using it")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-updateDone)
	require.NoError(t, <-dropDone)

	// Use-after-close: the shard's own owner is now closed (Drop's s.close()
	// called nativeStore.close(), which calls owner.Close()); a fresh
	// attempt to use the SAME store must fail cleanly, not panic or corrupt
	// state, confirming Drop's close really took effect rather than merely
	// returning.
	batchErr := sd.nativeStore.batch(context.Background(), index.Documents{{EntityValues: []byte("after-close")}}, nil)
	require.ErrorIs(t, batchErr, native.ErrOwnerClosed, "the shard's store must refuse use after Drop closed it")
}

func TestRepairAndDropCoordinateShardLifetime(t *testing.T) {
	gate := newBlockGate()
	sd := newRealMemoryShard(t)
	sd.testBeforeNativeRead = gate.hook
	shardList := []*shard{sd}
	group := &groupShards{location: t.TempDir()}
	group.shards.Store(&shardList)
	var db database
	db.lfs = fs.NewLocalFileSystem()
	db.groups.Store("g", group)
	repairDone := make(chan error, 1)
	go func() {
		repairDone <- db.Repair(context.Background(), []byte("id"), 0, &propertyv1.Property{
			Metadata: &commonv1.Metadata{Group: "g"},
			Id:       "id",
		}, 0)
	}()
	<-gate.entered
	dropDone := make(chan error, 1)
	go func() { dropDone <- db.Drop("g") }()
	select {
	case <-dropDone:
		t.Fatal("Drop closed a shard while Repair was using it")
	case <-time.After(10 * time.Millisecond):
	}
	close(gate.release)
	require.NoError(t, <-repairDone)
	require.NoError(t, <-dropDone)
}

func TestQueryAndDropCoordinateShardLifetime(t *testing.T) {
	gate := newBlockGate()
	sd := newRealMemoryShard(t)
	sd.testBeforeNativeRead = gate.hook
	shardList := []*shard{sd}
	group := &groupShards{location: t.TempDir()}
	group.shards.Store(&shardList)
	var db database
	db.lfs = fs.NewLocalFileSystem()
	db.groups.Store("g", group)
	queryDone := make(chan error, 1)
	go func() {
		_, err := db.Query(context.Background(), &propertyv1.QueryRequest{Groups: []string{"g"}})
		queryDone <- err
	}()
	<-gate.entered
	dropDone := make(chan error, 1)
	go func() { dropDone <- db.Drop("g") }()
	select {
	case <-dropDone:
		t.Fatal("Drop closed a shard while Query was using it")
	case <-time.After(10 * time.Millisecond):
	}
	close(gate.release)
	require.NoError(t, <-queryDone)
	require.NoError(t, <-dropDone)
}
