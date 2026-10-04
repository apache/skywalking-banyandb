// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// Apache Software Foundation (ASF) licenses this file to you under the
// Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
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
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// transitionBlockingStore makes an admitted update observable without relying
// on a timer. Its persistent callback is delivered only after release, which
// lets the transition test prove that SwitchIndexWriter waits for the update.
type transitionBlockingStore struct {
	index.SeriesStore
	entered   chan struct{}
	release   chan struct{}
	closed    chan struct{}
	enterOnce sync.Once
	closeOnce sync.Once
}

func (s *transitionBlockingStore) UpdateSeriesBatch(batch index.Batch) error {
	s.enterOnce.Do(func() { close(s.entered) })
	<-s.release
	if batch.PersistentCallback != nil {
		batch.PersistentCallback(nil)
	}
	return nil
}

func (s *transitionBlockingStore) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func TestSwitchIndexWriterStopsAdmissionDrainsUpdatesAndRetainsLease(t *testing.T) {
	location, cleanup, err := test.NewSpace()
	require.NoError(t, err)
	defer cleanup()

	ctx := context.Background()
	dbAPI, err := OpenDB(ctx, Config{
		Location:         location,
		MetricsScopeName: "property_writer_transition_test",
		FlushInterval:    time.Second,
		Index:            IndexConfig{WaitForPersistence: true},
	}, observability.BypassRegistry, fs.NewLocalFileSystem())
	require.NoError(t, err)

	seed := generateProperty("transition-seed", time.Now().UnixNano(), 1)
	require.NoError(t, dbAPI.Update(ctx, 0, GetPropertyID(seed), seed))
	db := dbAPI.(*database)
	shard, ok := db.getShard(testPropertyGroup, 0)
	require.True(t, ok)
	// Close the real writer after its seed is durable, then replace it with a
	// barrier-backed store. SwitchIndexWriter must still reopen the persisted
	// shard after the barrier is released.
	require.NoError(t, shard.store.Close())
	blocking := &transitionBlockingStore{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
	shard.store = blocking

	updateDone := make(chan error, 1)
	update := generateProperty("transition-inflight", time.Now().UnixNano(), 2)
	go func() {
		updateDone <- db.Update(ctx, 0, GetPropertyID(update), update)
	}()
	<-blocking.entered

	switchDone := make(chan error, 1)
	go func() { switchDone <- db.SwitchIndexWriter(ctx, true) }()
	// The transition flag is set before it waits for the admitted update's
	// read lock. A scheduler or timer is not needed to synchronize this test.
	for !db.transition.Load() {
		runtime.Gosched()
	}
	require.Error(t, db.Update(ctx, 0, GetPropertyID(generateProperty("rejected", time.Now().UnixNano(), 3)),
		generateProperty("rejected", time.Now().UnixNano(), 3)))

	_, contentionErr := OpenDB(ctx, Config{
		Location:         location,
		MetricsScopeName: "property_writer_transition_competing_test",
		Index:            IndexConfig{NativeWriter: true},
	}, observability.BypassRegistry, fs.NewLocalFileSystem())
	require.Error(t, contentionErr, "the root lease must remain held during transition")

	close(blocking.release)
	require.NoError(t, <-updateDone)
	require.NoError(t, <-switchDone)
	select {
	case <-blocking.closed:
	default:
		t.Fatal("transition returned before closing the old writer")
	}
	require.NoError(t, db.Close())
}

func TestSwitchIndexWriterFailureCleansUpBeforeReleasingLease(t *testing.T) {
	location, cleanup, err := test.NewSpace()
	require.NoError(t, err)
	defer cleanup()

	ctx := context.Background()
	dbAPI, err := OpenDB(ctx, Config{
		Location:         location,
		MetricsScopeName: "property_writer_reopen_failure_test",
		FlushInterval:    time.Second,
		Index:            IndexConfig{WaitForPersistence: true},
	}, observability.BypassRegistry, fs.NewLocalFileSystem())
	require.NoError(t, err)
	seed := generateProperty("reopen-failure-seed", time.Now().UnixNano(), 1)
	require.NoError(t, dbAPI.Update(ctx, 0, GetPropertyID(seed), seed))

	groupDir := filepath.Join(location, testPropertyGroup)
	bogusPath := filepath.Join(groupDir, "shard-bogus")
	require.NoError(t, os.MkdirAll(bogusPath, 0o755))

	db := dbAPI.(*database)
	switchErr := db.SwitchIndexWriter(ctx, true)
	require.Error(t, switchErr, "a malformed later shard must fail the reopen")
	require.True(t, db.closed.Load(), "a failed transition must fail closed")

	// The malformed directory is removed only after SwitchIndexWriter returns;
	// a successful opener now proves all partially reopened writers and the
	// native owner were cleaned up before the root lease was released.
	require.NoError(t, os.RemoveAll(bogusPath))
	reopened, reopenErr := OpenDB(ctx, Config{
		Location:         location,
		MetricsScopeName: "property_writer_reopen_after_failure_test",
		Index:            IndexConfig{NativeWriter: true},
	}, observability.BypassRegistry, fs.NewLocalFileSystem())
	require.NoError(t, reopenErr)
	require.NoError(t, reopened.Close())
}
