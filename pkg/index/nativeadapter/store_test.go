// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
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

package nativeadapter

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

func TestStoreBatchWaitsForPersistenceBeforeReturning(t *testing.T) {
	root := t.TempDir()
	lock, err := fs.NewLocalFileSystem().CreateLockFile(filepath.Join(root, "lock"), 0o600)
	require.NoError(t, err)
	lease, err := native.NewFileRootLease(lock, root)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lease.Revoke())
		require.NoError(t, lock.Close())
	}()

	store, err := NewStore(filepath.Join(root, "index"), lease, SearcherOptions{MaxTerms: 32})
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	var callbackCount atomic.Int32
	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	var releaseCallbackOnce sync.Once
	defer releaseCallbackOnce.Do(func() { close(releaseCallback) })
	callbackErr := make(chan error, 1)
	done := make(chan error, 1)
	run.Go(context.Background(), "nativeadapter.test.callback", nil, func(testCtx context.Context) {
		field := index.NewStringField(index.FieldKey{IndexRuleID: 1, SeriesID: 1}, "value")
		done <- store.Batch(testCtx, index.Batch{
			Documents: []index.Document{{DocID: 7, Timestamp: 11, Fields: []index.Field{field}}},
			PersistentCallback: func(err error) {
				callbackCount.Add(1)
				close(callbackEntered)
				<-releaseCallback
				callbackErr <- err
			},
		})
	})

	<-callbackEntered
	var batchErr error
	batchCompleted := false
	select {
	case batchErr = <-done:
		batchCompleted = true
	case <-time.After(time.Second):
	}
	releaseCallbackOnce.Do(func() { close(releaseCallback) })
	if !batchCompleted {
		batchErr = <-done
	}
	require.True(t, batchCompleted, "synchronous Batch waited for caller callback")
	require.NoError(t, batchErr)
	require.NoError(t, <-callbackErr)
	require.Equal(t, int32(1), callbackCount.Load())
}

func TestStoreSynchronousBatchWaitsForPersistence(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	owner, err := native.NewOwner(native.OwnerOptions{
		Lease: testLease{},
		Persist: func(context.Context, *native.ReadView) error {
			close(started)
			<-release
			return nil
		},
	})
	require.NoError(t, err)
	store := &Store{Owner: owner, options: SearcherOptions{MaxTerms: 32}}
	defer func() { require.NoError(t, owner.Close()) }()
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })

	field := index.NewStringField(index.FieldKey{IndexRuleID: 1, SeriesID: 1}, "value")
	batchDone := make(chan error, 1)
	run.Go(context.Background(), "nativeadapter.test.persist", nil, func(testCtx context.Context) {
		batchDone <- store.Batch(testCtx, index.Batch{
			Documents: []index.Document{{DocID: 7, Fields: []index.Field{field}}},
		})
	})
	<-started
	select {
	case err := <-batchDone:
		t.Fatalf("Batch returned before persistence completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-batchDone)
}

func TestStoreSynchronousBatchReturnsPersistenceError(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	wantErr := errors.New("persist failed")
	owner, err := native.NewOwner(native.OwnerOptions{
		Lease: testLease{},
		Persist: func(context.Context, *native.ReadView) error {
			close(started)
			<-release
			return wantErr
		},
	})
	require.NoError(t, err)
	store := &Store{Owner: owner, options: SearcherOptions{MaxTerms: 32}}
	defer func() { require.NoError(t, owner.Close()) }()
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })

	batchDone := make(chan error, 1)
	run.Go(context.Background(), "nativeadapter.test.persist-error", nil, func(testCtx context.Context) {
		batchDone <- store.Batch(testCtx, index.Batch{Documents: []index.Document{{DocID: 8}}})
	})
	<-started
	select {
	case err := <-batchDone:
		t.Fatalf("Batch returned before persistence completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	require.ErrorIs(t, <-batchDone, wantErr)
}

func TestStoreAsyncPersistenceInvokesCallbackOnce(t *testing.T) {
	root := t.TempDir()
	lock, err := fs.NewLocalFileSystem().CreateLockFile(filepath.Join(root, "lock"), 0o600)
	require.NoError(t, err)
	lease, err := native.NewFileRootLease(lock, root)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lease.Revoke())
		require.NoError(t, lock.Close())
	}()
	store, err := NewStore(filepath.Join(root, "index"), lease, SearcherOptions{MaxTerms: 32, AsyncPersistence: true})
	require.NoError(t, err)
	var callbacks atomic.Int32
	callbackDone := make(chan struct{})
	field := index.NewStringField(index.FieldKey{IndexRuleID: 1, SeriesID: 1}, "value")
	require.NoError(t, store.Batch(context.Background(), index.Batch{
		Documents:          []index.Document{{DocID: 7, Fields: []index.Field{field}}},
		PersistentCallback: func(error) { callbacks.Add(1); close(callbackDone) },
	}))
	require.NoError(t, store.Close())
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("asynchronous persistence callback was not delivered")
	}
	require.Equal(t, int32(1), callbacks.Load())
}
