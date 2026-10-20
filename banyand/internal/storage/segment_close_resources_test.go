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

package storage

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// TestCloseResourcesLocked_SeriesIndexCloseErrorDoesNotPanic pins the NIDX-03
// §8/§11 fix: a series-index close error (here, the native owner's
// background persistence hitting an induced, always-failing PersistFunc test
// seam) must be logged and recorded on the segment instead of panicking the
// node. The segment's resources are still released (index set to nil) so it
// remains reopenable.
func TestCloseResourcesLocked_SeriesIndexCloseErrorDoesNotPanic(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "error"}))

	owner, err := native.NewOwner(native.OwnerOptions{
		Lease: &testRootLease{},
		Persist: func(context.Context, *native.ReadView) error {
			return errors.New("induced persistence failure")
		},
	})
	require.NoError(t, err)

	// Admit a document with no PersistentCallback: drainPersistence (run by
	// either the background worker or Close's own final flush) records the
	// induced failure into o.persistErr regardless of which one gets to it
	// first, so the eventual Owner.Close() error is deterministic.
	require.NoError(t, owner.Batch(context.Background(), native.Batch{
		Documents: []native.Document{{Identifier: []byte("series-a")}},
	}))

	si := &seriesIndex{owner: owner, l: logger.GetLogger("test")}
	seg := &segment[*MockTSTable, any]{index: si, l: logger.GetLogger("test")}

	require.NotPanics(t, func() {
		seg.mu.Lock()
		seg.closeResourcesLocked()
		seg.mu.Unlock()
	})
	require.True(t, seg.CloseFailed(), "a series-index close error must mark the segment failed instead of panicking")
	require.Nil(t, seg.index, "resources are still released despite the close error")

	// Idempotent: a second call (as closeIfIdle/performDelete may perform)
	// must not panic either.
	require.NotPanics(t, func() {
		seg.mu.Lock()
		seg.closeResourcesLocked()
		seg.mu.Unlock()
	})
}

// TestSegment_Initialize_ClearsCloseFailedOnSuccessfulReopen is the L1
// CloseFailed-lifecycle decision: closeFailed records only the most recent
// closeResourcesLocked call, and this segment's directory and on-disk data
// are exactly what loadShards and the series index just validated by
// opening them again. A successful reopen must therefore clear a stale
// closeFailed from an earlier close, rather than leaving a segment that
// reopens and operates normally forever reporting CloseFailed()==true from
// one transient persistence fault.
func TestSegment_Initialize_ClearsCloseFailedOnSuccessfulReopen(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "error"}))
	dir, fn := setUp(require.New(t))
	defer fn()

	seg := &segment[*MockTSTable, any]{
		location: dir,
		l:        logger.GetLogger("test"),
		tsdbOpts: &TSDBOpts[*MockTSTable, any]{RootLease: &testRootLease{}},
	}
	seg.closeFailed.Store(true) // simulate a previous close failure.

	seg.mu.Lock()
	initErr := seg.initialize(context.Background())
	seg.mu.Unlock()
	require.NoError(t, initErr)
	require.False(t, seg.CloseFailed(), "a successful reopen must clear a stale closeFailed from an earlier close")

	seg.mu.Lock()
	seg.closeResourcesLocked()
	seg.mu.Unlock()
}
