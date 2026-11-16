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
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// A query snapshots the segment list and reopens the segments one by one
// after releasing the controller lock. A group being closed or dropped (schema
// deletion, node shutdown) can therefore run between the snapshot and the
// reopen of a later segment. Such a query must fail instead of reopening a
// segment the shutdown already released: nothing would ever close it again,
// and for Drop the reopen would recreate files under the removed directory.

// runShutdownDuringQueryReopen idle-closes both segments, parks a query over
// both days inside the reopen of the newer one (selectSegments visits newest
// first), runs shutdown until it has released the older segment and is
// waiting for the newer one, then lets the query continue. It returns the
// older segment.
func runShutdownDuringQueryReopen(t *testing.T, shutdown func(db *database[*MockTSTable, any]) error) *segment[*MockTSTable, any] {
	t.Helper()
	f := newColdReopenFixture(t)
	hot := f.segmentAt(t, f.hotDay)
	hot.lastAccessed.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	require.Equal(t, 1, f.db.segmentController.closeIdleSegments())
	cold := f.segmentAt(t, f.coldDay)

	f.holdReopen(t, f.hotDay)
	type queryResult struct {
		err   error
		panic any
		n     int
	}
	query := make(chan queryResult, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				query <- queryResult{panic: p}
			}
		}()
		tr := timestamp.NewInclusiveTimeRange(f.coldDay.Add(time.Hour), f.hotDay.Add(23*time.Hour))
		ss, err := f.db.SelectSegments(tr, true)
		for _, s := range ss {
			s.DecRef()
		}
		query <- queryResult{err: err, n: len(ss)}
	}()
	f.gate.waitEntered(t)

	closed := make(chan error, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				closed <- fmt.Errorf("shutdown panicked: %v", p)
			}
		}()
		closed <- shutdown(f.db)
	}()
	require.Eventually(t, func() bool {
		cold.mu.RLock()
		defer cold.mu.RUnlock()
		return cold.shutdown
	}, 5*time.Second, time.Millisecond, "shutdown must release the older segment first")
	f.gate.unblock()

	requireFinished(t, closed, "shutdown")
	var r queryResult
	select {
	case r = <-query:
	case <-time.After(5 * time.Second):
		t.Fatal("the query did not finish")
	}
	require.Nil(t, r.panic, "the query panicked")
	require.ErrorIs(t, r.err, errSegmentShutdown, "the query must fail instead of returning %d segments", r.n)
	require.False(t, segmentIsOpen(cold), "the query must not reopen a segment released by shutdown")
	require.False(t, segmentIsOpen(hot), "shutdown must close the segment the query reopened before it")
	require.Zero(t, f.coldOpens.Load(), "the older segment's shard must not be reopened")
	require.Zero(t, atomic.LoadInt32(&cold.refCount))
	return cold
}

func segmentIsOpen(s *segment[*MockTSTable, any]) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index != nil
}

func TestSelectSegments_NoReopenAfterDatabaseClose(t *testing.T) {
	cold := runShutdownDuringQueryReopen(t, func(db *database[*MockTSTable, any]) error { return db.Close() })
	require.DirExists(t, cold.location, "Close keeps the data on disk")
}

func TestSelectSegments_NoReopenAfterDatabaseDrop(t *testing.T) {
	cold := runShutdownDuringQueryReopen(t, func(db *database[*MockTSTable, any]) error { return db.Drop() })
	require.NoDirExists(t, cold.location, "a late reopen must not recreate the dropped segment's directory")
}

// A write or a rotation pass that is already past the database's closed check
// when the group shuts down reaches segmentController.create afterwards. It must
// fail instead of recreating a segment over a directory that close kept (which
// panicked with "directory is exist") or opening a new one nothing would close.
func TestCreateSegment_RefusedAfterControllerClose(t *testing.T) {
	f := newColdReopenFixture(t)
	sc := f.db.segmentController
	require.NoError(t, f.db.Close())

	_, err := sc.createSegment(f.hotDay.Add(13 * time.Hour))
	require.ErrorIs(t, err, errSegmentShutdown, "a write into an existing day must fail after close")

	newDay := f.hotDay.AddDate(0, 0, 1)
	_, err = sc.createSegment(newDay.Add(time.Hour))
	require.ErrorIs(t, err, errSegmentShutdown, "a write into a new day must fail after close")
	require.NoDirExists(t, filepath.Join(sc.location, fmt.Sprintf(segTemplate, sc.format(newDay))),
		"no segment directory may be created after close")
	require.Empty(t, sc.copySegments())
}
