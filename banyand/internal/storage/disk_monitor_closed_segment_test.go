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
	"time"

	"github.com/stretchr/testify/require"

	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
)

// On a hot data node the oldest segments of a group are almost always
// idle-closed: nothing reads or writes them. These tests cover the maintenance
// paths that must still reclaim their disk space -- the disk monitor's forced
// cleanup and TTL retention -- without reopening them.

type closedSegController = segmentController[mockTSTable, mockTSTableOpener]

// controllerRetentionService adapts real segment controllers (one per group)
// to RetentionService so the DiskMonitor victim-selection logic runs against
// real peekOldestSegmentEndTime / removeOldest.
type controllerRetentionService struct {
	byGroup map[string]*closedSegController
	groups  []resourceSchema.Group
}

func (c *controllerRetentionService) GetDataPath() string                     { return "" }
func (c *controllerRetentionService) GetSnapshotDir() string                  { return "" }
func (c *controllerRetentionService) LoadAllGroups() []resourceSchema.Group   { return c.groups }
func (c *controllerRetentionService) CleanupOldSnapshots(time.Duration) error { return nil }
func (c *controllerRetentionService) GetServiceName() string                  { return "closed-segment" }

func (c *controllerRetentionService) PeekOldestSegmentEndTimeInGroup(g string) (time.Time, bool) {
	return c.byGroup[g].peekOldestSegmentEndTime()
}

func (c *controllerRetentionService) DeleteOldestSegmentInGroup(g string) (bool, error) {
	return c.byGroup[g].removeOldest()
}

// idleCloseSegment force-closes a dormant segment the way the idle reclaimer does.
func idleCloseSegment(t *testing.T, s *segment[mockTSTable, mockTSTableOpener]) {
	t.Helper()
	s.lastAccessed.Store(time.Now().Add(-time.Hour).UnixNano())
	require.True(t, s.closeIfIdle(time.Now().UnixNano()))
	require.Nil(t, s.index)
}

func newClosedSegController(t *testing.T) (*closedSegController, context.Context, string) {
	t.Helper()
	tempDir, cleanup := setupTestEnvironment(t)
	t.Cleanup(cleanup)
	sc, ctx := newReclaimTestController(t, tempDir, time.Hour)
	t.Cleanup(sc.close)
	return sc, ctx, tempDir
}

// End to end through DiskMonitor.deleteOldestSegment: a single group whose
// oldest segment is closed must still have that segment evicted under disk
// pressure.
func TestDiskMonitor_EvictsClosedOldestSegment(t *testing.T) {
	sc, ctx, dir := newClosedSegController(t)
	day := reclaimTestDay(t)
	oldest := openReclaimTestSegment(ctx, t, sc, dir, day.Add(-48*time.Hour))
	openReclaimTestSegment(ctx, t, sc, dir, day.Add(-24*time.Hour))
	openReclaimTestSegment(ctx, t, sc, dir, day)
	idleCloseSegment(t, oldest)

	svc := &controllerRetentionService{
		byGroup: map[string]*closedSegController{"g": sc},
		groups:  []resourceSchema.Group{createMockGroup("g")},
	}
	dm := NewDiskMonitor(svc, RetentionConfig{}, createMockMetricsRegistry())
	require.True(t, dm.deleteOldestSegment("closed-segment"), "forced cleanup must evict the closed oldest segment")
	require.Len(t, sc.lst, 2)
	require.NoDirExists(t, oldest.location)
}

// Victim selection across groups: group "old" holds the globally oldest data
// but its oldest segment is closed; group "new" has a newer, open oldest
// segment. The monitor must pick "old" rather than evict younger data.
func TestDiskMonitor_VictimSelectionIgnoresClosedState(t *testing.T) {
	scOld, ctxOld, dirOld := newClosedSegController(t)
	scNew, ctxNew, dirNew := newClosedSegController(t)
	day := reclaimTestDay(t)

	oldOldest := openReclaimTestSegment(ctxOld, t, scOld, dirOld, day.Add(-96*time.Hour))
	openReclaimTestSegment(ctxOld, t, scOld, dirOld, day)
	idleCloseSegment(t, oldOldest)

	openReclaimTestSegment(ctxNew, t, scNew, dirNew, day.Add(-24*time.Hour))
	openReclaimTestSegment(ctxNew, t, scNew, dirNew, day)

	svc := &controllerRetentionService{
		byGroup: map[string]*closedSegController{"old": scOld, "new": scNew},
		groups:  []resourceSchema.Group{createMockGroup("new"), createMockGroup("old")},
	}
	dm := NewDiskMonitor(svc, RetentionConfig{}, createMockMetricsRegistry())
	require.Equal(t, "old", dm.findGroupWithOldestSegment(svc.groups),
		"the group with the globally oldest segment must be chosen even if that segment is idle-closed")
}

// Forced cleanup removes the oldest segment while a reader still holds it. The
// directory stays, and the time range cannot be recreated, until the reader
// releases it; afterwards a new segment can be created at the same path.
func TestRemoveOldest_HeldOldestBlocksRecreateUntilDeleted(t *testing.T) {
	g := newGatedController(t, 2)
	older := g.segs[0]
	require.NoError(t, older.incRef(g.ctx))

	ok, err := g.sc.removeOldest()
	require.NoError(t, err)
	require.True(t, ok)
	g.sc.RLock()
	tracked := g.sc.deleting[older.location]
	g.sc.RUnlock()
	require.Same(t, older, tracked, "a held removed segment must be tracked until its directory is gone")
	_, err = g.sc.create(g.ctx, older.Start)
	require.ErrorIs(t, err, ErrSegmentDeleting)

	// Keep-one: the remaining segment is never evicted.
	ok, err = g.sc.removeOldest()
	require.NoError(t, err)
	require.False(t, ok)

	older.DecRef()
	require.NoDirExists(t, older.location)
	recreated, err := g.sc.create(g.ctx, older.Start)
	require.NoError(t, err)
	require.NotSame(t, older, recreated)
}

// The delete of a held oldest segment can be triggered twice: by the reader's
// last DecRef and by removeOldest's own delete(). Once the first one removed the
// directory a write may recreate the segment at the same path, so the second
// one must not touch that path again.
func TestRemoveOldest_LateSecondDeleteKeepsRecreatedSegment(t *testing.T) {
	g := newGatedController(t, 2)
	older := g.segs[0]
	require.NoError(t, older.incRef(g.ctx))

	ok, err := g.sc.removeOldest()
	require.NoError(t, err)
	require.True(t, ok)
	older.DecRef() // the reader's release runs the deferred delete
	require.NoDirExists(t, older.location)

	recreated, err := g.sc.create(g.ctx, older.Start)
	require.NoError(t, err)
	require.Equal(t, older.location, recreated.location)

	older.performDelete() // the delete() side of the same race, arriving late
	require.DirExists(t, recreated.location, "a late delete of the old segment removed the recreated segment's directory")
}

// Forced cleanup across groups evicts segments globally oldest first, whether
// they are open or idle-closed, and stops once every group is down to one.
func TestDiskMonitor_EvictsGloballyOldestUntilKeepOne(t *testing.T) {
	day := reclaimTestDay(t)
	groups := []struct {
		name string
		offs []int
	}{{"a", []int{-10, -7, -3, 0}}, {"b", []int{-9, -8, -1, 0}}, {"c", []int{-5, 0}}}
	svc := &controllerRetentionService{byGroup: map[string]*closedSegController{}}
	var all []*segment[mockTSTable, mockTSTableOpener]
	for gi, gr := range groups {
		sc, ctx, _ := newClosedSegController(t)
		svc.byGroup[gr.name] = sc
		svc.groups = append(svc.groups, createMockGroup(gr.name))
		for j, o := range gr.offs {
			s, err := sc.create(ctx, day.Add(time.Duration(o)*24*time.Hour))
			require.NoError(t, err)
			if (gi+j)%2 == 0 {
				idleCloseSegment(t, s)
			}
			all = append(all, s)
		}
	}
	dm := NewDiskMonitor(svc, RetentionConfig{}, createMockMetricsRegistry())
	var order []time.Time
	for n := 0; n < 100; n++ {
		deletedBefore := map[*segment[mockTSTable, mockTSTableOpener]]bool{}
		for _, s := range all {
			deletedBefore[s] = s.deleted.Load()
		}
		if !dm.deleteOldestSegment("test") {
			break
		}
		for _, s := range all {
			if !deletedBefore[s] && s.deleted.Load() {
				order = append(order, s.Start)
			}
		}
	}
	require.Len(t, order, 7)
	for i := 1; i < len(order); i++ {
		require.True(t, order[i-1].Before(order[i]), "segments not evicted globally oldest first: %v", order)
	}
	for name, sc := range svc.byGroup {
		require.Len(t, sc.copySegments(), 1, "group %s must keep one segment", name)
	}
}
