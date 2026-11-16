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
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// Randomized concurrent stress of the segment lifecycle: writers (including
// late writes into idle-closed days), queriers, rotation ticks, the idle
// reclaimer and forced cleanup all race on one real database, and every
// holder checks that its segment stays open and referenced while held.
//
// Stats peeks (SelectSegments(_, false) + DecRef) always run.
// SEGMENT_STRESS_SECONDS overrides the default ~1s run. SEGMENT_STRESS_FULL=1
// additionally runs scheduled retention inside rotation ticks and an explicit
// retention remove() with a moving deadline in place of forced cleanup.

type stressSeg = segment[*MockTSTable, any]

type stressViolations struct {
	msgs map[string]int
	ex   map[string]string
	mu   sync.Mutex
}

func (v *stressViolations) add(kind, detail string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.msgs == nil {
		v.msgs = map[string]int{}
		v.ex = map[string]string{}
	}
	v.msgs[kind]++
	if _, ok := v.ex[kind]; !ok {
		v.ex[kind] = detail
	}
}

func stressDuration() time.Duration {
	if s := os.Getenv("SEGMENT_STRESS_SECONDS"); s != "" {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return time.Duration(f * float64(time.Second))
		}
	}
	return time.Second
}

//nolint:gocyclo // one randomized run drives every lifecycle actor and checks the shared invariants.
func TestSegmentLifecycle_Stress(t *testing.T) {
	dir, defFn := test.Space(require.New(t))
	defer defFn()

	base := time.Date(2024, 5, 1, 0, 0, 0, 0, time.Local)
	const days = 12
	basicMode := os.Getenv("SEGMENT_STRESS_FULL") != "1"

	opts := TSDBOpts[*MockTSTable, any]{
		RootLease:       staticTestLease{},
		Location:        dir,
		SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
		// Keep the clock-based query filter out of the way; retention is
		// driven explicitly with a moving deadline (and by rotation ticks).
		TTL:                IntervalRule{Unit: DAY, Num: 4},
		ShardNum:           2,
		SegmentIdleTimeout: time.Millisecond,
		TSTableCreator:     MockTSTableCreator,
		DisableRetention:   basicMode,
	}
	ctx := context.Background()
	mc := timestamp.NewMockClock()
	mc.Set(base)
	ctx = timestamp.SetClock(ctx, mc)
	tsdb, err := OpenTSDB(ctx, opts, NewServiceCache(), group)
	require.NoError(t, err)
	db := tsdb.(*database[*MockTSTable, any])
	sc := db.segmentController
	// Close before the deferred directory cleanup.
	defer tsdb.Close()

	var (
		progress   atomic.Int64 // current "today" offset in minutes from base
		seen       sync.Map     // *stressSeg -> struct{}
		viol       stressViolations
		unexpected sync.Map // error string -> count
		counts     struct {
			writes, writeErrs, queries, querySegs, peeks, ticks, closes, removed atomic.Int64
		}
	)
	totalMinutes := int64((days - 1) * 24 * 60)
	now := func() time.Time { return base.Add(time.Duration(progress.Load()) * time.Minute) }

	expected := func(err error) bool {
		return errors.Is(err, ErrSegmentClosed) || errors.Is(err, ErrSegmentDeleting) || errors.Is(err, ErrInvalidSegmentTimestamp)
	}
	recordErr := func(op string, err error) {
		if expected(err) {
			return
		}
		key := op + ": " + err.Error()
		v, _ := unexpected.LoadOrStore(key, new(atomic.Int64))
		v.(*atomic.Int64).Add(1)
	}

	checkHeld := func(who string, s *stressSeg) {
		seen.Store(s, struct{}{})
		if rc := atomic.LoadInt32(&s.refCount); rc <= 0 {
			viol.add(who+": refCount<=0 while held", fmt.Sprintf("%s rc=%d", s, rc))
		}
		s.mu.RLock()
		idx := s.index
		s.mu.RUnlock()
		if idx == nil {
			viol.add(who+": index nil while held", s.String())
		}
		if s.deleted.Load() {
			viol.add(who+": directory deleted while held", s.String())
		}
	}

	runFor := stressDuration()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var panics atomic.Int64
	goLoop := func(seed int64, fn func(r *rand.Rand)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if p := recover(); p != nil {
					panics.Add(1)
					buf := make([]byte, 8192)
					n := runtime.Stack(buf, false)
					viol.add("panic", fmt.Sprintf("%v\n%s", p, buf[:n]))
				}
			}()
			r := rand.New(rand.NewSource(seed)) //nolint:gosec // seeded so a failing schedule can be replayed.
			for {
				select {
				case <-stop:
					return
				default:
				}
				fn(r)
			}
		}()
	}
	seed := time.Now().UnixNano()
	t.Logf("seed=%d duration=%s basicMode=%v", seed, runFor, basicMode)

	// Clock driver: advance "today" across the whole window.
	goLoop(seed, func(_ *rand.Rand) {
		time.Sleep(time.Millisecond)
		step := totalMinutes / (int64(runFor/time.Millisecond) + 1)
		if step < 1 {
			step = 1
		}
		if p := progress.Load() + step; p <= totalMinutes {
			progress.Store(p)
		}
	})

	// Writers.
	for i := 0; i < 6; i++ {
		goLoop(seed+int64(100+i), func(r *rand.Rand) {
			ts := now()
			if r.Intn(10) < 3 {
				ts = ts.Add(-time.Duration(r.Intn(5*24)) * time.Hour) // late write
			}
			ts = ts.Add(time.Duration(r.Intn(60)) * time.Minute)
			seg, err := db.CreateSegmentIfNotExist(ts)
			counts.writes.Add(1)
			if err != nil {
				counts.writeErrs.Add(1)
				recordErr("write", err)
				return
			}
			s := seg.(*stressSeg)
			checkHeld("writer", s)
			if _, err = seg.CreateTSTableIfNotExist(common.ShardID(r.Intn(2))); err != nil {
				recordErr("createTable", err)
			}
			_ = seg.IndexDB()
			runtime.Gosched()
			checkHeld("writer", s)
			seg.DecRef()
			db.Tick(ts.UnixNano())
		})
	}

	// Queriers.
	for i := 0; i < 4; i++ {
		goLoop(seed+int64(200+i), func(r *rand.Rand) {
			end := now().Add(time.Duration(r.Intn(24)) * time.Hour)
			start := end.Add(-time.Duration(1+r.Intn(6*24)) * time.Hour)
			ss, err := db.SelectSegments(timestamp.NewInclusiveTimeRange(start, end), true)
			counts.queries.Add(1)
			if err != nil {
				recordErr("query", err)
				return
			}
			counts.querySegs.Add(int64(len(ss)))
			for _, seg := range ss {
				s := seg.(*stressSeg)
				checkHeld("querier", s)
				if seg.IndexDB() == nil {
					viol.add("querier: IndexDB()==nil while held", s.String())
				}
				_, _ = seg.Tables()
			}
			time.Sleep(time.Duration(r.Intn(200)) * time.Microsecond)
			for _, seg := range ss {
				checkHeld("querier", seg.(*stressSeg))
				seg.DecRef()
			}
		})
	}

	// Stats peekers: their DecRef must never release a reference a writer or
	// querier holds.
	for i := 0; i < 2; i++ {
		goLoop(seed+int64(300+i), func(r *rand.Rand) {
			end := now().Add(24 * time.Hour)
			start := end.Add(-time.Duration(1+r.Intn(8*24)) * time.Hour)
			tr := timestamp.NewInclusiveTimeRange(start, end)
			ss, err := db.SelectSegments(tr, false)
			if err != nil {
				recordErr("peek", err)
				return
			}
			for _, seg := range ss {
				_ = seg.GetTimeRange()
				seg.DecRef()
			}
			// PeekSegments is not exercised: it lists each segment's directory
			// with walkDir, which panics when retention removes the directory
			// between the snapshot and the listing (a separate, known issue).
			counts.peeks.Add(1)
			time.Sleep(50 * time.Microsecond)
		})
	}

	// Rotation ticks (direct sends; Tick is throttled to one per 10 minutes).
	goLoop(seed+400, func(r *rand.Rand) {
		ts := now().Add(time.Duration(r.Intn(24*60)) * time.Minute)
		select {
		case db.tsEventCh <- ts.UnixNano():
			counts.ticks.Add(1)
		default:
		}
		time.Sleep(200 * time.Microsecond)
	})

	// Idle reclaimer.
	goLoop(seed+500, func(r *rand.Rand) {
		for _, s := range sc.copySegments() {
			if r.Intn(2) == 0 {
				s.lastAccessed.Store(time.Now().Add(-time.Hour).UnixNano())
			}
		}
		counts.closes.Add(int64(sc.closeIdleSegments()))
		time.Sleep(100 * time.Microsecond)
	})

	// Retention with a moving deadline: everything older than today-3d.
	if !basicMode {
		goLoop(seed+600, func(r *rand.Rand) {
			before := len(sc.copySegments())
			deadline := now().Add(-time.Duration(3*24+r.Intn(24)) * time.Hour)
			// Serialize with the scheduled retention run like production does.
			db.retentionGate <- struct{}{}
			if _, err := sc.remove(context.Background(), deadline); err != nil {
				recordErr("remove", err)
			}
			<-db.retentionGate
			if d := before - len(sc.copySegments()); d > 0 {
				counts.removed.Add(int64(d))
			}
			time.Sleep(time.Millisecond)
		})
	} else {
		goLoop(seed+600, func(_ *rand.Rand) {
			ss := sc.copySegments()
			if len(ss) > 5 && ss[0].End.Before(now().Add(-3*24*time.Hour)) {
				if ok, _ := db.DeleteOldestSegment(); ok {
					counts.removed.Add(1)
				}
			}
			time.Sleep(time.Millisecond)
		})
	}

	// refCount sampler.
	goLoop(seed+700, func(_ *rand.Rand) {
		for _, s := range sc.copySegments() {
			if rc := atomic.LoadInt32(&s.refCount); rc < 0 {
				viol.add("negative refCount", fmt.Sprintf("%s rc=%d", s, rc))
			}
		}
		time.Sleep(100 * time.Microsecond)
	})

	time.Sleep(runFor)
	close(stop)
	wg.Wait()
	// Let the rotation task drain.
	require.Eventually(t, func() bool { return !db.rotationProcessOn.Load() }, 10*time.Second, time.Millisecond)

	// Final invariants.
	live := map[string]*stressSeg{}
	for _, s := range sc.copySegments() {
		seen.Store(s, struct{}{})
		live[s.location] = s
		if rc := atomic.LoadInt32(&s.refCount); rc != 0 {
			viol.add("leaked reference on live segment", fmt.Sprintf("%s rc=%d", s, rc))
		}
	}
	seen.Range(func(k, _ any) bool {
		s := k.(*stressSeg)
		rc := atomic.LoadInt32(&s.refCount)
		if rc < 0 {
			viol.add("negative refCount (final)", fmt.Sprintf("%s rc=%d", s, rc))
		}
		if _, isLive := live[s.location]; isLive && live[s.location] == s {
			return true
		}
		// Removed from the list.
		if atomic.LoadUint32(&s.mustBeDeleted) == 0 {
			viol.add("segment dropped from list without delete flag", s.String())
			return true
		}
		if !s.deleted.Load() {
			viol.add("removed segment never deleted (deferred delete lost)", fmt.Sprintf("%s rc=%d", s, rc))
			return true
		}
		if rc != 0 {
			viol.add("deleted segment still referenced", fmt.Sprintf("%s rc=%d", s, rc))
		}
		if _, err := os.Stat(s.location); err == nil {
			if _, owned := live[s.location]; !owned {
				viol.add("deleted segment directory recreated (no live owner)", s.location)
			}
		}
		return true
	})

	t.Logf("writes=%d writeErrs=%d queries=%d querySegs=%d peeks=%d ticks=%d idleCloses=%d removed=%d live=%d",
		counts.writes.Load(), counts.writeErrs.Load(), counts.queries.Load(), counts.querySegs.Load(),
		counts.peeks.Load(), counts.ticks.Load(), counts.closes.Load(), counts.removed.Load(), len(live))
	unexpected.Range(func(k, v any) bool {
		t.Errorf("unexpected error x%d: %s", v.(*atomic.Int64).Load(), k)
		return true
	})
	viol.mu.Lock()
	defer viol.mu.Unlock()
	for kind, n := range viol.msgs {
		t.Errorf("VIOLATION x%d: %s; first: %s", n, kind, viol.ex[kind])
	}
}
