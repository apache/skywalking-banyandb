// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package storage

import (
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const activeSeriesMetric = "banyandb_active_series_test_active_series"

var activeSeriesBase = time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)

// countingGauge counts the calls that reach the underlying gauge.
type countingGauge struct {
	meter.Gauge
	// beforeSet runs before every Set when it is not nil. A test assigns it before any concurrent use.
	beforeSet func()
	sets      atomic.Int64
	deletes   atomic.Int64
}

func (g *countingGauge) Set(value float64, labelValues ...string) {
	g.sets.Add(1)
	if g.beforeSet != nil {
		g.beforeSet()
	}
	g.Gauge.Set(value, labelValues...)
}

func (g *countingGauge) Delete(labelValues ...string) bool {
	g.deletes.Add(1)
	return g.Gauge.Delete(labelValues...)
}

type activeSeriesFactory struct {
	reg   *prometheus.Registry
	gauge *countingGauge
}

func newActiveSeriesFactory() *activeSeriesFactory {
	return &activeSeriesFactory{reg: prometheus.NewRegistry()}
}

func (f *activeSeriesFactory) NewGauge(name string, labelNames ...string) meter.Gauge {
	scope := observability.RootScope.SubScope("active_series_test")
	f.gauge = &countingGauge{Gauge: prom.NewProvider(scope, f.reg).Gauge(name, labelNames...)}
	return f.gauge
}

func (f *activeSeriesFactory) NewCounter(string, ...string) meter.Counter { return nil }
func (f *activeSeriesFactory) NewHistogram(string, meter.Buckets, ...string) meter.Histogram {
	return nil
}
func (f *activeSeriesFactory) Close() {}

// exported returns the gauge value of every shard label.
func (f *activeSeriesFactory) exported(tb testing.TB) map[string]float64 {
	tb.Helper()
	families, err := f.reg.Gather()
	require.NoError(tb, err)
	values := make(map[string]float64)
	for _, family := range families {
		if family.GetName() != activeSeriesMetric {
			continue
		}
		for _, m := range family.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "shard" {
					values[l.GetValue()] = m.GetGauge().GetValue()
				}
			}
		}
	}
	return values
}

type activeSeriesFixture struct {
	clock   timestamp.MockClock
	factory *activeSeriesFactory
	tracker *ActiveSeries
}

// newActiveSeriesFixture creates a tracker whose clock stands at activeSeriesBase plus offset.
func newActiveSeriesFixture(offset time.Duration, shardNum uint32) *activeSeriesFixture {
	clock := timestamp.NewMockClock()
	clock.Set(activeSeriesBase.Add(offset))
	factory := newActiveSeriesFactory()
	return &activeSeriesFixture{clock: clock, factory: factory, tracker: NewActiveSeries(factory, clock, shardNum)}
}

func (f *activeSeriesFixture) setClock(offset time.Duration) {
	f.clock.Set(activeSeriesBase.Add(offset))
}

func randomIDs(seed int64, n int) []uint64 {
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test data
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = rng.Uint64()
	}
	return ids
}

func TestActiveSeriesExportsLastCompleteWindow(t *testing.T) {
	f := newActiveSeriesFixture(0, 2)
	f.tracker.AddIDs(0, randomIDs(1, 1000))
	f.tracker.AddIDs(1, randomIDs(2, 2000))

	f.setClock(30 * time.Minute)
	f.tracker.Collect()
	assert.Empty(t, f.factory.exported(t), "the window in progress is not exported")

	f.setClock(time.Hour)
	f.tracker.Collect()
	got := f.factory.exported(t)
	require.Len(t, got, 2)
	assert.InEpsilon(t, 1000, got["0"], 0.02)
	assert.InEpsilon(t, 2000, got["1"], 0.02)

	f.tracker.AddIDs(0, randomIDs(3, 500))
	f.setClock(2*time.Hour + time.Minute)
	f.tracker.Collect()
	got = f.factory.exported(t)
	assert.InEpsilon(t, 500, got["0"], 0.02)
	assert.NotContains(t, got, "1", "an idle shard loses its label after one empty window")
}

func TestActiveSeriesWindowOpenedMidwayNotExported(t *testing.T) {
	f := newActiveSeriesFixture(20*time.Minute, 1)
	f.tracker.AddIDs(0, randomIDs(1, 1000))
	f.setClock(time.Hour + time.Minute)
	f.tracker.Collect()
	assert.Empty(t, f.factory.exported(t))
	assert.EqualValues(t, 1, f.factory.gauge.deletes.Load())

	f.tracker.AddIDs(0, randomIDs(2, 1000))
	f.setClock(2*time.Hour + time.Minute)
	f.tracker.Collect()
	assert.InEpsilon(t, 1000, f.factory.exported(t)["0"], 0.02)
}

func TestActiveSeriesWindowOpenedAtStartExported(t *testing.T) {
	f := newActiveSeriesFixture(0, 1)
	f.tracker.AddIDs(0, randomIDs(1, 1000))
	f.setClock(time.Hour)
	f.tracker.Collect()
	assert.InEpsilon(t, 1000, f.factory.exported(t)["0"], 0.02)
}

func TestActiveSeriesAddRotates(t *testing.T) {
	f := newActiveSeriesFixture(0, 1)
	f.tracker.AddIDs(0, randomIDs(1, 1000))
	f.setClock(time.Hour + time.Minute)
	f.tracker.AddIDs(0, randomIDs(2, 10))
	assert.InEpsilon(t, 1000, f.factory.exported(t)["0"], 0.02, "a write rotates the window without Collect")
}

func TestActiveSeriesAddDocs(t *testing.T) {
	f := newActiveSeriesFixture(0, 1)
	ids := randomIDs(1, 1000)
	docs := make(index.Documents, len(ids))
	for i, id := range ids {
		docs[i].DocID = id
	}
	f.tracker.AddDocs(0, docs)
	f.tracker.AddDocs(0, nil)
	f.setClock(time.Hour)
	f.tracker.Collect()
	assert.InEpsilon(t, 1000, f.factory.exported(t)["0"], 0.02)
}

func TestActiveSeriesShardNumMasksTwoWindows(t *testing.T) {
	f := newActiveSeriesFixture(0, 4)
	write := func(seed int64) { f.tracker.AddIDs(0, randomIDs(seed, 1000)) }
	write(1)
	f.setClock(10 * time.Minute)
	f.tracker.UpdateShardNum(4)
	f.setClock(time.Hour)
	f.tracker.Collect()
	assert.InEpsilon(t, 1000, f.factory.exported(t)["0"], 0.02, "the same shard number masks nothing")

	write(2)
	f.setClock(time.Hour + 10*time.Minute)
	f.tracker.UpdateShardNum(2)
	f.setClock(2 * time.Hour)
	f.tracker.Collect()
	assert.Empty(t, f.factory.exported(t), "the window of the change is masked")

	write(3)
	f.setClock(3 * time.Hour)
	f.tracker.Collect()
	assert.Empty(t, f.factory.exported(t), "the window after the change is masked")

	write(4)
	f.setClock(4 * time.Hour)
	f.tracker.Collect()
	assert.InEpsilon(t, 1000, f.factory.exported(t)["0"], 0.02)
}

func TestActiveSeriesIdleShardRemoved(t *testing.T) {
	f := newActiveSeriesFixture(0, 1)
	f.tracker.AddIDs(0, randomIDs(1, 100))
	f.setClock(time.Hour)
	f.tracker.Collect()
	assert.Contains(t, f.factory.exported(t), "0")

	f.setClock(2 * time.Hour)
	f.tracker.Collect()
	assert.Empty(t, f.factory.exported(t))
	f.tracker.mu.RLock()
	assert.Empty(t, f.tracker.shards)
	f.tracker.mu.RUnlock()
}

func TestActiveSeriesShardNumShrinkLeavesNoLabel(t *testing.T) {
	f := newActiveSeriesFixture(0, 4)
	for shard := common.ShardID(0); shard < 4; shard++ {
		f.tracker.AddIDs(shard, randomIDs(int64(shard), 1000))
	}
	f.setClock(time.Hour)
	f.tracker.Collect()
	require.Len(t, f.factory.exported(t), 4)

	f.tracker.UpdateShardNum(2)
	for hour := 1; hour <= 5; hour++ {
		f.setClock(time.Duration(hour) * time.Hour)
		f.tracker.AddIDs(0, randomIDs(10+int64(hour), 1000))
		f.tracker.AddIDs(1, randomIDs(20+int64(hour), 1000))
		f.setClock(time.Duration(hour+1) * time.Hour)
		f.tracker.Collect()
	}
	got := f.factory.exported(t)
	assert.Len(t, got, 2)
	assert.NotContains(t, got, "2")
	assert.NotContains(t, got, "3")
}

func TestActiveSeriesClockJumpDeletesOldValue(t *testing.T) {
	f := newActiveSeriesFixture(0, 1)
	f.tracker.AddIDs(0, randomIDs(1, 1000))
	f.setClock(time.Hour)
	f.tracker.Collect()
	require.Contains(t, f.factory.exported(t), "0")

	f.tracker.AddIDs(0, randomIDs(2, 1000))
	f.setClock(5*time.Hour + time.Minute)
	f.tracker.Collect()
	assert.Empty(t, f.factory.exported(t), "a window that is not the previous one is not exported")

	f.tracker.AddIDs(0, randomIDs(3, 700))
	f.setClock(6*time.Hour + time.Minute)
	f.tracker.Collect()
	assert.InEpsilon(t, 700, f.factory.exported(t)["0"], 0.02)
}

func TestActiveSeriesSameSetSameValue(t *testing.T) {
	f := newActiveSeriesFixture(0, 1)
	ids := randomIDs(1, 5000)
	f.tracker.AddIDs(0, ids)
	f.setClock(time.Hour)
	f.tracker.Collect()
	first := f.factory.exported(t)["0"]
	reversed := make([]uint64, len(ids))
	for i, id := range ids {
		reversed[len(ids)-1-i] = id
	}
	f.tracker.AddIDs(0, reversed)
	f.tracker.AddIDs(0, ids)
	f.setClock(2 * time.Hour)
	f.tracker.Collect()
	assert.Equal(t, first, f.factory.exported(t)["0"])
}

func TestActiveSeriesStaleClockNoBackwardRotate(t *testing.T) {
	f := newActiveSeriesFixture(0, 1)
	f.tracker.AddIDs(0, randomIDs(1, 1000))
	f.setClock(time.Hour + time.Minute)
	f.tracker.AddIDs(0, randomIDs(2, 300))
	sets, deletes := f.factory.gauge.sets.Load(), f.factory.gauge.deletes.Load()
	require.EqualValues(t, 1, sets)

	f.setClock(59 * time.Minute)
	f.tracker.AddIDs(0, randomIDs(3, 300))
	f.tracker.Collect()
	assert.Equal(t, sets, f.factory.gauge.sets.Load())
	assert.Equal(t, deletes, f.factory.gauge.deletes.Load())
	assert.InEpsilon(t, 1000, f.factory.exported(t)["0"], 0.02)

	f.setClock(2*time.Hour + time.Minute)
	f.tracker.Collect()
	assert.Empty(t, f.factory.exported(t), "a window that received writes of a stale reading is not exported")
}

// blockedOn counts the goroutines whose stack holds both fn and the blocking primitive.
func blockedOn(fn, primitive string) int {
	buf := make([]byte, 1<<22)
	buf = buf[:runtime.Stack(buf, true)]
	n := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, fn) && strings.Contains(g, primitive) {
			n++
		}
	}
	return n
}

// waitFor yields until cond holds. It is a signal wait, not a timing assumption; the deadline only ends a test that can never succeed.
func waitFor(tb testing.TB, what string, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			tb.Fatalf("condition never held: %s", what)
		}
		runtime.Gosched()
	}
}

func (a *ActiveSeries) shardZero() *shardWindow {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.shards[0]
}

// gateClock blocks the next call of Now after it was armed, once it has read the time.
type gateClock struct {
	timestamp.MockClock
	reached chan struct{}
	release chan struct{}
	once    sync.Once
	armed   atomic.Bool
}

func newGateClock() *gateClock {
	mock := timestamp.NewMockClock()
	mock.Set(activeSeriesBase)
	return &gateClock{MockClock: mock, reached: make(chan struct{}), release: make(chan struct{})}
}

func (c *gateClock) Now() time.Time {
	now := c.MockClock.Now()
	if c.armed.CompareAndSwap(true, false) {
		close(c.reached)
		<-c.release
	}
	return now
}

func (c *gateClock) open() { c.once.Do(func() { close(c.release) }) }

func (c *gateClock) setOffset(offset time.Duration) { c.MockClock.Set(activeSeriesBase.Add(offset)) }

func TestActiveSeriesConcurrentRotateOnce(t *testing.T) {
	f := newActiveSeriesFixture(0, 1)
	f.tracker.AddIDs(0, randomIDs(1, 1000))
	f.setClock(time.Hour + time.Minute)

	reached := make(chan struct{})
	release := make(chan struct{})
	f.factory.gauge.beforeSet = func() {
		close(reached)
		<-release
	}
	const followers = 7
	var wg sync.WaitGroup
	wg.Add(1)
	//panicdiag:allow-rawgo test helper goroutine
	go func() {
		defer wg.Done()
		f.tracker.Collect() // rotates the window and stops inside Set
	}()
	<-reached
	for i := 0; i < followers; i++ {
		wg.Add(1)
		//panicdiag:allow-rawgo test helper goroutine
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				f.tracker.Collect()
				return
			}
			f.tracker.AddIDs(0, randomIDs(int64(100+i), 10))
		}(i)
	}
	waitFor(t, "every follower waits for the rotating goroutine", func() bool {
		return blockedOn("(*ActiveSeries)", "SemacquireRWMutex") == followers
	})
	close(release)
	wg.Wait()
	assert.EqualValues(t, 1, f.factory.gauge.sets.Load())
	assert.EqualValues(t, 0, f.factory.gauge.deletes.Load())
	assert.InEpsilon(t, 1000, f.factory.exported(t)["0"], 0.02)
}

func TestActiveSeriesUpdateShardNumRacesRotate(t *testing.T) {
	clock := newGateClock()
	factory := newActiveSeriesFactory()
	tracker := NewActiveSeries(factory, clock, 4)
	tracker.AddIDs(0, randomIDs(1, 1000))
	clock.setOffset(30 * time.Minute)

	var wg sync.WaitGroup
	clock.armed.Store(true)
	wg.Add(1)
	//panicdiag:allow-rawgo test helper goroutine
	go func() {
		defer wg.Done()
		tracker.UpdateShardNum(2) // reads 10:30, then waits at the gate
	}()
	<-clock.reached

	clock.setOffset(time.Hour + time.Second)
	wg.Add(1)
	//panicdiag:allow-rawgo test helper goroutine
	go func() {
		defer wg.Done()
		tracker.Collect()
	}()
	waitFor(t, "the rotation waits for the shard number change", func() bool {
		return blockedOn("(*ActiveSeries).export", "SemacquireMutex") == 1
	})
	clock.open()
	wg.Wait()

	assert.Empty(t, factory.exported(t), "the window in which the shard number changed must not be exported")
	assert.EqualValues(t, 0, factory.gauge.sets.Load())
}

func TestActiveSeriesShardNumChangeWaitsForExport(t *testing.T) {
	f := newActiveSeriesFixture(0, 4)
	f.tracker.AddIDs(0, randomIDs(1, 1000))
	reached, release := make(chan struct{}), make(chan struct{})
	var maskedAtSet bool
	f.factory.gauge.beforeSet = func() {
		close(reached)
		<-release
		maskedAtSet = f.tracker.dirtyUntil > activeSeriesBase.UnixNano()
	}
	f.setClock(time.Hour + time.Second)

	var wg sync.WaitGroup
	wg.Add(1)
	//panicdiag:allow-rawgo test helper goroutine
	go func() {
		defer wg.Done()
		f.tracker.Collect() // exports the window 10:00, then waits in Set
	}()
	<-reached

	f.setClock(30 * time.Minute) // the clock steps back into the window that is being exported
	updated := make(chan struct{})
	//panicdiag:allow-rawgo test helper goroutine
	go func() {
		defer close(updated)
		f.tracker.UpdateShardNum(2)
	}()
	waitFor(t, "the shard number change either applies or waits for the export", func() bool {
		select {
		case <-updated:
			return true
		default:
			return blockedOn("(*ActiveSeries).UpdateShardNum", "SemacquireMutex") == 1
		}
	})
	close(release)
	wg.Wait()
	<-updated

	assert.False(t, maskedAtSet, "the label was set for a window that the shard number change had already masked")
	assert.InEpsilon(t, 1000, f.factory.exported(t)["0"], 0.02)
	assert.Equal(t, activeSeriesBase.Add(3*time.Hour).UnixNano(), f.tracker.dirtyUntil,
		"the shard has reached the window 11:00 although the clock reads 10:30, so the change masks the windows before 13:00")
}

func TestActiveSeriesRotationWaitsForInFlightBatch(t *testing.T) {
	clock := newGateClock()
	t.Cleanup(clock.open)
	factory := newActiveSeriesFactory()
	tracker := NewActiveSeries(factory, clock, 1)
	tracker.AddIDs(0, randomIDs(1, 1000))
	clock.setOffset(59 * time.Minute)

	clock.armed.Store(true)
	writerDone := make(chan struct{})
	//panicdiag:allow-rawgo test helper goroutine
	go func() {
		defer close(writerDone)
		tracker.AddIDs(0, randomIDs(2, 500)) // reads 10:59 under the read lock, then waits at the gate
	}()
	<-clock.reached

	clock.setOffset(time.Hour + time.Second)
	collectDone := make(chan struct{})
	//panicdiag:allow-rawgo test helper goroutine
	go func() {
		defer close(collectDone)
		tracker.Collect()
	}()
	sw := tracker.shardZero()
	require.NotNil(t, sw)
	waitingForWriteLock := func() bool {
		select {
		case <-collectDone:
			return true
		default:
		}
		if sw.mu.TryRLock() {
			sw.mu.RUnlock()
			return false
		}
		return true
	}
	waitFor(t, "the rotation requests the write lock", waitingForWriteLock)
	select {
	case <-collectDone:
		clock.open()
		<-writerDone
		t.Fatal("the rotation did not wait for the batch that holds the read lock")
	default:
	}
	clock.open()
	<-writerDone
	<-collectDone
	assert.InEpsilon(t, 1500, factory.exported(t)["0"], 0.02, "the whole in-flight batch lands in the ending window")

	tracker.AddIDs(0, randomIDs(3, 300))
	clock.setOffset(2*time.Hour + time.Second)
	tracker.Collect()
	assert.InEpsilon(t, 300, factory.exported(t)["0"], 0.02, "the next window holds only its own IDs")
}

func TestActiveSeriesRetriesAfterShardRemoved(t *testing.T) {
	f := newActiveSeriesFixture(0, 1)
	f.tracker.AddIDs(0, randomIDs(1, 10))
	sw := f.tracker.shardZero()
	require.NotNil(t, sw)

	sw.mu.Lock() // stands for a rotation that removes the shard
	done := make(chan struct{})
	//panicdiag:allow-rawgo test helper goroutine
	go func() {
		defer close(done)
		f.tracker.AddIDs(0, randomIDs(2, 700))
	}()
	waitFor(t, "the writer holds the shard that is being removed", func() bool {
		return blockedOn("(*ActiveSeries).acquire", "SemacquireRWMutexR") == 1
	})
	sw.removed = true
	f.tracker.mu.Lock()
	delete(f.tracker.shards, 0)
	f.tracker.mu.Unlock()
	sw.mu.Unlock()
	<-done

	f.setClock(time.Hour)
	f.tracker.Collect()
	assert.InEpsilon(t, 700, f.factory.exported(t)["0"], 0.02, "the batch is recorded in the replacement shard")
}

func TestActiveSeriesAdjacentWindowsExportOwnIDs(t *testing.T) {
	f := newActiveSeriesFixture(0, 1)
	f.tracker.AddIDs(0, randomIDs(1, 1000))
	f.setClock(time.Hour)
	f.tracker.AddIDs(0, randomIDs(2, 600))
	assert.InEpsilon(t, 1000, f.factory.exported(t)["0"], 0.02)
	f.setClock(2 * time.Hour)
	f.tracker.Collect()
	assert.InEpsilon(t, 600, f.factory.exported(t)["0"], 0.02)
}

func TestActiveSeriesConcurrentWriters(t *testing.T) {
	f := newActiveSeriesFixture(0, 4)
	const writers = 8
	var ledger windowLedger
	var next atomic.Uint64
	done := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		//panicdiag:allow-rawgo test helper goroutine
		go func() {
			defer wg.Done()
			ids := make([]uint64, ledgerBatch)
			for {
				select {
				case <-done:
					return
				default:
				}
				lo := ledger.begin()
				for i := range ids {
					ids[i] = splitMix(next.Add(1))
				}
				f.tracker.AddIDs(0, ids)
				ledger.end(lo)
				runtime.Gosched()
			}
		}()
	}
	for w := int64(1); w <= ledgerWindows; w++ {
		waitFor(t, "writers add a batch in the current window", func() bool { return ledger.lower(w-1) > 0 })
		lower := ledger.lower(w - 1)
		ledger.advance(w, func() { f.setClock(time.Duration(w) * time.Hour) })
		f.tracker.Collect()
		got, ok := f.factory.exported(t)["0"]
		upper := ledger.upper(w - 1)
		if assert.True(t, ok, "window %d is exported", w-1) {
			assert.GreaterOrEqual(t, got, lower*0.97, "window %d lost batches", w-1)
			assert.LessOrEqual(t, got, upper*1.03, "window %d holds IDs that were added in another window", w-1)
		}
	}
	close(done)
	wg.Wait()
}

func splitMix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func TestActiveSeriesCloseDeletesLabelsAndStopsExport(t *testing.T) {
	f := newActiveSeriesFixture(0, 2)
	f.tracker.AddIDs(0, randomIDs(1, 1000))
	f.tracker.AddIDs(1, randomIDs(2, 500))
	f.setClock(time.Hour)
	f.tracker.Collect()
	require.Len(t, f.factory.exported(t), 2)
	f.tracker.AddIDs(0, randomIDs(3, 400)) // the window in progress

	f.tracker.Close()
	assert.Empty(t, f.factory.exported(t), "closing removes every exported label")
	sets := f.factory.gauge.sets.Load()

	f.tracker.AddIDs(0, randomIDs(4, 400))
	f.tracker.AddIDs(1, randomIDs(5, 400))
	f.tracker.AddDocs(1, index.Documents{{DocID: 1}})
	f.setClock(2*time.Hour + time.Minute)
	f.tracker.AddIDs(0, randomIDs(6, 400))
	f.tracker.Collect()
	f.tracker.UpdateShardNum(8)
	f.setClock(4 * time.Hour)
	f.tracker.Collect()
	assert.Empty(t, f.factory.exported(t), "a closed tracker exports nothing")
	assert.Equal(t, sets, f.factory.gauge.sets.Load())
	assert.Empty(t, f.tracker.shards, "a closed tracker creates no shard")

	assert.NotPanics(t, f.tracker.Close, "closing twice does nothing")
	var nilTracker *ActiveSeries
	assert.NotPanics(t, nilTracker.Close)
}

func TestActiveSeriesCloseWaitsForInFlightWriter(t *testing.T) {
	clock := newGateClock()
	t.Cleanup(clock.open)
	factory := newActiveSeriesFactory()
	tracker := NewActiveSeries(factory, clock, 1)
	tracker.AddIDs(0, randomIDs(1, 1000))
	sw := tracker.shardZero() // Close takes the shard out of the map, so it is looked up before Close starts
	require.NotNil(t, sw)
	clock.setOffset(time.Hour + time.Second)

	clock.armed.Store(true)
	writerDone := make(chan struct{})
	//panicdiag:allow-rawgo test helper goroutine
	go func() {
		defer close(writerDone)
		tracker.AddIDs(0, randomIDs(2, 500)) // reads 11:00:01 under the read lock, then waits at the gate
	}()
	<-clock.reached
	closeDone := make(chan struct{})
	//panicdiag:allow-rawgo test helper goroutine
	go func() {
		defer close(closeDone)
		tracker.Close()
	}()
	waitFor(t, "Close waits for the write lock of the shard", func() bool {
		return blockedOn("(*ActiveSeries).Close", "SemacquireRWMutex") == 1
	})
	clock.open()
	<-writerDone
	<-closeDone
	assert.Empty(t, factory.exported(t), "the batch that arrives during Close must not rotate and export")
	assert.EqualValues(t, 0, factory.gauge.sets.Load())
	assert.Empty(t, tracker.shards)
	assert.True(t, sw.removed, "Close removed the shard the batch held")
}

func TestActiveSeriesClockBackwardKeepsShardNumMask(t *testing.T) {
	f := newActiveSeriesFixture(0, 4)
	f.tracker.AddIDs(0, randomIDs(1, 10))
	f.setClock(time.Hour)
	f.tracker.UpdateShardNum(2) // masks the windows that start before 13:00

	f.setClock(0)
	f.tracker.UpdateShardNum(3) // must not shorten the mask to 12:00

	f.setClock(2 * time.Hour)
	f.tracker.AddIDs(0, randomIDs(2, 1000))
	f.setClock(3 * time.Hour)
	f.tracker.Collect()
	assert.Empty(t, f.factory.exported(t), "the window 12:00 stays masked")
}

func TestActiveSeriesClockBackwardAfterIdleRemovalKeepsWindowClosed(t *testing.T) {
	f := newActiveSeriesFixture(0, 1)
	f.tracker.AddIDs(0, randomIDs(1, 1000))
	f.setClock(time.Hour)
	f.tracker.Collect()
	require.Contains(t, f.factory.exported(t), "0")
	f.setClock(2 * time.Hour)
	f.tracker.Collect()
	require.Empty(t, f.factory.exported(t))
	require.Empty(t, f.tracker.shards, "the idle shard is removed")

	f.setClock(30 * time.Minute)
	f.tracker.AddIDs(0, randomIDs(2, 500))
	f.setClock(time.Hour + time.Minute)
	f.tracker.Collect()
	assert.Empty(t, f.factory.exported(t), "the ended window 10:00 is not opened again")

	f.setClock(3 * time.Hour)
	f.tracker.Collect()
	assert.Empty(t, f.factory.exported(t), "writes of a stale reading are not exported")
}

func TestActiveSeriesClockJumpedAheadThenCorrectedNotExported(t *testing.T) {
	f := newActiveSeriesFixture(0, 1)
	f.tracker.AddIDs(0, randomIDs(1, 1000))
	f.setClock(24 * time.Hour)
	f.tracker.AddIDs(0, randomIDs(2, 5))
	for h := 1; h <= 23; h++ {
		f.setClock(time.Duration(h)*time.Hour + time.Minute)
		f.tracker.AddIDs(0, randomIDs(int64(100+h), 1000))
		f.tracker.Collect()
	}
	f.setClock(25*time.Hour + time.Minute)
	f.tracker.Collect()
	assert.Empty(t, f.factory.exported(t), "the window of the wrong clock mixes many hours")

	f.tracker.AddIDs(0, randomIDs(200, 800))
	f.setClock(26*time.Hour + time.Minute)
	f.tracker.Collect()
	assert.InEpsilon(t, 800, f.factory.exported(t)["0"], 0.02, "export resumes with the next honest window")
}

// TestActiveSeriesCollectStaleReadingDoesNotExportEarly lets a collection read 11:00:01 and stop there. The clock is then
// corrected to 10:20, so the window 10:00 is not complete and must not be exported, however late the collection resumes.
func TestActiveSeriesCollectStaleReadingDoesNotExportEarly(t *testing.T) {
	clock := newGateClock()
	t.Cleanup(clock.open)
	factory := newActiveSeriesFactory()
	clock.setOffset(-30 * time.Minute)
	tracker := NewActiveSeries(factory, clock, 1) // created at 09:30, so the window 10:00 can be exported
	clock.setOffset(0)
	tracker.AddIDs(0, randomIDs(1, 500))
	clock.setOffset(time.Hour + time.Second)

	clock.armed.Store(true)
	collectDone := make(chan struct{})
	//panicdiag:allow-rawgo test helper goroutine
	go func() {
		defer close(collectDone)
		tracker.Collect() // reads 11:00:01, then waits at the gate
	}()
	<-clock.reached
	clock.setOffset(20 * time.Minute)
	tracker.AddIDs(0, randomIDs(2, 500))
	clock.open()
	<-collectDone
	assert.Empty(t, factory.exported(t), "at 10:20 the window 10:00 is not complete and must not be exported")
}

// TestActiveSeriesForwardJumpCorrectedByCollectNotMixed moves a shard into the window 11:00 with a clock that is ahead,
// then lets only a collection see the corrected clock. The writes of the real 11:30 join the window, so it holds the
// IDs of two real hours and must not be exported.
func TestActiveSeriesForwardJumpCorrectedByCollectNotMixed(t *testing.T) {
	f := newActiveSeriesFixture(-30*time.Minute, 1)
	f.setClock(0)
	f.tracker.AddIDs(0, randomIDs(1, 1000)) // A, real 10:00
	f.setClock(time.Hour + 5*time.Minute)
	f.tracker.AddIDs(0, randomIDs(2, 300)) // B, the clock is ahead: real 10:05
	f.setClock(6 * time.Minute)            // corrected to 10:06
	f.tracker.Collect()                    // only a collection sees the corrected clock
	f.setClock(time.Hour + 30*time.Minute)
	f.tracker.AddIDs(0, randomIDs(3, 400)) // C, real 11:30
	f.setClock(2*time.Hour + time.Second)
	f.tracker.Collect()
	if got, ok := f.factory.exported(t)["0"]; ok {
		assert.LessOrEqual(t, got, 400*1.02, "the window 11:00 holds B, written in the real hour 10:00, together with C")
	}
}

// TestActiveSeriesShardNumMaskFollowsLatestWindow changes the shard count while the clock reads earlier than the window
// the shard is in. The change belongs to the window 11:00 of the shard, so 11:00 and 12:00 are masked.
func TestActiveSeriesShardNumMaskFollowsLatestWindow(t *testing.T) {
	f := newActiveSeriesFixture(-30*time.Minute, 2)
	f.setClock(0)
	f.tracker.AddIDs(0, randomIDs(1, 100))
	f.setClock(time.Hour + 5*time.Minute)
	f.tracker.AddIDs(0, randomIDs(2, 100)) // window 11:00
	f.setClock(35 * time.Minute)
	f.tracker.UpdateShardNum(3)
	f.setClock(2*time.Hour + 30*time.Minute)
	f.tracker.AddIDs(0, randomIDs(3, 500)) // window 12:00
	f.setClock(3*time.Hour + time.Second)
	f.tracker.Collect()
	assert.Empty(t, f.factory.exported(t), "the window 12:00 follows the window of the change")
}

// TestActiveSeriesSecondRotationWithTheSameTargetChangesNothing lets two batches read 11:00:01 under the read lock, so that
// both rotate the shard to the window 11:00. The second rotation finds the shard already there and must not touch the
// label or the IDs of the new window.
func TestActiveSeriesSecondRotationWithTheSameTargetChangesNothing(t *testing.T) {
	clock := newGateClock()
	t.Cleanup(clock.open)
	factory := newActiveSeriesFactory()
	tracker := NewActiveSeries(factory, clock, 1)
	tracker.AddIDs(0, randomIDs(1, 1000))
	clock.setOffset(time.Hour + time.Second)

	clock.armed.Store(true)
	firstDone, secondDone := make(chan struct{}), make(chan struct{})
	//panicdiag:allow-rawgo test helper goroutine
	go func() {
		defer close(firstDone)
		tracker.AddIDs(0, randomIDs(2, 300)) // reads 11:00:01 under the read lock, then waits at the gate
	}()
	<-clock.reached
	//panicdiag:allow-rawgo test helper goroutine
	go func() {
		defer close(secondDone)
		tracker.AddIDs(0, randomIDs(3, 400)) // reads 11:00:01 as well and queues for the write lock
	}()
	waitFor(t, "the second batch waits to rotate the shard", func() bool {
		return blockedOn("(*ActiveSeries).rotate", "SemacquireRWMutex") == 1
	})
	clock.open()
	<-firstDone
	<-secondDone

	assert.EqualValues(t, 1, factory.gauge.sets.Load(), "the window 10:00 is exported once")
	assert.EqualValues(t, 0, factory.gauge.deletes.Load(), "the second rotation does not delete the label")
	assert.InEpsilon(t, 1000, factory.exported(t)["0"], 0.02)
	clock.setOffset(2*time.Hour + time.Second)
	tracker.Collect()
	assert.InEpsilon(t, 700, factory.exported(t)["0"], 0.02, "the IDs of both batches stay in the window 11:00")
}

// TestActiveSeriesLateRotationOfRemovedShardChangesNothing rotates a shard that was removed and replaced after a collection
// had taken it. The late rotation must leave the label and the IDs of the replacement alone.
func TestActiveSeriesLateRotationOfRemovedShardChangesNothing(t *testing.T) {
	f := newActiveSeriesFixture(0, 1)
	f.tracker.AddIDs(0, randomIDs(1, 1000))
	removed := f.tracker.shardZero()
	require.NotNil(t, removed)
	f.setClock(time.Hour)
	f.tracker.Collect()
	f.setClock(2 * time.Hour)
	f.tracker.Collect()
	require.Empty(t, f.tracker.shards, "the idle shard is removed")
	require.True(t, removed.removed)

	f.setClock(2*time.Hour + 10*time.Minute)
	f.tracker.AddIDs(0, randomIDs(2, 500))
	f.setClock(3*time.Hour + 10*time.Minute)
	f.tracker.AddIDs(0, randomIDs(3, 300)) // rotates the replacement and exports 500
	replacement := f.tracker.shardZero()
	require.NotNil(t, replacement)
	require.NotSame(t, removed, replacement)
	require.InEpsilon(t, 500, f.factory.exported(t)["0"], 0.02)
	sets, deletes := f.factory.gauge.sets.Load(), f.factory.gauge.deletes.Load()
	replacementStart, replacementIDs := replacement.start, replacement.sketch.Estimate()

	f.tracker.rotate(0, removed, activeSeriesBase.Add(3*time.Hour).UnixNano())

	assert.Equal(t, sets, f.factory.gauge.sets.Load())
	assert.Equal(t, deletes, f.factory.gauge.deletes.Load())
	assert.InEpsilon(t, 500, f.factory.exported(t)["0"], 0.02, "the label of the replacement stays")
	assert.Same(t, replacement, f.tracker.shardZero())
	assert.Equal(t, replacementStart, replacement.start)
	assert.Equal(t, replacementIDs, replacement.sketch.Estimate(), "the IDs of the replacement stay")
	f.setClock(4*time.Hour + time.Second)
	f.tracker.Collect()
	assert.InEpsilon(t, 300, f.factory.exported(t)["0"], 0.02)
}

// TestActiveSeriesSecondCloseWaitsForTheFirst closes a tracker whose first Close still waits for a batch in flight.
// The second Close must not return before the label is deleted.
func TestActiveSeriesSecondCloseWaitsForTheFirst(t *testing.T) {
	clock := newGateClock()
	t.Cleanup(clock.open)
	factory := newActiveSeriesFactory()
	tracker := NewActiveSeries(factory, clock, 1)
	tracker.AddIDs(0, randomIDs(1, 1000))
	clock.setOffset(time.Hour + 30*time.Minute)
	tracker.Collect()
	require.Contains(t, factory.exported(t), "0")

	clock.armed.Store(true)
	writerDone, firstDone, secondDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	//panicdiag:allow-rawgo test helper goroutine
	go func() {
		defer close(writerDone)
		tracker.AddIDs(0, randomIDs(2, 500)) // holds the read lock at the gate
	}()
	<-clock.reached
	//panicdiag:allow-rawgo test helper goroutine
	go func() {
		defer close(firstDone)
		tracker.Close()
	}()
	waitFor(t, "the first Close waits for the batch in flight", func() bool {
		return blockedOn("(*ActiveSeries).Close", "SemacquireRWMutex") == 1
	})
	//panicdiag:allow-rawgo test helper goroutine
	go func() {
		defer close(secondDone)
		tracker.Close()
	}()
	waitFor(t, "the second Close returns or waits for the first", func() bool {
		select {
		case <-secondDone:
			return true
		default:
			return blockedOn("(*ActiveSeries).Close", "SemacquireMutex") == 1
		}
	})
	select {
	case <-secondDone:
		assert.Fail(t, "the second Close returned while the first one had not deleted the labels")
	default:
	}
	clock.open()
	<-writerDone
	<-firstDone
	<-secondDone
	assert.Empty(t, factory.exported(t))
}

const (
	// ledgerWindows is the number of numbered windows a windowLedger can attribute IDs to.
	ledgerWindows = 20
	// ledgerBatch is the number of IDs of every batch a windowLedger records.
	ledgerBatch = 50
)

// windowLedger attributes the IDs of concurrent batches to the numbered windows they can have landed in. A driver moves a
// controlled clock to window w with advance; a batch brackets its AddIDs call with begin and end. The window a batch
// lands in is read by AddIDs from the clock, under the read lock, so it lies between the window number the batch
// read before the call (the clock is at that window or later) and the highest window announced after the call (the
// clock never moves ahead of the announcements).
type windowLedger struct {
	started, finished, exact [ledgerWindows + 1]atomic.Int64
	announced, current       atomic.Int64
}

// advance announces window w, moves the clock to it with setClock, then publishes it as the current window.
func (l *windowLedger) advance(w int64, setClock func()) {
	l.announced.Store(w)
	setClock()
	l.current.Store(w)
}

// begin records a batch of ledgerBatch IDs about to be added and returns the window number it read.
func (l *windowLedger) begin() int64 {
	lo := l.current.Load()
	l.started[lo].Add(ledgerBatch)
	return lo
}

// end records that the batch begun in window lo has been added.
func (l *windowLedger) end(lo int64) {
	hi := l.announced.Load()
	l.finished[hi].Add(ledgerBatch)
	if lo == hi {
		l.exact[lo].Add(ledgerBatch)
	}
}

// lower is the number of IDs that certainly landed in window w, when read before the clock leaves w.
func (l *windowLedger) lower(w int64) float64 {
	return float64(l.exact[w].Load())
}

// upper is the number of IDs that can have landed in window w, when read after the window was exported: the batches that
// began in window w or earlier, without those that ended before w.
func (l *windowLedger) upper(w int64) float64 {
	var finished, started int64
	for k := int64(0); k < w; k++ {
		finished += l.finished[k].Load()
	}
	for k := int64(0); k <= w; k++ {
		started += l.started[k].Load()
	}
	return float64(started - finished)
}

// TestActiveSeriesBatchBeginningInEarlierWindowLandsInLaterOne parks three batches after they read window 0 and
// releases them in window 1. They are part of the window 1 that is exported, and the window 0 export holds only the
// batch that was added in it.
func TestActiveSeriesBatchBeginningInEarlierWindowLandsInLaterOne(t *testing.T) {
	f := newActiveSeriesFixture(0, 4)
	const parked = 3
	var ledger windowLedger
	var next atomic.Uint64
	write := func(lo int64) {
		ids := make([]uint64, ledgerBatch)
		for i := range ids {
			ids[i] = splitMix(next.Add(1))
		}
		f.tracker.AddIDs(0, ids)
		ledger.end(lo)
	}
	write(ledger.begin())
	begun := make(chan struct{}, parked)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < parked; i++ {
		wg.Add(1)
		//panicdiag:allow-rawgo test helper goroutine
		go func() {
			defer wg.Done()
			lo := ledger.begin()
			begun <- struct{}{}
			<-release
			write(lo)
		}()
	}
	for i := 0; i < parked; i++ {
		<-begun
	}
	closeWindow := func(w int64) (got, lower, upper float64) {
		lower = ledger.lower(w - 1)
		ledger.advance(w, func() { f.setClock(time.Duration(w) * time.Hour) })
		f.tracker.Collect()
		got = f.factory.exported(t)["0"]
		return got, lower, ledger.upper(w - 1)
	}
	got, lower, upper := closeWindow(1)
	assert.InEpsilon(t, ledgerBatch, got, 0.02, "the window 0 holds the batch that was added in it")
	assert.GreaterOrEqual(t, got, lower*0.97)
	assert.LessOrEqual(t, got, upper*1.03)

	close(release)
	wg.Wait()
	write(ledger.begin())
	got, lower, upper = closeWindow(2)
	assert.InEpsilon(t, (parked+1)*ledgerBatch, got, 0.02, "the window 1 holds the parked batches and its own")
	assert.GreaterOrEqual(t, got, lower*0.97, "window 1 lost batches")
	assert.LessOrEqual(t, got, upper*1.03, "window 1 holds IDs of an earlier window")
}

func TestActiveSeriesNilTracker(t *testing.T) {
	var a *ActiveSeries
	assert.NotPanics(t, func() {
		a.AddIDs(0, []uint64{1})
		a.AddDocs(0, index.Documents{{DocID: 1}})
		a.UpdateShardNum(3)
		a.Collect()
	})
	assert.Nil(t, NewActiveSeries(nil, timestamp.NewMockClock(), 1))
}

func TestActiveSeriesWindowHook(t *testing.T) {
	assert.Equal(t, time.Hour, activeSeriesWindow())
	restore := SetActiveSeriesWindowForTest(10 * time.Second)
	t.Cleanup(restore)
	assert.Equal(t, 10*time.Second, activeSeriesWindow())

	f := newActiveSeriesFixture(0, 1)
	f.tracker.AddIDs(0, randomIDs(1, 1000))
	f.setClock(10 * time.Second)
	f.tracker.Collect()
	assert.InEpsilon(t, 1000, f.factory.exported(t)["0"], 0.02)

	restore()
	assert.Equal(t, time.Hour, activeSeriesWindow())
	assert.Panics(t, func() { SetActiveSeriesWindowForTest(0) })
}

func TestActiveSeriesAddIDsSteadyStateAllocs(t *testing.T) {
	f := newActiveSeriesFixture(0, 1)
	ids := randomIDs(1, 100)
	f.tracker.AddIDs(0, ids)
	assert.Zero(t, testing.AllocsPerRun(100, func() { f.tracker.AddIDs(0, ids) }))
}

func BenchmarkActiveSeriesAddIDs(b *testing.B) {
	for _, batch := range []int{1, 100, 1000} {
		b.Run(benchName(batch), func(b *testing.B) {
			tracker := NewActiveSeries(newActiveSeriesFactory(), timestamp.NewClock(), 8)
			var next atomic.Int64
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				shard := common.ShardID(next.Add(1) % 8)
				ids := randomIDs(int64(shard), batch)
				tracker.AddIDs(shard, ids)
				for pb.Next() {
					tracker.AddIDs(shard, ids)
				}
			})
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(batch), "ns/id")
		})
	}
}

// stepClock is a clock without the sleeps of the mock clock, so that benchmarks measure only the tracker.
type stepClock struct {
	timestamp.Clock
	now atomic.Int64
}

func (c *stepClock) Now() time.Time { return time.Unix(0, c.now.Load()) }

func BenchmarkActiveSeriesRotate(b *testing.B) {
	clock := &stepClock{Clock: timestamp.NewClock()}
	clock.now.Store(activeSeriesBase.UnixNano())
	tracker := NewActiveSeries(newActiveSeriesFactory(), clock, 1)
	ids := randomIDs(1, 100000)
	tracker.AddIDs(0, ids)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		clock.now.Store(activeSeriesBase.Add(time.Duration(i) * time.Hour).UnixNano())
		tracker.Collect()
		tracker.AddIDs(0, ids[:1])
	}
}

func benchName(batch int) string {
	switch batch {
	case 1:
		return "batch=1"
	case 100:
		return "batch=100"
	default:
		return "batch=1000"
	}
}
