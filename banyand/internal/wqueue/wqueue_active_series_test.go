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

package wqueue

import (
	"context"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const queueActiveSeriesMetric = "banyandb_wqueue_active_series_test_active_series"

var queueActiveSeriesBase = time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)

// collectorRegistered reports whether a getter is registered under name in the global metrics collector.
func collectorRegistered(name string) bool {
	collector := reflect.ValueOf(&obsservice.MetricsCollector).Elem()
	mu := (*sync.RWMutex)(unsafe.Pointer(collector.FieldByName("gMux").UnsafeAddr())) //nolint:gosec // a test reads the registry it cannot list
	mu.RLock()
	defer mu.RUnlock()
	getters := collector.FieldByName("getters")
	getters = reflect.NewAt(getters.Type(), unsafe.Pointer(getters.UnsafeAddr())).Elem() //nolint:gosec // a test reads the registry it cannot list
	return getters.MapIndex(reflect.ValueOf(name)).IsValid()
}

// queueMetricsFactory backs the instruments of a queue with a prometheus registry.
type queueMetricsFactory struct {
	reg      *prometheus.Registry
	provider meter.Provider
	closed   int
	// keepOnClose leaves the exported labels in place when the factory closes, as the native provider does.
	keepOnClose bool
}

func newQueueMetricsFactory() *queueMetricsFactory {
	reg := prometheus.NewRegistry()
	scope := observability.RootScope.SubScope("wqueue_active_series_test")
	return &queueMetricsFactory{reg: reg, provider: prom.NewProvider(scope, reg)}
}

func (f *queueMetricsFactory) NewCounter(name string, labelNames ...string) meter.Counter {
	return f.provider.Counter(name, labelNames...)
}

func (f *queueMetricsFactory) NewGauge(name string, labelNames ...string) meter.Gauge {
	return f.provider.Gauge(name, labelNames...)
}

func (f *queueMetricsFactory) NewHistogram(name string, buckets meter.Buckets, labelNames ...string) meter.Histogram {
	return f.provider.Histogram(name, buckets, labelNames...)
}

func (f *queueMetricsFactory) Close() {
	f.closed++
	if !f.keepOnClose {
		f.provider.Close()
	}
}

// exported returns the active_series value of every shard label.
func (f *queueMetricsFactory) exported(tb testing.TB) map[string]float64 {
	tb.Helper()
	families, err := f.reg.Gather()
	require.NoError(tb, err)
	values := make(map[string]float64)
	for _, family := range families {
		if family.GetName() != queueActiveSeriesMetric {
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

func randomSeriesIDs(seed int64, n int) []uint64 {
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test data
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = rng.Uint64()
	}
	return ids
}

// openActiveSeriesQueue opens a queue whose clock stands at queueActiveSeriesBase.
func openActiveSeriesQueue(
	t *testing.T, factory observability.Factory, collect func(*collectSubQueue),
) (*Queue[*collectSubQueue, mockSubQueueOption], timestamp.MockClock) {
	t.Helper()
	clock := timestamp.NewMockClock()
	clock.Set(queueActiveSeriesBase)
	ctx := timestamp.SetClock(context.Background(), clock)
	opts := Opts[*collectSubQueue, mockSubQueueOption]{
		Group:           "test-group",
		Location:        t.TempDir(),
		ShardNum:        1,
		SegmentInterval: storage.IntervalRule{Unit: storage.DAY, Num: 1},
		GetNodes:        func(common.ShardID) []string { return nil },
		SubQueueCreator: func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
			_ mockSubQueueOption, _ any, _ string, shardID common.ShardID, _ func() []string,
		) (*collectSubQueue, error) {
			return &collectSubQueue{id: shardID}, nil
		},
		Collect: collect,
	}
	if factory != nil {
		opts.ActiveSeriesFactory = factory
	}
	q, err := Open(ctx, opts, "test-group")
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	return q, clock
}

func updateQueueShardNum(q *Queue[*collectSubQueue, mockSubQueueOption], shardNum uint32) {
	q.UpdateOptions(&commonv1.ResourceOpts{
		ShardNum:        shardNum,
		SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
		Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 3},
	})
}

func TestQueueActiveSeriesWithoutFactory(t *testing.T) {
	q, clock := openActiveSeriesQueue(t, nil, nil)
	assert.Nil(t, q.ActiveSeries())
	assert.Nil(t, storage.ActiveSeriesOf(q))
	clock.Set(queueActiveSeriesBase.Add(time.Hour))
	assert.NotPanics(t, func() {
		obsservice.MetricsCollector.Collect()
		updateQueueShardNum(q, 2)
	})
	assert.NoError(t, q.Close(), "closing a queue without the factory must not panic")
}

func TestQueueActiveSeriesRegistersCollectorAlone(t *testing.T) {
	factory := newQueueMetricsFactory()
	q, clock := openActiveSeriesQueue(t, factory, nil)
	tracker := storage.ActiveSeriesOf(q)
	require.NotNil(t, tracker)
	require.Same(t, q.ActiveSeries(), tracker)

	tracker.AddIDs(0, randomSeriesIDs(1, 500))
	clock.Set(queueActiveSeriesBase.Add(30 * time.Minute))
	obsservice.MetricsCollector.Collect()
	assert.Empty(t, factory.exported(t), "the window in progress is not exported")

	clock.Set(queueActiveSeriesBase.Add(time.Hour))
	obsservice.MetricsCollector.Collect()
	got := factory.exported(t)
	require.Contains(t, got, "0")
	assert.InEpsilon(t, 500, got["0"], 0.05)
}

func TestQueueActiveSeriesWithSubQueueCollect(t *testing.T) {
	factory := newQueueMetricsFactory()
	collected := make(map[common.ShardID]int)
	q, clock := openActiveSeriesQueue(t, factory, func(sq *collectSubQueue) { collected[sq.id]++ })
	_, err := q.GetOrCreateShard(0)
	require.NoError(t, err)

	tracker := q.ActiveSeries()
	require.NotNil(t, tracker)
	tracker.AddIDs(0, randomSeriesIDs(1, 500))
	clock.Set(queueActiveSeriesBase.Add(time.Hour))
	obsservice.MetricsCollector.Collect()

	assert.Equal(t, map[common.ShardID]int{0: 1}, collected, "the sub-queue collector still runs")
	assert.Contains(t, factory.exported(t), "0", "the tracker is collected by the same getter")
}

func TestQueueActiveSeriesUpdateOptionsShardNum(t *testing.T) {
	factory := newQueueMetricsFactory()
	q, clock := openActiveSeriesQueue(t, factory, nil)
	tracker := q.ActiveSeries()
	require.NotNil(t, tracker)
	at := func(h int) { clock.Set(queueActiveSeriesBase.Add(time.Duration(h) * time.Hour)) }

	tracker.AddIDs(0, randomSeriesIDs(1, 500))
	at(1)
	obsservice.MetricsCollector.Collect()
	require.Contains(t, factory.exported(t), "0")

	// A change of the shard count masks the window [11,12) and the next one [12,13).
	tracker.AddIDs(0, randomSeriesIDs(2, 600))
	updateQueueShardNum(q, 2)
	at(2)
	obsservice.MetricsCollector.Collect()
	assert.Empty(t, factory.exported(t), "the window of the change is not exported")

	tracker.AddIDs(0, randomSeriesIDs(3, 700))
	at(3)
	obsservice.MetricsCollector.Collect()
	assert.Empty(t, factory.exported(t), "the window after the change is not exported")

	tracker.AddIDs(0, randomSeriesIDs(4, 800))
	at(4)
	obsservice.MetricsCollector.Collect()
	got := factory.exported(t)
	require.Contains(t, got, "0")
	assert.InEpsilon(t, 800, got["0"], 0.05)
}

func TestQueueActiveSeriesCloseClosesFactory(t *testing.T) {
	factory := newQueueMetricsFactory()
	q, clock := openActiveSeriesQueue(t, factory, nil)
	q.ActiveSeries().AddIDs(0, randomSeriesIDs(1, 500))
	clock.Set(queueActiveSeriesBase.Add(time.Hour))
	obsservice.MetricsCollector.Collect()
	require.NotEmpty(t, factory.exported(t))

	require.True(t, collectorRegistered(q.location), "the open queue registers its collector")

	require.NoError(t, q.Close())
	assert.Equal(t, 1, factory.closed)
	assert.Empty(t, factory.exported(t), "closing the factory unregisters the gauge")
	assert.False(t, collectorRegistered(q.location), "closing the queue unregisters its collector")

	// A later collection does not register the gauge again.
	clock.Set(queueActiveSeriesBase.Add(2 * time.Hour))
	assert.NotPanics(t, obsservice.MetricsCollector.Collect)
	assert.Empty(t, factory.exported(t))
	assert.NoError(t, q.Close())
	assert.Equal(t, 1, factory.closed, "a second Close does nothing")
}

func TestQueueActiveSeriesCloseDeletesLabels(t *testing.T) {
	factory := newQueueMetricsFactory()
	factory.keepOnClose = true
	q, clock := openActiveSeriesQueue(t, factory, nil)
	tracker := q.ActiveSeries()
	tracker.AddIDs(0, randomSeriesIDs(1, 500))
	clock.Set(queueActiveSeriesBase.Add(time.Hour))
	obsservice.MetricsCollector.Collect()
	require.NotEmpty(t, factory.exported(t))

	require.NoError(t, q.Close())
	assert.Empty(t, factory.exported(t), "the tracker deletes its labels because the factory does not")

	tracker.AddIDs(0, randomSeriesIDs(2, 500))
	clock.Set(queueActiveSeriesBase.Add(2*time.Hour + time.Minute))
	tracker.Collect()
	obsservice.MetricsCollector.Collect()
	assert.Empty(t, factory.exported(t), "a callback that arrives after the close neither counts nor exports")
}

func TestQueueActiveSeriesCloseStopsSubQueueCollect(t *testing.T) {
	collected := make(map[common.ShardID]int)
	q, _ := openActiveSeriesQueue(t, newQueueMetricsFactory(), func(sq *collectSubQueue) { collected[sq.id]++ })
	_, err := q.GetOrCreateShard(0)
	require.NoError(t, err)
	require.True(t, collectorRegistered(q.location))
	obsservice.MetricsCollector.Collect()
	require.Equal(t, map[common.ShardID]int{0: 1}, collected)

	require.NoError(t, q.Close())
	assert.False(t, collectorRegistered(q.location), "closing the queue unregisters its collector")
	obsservice.MetricsCollector.Collect()
	assert.Equal(t, map[common.ShardID]int{0: 1}, collected, "the sub-queues are not collected after the queue closed")
}
