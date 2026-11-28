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
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const tsdbActiveSeriesMetric = "banyandb_tsdb_active_series_test_active_series"

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

// tsdbMetricsFactory backs every instrument of a database with a prometheus registry.
type tsdbMetricsFactory struct {
	reg      *prometheus.Registry
	provider meter.Provider
	closed   int
	// keepOnClose leaves the exported labels in place when the factory closes, as the native provider does.
	keepOnClose bool
}

func newTSDBMetricsFactory() *tsdbMetricsFactory {
	reg := prometheus.NewRegistry()
	scope := observability.RootScope.SubScope("tsdb_active_series_test")
	return &tsdbMetricsFactory{reg: reg, provider: prom.NewProvider(scope, reg)}
}

func (f *tsdbMetricsFactory) NewCounter(name string, labelNames ...string) meter.Counter {
	return f.provider.Counter(name, labelNames...)
}

func (f *tsdbMetricsFactory) NewGauge(name string, labelNames ...string) meter.Gauge {
	return f.provider.Gauge(name, labelNames...)
}

func (f *tsdbMetricsFactory) NewHistogram(name string, buckets meter.Buckets, labelNames ...string) meter.Histogram {
	return f.provider.Histogram(name, buckets, labelNames...)
}

func (f *tsdbMetricsFactory) Close() {
	f.closed++
	if !f.keepOnClose {
		f.provider.Close()
	}
}

// metricNames returns the names of the metric families in the registry.
func (f *tsdbMetricsFactory) metricNames(tb testing.TB) []string {
	tb.Helper()
	families, err := f.reg.Gather()
	require.NoError(tb, err)
	names := make([]string, 0, len(families))
	for _, family := range families {
		names = append(names, family.GetName())
	}
	return names
}

// exported returns the active_series value of every shard label.
func (f *tsdbMetricsFactory) exported(tb testing.TB) map[string]float64 {
	tb.Helper()
	families, err := f.reg.Gather()
	require.NoError(tb, err)
	values := make(map[string]float64)
	for _, family := range families {
		if family.GetName() != tsdbActiveSeriesMetric {
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

type tsdbActiveSeriesFixture struct {
	db      *database[*MockTSTable, any]
	clock   timestamp.MockClock
	factory *tsdbMetricsFactory
}

// openActiveSeriesTSDB opens a database whose clock stands at activeSeriesBase. A nil factory leaves the metrics factory unset.
func openActiveSeriesTSDB(t *testing.T, count bool, factory *tsdbMetricsFactory) *tsdbActiveSeriesFixture {
	t.Helper()
	logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})
	dir, defFn := test.Space(require.New(t))
	t.Cleanup(defFn)
	opts := TSDBOpts[*MockTSTable, any]{
		RootLease:         staticTestLease{},
		Location:          dir,
		SegmentInterval:   IntervalRule{Unit: DAY, Num: 1},
		TTL:               IntervalRule{Unit: DAY, Num: 3},
		ShardNum:          1,
		TSTableCreator:    MockTSTableCreator,
		CountActiveSeries: count,
	}
	if factory != nil {
		opts.StorageMetricsFactory = factory
	}
	clock := timestamp.NewMockClock()
	clock.Set(activeSeriesBase)
	ctx := timestamp.SetClock(context.Background(), clock)
	tsdb, err := OpenTSDB(ctx, opts, NewServiceCache(), group)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tsdb.Close() })
	return &tsdbActiveSeriesFixture{db: tsdb.(*database[*MockTSTable, any]), clock: clock, factory: factory}
}

func (f *tsdbActiveSeriesFixture) setClock(offset time.Duration) {
	f.clock.Set(activeSeriesBase.Add(offset))
}

func updateShardNum(db *database[*MockTSTable, any], shardNum uint32) {
	db.UpdateOptions(&commonv1.ResourceOpts{
		ShardNum:        shardNum,
		SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
		Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 3},
	})
}

func TestTSDBActiveSeriesDisabledByDefault(t *testing.T) {
	f := openActiveSeriesTSDB(t, false, newTSDBMetricsFactory())
	assert.Nil(t, f.db.ActiveSeries())
	assert.Nil(t, ActiveSeriesOf(f.db))
	for _, name := range f.factory.metricNames(t) {
		assert.NotEqual(t, tsdbActiveSeriesMetric, name)
	}
	f.setClock(time.Hour)
	assert.NotPanics(t, f.db.collect)
	assert.NotPanics(t, func() { updateShardNum(f.db, 2) })
}

func TestTSDBActiveSeriesNilFactory(t *testing.T) {
	f := openActiveSeriesTSDB(t, true, nil)
	assert.Nil(t, f.db.ActiveSeries(), "no factory, no tracker")
	assert.Nil(t, ActiveSeriesOf(f.db))
	assert.NotPanics(t, func() {
		f.db.collect()
		obsservice.MetricsCollector.Collect()
		updateShardNum(f.db, 2)
	})
}

func TestTSDBActiveSeriesCollectExports(t *testing.T) {
	f := openActiveSeriesTSDB(t, true, newTSDBMetricsFactory())
	tracker := ActiveSeriesOf(f.db)
	require.NotNil(t, tracker)
	require.Same(t, f.db.ActiveSeries(), tracker)

	tracker.AddIDs(0, randomIDs(1, 500))
	f.setClock(30 * time.Minute)
	obsservice.MetricsCollector.Collect()
	assert.Empty(t, f.factory.exported(t), "the window in progress is not exported")

	f.setClock(time.Hour)
	obsservice.MetricsCollector.Collect()
	got := f.factory.exported(t)
	require.Contains(t, got, "0")
	assert.InEpsilon(t, 500, got["0"], 0.05)
}

func TestTSDBActiveSeriesCloseUnregisters(t *testing.T) {
	f := openActiveSeriesTSDB(t, true, newTSDBMetricsFactory())
	tracker := ActiveSeriesOf(f.db)
	require.NotNil(t, tracker)
	tracker.AddIDs(0, randomIDs(1, 500))
	f.setClock(time.Hour)
	f.db.collect()
	require.NotEmpty(t, f.factory.exported(t))

	require.True(t, collectorRegistered(f.db.location), "the open database registers its collector")

	require.NoError(t, f.db.Close())
	assert.Equal(t, 1, f.factory.closed)
	assert.Empty(t, f.factory.metricNames(t), "closing the factory unregisters the gauge")
	assert.False(t, collectorRegistered(f.db.location), "closing the database unregisters its collector")

	// A later collection neither panics nor registers the gauge again.
	f.setClock(2 * time.Hour)
	assert.NotPanics(t, obsservice.MetricsCollector.Collect)
	assert.Empty(t, f.factory.metricNames(t))
}

func TestTSDBActiveSeriesUpdateOptionsShardNum(t *testing.T) {
	f := openActiveSeriesTSDB(t, true, newTSDBMetricsFactory())
	tracker := ActiveSeriesOf(f.db)
	require.NotNil(t, tracker)

	// Window [10,11) is complete at 11:00 and exported.
	tracker.AddIDs(0, randomIDs(1, 500))
	f.setClock(time.Hour)
	f.db.collect()
	require.Contains(t, f.factory.exported(t), "0")

	// An update that keeps the shard count masks nothing: [11,12) is exported.
	tracker.AddIDs(0, randomIDs(2, 600))
	updateShardNum(f.db, 1)
	f.setClock(2 * time.Hour)
	f.db.collect()
	got := f.factory.exported(t)
	require.Contains(t, got, "0")
	assert.InEpsilon(t, 600, got["0"], 0.05)

	// Changing the shard count masks the current window [12,13) and the next one [13,14).
	tracker.AddIDs(0, randomIDs(3, 700))
	updateShardNum(f.db, 2)
	f.setClock(3 * time.Hour)
	f.db.collect()
	assert.Empty(t, f.factory.exported(t), "the window of the change is not exported")

	tracker.AddIDs(0, randomIDs(4, 800))
	f.setClock(4 * time.Hour)
	f.db.collect()
	assert.Empty(t, f.factory.exported(t), "the window after the change is not exported")

	tracker.AddIDs(0, randomIDs(5, 900))
	f.setClock(5 * time.Hour)
	f.db.collect()
	got = f.factory.exported(t)
	require.Contains(t, got, "0")
	assert.InEpsilon(t, 900, got["0"], 0.05)
}

func TestTSDBActiveSeriesNoSeriesCountMetric(t *testing.T) {
	f := openActiveSeriesTSDB(t, true, newTSDBMetricsFactory())
	seg, err := f.db.CreateSegmentIfNotExist(activeSeriesBase)
	require.NoError(t, err)
	seg.DecRef()
	f.setClock(time.Hour)
	f.db.collect()
	for _, name := range f.factory.metricNames(t) {
		assert.False(t, strings.Contains(name, "series_count"), "unexpected metric %s", name)
	}
}

func TestTSDBActiveSeriesCloseDeletesLabels(t *testing.T) {
	factory := newTSDBMetricsFactory()
	factory.keepOnClose = true
	f := openActiveSeriesTSDB(t, true, factory)
	tracker := ActiveSeriesOf(f.db)
	require.NotNil(t, tracker)
	tracker.AddIDs(0, randomIDs(1, 500))
	f.setClock(time.Hour)
	f.db.collect()
	require.NotEmpty(t, factory.exported(t))

	require.NoError(t, f.db.Close())
	assert.Empty(t, factory.exported(t), "the tracker deletes its labels because the factory does not")

	// A callback that arrives after the close neither counts nor exports.
	tracker.AddIDs(0, randomIDs(2, 500))
	f.setClock(2*time.Hour + time.Minute)
	tracker.Collect()
	obsservice.MetricsCollector.Collect()
	assert.Empty(t, factory.exported(t))
}

func TestTSDBActiveSeriesOpenFailureLeavesNoGauge(t *testing.T) {
	logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})
	dir, defFn := test.Space(require.New(t))
	t.Cleanup(defFn)
	factory := newTSDBMetricsFactory()
	opts := TSDBOpts[*MockTSTable, any]{
		RootLease: staticTestLease{},
		RootLeaseFactory: func(fs.File, string) (RootLease, error) {
			return nil, errors.New("injected root lease failure")
		},
		Location:              dir,
		SegmentInterval:       IntervalRule{Unit: DAY, Num: 1},
		TTL:                   IntervalRule{Unit: DAY, Num: 3},
		ShardNum:              1,
		TSTableCreator:        MockTSTableCreator,
		CountActiveSeries:     true,
		StorageMetricsFactory: factory,
	}
	_, err := OpenTSDB(context.Background(), opts, NewServiceCache(), group)
	require.Error(t, err)

	// A collector with the same descriptor can be unregistered only while the gauge is still registered.
	probe := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: tsdbActiveSeriesMetric}, []string{"shard"})
	assert.False(t, factory.reg.Unregister(probe), "the active_series gauge is registered after a failed open")
	assert.False(t, collectorRegistered(filepath.Clean(dir)), "a failed open leaves no collector")
}

// TestTSDBActiveSeriesSegmentOpenFailureLeavesNoGauge fails the open after the root lease is created, while the existing
// segments are loaded: the tracker is created only once every step that can fail has succeeded.
func TestTSDBActiveSeriesSegmentOpenFailureLeavesNoGauge(t *testing.T) {
	logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})
	dir, defFn := test.Space(require.New(t))
	t.Cleanup(defFn)
	segment := filepath.Join(dir, "seg-"+activeSeriesBase.Format("20060102"))
	require.NoError(t, os.MkdirAll(segment, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(segment, "metadata"), []byte("not a segment metadata"), 0o600))
	factory := newTSDBMetricsFactory()
	opts := TSDBOpts[*MockTSTable, any]{
		RootLease:             staticTestLease{},
		Location:              dir,
		SegmentInterval:       IntervalRule{Unit: DAY, Num: 1},
		TTL:                   IntervalRule{Unit: DAY, Num: 3},
		ShardNum:              1,
		TSTableCreator:        MockTSTableCreator,
		CountActiveSeries:     true,
		StorageMetricsFactory: factory,
	}
	_, err := OpenTSDB(context.Background(), opts, NewServiceCache(), group)
	require.Error(t, err, "the segment with unreadable metadata fails the open")

	probe := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: tsdbActiveSeriesMetric}, []string{"shard"})
	assert.False(t, factory.reg.Unregister(probe), "the active_series gauge is registered after a failed open")
	assert.False(t, collectorRegistered(filepath.Clean(dir)), "a failed open leaves no collector")
}
