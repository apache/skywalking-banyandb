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

package stream

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	activeSeriesTestGroup  = "active-series-group"
	activeSeriesTestStream = "active-series-stream"
	activeSeriesTestShards = 2
	// activeSeriesTestWindow is long enough to write a batch inside one window and short enough to keep the tests quick.
	activeSeriesTestWindow = 2 * time.Second
)

// activeSeriesTestRegistry hands out factories backed by prometheus registries, so a test reads the exported gauge.
type activeSeriesTestRegistry struct {
	observability.MetricsRegistry
	regs []*prometheus.Registry
	// gauges holds the name of every gauge a factory created, including gauges that never get a value.
	gauges []string
	mu     sync.Mutex
}

func (r *activeSeriesTestRegistry) With(meter.Scope) observability.Factory {
	reg := prometheus.NewRegistry()
	r.mu.Lock()
	r.regs = append(r.regs, reg)
	r.mu.Unlock()
	return &activeSeriesTestFactory{registry: r, provider: prom.NewProvider(observability.RootScope.SubScope("stream_active_series_test"), reg)}
}

func (r *activeSeriesTestRegistry) NativeEnabled() bool { return false }

// registeredActiveSeries reports whether any factory created an active_series gauge.
func (r *activeSeriesTestRegistry) registeredActiveSeries() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Contains(r.gauges, "active_series")
}

// exported returns the active_series value of every shard label.
func (r *activeSeriesTestRegistry) exported(t *testing.T) map[string]float64 {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	values := make(map[string]float64)
	for _, reg := range r.regs {
		families, err := reg.Gather()
		require.NoError(t, err)
		for _, family := range families {
			if !strings.HasSuffix(family.GetName(), "_active_series") {
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
	}
	return values
}

type activeSeriesTestFactory struct {
	registry *activeSeriesTestRegistry
	provider meter.Provider
}

func (f *activeSeriesTestFactory) NewCounter(name string, labelNames ...string) meter.Counter {
	return f.provider.Counter(name, labelNames...)
}

func (f *activeSeriesTestFactory) NewGauge(name string, labelNames ...string) meter.Gauge {
	f.registry.mu.Lock()
	f.registry.gauges = append(f.registry.gauges, name)
	f.registry.mu.Unlock()
	return f.provider.Gauge(name, labelNames...)
}

func (f *activeSeriesTestFactory) NewHistogram(name string, buckets meter.Buckets, labelNames ...string) meter.Histogram {
	return f.provider.Histogram(name, buckets, labelNames...)
}

func (f *activeSeriesTestFactory) Close() { f.provider.Close() }

// activeSeriesTestRepo serves one group that holds a real database and one stream.
type activeSeriesTestRepo struct {
	resourceSchema.Repository
	group  resourceSchema.Group
	stream *stream
}

func (r *activeSeriesTestRepo) LoadGroup(name string) (resourceSchema.Group, bool) {
	return r.group, name == activeSeriesTestGroup
}

func (r *activeSeriesTestRepo) LoadResource(metadata *commonv1.Metadata) (resourceSchema.Resource, bool) {
	if metadata.GetGroup() != activeSeriesTestGroup || metadata.GetName() != activeSeriesTestStream {
		return nil, false
	}
	return activeSeriesTestResource{s: r.stream}, true
}

type activeSeriesTestResource struct {
	resourceSchema.Resource
	s *stream
}

func (r activeSeriesTestResource) Delegated() resourceSchema.IndexListener { return r.s }

type activeSeriesTestGroupHolder struct {
	db io.Closer
}

func (activeSeriesTestGroupHolder) GetSchema() *commonv1.Group { return nil }

func (g activeSeriesTestGroupHolder) SupplyTSDB() io.Closer { return g.db }

// activeSeriesTestEnv is a service built by a constructor, with a real database opened by its supplier.
type activeSeriesTestEnv struct {
	registry *activeSeriesTestRegistry
	db       storage.TSDB[*tsTable, option]
	tracker  *storage.ActiveSeries
	// clock is the clock of the tracker.
	clock timestamp.MockClock
	// listener is the write subscription and importer the import subscription, both as PreRun registers them.
	listener bus.MessageListener
	importer bus.MessageListener
	svc      *standalone
}

// activeSeriesTestPipeline records the listeners a service subscribes.
type activeSeriesTestPipeline struct {
	queue.Server
	listeners map[bus.Topic]bus.MessageListener
}

func (p *activeSeriesTestPipeline) Subscribe(topic bus.Topic, listener bus.MessageListener) error {
	p.listeners[topic] = listener
	return nil
}

func (p *activeSeriesTestPipeline) RegisterChunkedSyncHandler(bus.Topic, queue.ChunkedSyncHandler) {}

// activeSeriesTestMetadata accepts the schema handler of the service and serves nothing else.
type activeSeriesTestMetadata struct {
	metadata.Repo
}

func (activeSeriesTestMetadata) RegisterHandler(string, schema.Kind, schema.EventHandler) {}

func newActiveSeriesTestEnv(t *testing.T, build func(omr observability.MetricsRegistry, pm protector.Memory) (Service, error)) *activeSeriesTestEnv {
	t.Helper()
	logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})
	restore := storage.SetActiveSeriesWindowForTest(activeSeriesTestWindow)
	t.Cleanup(restore)
	clock := timestamp.NewMockClock()
	clock.Set(time.Unix(0, 0).Add(1000 * activeSeriesTestWindow))
	t.Cleanup(storage.SetActiveSeriesClockForTest(clock))

	registry := &activeSeriesTestRegistry{}
	pm := protector.Nop{}
	svc, err := build(registry, pm)
	require.NoError(t, err)
	s := svc.(*standalone)
	s.FlagSet()
	s.root = t.TempDir()
	s.metadata = activeSeriesTestMetadata{}
	pipeline := &activeSeriesTestPipeline{listeners: make(map[bus.Topic]bus.MessageListener)}
	s.pipeline = pipeline
	ctx := context.WithValue(context.Background(), common.ContextNodeKey, common.Node{NodeID: "active-series-test"})
	require.NoError(t, s.PreRun(ctx))
	t.Cleanup(s.GracefulStop)

	groupSchema := &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: activeSeriesTestGroup},
		Catalog:  commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        activeSeriesTestShards,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	}
	opened, err := newSupplier(t.TempDir(), s, nil).OpenDB(groupSchema)
	require.NoError(t, err)
	t.Cleanup(func() { _ = opened.Close() })
	db, ok := opened.(storage.TSDB[*tsTable, option])
	require.True(t, ok)

	// The listeners PreRun subscribed share s.schemaRepo; it serves the database opened above.
	s.schemaRepo.Repository = &activeSeriesTestRepo{
		Repository: s.schemaRepo.Repository,
		group:      activeSeriesTestGroupHolder{db: opened},
		stream: openStream(streamSpec{schema: &databasev1.Stream{
			Metadata: &commonv1.Metadata{Group: activeSeriesTestGroup, Name: activeSeriesTestStream},
			Entity:   &databasev1.Entity{TagNames: []string{"id"}},
			TagFamilies: []*databasev1.TagFamilySpec{{Name: "searchable", Tags: []*databasev1.TagSpec{
				{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "msg", Type: databasev1.TagType_TAG_TYPE_STRING},
			}}},
		}}, s.l, pm, &s.schemaRepo, s.option.vectorized),
	}
	return &activeSeriesTestEnv{
		registry: registry,
		clock:    clock,
		db:       db,
		tracker:  storage.ActiveSeriesOf(db),
		listener: pipeline.listeners[data.TopicStreamWrite],
		importer: pipeline.listeners[data.TopicStreamImport],
		svc:      s,
	}
}

func activeSeriesTestStr(v string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

// activeSeriesTestEvents returns elements of the series id-0 .. id-(series-1), perSeries elements each, spread over the shards by series.
func activeSeriesTestEvents(prefix string, series, perSeries int) []any {
	ts := timestamppb.New(time.Now().Truncate(time.Millisecond))
	events := make([]any, 0, series*perSeries)
	for s := 0; s < series; s++ {
		id := fmt.Sprintf("%s-%d", prefix, s)
		for e := 0; e < perSeries; e++ {
			events = append(events, &streamv1.InternalWriteRequest{
				ShardId:      uint32(s % activeSeriesTestShards),
				EntityValues: []*modelv1.TagValue{activeSeriesTestStr(id)},
				Request: &streamv1.WriteRequest{
					Metadata: &commonv1.Metadata{Group: activeSeriesTestGroup, Name: activeSeriesTestStream},
					Element: &streamv1.ElementValue{
						ElementId: fmt.Sprintf("%s-%d", id, e),
						Timestamp: ts,
						TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
							activeSeriesTestStr(id), activeSeriesTestStr(fmt.Sprintf("message-%d", e)),
						}}},
					},
				},
			})
		}
	}
	return events
}

// write sends events to l and requires that every row is accepted.
func writeActiveSeriesEvents(t *testing.T, l bus.MessageListener, events []any) {
	t.Helper()
	recorder := common.NewWriteRecorder(time.Time{})
	l.Rev(common.WithWriteRecorder(context.Background(), recorder), bus.NewMessage(1, events))
	require.Empty(t, recorder.RowFailures())
	require.Empty(t, recorder.Failures())
}

// importActiveSeriesEvents sends events through the import subscription.
func importActiveSeriesEvents(t *testing.T, l bus.MessageListener, events []any) {
	t.Helper()
	frames := []any{&transferv1.ImportWrite{Init: &transferv1.ImportWriteInit{}}}
	for _, e := range events {
		frames = append(frames, &transferv1.ImportWrite{Row: &transferv1.ImportWrite_Stream{Stream: e.(*streamv1.InternalWriteRequest)}})
	}
	require.Nil(t, l.Rev(context.Background(), bus.NewMessage(1, frames)).Data(), "every imported row is written")
}

// inOneWindow runs writes inside the first window of the tracker, then moves the clock to the next window, so that the
// tracker can export the first one.
func (e *activeSeriesTestEnv) inOneWindow(writes func()) {
	writes()
	e.clock.Add(activeSeriesTestWindow)
}

func sumExported(values map[string]float64) float64 {
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum
}

func TestStandaloneActiveSeriesCountsNormalWrites(t *testing.T) {
	const series, perSeries = 100, 5
	env := newActiveSeriesTestEnv(t, standaloneBuilder)
	require.NotNil(t, env.tracker)
	assert.True(t, env.registry.registeredActiveSeries(), "standalone registers the active_series gauge")

	env.inOneWindow(func() { writeActiveSeriesEvents(t, env.listener, activeSeriesTestEvents("normal", series, perSeries)) })
	env.tracker.Collect()

	exported := env.registry.exported(t)
	assert.Len(t, exported, activeSeriesTestShards)
	assert.InEpsilon(t, series, sumExported(exported), 0.1, "N series with several elements each count about N, not N times the elements")
}

func TestStandaloneActiveSeriesCountsASeriesOnce(t *testing.T) {
	env := newActiveSeriesTestEnv(t, standaloneBuilder)
	require.NotNil(t, env.tracker)

	env.inOneWindow(func() {
		writeActiveSeriesEvents(t, env.listener, activeSeriesTestEvents("same", 1, 50))
		writeActiveSeriesEvents(t, env.listener, activeSeriesTestEvents("same", 1, 50))
	})
	env.tracker.Collect()

	exported := env.registry.exported(t)
	assert.Len(t, exported, 1)
	assert.InDelta(t, 1, sumExported(exported), 0.5)
}

func TestStandaloneActiveSeriesSkipsImport(t *testing.T) {
	const normal = 30
	env := newActiveSeriesTestEnv(t, standaloneBuilder)
	require.NotNil(t, env.tracker)

	env.inOneWindow(func() {
		importActiveSeriesEvents(t, env.importer, activeSeriesTestEvents("import", 80, 3))
		writeActiveSeriesEvents(t, env.listener, activeSeriesTestEvents("normal", normal, 3))
	})
	env.tracker.Collect()

	assert.InEpsilon(t, normal, sumExported(env.registry.exported(t)), 0.1, "only the series of the normal write are counted")
}

func TestStandaloneActiveSeriesImportOnlyExportsNothing(t *testing.T) {
	env := newActiveSeriesTestEnv(t, standaloneBuilder)
	require.NotNil(t, env.tracker)

	env.inOneWindow(func() {
		importActiveSeriesEvents(t, env.importer, activeSeriesTestEvents("import", 80, 3))
	})
	env.tracker.Collect()

	assert.Empty(t, env.registry.exported(t))
}

func TestStandaloneActiveSeriesAddDocsSteadyStateAllocs(t *testing.T) {
	env := newActiveSeriesTestEnv(t, standaloneBuilder)
	require.NotNil(t, env.tracker)
	docs := make(index.Documents, 100)
	for i := range docs {
		docs[i].DocID = uint64(i + 1)
	}
	env.tracker.AddDocs(0, docs)

	assert.Zero(t, testing.AllocsPerRun(100, func() { env.tracker.AddDocs(0, docs) }))
}

func TestDataServiceDoesNotCountActiveSeries(t *testing.T) {
	env := newActiveSeriesTestEnv(t, func(omr observability.MetricsRegistry, pm protector.Memory) (Service, error) {
		return NewDataSVC(nil, nil, omr, pm, nil)
	})
	assert.Nil(t, env.tracker)
	assert.Nil(t, storage.ActiveSeriesOf(env.db))
	assert.False(t, env.registry.registeredActiveSeries(), "no active_series gauge is registered in data mode")

	assert.NotPanics(t, func() {
		env.inOneWindow(func() { writeActiveSeriesEvents(t, env.listener, activeSeriesTestEvents("data", 50, 3)) })
		env.tracker.Collect()
		obsservice.MetricsCollector.Collect()
	})
	assert.Empty(t, env.registry.exported(t))
}

// publishLocal sends events to the write subscription of the service's local pipeline, which the in-process writers use.
func (e *activeSeriesTestEnv) publishLocal(t *testing.T, events []any) {
	t.Helper()
	_, err := e.svc.localPipeline.Publish(context.Background(), data.TopicStreamWrite, bus.NewMessage(1, events))
	require.NoError(t, err)
}

func TestStandaloneActiveSeriesCountsLocalPipelineWrites(t *testing.T) {
	const series = 40
	env := newActiveSeriesTestEnv(t, standaloneBuilder)
	require.NotNil(t, env.tracker)

	env.inOneWindow(func() { env.publishLocal(t, activeSeriesTestEvents("local", series, 3)) })
	env.tracker.Collect()

	assert.InEpsilon(t, series, sumExported(env.registry.exported(t)), 0.1, "the local pipeline subscribes the counting write listener")
}

// TestDataServiceDoesNotCountAnySubscription writes through every subscription of a data service that accepts rows:
// the write and import subscriptions of its pipeline, the internal write pipeline and the local pipeline.
func TestDataServiceDoesNotCountAnySubscription(t *testing.T) {
	internal := &activeSeriesTestPipeline{listeners: make(map[bus.Topic]bus.MessageListener)}
	env := newActiveSeriesTestEnv(t, func(omr observability.MetricsRegistry, pm protector.Memory) (Service, error) {
		return NewDataSVC(nil, nil, omr, pm, internal)
	})
	require.Nil(t, env.tracker)
	require.NotNil(t, env.listener)
	require.NotNil(t, env.importer)
	internalListener := internal.listeners[data.TopicStreamWrite]
	require.NotNil(t, internalListener)

	env.inOneWindow(func() {
		writeActiveSeriesEvents(t, env.listener, activeSeriesTestEvents("pipeline", 30, 2))
		importActiveSeriesEvents(t, env.importer, activeSeriesTestEvents("import", 30, 2))
		writeActiveSeriesEvents(t, internalListener, activeSeriesTestEvents("internal", 30, 2))
		env.publishLocal(t, activeSeriesTestEvents("local", 30, 2))
	})
	obsservice.MetricsCollector.Collect()

	assert.Empty(t, env.registry.exported(t))
	assert.False(t, env.registry.registeredActiveSeries(), "no active_series gauge is registered in data mode")
}

func TestServiceConstructorsSetCountActiveSeries(t *testing.T) {
	omr := observability.NewBypassRegistry()
	pm := protector.Nop{}
	standaloneSvc, err := NewStandalone(nil, nil, omr, pm)
	require.NoError(t, err)
	assert.True(t, standaloneSvc.(*standalone).countActiveSeries)
	dataSvc, err := NewDataSVC(nil, nil, omr, pm, nil)
	require.NoError(t, err)
	assert.False(t, dataSvc.(*standalone).countActiveSeries)
}

func standaloneBuilder(omr observability.MetricsRegistry, pm protector.Memory) (Service, error) {
	return NewStandalone(nil, nil, omr, pm)
}
