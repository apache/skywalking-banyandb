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

package measure

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	metadataschema "github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	activeSeriesTestWindow = 2 * time.Second
	activeSeriesTestSeries = 40
	activeSeriesTestRows   = 3
	activeSeriesShardNum   = 2
	activeSeriesMetricSfx  = "_active_series"
)

// activeSeriesRegistry is a metrics registry whose factories export to reg.
type activeSeriesRegistry struct {
	observability.MetricsRegistry
	reg *prometheus.Registry
}

func (r *activeSeriesRegistry) With(scope meter.Scope) observability.Factory {
	return activeSeriesFactory{Provider: prom.NewProvider(scope, r.reg)}
}

type activeSeriesFactory struct{ meter.Provider }

func (f activeSeriesFactory) NewCounter(name string, labelNames ...string) meter.Counter {
	return f.Counter(name, labelNames...)
}

func (f activeSeriesFactory) NewGauge(name string, labelNames ...string) meter.Gauge {
	return f.Gauge(name, labelNames...)
}

func (f activeSeriesFactory) NewHistogram(name string, buckets meter.Buckets, labelNames ...string) meter.Histogram {
	return f.Histogram(name, buckets, labelNames...)
}

func (f activeSeriesFactory) Close() { f.Provider.Close() }

// standaloneHarness is a schema repository over the real supplier, so groups are opened by OpenDB and measures by OpenResource.
type standaloneHarness struct {
	reg    *prometheus.Registry
	repo   *schemaRepo
	groups map[string]*commonv1.Group
	// clock is the clock of the trackers of the groups opened after the harness was created.
	clock timestamp.MockClock
}

// newStandaloneHarness opens a schema repository whose groups count active series when countActive is set, the way the
// standalone supplier does; a data node's supplier does not.
func newStandaloneHarness(t *testing.T, countActive bool) *standaloneHarness {
	t.Helper()
	restore := storage.SetActiveSeriesWindowForTest(activeSeriesTestWindow)
	t.Cleanup(restore)
	clock := timestamp.NewMockClock()
	clock.Set(time.Unix(0, 0).Add(1000 * activeSeriesTestWindow))
	t.Cleanup(storage.SetActiveSeriesClockForTest(clock))
	tmpPath, cleanup := test.Space(require.New(t))
	t.Cleanup(cleanup)
	h := &standaloneHarness{reg: prometheus.NewRegistry(), groups: make(map[string]*commonv1.Group), clock: clock}
	omr := &activeSeriesRegistry{MetricsRegistry: observability.BypassRegistry, reg: h.reg}
	l := logger.GetLogger("test-measure-active-series")

	ctrl := gomock.NewController(t)
	groupRegistry := metadataschema.NewMockGroup(ctrl)
	groupRegistry.EXPECT().GetGroup(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, name string) (*commonv1.Group, error) { return h.groups[name], nil }).AnyTimes()
	md := metadata.NewMockRepo(ctrl)
	md.EXPECT().GroupRegistry().Return(groupRegistry).AnyTimes()

	h.repo = &schemaRepo{
		path:          tmpPath,
		l:             l,
		metadata:      md,
		closingGroups: make(map[string]struct{}),
		role:          databasev1.Role_ROLE_DATA,
	}
	h.repo.ctx, h.repo.cancel = context.WithCancel(context.Background())
	sup := &supplier{
		path:       tmpPath,
		metadata:   md,
		l:          l,
		omr:        omr,
		pm:         protector.Nop{},
		schemaRepo: h.repo,
		option: option{
			protector:    protector.Nop{},
			mergePolicy:  newDefaultMergePolicyForTesting(),
			flushTimeout: time.Hour,
		},
		countActiveSeries: countActive,
	}
	h.repo.Repository = resourceSchema.NewRepository(md, l, sup, resourceSchema.NewMetrics(omr.With(metadataScope)))
	t.Cleanup(h.repo.Close)
	return h
}

func (h *standaloneHarness) addGroup(t *testing.T, name string) {
	t.Helper()
	h.groups[name] = &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: name},
		Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        activeSeriesShardNum,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	}
	h.repo.SendMetadataEvent(resourceSchema.MetadataEvent{Typ: resourceSchema.EventAddOrUpdate, Kind: resourceSchema.EventKindGroup, Metadata: h.groups[name]})
	_, err := h.repo.loadTSDB(name)
	require.NoError(t, err)
}

// putMeasure stores the measure the way a schema update does, through the repository's resource event.
func (h *standaloneHarness) putMeasure(m *databasev1.Measure) {
	h.repo.SendMetadataEvent(resourceSchema.MetadataEvent{Typ: resourceSchema.EventAddOrUpdate, Kind: resourceSchema.EventKindResource, Metadata: m})
}

func (h *standaloneHarness) collect(t *testing.T, group string) {
	t.Helper()
	tsdb, err := h.repo.loadTSDB(group)
	require.NoError(t, err)
	storage.ActiveSeriesOf(tsdb).Collect()
}

// activeSeries returns the exported active_series of group by shard label.
func (h *standaloneHarness) activeSeries(t *testing.T, group string) map[string]float64 {
	t.Helper()
	families, err := h.reg.Gather()
	require.NoError(t, err)
	values := make(map[string]float64)
	for _, family := range families {
		if !strings.HasSuffix(family.GetName(), activeSeriesMetricSfx) {
			continue
		}
		for _, m := range family.GetMetric() {
			labels := make(map[string]string)
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["group"] == group {
				values[labels["shard"]] = m.GetGauge().GetValue()
			}
		}
	}
	return values
}

func (h *standaloneHarness) activeTotal(t *testing.T, group string) float64 {
	t.Helper()
	var total float64
	for _, v := range h.activeSeries(t, group) {
		total += v
	}
	return total
}

func (h *standaloneHarness) write(t *testing.T, listener bus.MessageListener, events []any) {
	t.Helper()
	recorder := common.NewWriteRecorder(time.Time{})
	listener.Rev(common.WithWriteRecorder(context.Background(), recorder), bus.NewMessage(1, events))
	require.Empty(t, recorder.RowFailures(), "no row of the batch is dropped")
}

func (h *standaloneHarness) listener() *writeCallback {
	return setUpWriteCallback(h.repo.l, h.repo, 100, protector.Nop{}, nil)
}

func activeSeriesMeasure(group, name string, indexMode bool, shardingKey ...string) *databasev1.Measure {
	m := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Group: group, Name: name, ModRevision: 1},
		Entity:   &databasev1.Entity{TagNames: []string{"a", "b"}},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "a", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "b", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "c", Type: databasev1.TagType_TAG_TYPE_STRING},
		}}},
		IndexMode: indexMode,
	}
	if !indexMode {
		m.Fields = []*databasev1.FieldSpec{{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT, EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA}}
	}
	if len(shardingKey) > 0 {
		m.ShardingKey = &databasev1.ShardingKey{TagNames: shardingKey}
	}
	return m
}

func strTag(v string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

// seriesWrites returns activeSeriesTestRows rows for each of activeSeriesTestSeries series of m.
func seriesWrites(m *databasev1.Measure) []*measurev1.InternalWriteRequest {
	base := time.Now().Truncate(time.Millisecond)
	var reqs []*measurev1.InternalWriteRequest
	for i := 0; i < activeSeriesTestSeries; i++ {
		for row := 0; row < activeSeriesTestRows; row++ {
			dp := &measurev1.DataPointValue{
				Timestamp: timestamppb.New(base.Add(time.Duration(row) * time.Millisecond)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
					strTag("svc"), strTag(fmt.Sprintf("instance-%d", i)), strTag("c"),
				}}},
			}
			if !m.IndexMode {
				dp.Fields = []*modelv1.FieldValue{{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(row)}}}}
			}
			reqs = append(reqs, &measurev1.InternalWriteRequest{
				Request:      &measurev1.WriteRequest{Metadata: m.Metadata, DataPoint: dp, MessageId: uint64(time.Now().UnixNano())},
				ShardId:      uint32(i % activeSeriesShardNum),
				EntityValues: []*modelv1.TagValue{strTag("svc"), strTag(fmt.Sprintf("instance-%d", i))},
			})
		}
	}
	return reqs
}

func asEvents(reqs []*measurev1.InternalWriteRequest) []any {
	events := make([]any, len(reqs))
	for i := range reqs {
		events[i] = reqs[i]
	}
	return events
}

func asImports(reqs []*measurev1.InternalWriteRequest) []any {
	events := make([]any, len(reqs))
	for i := range reqs {
		events[i] = &data.ImportedMeasureWrite{InternalWriteRequest: reqs[i]}
	}
	return events
}

// nextWindow moves the clock of the trackers to the start of the next window.
func (h *standaloneHarness) nextWindow() {
	h.clock.Add(activeSeriesTestWindow)
}

func TestStandaloneWriteCountsActiveSeries(t *testing.T) {
	h := newStandaloneHarness(t, true)
	counted := activeSeriesMeasure("g-counted", "m", false)
	imported := activeSeriesMeasure("g-import", "m", false)
	topN := activeSeriesMeasure("g-topn", TopNSchemaName, false)
	indexMode := activeSeriesMeasure("g-index", "m", true)
	for _, m := range []*databasev1.Measure{counted, imported, topN, indexMode} {
		h.addGroup(t, m.Metadata.Group)
		h.putMeasure(m)
	}
	dataNode := newStandaloneHarness(t, false)
	dataNodeMeasure := activeSeriesMeasure("g-data", "m", false)
	dataNode.addGroup(t, "g-data")
	dataNode.putMeasure(dataNodeMeasure)
	tsdb, err := dataNode.repo.loadTSDB("g-data")
	require.NoError(t, err)
	require.Nil(t, storage.ActiveSeriesOf(tsdb), "a database that does not count active series has no tracker")

	h.write(t, h.listener(), asEvents(seriesWrites(counted)))
	h.write(t, withoutActiveSeries(h.listener()), asImports(seriesWrites(imported)))
	h.write(t, h.listener(), asEvents(seriesWrites(topN)))
	h.write(t, h.listener(), asEvents(seriesWrites(indexMode)))
	dataNode.write(t, dataNode.listener(), asEvents(seriesWrites(dataNodeMeasure)))
	h.nextWindow()
	for _, group := range []string{"g-counted", "g-import", "g-topn", "g-index"} {
		h.collect(t, group)
	}
	dataNode.collect(t, "g-data")

	assert.InDelta(t, activeSeriesTestSeries, h.activeTotal(t, "g-counted"), 2, "each series counts once however many rows it has")
	assert.Len(t, h.activeSeries(t, "g-counted"), activeSeriesShardNum, "every shard that was written to exports its own value")
	assert.InDelta(t, activeSeriesTestSeries, h.activeTotal(t, "g-index"), 2, "an index-mode measure counts")
	assert.Empty(t, h.activeSeries(t, "g-import"), "the import listener does not count")
	assert.Empty(t, h.activeSeries(t, "g-topn"), "the Top-N result measure does not count")
	assert.Empty(t, dataNode.activeSeries(t, "g-data"), "a data node does not count")
}

func TestStandaloneSchemaUpdateRefreshesActiveSeriesCounting(t *testing.T) {
	h := newStandaloneHarness(t, true)
	const group = "g-update"
	safe := activeSeriesMeasure(group, "m", false)
	h.addGroup(t, group)
	h.putMeasure(safe)
	listener := h.listener()
	md := safe.Metadata

	first, ok := h.repo.loadMeasure(md)
	require.True(t, ok)
	require.True(t, first.countActiveSeries)

	// update replaces the measure through the repository, as a schema update does, with a higher revision.
	update := func(t *testing.T, revision int64, shardingKey ...string) *measure {
		t.Helper()
		m := activeSeriesMeasure(group, "m", false, shardingKey...)
		m.Metadata.ModRevision = revision
		h.putMeasure(m)
		got, found := h.repo.loadMeasure(md)
		require.True(t, found)
		return got
	}

	h.write(t, listener, asEvents(seriesWrites(safe)))
	h.nextWindow()
	h.collect(t, group)
	assert.InDelta(t, activeSeriesTestSeries, h.activeTotal(t, group), 2, "a measure without a sharding key counts")

	unsafe := update(t, 2, "c")
	require.NotSame(t, first, unsafe, "the schema update delivers a new measure object to the write callback")
	require.False(t, unsafe.countActiveSeries)
	h.nextWindow()
	h.collect(t, group)
	h.write(t, listener, asEvents(seriesWrites(safe)))
	h.nextWindow()
	h.collect(t, group)
	assert.Empty(t, h.activeSeries(t, group), "a sharding key outside the entity stops the counting")

	restored := update(t, 3)
	require.NotSame(t, unsafe, restored)
	require.True(t, restored.countActiveSeries)
	h.nextWindow()
	h.collect(t, group)
	h.write(t, listener, asEvents(seriesWrites(safe)))
	h.nextWindow()
	h.collect(t, group)
	assert.InDelta(t, activeSeriesTestSeries, h.activeTotal(t, group), 2, "removing the sharding key resumes the counting")
}

// TestServiceSuppliersCountActiveSeriesByMode opens a group with the supplier each service builds: the standalone service's
// database tracks active series, the data service's has no tracker.
func TestServiceSuppliersCountActiveSeriesByMode(t *testing.T) {
	h := newStandaloneHarness(t, true)
	omr := &activeSeriesRegistry{MetricsRegistry: observability.BypassRegistry, reg: h.reg}
	l := logger.GetLogger("test-measure-active-series-supplier")
	opt := option{protector: protector.Nop{}, mergePolicy: newDefaultMergePolicyForTesting(), flushTimeout: time.Hour}
	group := &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: "g-supplier"},
		Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        activeSeriesShardNum,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	}
	standaloneSupplier := newSupplier(t.TempDir(), &standalone{l: l, pm: protector.Nop{}, omr: omr, option: opt}, h.repo, nil)
	dataSupplier := newDataSupplier(t.TempDir(), &dataSVC{l: l, pm: protector.Nop{}, omr: omr, option: opt}, h.repo, nil)

	standaloneDB, err := standaloneSupplier.OpenDB(group)
	require.NoError(t, err)
	t.Cleanup(func() { _ = standaloneDB.Close() })
	dataDB, err := dataSupplier.OpenDB(group)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataDB.Close() })

	assert.NotNil(t, storage.ActiveSeriesOf(standaloneDB), "the standalone service counts active series")
	assert.Nil(t, storage.ActiveSeriesOf(dataDB), "the data service does not count active series")
}
