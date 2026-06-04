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

package lifecycle

import (
	"context"
	"encoding/base64"
	"fmt"
	gofs "io/fs"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumpmeasure "github.com/apache/skywalking-banyandb/banyand/internal/dump/measure"
	dumpstream "github.com/apache/skywalking-banyandb/banyand/internal/dump/stream"
	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	metadataservice "github.com/apache/skywalking-banyandb/banyand/metadata/service"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	localfs "github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// TestDecodeSeriesEntityValues_RoundTrip verifies that a marshaled Series
// round-trips through decodeSeriesEntityValues.
func TestDecodeSeriesEntityValues_RoundTrip(t *testing.T) {
	t.Run("subject_with_string_entity_tags", func(t *testing.T) {
		original := &pbv1.Series{
			Subject: "service_cpm_minute",
			EntityValues: []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "checkout"}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "us-east-1"}}},
			},
		}
		require.NoError(t, original.Marshal())

		subject, evList, err := decodeSeriesEntityValues(original.Buffer)
		require.NoError(t, err)
		assert.Equal(t, "service_cpm_minute", subject)
		require.Len(t, evList, 2)
		assert.Equal(t, "checkout", evList[0].GetStr().GetValue())
		assert.Equal(t, "us-east-1", evList[1].GetStr().GetValue())
	})

	t.Run("empty_bytes_rejected", func(t *testing.T) {
		_, _, err := decodeSeriesEntityValues(nil)
		require.Error(t, err)
	})

	t.Run("empty_subject_rejected", func(t *testing.T) {
		s := &pbv1.Series{Subject: "", EntityValues: nil}
		require.NoError(t, s.Marshal())
		_, _, err := decodeSeriesEntityValues(s.Buffer)
		require.Error(t, err)
	})
}

// TestTagTypeToValueType verifies every TagType maps to the expected ValueType.
func TestTagTypeToValueType(t *testing.T) {
	cases := []struct {
		in   databasev1.TagType
		want pbv1.ValueType
	}{
		{databasev1.TagType_TAG_TYPE_STRING, pbv1.ValueTypeStr},
		{databasev1.TagType_TAG_TYPE_INT, pbv1.ValueTypeInt64},
		{databasev1.TagType_TAG_TYPE_DATA_BINARY, pbv1.ValueTypeBinaryData},
		{databasev1.TagType_TAG_TYPE_STRING_ARRAY, pbv1.ValueTypeStrArr},
		{databasev1.TagType_TAG_TYPE_INT_ARRAY, pbv1.ValueTypeInt64Arr},
		{databasev1.TagType_TAG_TYPE_TIMESTAMP, pbv1.ValueTypeTimestamp},
		{databasev1.TagType_TAG_TYPE_UNSPECIFIED, pbv1.ValueTypeUnknown},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, tagTypeToValueType(c.in), "TagType=%v", c.in)
	}
}

// TestFieldTypeToValueType verifies every FieldType maps to the expected ValueType.
func TestFieldTypeToValueType(t *testing.T) {
	cases := []struct {
		in   databasev1.FieldType
		want pbv1.ValueType
	}{
		{databasev1.FieldType_FIELD_TYPE_STRING, pbv1.ValueTypeStr},
		{databasev1.FieldType_FIELD_TYPE_INT, pbv1.ValueTypeInt64},
		{databasev1.FieldType_FIELD_TYPE_FLOAT, pbv1.ValueTypeFloat64},
		{databasev1.FieldType_FIELD_TYPE_DATA_BINARY, pbv1.ValueTypeBinaryData},
		{databasev1.FieldType_FIELD_TYPE_UNSPECIFIED, pbv1.ValueTypeUnknown},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, fieldTypeToValueType(c.in), "FieldType=%v", c.in)
	}
}

// TestBuildEntityTagIndex verifies positional mapping of Entity.TagNames to EntityValues.
func TestBuildEntityTagIndex(t *testing.T) {
	entity := &databasev1.Entity{TagNames: []string{"service", "region"}}
	evList := pbv1.EntityValues{
		&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "checkout"}}},
		&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "us-east-1"}}},
	}
	idx := buildEntityTagIndex(entity, evList)
	require.Len(t, idx, 2)
	assert.Equal(t, "checkout", idx["service"].GetStr().GetValue())
	assert.Equal(t, "us-east-1", idx["region"].GetStr().GetValue())

	// A nil entity yields no index.
	assert.Nil(t, buildEntityTagIndex(nil, evList))

	// Fewer values than names: trailing names are dropped, no panic.
	short := buildEntityTagIndex(entity, evList[:1])
	require.Len(t, short, 1)
	assert.Equal(t, "checkout", short["service"].GetStr().GetValue())
}

// TestDeriveMergedRuleToTag verifies index-rule -> tag-spec resolution,
// including skipping rules for undeclared tags.
func TestDeriveMergedRuleToTag(t *testing.T) {
	measures := []*databasev1.Measure{
		{
			Metadata: &commonv1.Metadata{Name: "m1"},
			TagFamilies: []*databasev1.TagFamilySpec{
				{
					Name: "searchable",
					Tags: []*databasev1.TagSpec{
						{Name: "endpoint_id", Type: databasev1.TagType_TAG_TYPE_STRING},
						{Name: "rpc_latency_buckets", Type: databasev1.TagType_TAG_TYPE_INT_ARRAY},
					},
				},
			},
		},
	}
	rules := []*databasev1.IndexRule{
		{
			Metadata: &commonv1.Metadata{Id: 101, Name: "idx_endpoint"},
			Tags:     []string{"endpoint_id"},
		},
		{
			Metadata: &commonv1.Metadata{Id: 102, Name: "idx_buckets"},
			Tags:     []string{"rpc_latency_buckets"},
		},
		{
			Metadata: &commonv1.Metadata{Id: 103, Name: "idx_unknown"},
			Tags:     []string{"not_declared_anywhere"},
		},
		{
			// Multi-tag (composite) rule: indexed under one ruleID field as a
			// composite term, not decodable into individual tag values.
			Metadata: &commonv1.Metadata{Id: 104, Name: "idx_multi"},
			Tags:     []string{"endpoint_id", "rpc_latency_buckets"},
		},
	}
	bindings := []*databasev1.IndexRuleBinding{
		{
			Subject: &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: "m1"},
			Rules:   []string{"idx_endpoint", "idx_buckets", "idx_unknown", "idx_multi"},
		},
	}
	out := deriveMergedRuleToTag(measures, rules, bindings)
	require.Len(t, out, 2)
	assert.Equal(t, dump.IndexedTagSpec{Family: "searchable", Name: "endpoint_id", Type: pbv1.ValueTypeStr}, out[101])
	assert.Equal(t, dump.IndexedTagSpec{Family: "searchable", Name: "rpc_latency_buckets", Type: pbv1.ValueTypeInt64Arr}, out[102])
	_, hasUnknown := out[103]
	assert.False(t, hasUnknown, "rule whose tag is undefined in the bound measure should be skipped")
	_, hasMulti := out[104]
	assert.False(t, hasMulti, "multi-tag (composite) rule must be skipped: its value is not a single decodable tag")
}

// TestDeriveMergedRuleToTag_PerMeasureScope pins the collision fix: two measures
// declare a tag with the same name but a different family/type. Each rule must
// resolve within the measure it is bound to, not a group-wide merge.
func TestDeriveMergedRuleToTag_PerMeasureScope(t *testing.T) {
	measures := []*databasev1.Measure{
		{
			Metadata: &commonv1.Metadata{Name: "m_str"},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "fa", Tags: []*databasev1.TagSpec{{Name: "x", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
		},
		{
			Metadata: &commonv1.Metadata{Name: "m_int"},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "fb", Tags: []*databasev1.TagSpec{{Name: "x", Type: databasev1.TagType_TAG_TYPE_INT}}},
			},
		},
	}
	rules := []*databasev1.IndexRule{
		{Metadata: &commonv1.Metadata{Id: 1, Name: "r_str"}, Tags: []string{"x"}},
		{Metadata: &commonv1.Metadata{Id: 2, Name: "r_int"}, Tags: []string{"x"}},
	}
	bindings := []*databasev1.IndexRuleBinding{
		{Subject: &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: "m_str"}, Rules: []string{"r_str"}},
		{Subject: &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: "m_int"}, Rules: []string{"r_int"}},
	}
	out := deriveMergedRuleToTag(measures, rules, bindings)
	require.Len(t, out, 2)
	assert.Equal(t, dump.IndexedTagSpec{Family: "fa", Name: "x", Type: pbv1.ValueTypeStr}, out[1],
		"rule bound to m_str must resolve tag x as STRING in family fa")
	assert.Equal(t, dump.IndexedTagSpec{Family: "fb", Name: "x", Type: pbv1.ValueTypeInt64}, out[2],
		"rule bound to m_int must resolve tag x as INT in family fb")
}

// TestDeriveMergedRuleToTag_EmptyInputs verifies nil/empty inputs return nil,
// including the no-bindings case (no rule is applied to any measure).
func TestDeriveMergedRuleToTag_EmptyInputs(t *testing.T) {
	assert.Nil(t, deriveMergedRuleToTag(nil, nil, nil))
	assert.Nil(t, deriveMergedRuleToTag(
		[]*databasev1.Measure{{Metadata: &commonv1.Metadata{Name: "m"}}},
		[]*databasev1.IndexRule{{Metadata: &commonv1.Metadata{Id: 1, Name: "r"}, Tags: []string{"x"}}},
		nil,
	), "no bindings must yield nil")
	assert.Nil(t, deriveMergedRuleToTag([]*databasev1.Measure{{Metadata: &commonv1.Metadata{Name: "m"}}}, nil,
		[]*databasev1.IndexRuleBinding{{Subject: &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: "m"}}}))
}

func stringTagValue(v string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

func intTagValue(v int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}}
}

func intFieldValue(v int64) *modelv1.FieldValue {
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: v}}}
}

func floatFieldValue(v float64) *modelv1.FieldValue {
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: v}}}
}

func mustMarshalTag(t *testing.T, tv *modelv1.TagValue) []byte {
	t.Helper()
	b, err := pbv1.MarshalTagValue(tv)
	require.NoError(t, err)
	return b
}

// TestBuildMeasureTagFamilies_PriorityOrder verifies tag resolution priority:
// column store > IndexResolver > EntityValues > null.
func TestBuildMeasureTagFamilies_PriorityOrder(t *testing.T) {
	families := []*databasev1.TagFamilySpec{
		{
			Name: "primary",
			Tags: []*databasev1.TagSpec{
				{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING}, // entity tag
				{Name: "method", Type: databasev1.TagType_TAG_TYPE_STRING},  // column tag
			},
		},
		{
			Name: "searchable",
			Tags: []*databasev1.TagSpec{
				{Name: "endpoint", Type: databasev1.TagType_TAG_TYPE_STRING}, // index-only tag
				{Name: "absent", Type: databasev1.TagType_TAG_TYPE_STRING},   // truly missing
			},
		},
	}
	entity := &databasev1.Entity{TagNames: []string{"service"}}
	evList := pbv1.EntityValues{stringTagValue("checkout")}

	row := dumpmeasure.Row{
		Tags: map[string][]byte{
			"primary.method": mustMarshalTag(t, stringTagValue("POST")),
		},
		TagTypes: map[string]pbv1.ValueType{
			"primary.method": pbv1.ValueTypeStr,
		},
	}
	indexedTyped := map[string]*modelv1.TagValue{
		"searchable.endpoint": stringTagValue("/checkout/v1"),
	}

	out := buildMeasureTagFamilies(families, entity, row, evList, indexedTyped)
	require.Len(t, out, 2)
	require.Len(t, out[0].Tags, 2)
	require.Len(t, out[1].Tags, 2)

	assert.Equal(t, "checkout", out[0].Tags[0].GetStr().GetValue())
	assert.Equal(t, "POST", out[0].Tags[1].GetStr().GetValue())
	assert.Equal(t, "/checkout/v1", out[1].Tags[0].GetStr().GetValue())
	assert.True(t, proto.Equal(pbv1.NullTagValue, out[1].Tags[1]),
		"absent tag must fall back to NullTagValue")
}

// TestBuildMeasureTagFamilies_ColumnOverridesIndexed verifies column store
// wins over index resolver when both supply the same tag.
func TestBuildMeasureTagFamilies_ColumnOverridesIndexed(t *testing.T) {
	families := []*databasev1.TagFamilySpec{
		{
			Name: "primary",
			Tags: []*databasev1.TagSpec{
				{Name: "biz_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		},
	}
	row := dumpmeasure.Row{
		Tags: map[string][]byte{
			"primary.biz_id": mustMarshalTag(t, stringTagValue("from-column")),
		},
		TagTypes: map[string]pbv1.ValueType{
			"primary.biz_id": pbv1.ValueTypeStr,
		},
	}
	indexedTyped := map[string]*modelv1.TagValue{
		"primary.biz_id": stringTagValue("from-index"),
	}
	out := buildMeasureTagFamilies(families, nil, row, nil, indexedTyped)
	require.Len(t, out, 1)
	require.Len(t, out[0].Tags, 1)
	assert.Equal(t, "from-column", out[0].Tags[0].GetStr().GetValue(),
		"column store must win over index resolver when both supply the same tag")
}

// TestBuildStreamTagFamilies_EntityAndColumn verifies stream tag resolution:
// column store > EntityValues > null.
func TestBuildStreamTagFamilies_EntityAndColumn(t *testing.T) {
	families := []*databasev1.TagFamilySpec{
		{
			Name: "trace",
			Tags: []*databasev1.TagSpec{
				{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING}, // entity tag
				{Name: "url", Type: databasev1.TagType_TAG_TYPE_STRING},     // column tag
				{Name: "absent", Type: databasev1.TagType_TAG_TYPE_STRING},  // null
			},
		},
	}
	entity := &databasev1.Entity{TagNames: []string{"service"}}
	evList := pbv1.EntityValues{stringTagValue("checkout")}
	row := dumpstream.Row{
		Tags: map[string][]byte{
			"trace.url": mustMarshalTag(t, stringTagValue("/checkout/v1")),
		},
		TagTypes: map[string]pbv1.ValueType{
			"trace.url": pbv1.ValueTypeStr,
		},
	}
	out := buildStreamTagFamilies(families, entity, row, evList)
	require.Len(t, out, 1)
	require.Len(t, out[0].Tags, 3)
	assert.Equal(t, "checkout", out[0].Tags[0].GetStr().GetValue())
	assert.Equal(t, "/checkout/v1", out[0].Tags[1].GetStr().GetValue())
	assert.True(t, proto.Equal(pbv1.NullTagValue, out[0].Tags[2]),
		"absent tag must fall back to NullTagValue")
}

const (
	roundtripMeasureGroup = "lc_rt_measure_group"
	roundtripMeasureName  = "lc_rt_measure"
	roundtripStreamGroup  = "lc_rt_stream_group"
	roundtripStreamName   = "lc_rt_stream"
	roundtripTraceGroup   = "lc_rt_trace_group"
	roundtripTraceName    = "lc_rt_trace"
)

// TestRoundtrip_Measure writes a measure row, flushes to disk, then verifies
// buildWriteRequest reconstructs a proto-equal WriteRequest.
func TestRoundtrip_Measure(t *testing.T) {
	req := require.New(t)
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	gomega.RegisterFailHandler(func(message string, _ ...int) { panic(message) })

	pipeline := queue.Local()
	metaSvc, err := metadataservice.NewService()
	req.NoError(err)
	metricSvc := obsservice.NewMetricService(metaSvc, pipeline, "test", nil)
	pm := protector.NewMemory(metricSvc)
	measureSvc, err := measure.NewStandalone(metaSvc, pipeline, nil, metricSvc, pm)
	req.NoError(err)

	metaPath, metaDefer, err := test.NewSpace()
	req.NoError(err)
	defer metaDefer()
	ports, err := test.AllocateFreePorts(1)
	req.NoError(err)
	rootPath, rootDefer, err := test.NewSpace()
	req.NoError(err)
	defer rootDefer()

	flags := []string{
		"--schema-server-root-path=" + metaPath,
		fmt.Sprintf("--schema-server-grpc-port=%d", ports[0]),
		"--schema-server-grpc-host=127.0.0.1",
		"--measure-root-path=" + rootPath,
		"--measure-flush-timeout=200ms",
	}
	moduleDefer := test.SetupModules(flags, pipeline, metaSvc, measureSvc)
	moduleStopped := false
	defer func() {
		if !moduleStopped {
			moduleDefer()
		}
	}()

	registerRoundtripMeasureSchema(t, metaSvc)
	require.Eventually(t, func() bool {
		_, ok := measureSvc.LoadGroup(roundtripMeasureGroup)
		return ok
	}, 30*time.Second, 200*time.Millisecond, "measure group not loaded")
	time.Sleep(time.Second)

	tsA := time.Now().Truncate(time.Millisecond)
	tsB := tsA.Add(time.Hour)
	// Two rows with fully distinct entity/tags/fields/version/timestamp, routed
	// to different shards under shardNum=2 (series=ent-1 -> shard 0,
	// ent-4 -> shard 1), so a parser that swaps or conflates rows fails.
	type measureExpect struct {
		wr        *measurev1.WriteRequest
		series    string
		wantShard uint32
	}
	entries := []measureExpect{
		{buildMeasureWR(tsA, "ent-1", "alpha", 7, 111, 1.5, 42), "ent-1", 0},
		{buildMeasureWR(tsB, "ent-4", "bravo", 8888, 222222, 2.71828, 99), "ent-4", 1},
	}
	expect := make(map[string]measureExpect, len(entries))

	bp := pipeline.NewBatchPublisher(5 * time.Second)
	for i, e := range entries {
		expect[e.series] = e
		_, errPub := bp.Publish(context.TODO(), data.TopicMeasureWrite, bus.NewMessage(bus.MessageID(i+1), &measurev1.InternalWriteRequest{
			ShardId:      e.wantShard,
			EntityValues: []*modelv1.TagValue{stringTagValue(e.series)},
			Request:      e.wr,
		}))
		req.NoError(errPub)
	}
	closeNodeErrs, closeErr := bp.Close()
	req.NoError(closeErr)
	req.Empty(closeNodeErrs)

	var partDirs []string
	require.Eventually(t, func() bool {
		partDirs = findRoundtripPartDirs(rootPath)
		return len(partDirs) >= 2
	}, 30*time.Second, 200*time.Millisecond, "measure parts for both shards not flushed")

	// Construct the replayer while the metadata service is still running so it
	// can enumerate measures + index rules; the IndexResolver below needs
	// exclusive access to the bluge index dir, so the service must be stopped
	// before any reader opens the segment.
	replayer, err := newMeasureRowReplayer(context.TODO(), roundtripMeasureGroup, 2, nil, pipeline,
		metaSvc, localfs.NewLocalFileSystem(), logger.GetLogger("test-replayer"), nil)
	req.NoError(err)
	defer replayer.Close()
	// Warm the schema cache so buildWriteRequest does not need the metadata
	// service after we stop it below.
	_, err = replayer.loadSchema(roundtripMeasureName)
	req.NoError(err)

	moduleDefer()
	moduleStopped = true

	fileSystem := localfs.NewLocalFileSystem()
	seen := make(map[string]bool)
	for _, partDir := range partDirs {
		partID, parseErr := strconv.ParseUint(filepath.Base(partDir), 16, 64)
		req.NoError(parseErr)
		shardPath := filepath.Dir(partDir)
		segPath := filepath.Dir(shardPath)
		reader, openErr := dumpmeasure.OpenPart(partID, shardPath, fileSystem)
		req.NoError(openErr)
		ir, irErr := replayer.loadIndexResolver(segPath)
		req.NoError(irErr)
		reader.SetIndexResolver(ir)
		it := reader.Iterator()
		for it.Next() {
			row := it.Row()
			wr, iwr, buildErr := replayer.buildWriteRequest(ir, row)
			req.NoError(buildErr)
			series := wr.GetDataPoint().GetTagFamilies()[0].GetTags()[0].GetStr().GetValue()
			e, ok := expect[series]
			require.Truef(t, ok, "replay produced an unexpected series %q", series)
			assertMeasureWriteRequestEqual(t, e.wr, wr)
			require.NotNil(t, iwr.Request, "iwr must wrap a WriteRequest")
			require.Equal(t, wr, iwr.Request, "iwr.Request must point at the same WriteRequest")
			require.Equalf(t, e.wantShard, iwr.ShardId, "series %q must route to shard %d under shardNum=2", series, e.wantShard)
			require.Equal(t, pbv1.EntityValues{stringTagValue(series)}.Encode(), iwr.EntityValues,
				"iwr.EntityValues must encode the entity tag (series)")
			require.Falsef(t, seen[series], "series %q reconstructed twice", series)
			seen[series] = true
		}
		require.NoError(t, it.Err())
		it.Close()
		reader.Close()
	}
	require.Len(t, seen, len(entries), "both rows (shard 0 and shard 1) must roundtrip exactly once")
}

// TestRoundtrip_Stream writes a stream element, flushes to disk, then verifies
// buildWriteRequest reconstructs a proto-equal WriteRequest.
func TestRoundtrip_Stream(t *testing.T) {
	req := require.New(t)
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	gomega.RegisterFailHandler(func(message string, _ ...int) { panic(message) })

	pipeline := queue.Local()
	metaSvc, err := metadataservice.NewService()
	req.NoError(err)
	metricSvc := obsservice.NewMetricService(metaSvc, pipeline, "test", nil)
	pm := protector.NewMemory(metricSvc)
	streamSvc, err := stream.NewService(metaSvc, pipeline, metricSvc, pm, nil)
	req.NoError(err)

	metaPath, metaDefer, err := test.NewSpace()
	req.NoError(err)
	defer metaDefer()
	ports, err := test.AllocateFreePorts(1)
	req.NoError(err)
	rootPath, rootDefer, err := test.NewSpace()
	req.NoError(err)
	defer rootDefer()

	flags := []string{
		"--schema-server-root-path=" + metaPath,
		fmt.Sprintf("--schema-server-grpc-port=%d", ports[0]),
		"--schema-server-grpc-host=127.0.0.1",
		"--stream-root-path=" + rootPath,
		"--stream-flush-timeout=200ms",
	}
	moduleDefer := test.SetupModules(flags, pipeline, metaSvc, streamSvc)
	moduleStopped := false
	defer func() {
		if !moduleStopped {
			moduleDefer()
		}
	}()

	registerRoundtripStreamSchema(t, metaSvc)
	require.Eventually(t, func() bool {
		_, ok := streamSvc.LoadGroup(roundtripStreamGroup)
		return ok
	}, 30*time.Second, 200*time.Millisecond, "stream group not loaded")
	time.Sleep(time.Second)

	tsA := time.Now().Truncate(time.Millisecond)
	tsB := tsA.Add(time.Hour)
	// Two rows routed to different shards under shardNum=2 (series=ent-3 -> shard 0,
	// ent-1 -> shard 1), with distinct tags, element ids and timestamps.
	type streamExpect struct {
		wr        *streamv1.WriteRequest
		series    string
		wantShard uint32
	}
	entries := []streamExpect{
		{buildStreamWR(tsA, "ent-3", "alpha", 7, "elem-a"), "ent-3", 0},
		{buildStreamWR(tsB, "ent-1", "bravo", 8888, "elem-b"), "ent-1", 1},
	}
	expect := make(map[string]streamExpect, len(entries))

	bp := pipeline.NewBatchPublisher(5 * time.Second)
	for i, e := range entries {
		expect[e.series] = e
		_, errPub := bp.Publish(context.TODO(), data.TopicStreamWrite, bus.NewMessage(bus.MessageID(i+1), &streamv1.InternalWriteRequest{
			ShardId:      e.wantShard,
			EntityValues: []*modelv1.TagValue{stringTagValue(e.series)},
			Request:      e.wr,
		}))
		req.NoError(errPub)
	}
	closeNodeErrs, closeErr := bp.Close()
	req.NoError(closeErr)
	req.Empty(closeNodeErrs)

	var partDirs []string
	require.Eventually(t, func() bool {
		partDirs = findRoundtripPartDirs(rootPath)
		return len(partDirs) >= 2
	}, 30*time.Second, 200*time.Millisecond, "stream parts for both shards not flushed")

	replayer := newStreamRowReplayer(roundtripStreamGroup, 2, nil, pipeline,
		metaSvc, localfs.NewLocalFileSystem(), logger.GetLogger("test-replayer"), nil)
	defer replayer.Close()
	// Warm the schema cache so buildWriteRequest does not need the metadata
	// service after we stop it below.
	_, err = replayer.loadSchema(context.TODO(), roundtripStreamName)
	req.NoError(err)

	moduleDefer()
	moduleStopped = true

	fileSystem := localfs.NewLocalFileSystem()
	seen := make(map[string]bool)
	for _, partDir := range partDirs {
		partID, parseErr := strconv.ParseUint(filepath.Base(partDir), 16, 64)
		req.NoError(parseErr)
		shardPath := filepath.Dir(partDir)
		segPath := filepath.Dir(shardPath)
		reader, openErr := dumpstream.OpenPart(partID, shardPath, fileSystem)
		req.NoError(openErr)
		ir, irErr := replayer.loadIndexResolver(segPath)
		req.NoError(irErr)
		reader.SetIndexResolver(ir)
		it := reader.Iterator()
		for it.Next() {
			row := it.Row()
			wr, iwr, buildErr := replayer.buildWriteRequest(context.TODO(), row)
			req.NoError(buildErr)
			series := wr.GetElement().GetTagFamilies()[0].GetTags()[0].GetStr().GetValue()
			e, ok := expect[series]
			require.Truef(t, ok, "replay produced an unexpected series %q", series)
			assertStreamWriteRequestEqual(t, e.wr, wr)
			require.NotNil(t, iwr.Request, "iwr must wrap a WriteRequest")
			require.Equal(t, wr, iwr.Request, "iwr.Request must point at the same WriteRequest")
			require.Equalf(t, e.wantShard, iwr.ShardId, "series %q must route to shard %d under shardNum=2", series, e.wantShard)
			require.Equal(t, pbv1.EntityValues{stringTagValue(series)}.Encode(), iwr.EntityValues,
				"iwr.EntityValues must encode the entity tag (series)")
			require.Equal(t, row.ElementID, iwr.RawElementId, "raw_element_id must equal source row.ElementID")
			decodedID, decErr := base64.StdEncoding.DecodeString(wr.Element.ElementId)
			require.NoError(t, decErr, "wr.Element.ElementId must be valid base64")
			require.Equal(t, row.ElementID, convert.BytesToUint64(decodedID),
				"wr.Element.ElementId must encode the same eID carried by raw_element_id")
			require.Falsef(t, seen[series], "series %q reconstructed twice", series)
			seen[series] = true
		}
		require.NoError(t, it.Err())
		it.Close()
		reader.Close()
	}
	require.Len(t, seen, len(entries), "both rows (shard 0 and shard 1) must roundtrip exactly once")
}

// TestRoundtrip_Trace writes a trace span, flushes to disk, then verifies
// buildWriteRequest reconstructs a proto-equal WriteRequest.
func TestRoundtrip_Trace(t *testing.T) {
	req := require.New(t)
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	gomega.RegisterFailHandler(func(message string, _ ...int) { panic(message) })

	pipeline := queue.Local()
	metaSvc, err := metadataservice.NewService()
	req.NoError(err)
	metricSvc := obsservice.NewMetricService(metaSvc, pipeline, "test", nil)
	pm := protector.NewMemory(metricSvc)
	traceSvc, err := trace.NewService(metaSvc, pipeline, metricSvc, pm)
	req.NoError(err)

	metaPath, metaDefer, err := test.NewSpace()
	req.NoError(err)
	defer metaDefer()
	ports, err := test.AllocateFreePorts(1)
	req.NoError(err)
	rootPath, rootDefer, err := test.NewSpace()
	req.NoError(err)
	defer rootDefer()

	flags := []string{
		"--schema-server-root-path=" + metaPath,
		fmt.Sprintf("--schema-server-grpc-port=%d", ports[0]),
		"--schema-server-grpc-host=127.0.0.1",
		"--trace-root-path=" + rootPath,
		"--trace-flush-timeout=200ms",
	}
	moduleDefer := test.SetupModules(flags, pipeline, metaSvc, traceSvc)
	moduleStopped := false
	defer func() {
		if !moduleStopped {
			moduleDefer()
		}
	}()

	registerRoundtripTraceSchema(t, metaSvc)
	require.Eventually(t, func() bool {
		_, ok := traceSvc.LoadGroup(roundtripTraceGroup)
		return ok
	}, 30*time.Second, 200*time.Millisecond, "trace group not loaded")
	time.Sleep(time.Second)

	tsA := time.Now().Truncate(time.Millisecond)
	tsB := tsA.Add(time.Hour)
	// Two spans routed to different shards under shardNum=2 (trace_id=trace-0 ->
	// shard 0, trace-abc -> shard 1), with distinct span ids, span payloads and
	// timestamps.
	type traceExpect struct {
		wr        *tracev1.WriteRequest
		traceID   string
		wantShard uint32
	}
	entries := []traceExpect{
		{buildTraceWR(tsA, "trace-0", "span-a", []byte("payload-a")), "trace-0", 0},
		{buildTraceWR(tsB, "trace-abc", "span-b", []byte("payload-b-longer")), "trace-abc", 1},
	}
	expect := make(map[string]traceExpect, len(entries))

	bp := pipeline.NewBatchPublisher(5 * time.Second)
	for i, e := range entries {
		expect[e.traceID] = e
		_, errPub := bp.Publish(context.TODO(), data.TopicTraceWrite, bus.NewMessage(bus.MessageID(i+1), &tracev1.InternalWriteRequest{
			ShardId: e.wantShard,
			Request: e.wr,
		}))
		req.NoError(errPub)
	}
	closeNodeErrs, closeErr := bp.Close()
	req.NoError(closeErr)
	req.Empty(closeNodeErrs)

	var partDirs []string
	require.Eventually(t, func() bool {
		partDirs = findRoundtripPartDirs(rootPath)
		return len(partDirs) >= 2
	}, 30*time.Second, 200*time.Millisecond, "trace parts for both shards not flushed")

	replayer, err := newTraceRowReplayer(context.TODO(), roundtripTraceGroup, 2, nil, pipeline,
		metaSvc, localfs.NewLocalFileSystem(), logger.GetLogger("test-replayer"), nil)
	req.NoError(err)
	defer replayer.Close()

	moduleDefer()
	moduleStopped = true

	fileSystem := localfs.NewLocalFileSystem()
	seen := make(map[string]bool)
	for _, partDir := range partDirs {
		partID, parseErr := strconv.ParseUint(filepath.Base(partDir), 16, 64)
		req.NoError(parseErr)
		shardPath := filepath.Dir(partDir)
		reader, openErr := dumptrace.OpenPart(partID, shardPath, fileSystem)
		req.NoError(openErr)
		it := reader.Iterator()
		for it.Next() {
			row := it.Row()
			wr, iwr := replayer.buildWriteRequest(row)
			traceID := wr.GetTags()[0].GetStr().GetValue()
			e, ok := expect[traceID]
			require.Truef(t, ok, "replay produced an unexpected trace_id %q", traceID)
			assertTraceWriteRequestEqual(t, e.wr, wr)
			require.NotNil(t, iwr.Request, "iwr must wrap a WriteRequest")
			require.Equal(t, wr, iwr.Request, "iwr.Request must point at the same WriteRequest")
			require.Equalf(t, e.wantShard, iwr.ShardId, "trace_id %q must route to shard %d under shardNum=2", traceID, e.wantShard)
			require.Falsef(t, seen[traceID], "trace_id %q reconstructed twice", traceID)
			seen[traceID] = true
		}
		require.NoError(t, it.Err())
		it.Close()
		reader.Close()
	}
	require.Len(t, seen, len(entries), "both spans (shard 0 and shard 1) must roundtrip exactly once")
}

// buildMeasureWR builds a measure WriteRequest whose entity (series), other
// tags, fields, version and timestamp are all caller-controlled so two rows can
// be made fully distinct.
func buildMeasureWR(ts time.Time, series, strTag string, intTag, intField int64, floatField float64, version int64) *measurev1.WriteRequest {
	return &measurev1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: roundtripMeasureGroup, Name: roundtripMeasureName},
		DataPoint: &measurev1.DataPointValue{
			Timestamp:   timestamppb.New(ts),
			Version:     version,
			TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{stringTagValue(series), stringTagValue(strTag), intTagValue(intTag)}}},
			Fields:      []*modelv1.FieldValue{intFieldValue(intField), floatFieldValue(floatField)},
		},
	}
}

func buildStreamWR(ts time.Time, series, strTag string, intTag int64, elementID string) *streamv1.WriteRequest {
	return &streamv1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: roundtripStreamGroup, Name: roundtripStreamName},
		Element: &streamv1.ElementValue{
			ElementId:   elementID,
			Timestamp:   timestamppb.New(ts),
			TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{stringTagValue(series), stringTagValue(strTag), intTagValue(intTag)}}},
		},
	}
}

func buildTraceWR(ts time.Time, traceID, spanID string, span []byte) *tracev1.WriteRequest {
	return &tracev1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: roundtripTraceGroup, Name: roundtripTraceName},
		Tags: []*modelv1.TagValue{
			stringTagValue(traceID),
			stringTagValue(spanID),
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(ts)}},
		},
		Span:    span,
		Version: 1,
	}
}

// assertMeasureWriteRequestEqual asserts proto.Equal ignoring MessageId.
func assertMeasureWriteRequestEqual(t *testing.T, want, got *measurev1.WriteRequest) {
	t.Helper()
	wantC := proto.Clone(want).(*measurev1.WriteRequest)
	gotC := proto.Clone(got).(*measurev1.WriteRequest)
	wantC.MessageId = 0
	gotC.MessageId = 0
	require.Truef(t, proto.Equal(wantC, gotC), "measure WriteRequest mismatch:\nwant: %v\ngot:  %v", wantC, gotC)
}

// assertStreamWriteRequestEqual asserts proto.Equal ignoring MessageId and
// ElementId (the replayer base64-encodes the storage uint64; receiver uses
// raw_element_id instead).
func assertStreamWriteRequestEqual(t *testing.T, want, got *streamv1.WriteRequest) {
	t.Helper()
	wantC := proto.Clone(want).(*streamv1.WriteRequest)
	gotC := proto.Clone(got).(*streamv1.WriteRequest)
	wantC.MessageId = 0
	gotC.MessageId = 0
	if wantC.Element != nil {
		wantC.Element.ElementId = ""
	}
	if gotC.Element != nil {
		gotC.Element.ElementId = ""
	}
	require.Truef(t, proto.Equal(wantC, gotC), "stream WriteRequest mismatch:\nwant: %v\ngot:  %v", wantC, gotC)
}

// assertTraceWriteRequestEqual asserts proto.Equal ignoring Version.
func assertTraceWriteRequestEqual(t *testing.T, want, got *tracev1.WriteRequest) {
	t.Helper()
	wantC := proto.Clone(want).(*tracev1.WriteRequest)
	gotC := proto.Clone(got).(*tracev1.WriteRequest)
	wantC.Version = 0
	gotC.Version = 0
	require.Truef(t, proto.Equal(wantC, gotC), "trace WriteRequest mismatch:\nwant: %v\ngot:  %v", wantC, gotC)
}

func registerRoundtripMeasureSchema(t *testing.T, metaSvc metadataservice.Service) {
	t.Helper()
	reg := metaSvc.SchemaRegistry()
	ctx := context.TODO()
	_, err := reg.CreateGroup(ctx, &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: roundtripMeasureGroup},
		Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        2,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	})
	require.NoError(t, err)
	_, err = reg.CreateMeasure(ctx, &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: roundtripMeasureName, Group: roundtripMeasureGroup},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{
				{Name: "series", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "strTag", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "intTag", Type: databasev1.TagType_TAG_TYPE_INT},
			},
		}},
		Fields: []*databasev1.FieldSpec{
			{
				Name:              "intField",
				FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			},
			{
				Name:              "floatField",
				FieldType:         databasev1.FieldType_FIELD_TYPE_FLOAT,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			},
		},
		Entity: &databasev1.Entity{TagNames: []string{"series"}},
	})
	require.NoError(t, err)
}

func registerRoundtripStreamSchema(t *testing.T, metaSvc metadataservice.Service) {
	t.Helper()
	reg := metaSvc.SchemaRegistry()
	ctx := context.TODO()
	_, err := reg.CreateGroup(ctx, &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: roundtripStreamGroup},
		Catalog:  commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        2,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	})
	require.NoError(t, err)
	_, err = reg.CreateStream(ctx, &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: roundtripStreamName, Group: roundtripStreamGroup},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{
				{Name: "series", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "strTag", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "intTag", Type: databasev1.TagType_TAG_TYPE_INT},
			},
		}},
		Entity: &databasev1.Entity{TagNames: []string{"series"}},
	})
	require.NoError(t, err)
}

func registerRoundtripTraceSchema(t *testing.T, metaSvc metadataservice.Service) {
	t.Helper()
	reg := metaSvc.SchemaRegistry()
	ctx := context.TODO()
	_, err := reg.CreateGroup(ctx, &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: roundtripTraceGroup},
		Catalog:  commonv1.Catalog_CATALOG_TRACE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        2,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	})
	require.NoError(t, err)
	_, err = reg.CreateTrace(ctx, &databasev1.Trace{
		Metadata: &commonv1.Metadata{Name: roundtripTraceName, Group: roundtripTraceGroup},
		Tags: []*databasev1.TraceTagSpec{
			{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
		},
		TraceIdTagName:   "trace_id",
		SpanIdTagName:    "span_id",
		TimestampTagName: "timestamp",
	})
	require.NoError(t, err)
}

func findRoundtripPartDirs(root string) []string {
	var dirs []string
	_ = filepath.WalkDir(root, func(path string, d gofs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if d.Name() == "metadata.json" {
			if _, parseErr := strconv.ParseUint(filepath.Base(filepath.Dir(path)), 16, 64); parseErr == nil {
				dirs = append(dirs, filepath.Dir(path))
			}
		}
		return nil
	})
	return dirs
}

// countingBatchPublisher is a queue.BatchPublisher fake that records how many
// times it was published to and closed, and how many messages each Publish
// carried. Close runs in the replayer's background confirmation goroutines, so
// the counters are atomic. closeCee injects a per-node delivery failure,
// publishErr injects a synchronous send failure, and panicOnClose makes Close
// panic to exercise the confirm goroutine's panic-safety.
type countingBatchPublisher struct {
	closeCee      map[string]*common.Error
	publishErr    error
	panicOnClose  bool
	publishCount  atomic.Int32
	publishedRows atomic.Int32
	closeCount    atomic.Int32
}

func (f *countingBatchPublisher) Publish(_ context.Context, _ bus.Topic, messages ...bus.Message) (bus.Future, error) {
	f.publishCount.Add(1)
	f.publishedRows.Add(int32(len(messages)))
	return nil, f.publishErr
}

func (f *countingBatchPublisher) Close() (map[string]*common.Error, error) {
	f.closeCount.Add(1)
	if f.panicOnClose {
		panic("simulated publisher close panic")
	}
	return f.closeCee, nil
}

// senderReturning wires a batchSender whose successive publishers are the given
// ones in order, then fresh clean publishers once the list is exhausted. It
// centralizes the mock-controller + NewBatchPublisher boilerplate and the
// "first publisher carries the first batch" rotation order for the sender tests.
// The returned accessor reports every publisher actually vended (provided and
// fresh) — including appends that happen after this returns — so a test can
// assert the exact number vended and inspect each, catching extra
// rotations/publishes that a pre-sized expectation would miss.
func senderReturning(t *testing.T, pubs ...*countingBatchPublisher) (*batchSender, func() []*countingBatchPublisher) {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	mockClient := queue.NewMockClient(ctrl)
	var vended []*countingBatchPublisher
	i := 0
	mockClient.EXPECT().NewBatchPublisher(measureReplayBatchTimeout).
		DoAndReturn(func(time.Duration) queue.BatchPublisher {
			var p *countingBatchPublisher
			if i < len(pubs) {
				p, i = pubs[i], i+1
			} else {
				p = &countingBatchPublisher{}
			}
			vended = append(vended, p)
			return p
		}).AnyTimes()
	return newBatchSender(mockClient, data.TopicMeasureWrite, measureReplayBatchSize, measureReplayBatchTimeout),
		func() []*countingBatchPublisher { return vended }
}

// totalPublished sums the publish counts across the given publishers.
func totalPublished(pubs []*countingBatchPublisher) int32 {
	var total int32
	for _, p := range pubs {
		total += p.publishCount.Load()
	}
	return total
}

// totalPublishedRows sums the message counts actually handed to Publish across
// the given publishers, i.e. how many source rows were really sent.
func totalPublishedRows(pubs []*countingBatchPublisher) int32 {
	var total int32
	for _, p := range pubs {
		total += p.publishedRows.Load()
	}
	return total
}

// cleanEmit returns a replay emit callback that enqueues one dummy row per call.
func cleanEmit(s *batchSender) func() error {
	return func() error {
		return s.enqueue(context.TODO(),
			bus.NewBatchMessageWithNode(bus.MessageID(1), "node-1", &measurev1.InternalWriteRequest{}))
	}
}

// failingEmit is cleanEmit that instead returns a build error on the failAt-th
// call, aborting the part with whatever was buffered/sent so far.
func failingEmit(s *batchSender, failAt int) func() error {
	clean := cleanEmit(s)
	calls := 0
	return func() error {
		calls++
		if calls == failAt {
			return fmt.Errorf("build failed")
		}
		return clean()
	}
}

// nodeErrCee extracts the per-node delivery errors carried by a failed replay
// error, failing the test if err is nil or not a *nodeReplayError.
func nodeErrCee(t *testing.T, err error) map[string]*common.Error {
	t.Helper()
	var nre *nodeReplayError
	require.ErrorAs(t, err, &nre)
	return nre.cee
}

// TestConfirmPipeline_BoundsInflightDepth proves the pipeline never keeps more
// than depth confirmations outstanding: a (depth+1)th add blocks until the
// oldest in-flight confirmation resolves, and drain surfaces the first failure.
func TestConfirmPipeline_BoundsInflightDepth(t *testing.T) {
	// rowReplayMaxInflight is 2, so a 3rd unconfirmed batch must wait.
	p := newConfirmPipeline()
	ch1 := make(chan error, 1)
	ch2 := make(chan error, 1)
	ch3 := make(chan error, 1)

	// Within depth: add must not block and has nothing to surface yet.
	require.NoError(t, p.add(ch1))
	require.NoError(t, p.add(ch2))

	// Full (2 in flight): the 3rd add must block until the oldest (ch1) resolves.
	added := make(chan error, 1)
	go func() { added <- p.add(ch3) }()
	select {
	case <-added:
		t.Fatal("add exceeded the in-flight depth without waiting for the oldest confirmation")
	case <-time.After(100 * time.Millisecond):
	}
	ch1 <- nil // oldest confirms cleanly
	select {
	case out := <-added:
		require.NoError(t, out, "a clean oldest confirmation must not report failure")
	case <-time.After(2 * time.Second):
		t.Fatal("add did not unblock after the oldest confirmation resolved")
	}

	// drain waits for the rest and returns the first failing error.
	ch2 <- nil
	ch3 <- newNodeReplayError(map[string]*common.Error{"node-1": common.NewError("boom")}, nil)
	require.Contains(t, nodeErrCee(t, p.drain()), "node-1")
}

// enqueueN feeds n messages through the sender, failing the test on any error
// surfaced early by the in-flight bound.
func enqueueN(t *testing.T, s *batchSender, n int) {
	t.Helper()
	emit := cleanEmit(s)
	for i := 0; i < n; i++ {
		require.NoError(t, emit())
	}
}

// TestBatchSender_SendsRotatesAndConfirmsEachBatch verifies the sender flow:
// every full batch is published once on its own publisher, the publisher is
// rotated for the next batch, and each sent batch is confirmed (closed) exactly
// once, with all confirmations drained before the part is considered durable.
func TestBatchSender_SendsRotatesAndConfirmsEachBatch(t *testing.T) {
	const batches = 3
	s, vended := senderReturning(t)
	enqueueN(t, s, batches*measureReplayBatchSize)
	require.NoError(t, s.drain())

	require.Len(t, vended(), batches+1, "exactly the initial publisher plus one rotation per sent batch")
	for i := 0; i < batches; i++ {
		require.Equalf(t, int32(1), vended()[i].publishCount.Load(), "publisher %d must send its batch once", i)
		require.Equalf(t, int32(1), vended()[i].closeCount.Load(), "publisher %d must be confirmed (closed) once", i)
	}
	require.Equal(t, int32(0), vended()[batches].publishCount.Load(), "the staged publisher holds no batch yet")
}

// TestBatchSender_SurfacesBatchNodeError verifies a per-node delivery failure on
// a confirmed batch propagates out of the sender (via the final drain) so the
// caller can mark the part errored and retry it on resume.
func TestBatchSender_SurfacesBatchNodeError(t *testing.T) {
	failing := &countingBatchPublisher{closeCee: map[string]*common.Error{"node-1": common.NewError("flush timeout")}}
	s, _ := senderReturning(t, failing) // the first publisher carries the batch and fails
	enqueueN(t, s, measureReplayBatchSize)
	require.Contains(t, nodeErrCee(t, s.drain()), "node-1",
		"a batch's per-node delivery error must surface when drained")
}

// fakeCursor yields n rows for the replay driver, with a zero Position. err, when
// set, is returned by Err() after the rows are exhausted to exercise the
// iterator-error path.
type fakeCursor struct {
	err error
	n   int
	i   int
}

func (c *fakeCursor) Next() bool              { c.i++; return c.i <= c.n }
func (c *fakeCursor) Err() error              { return c.err }
func (c *fakeCursor) Position() dump.Position { return dump.Position{} }

// TestBatchSender_SurfacesClosePanic pins the panic-safety of the confirm
// goroutine: if a publisher's Close panics, the goroutine must still deliver a
// (failed) error so the pipeline never deadlocks. Without the deferred
// recover+send this would hang (or crash), so the bounded wait turns a
// regression into a failed assertion rather than a hung test.
func TestBatchSender_SurfacesClosePanic(t *testing.T) {
	s, _ := senderReturning(t, &countingBatchPublisher{panicOnClose: true})
	enqueueN(t, s, measureReplayBatchSize) // one full batch on the panicking publisher

	done := make(chan error, 1)
	go func() { done <- s.drain() }()
	select {
	case out := <-done:
		require.ErrorContains(t, out, "panic", "a panicking Close must surface as an error")
	case <-time.After(2 * time.Second):
		t.Fatal("drain hung: the confirm goroutine did not deliver an error after Close panicked")
	}
}

// TestConfirmPipeline_DrainKeepsFirstFailure pins that drain returns the EARLIEST
// failing error, not a later one, when several in-flight batches fail.
func TestConfirmPipeline_DrainKeepsFirstFailure(t *testing.T) {
	p := newConfirmPipeline()
	ch1 := make(chan error, 1)
	ch2 := make(chan error, 1)
	require.NoError(t, p.add(ch1))
	require.NoError(t, p.add(ch2))

	ch1 <- fmt.Errorf("first failure")
	ch2 <- fmt.Errorf("second failure")
	require.ErrorContains(t, p.drain(), "first failure", "drain must keep the earliest failure")
}

// TestBatchSender_SurfacesFailureViaInflightBound pins the realistic mid-stream
// abort path: a batch's failure is surfaced through the in-flight bound's
// add-await (i.e. out of enqueue), not only at the final drain.
func TestBatchSender_SurfacesFailureViaInflightBound(t *testing.T) {
	failing := &countingBatchPublisher{closeCee: map[string]*common.Error{"node-1": common.NewError("boom")}}
	s, _ := senderReturning(t, failing) // batch 1 fails; later batches are clean

	emit := cleanEmit(s)
	var surfaced error
	for i := 0; i < 3*measureReplayBatchSize; i++ {
		if out := emit(); out != nil {
			surfaced = out
		}
	}
	require.Contains(t, nodeErrCee(t, surfaced), "node-1",
		"batch 1's failure must surface via the in-flight bound during enqueue, not only at drain")
	s.drain()
}

// TestReplay_RowCountBoundaries pins the driver's batching across small and large
// parts: for every part size the reported row count is exact, a successful part
// (even an empty one) counts once, the number of published batches matches the
// 2000-row boundary, and a subsequent close never publishes.
func TestReplay_RowCountBoundaries(t *testing.T) {
	cases := []struct {
		name        string
		rows        int
		wantBatches int
	}{
		{"empty", 0, 0},
		{"single_row", 1, 1},
		{"just_under_one_batch", measureReplayBatchSize - 1, 1},
		{"exactly_one_batch", measureReplayBatchSize, 1},
		{"one_over_one_batch", measureReplayBatchSize + 1, 2},
		{"exactly_two_batches", 2 * measureReplayBatchSize, 2},
		{"two_batches_and_a_tail", 2*measureReplayBatchSize + 5, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, vended := senderReturning(t)
			var counter uint64
			rows, err := s.replay(context.TODO(), logger.GetLogger("test-replayer"), "g", "p", &counter, &fakeCursor{n: tc.rows}, cleanEmit(s))
			require.NoError(t, err)
			require.Equal(t, tc.rows, rows, "rowCount must equal the source row count")
			require.Equal(t, uint64(1), counter, "every successful part counts once, even when empty")
			// Sum over EVERY vended publisher so an extra batch on a rotated-beyond
			// publisher is still counted, and pin the exact rotation count.
			require.Equal(t, int32(tc.wantBatches), totalPublished(vended()), "published batch count")
			require.Len(t, vended(), tc.wantBatches+1, "exactly one publisher per batch plus the staged one")
			// Every source row must reach Publish exactly once: no row dropped at a
			// batch boundary and none lost in the trailing tail.
			require.Equal(t, int32(tc.rows), totalPublishedRows(vended()), "every source row must be published exactly once")

			_, cerr := s.close()
			require.NoError(t, cerr)
			require.Equal(t, int32(tc.wantBatches), totalPublished(vended()), "close must not publish after a successful part")
		})
	}
}

// TestReplay_BatchPlusTailSendsEveryRow pins the "a few rows past a flush
// boundary" case: with two full batches plus a short tail, each full batch
// carries exactly batchSize messages, the tail batch carries the remainder, the
// staged publisher carries nothing, and the grand total equals the row count —
// so no row is dropped at a boundary nor lost in the trailing tail.
func TestReplay_BatchPlusTailSendsEveryRow(t *testing.T) {
	const tail = 5
	const rows = 2*measureReplayBatchSize + tail
	s, vended := senderReturning(t)
	var counter uint64
	got, err := s.replay(context.TODO(), logger.GetLogger("test-replayer"), "g", "p", &counter, &fakeCursor{n: rows}, cleanEmit(s))
	require.NoError(t, err)
	require.Equal(t, rows, got, "rowCount must equal the source row count")

	pubs := vended()
	require.Len(t, pubs, 4, "two full batches + the tail batch + the staged publisher")
	require.Equal(t, int32(measureReplayBatchSize), pubs[0].publishedRows.Load(), "first full batch carries batchSize rows")
	require.Equal(t, int32(measureReplayBatchSize), pubs[1].publishedRows.Load(), "second full batch carries batchSize rows")
	require.Equal(t, int32(tail), pubs[2].publishedRows.Load(), "the trailing batch carries exactly the tail")
	require.Equal(t, int32(0), pubs[3].publishedRows.Load(), "the staged publisher holds nothing")
	require.Equal(t, int32(rows), totalPublishedRows(pubs), "every row sent exactly once")

	_, cerr := s.close()
	require.NoError(t, cerr)
}

// TestReplay_AbortDrainsInflightAndDiscards pins the abort path across small and
// large parts and at the batch boundary: whatever full batches were already sent
// are still confirmed (drained), the un-flushed residual is discarded (never
// resurrected by a later close), the failed part is not counted, and no row of an
// aborted part is published beyond the batches that left before the failure.
func TestReplay_AbortDrainsInflightAndDiscards(t *testing.T) {
	cases := []struct {
		name            string
		failAt          int // the 1-based emit call that returns a build error
		wantRows        int // rows counted before the failure
		wantSentBatches int // full batches flushed before the failure
	}{
		{"first_row_nothing_buffered", 1, 0, 0},
		{"small_residual_no_batch", 2, 1, 0},
		{"empty_residual_at_batch_boundary", measureReplayBatchSize + 1, measureReplayBatchSize, 1},
		{"residual_after_two_batches", 2*measureReplayBatchSize + 501, 2*measureReplayBatchSize + 500, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, vended := senderReturning(t)
			var counter uint64
			rows, err := s.replay(context.TODO(), logger.GetLogger("test-replayer"), "g", "p", &counter, &fakeCursor{n: tc.failAt}, failingEmit(s, tc.failAt))
			require.Error(t, err, "the part must report failure")
			require.Equal(t, tc.wantRows, rows, "rowCount counts the rows enqueued before the failure")
			require.Zero(t, counter, "a failed part must not be counted")

			require.Len(t, vended(), tc.wantSentBatches+1, "one publisher per sent batch plus the staged one")
			for i := 0; i < tc.wantSentBatches; i++ {
				require.Equalf(t, int32(1), vended()[i].closeCount.Load(), "sent batch %d must be confirmed (drained)", i)
			}
			require.Equal(t, int32(tc.wantSentBatches), totalPublished(vended()), "only the batches sent before the failure are published")
			require.Nil(t, s.take(), "the residual must be discarded")
			_, cerr := s.close()
			require.NoError(t, cerr)
		})
	}
}

// TestReplay_IteratorErrorDrainsAndDiscards covers the distinct abort branch where
// the source iterator fails after yielding rows (cur.Err() != nil). The already-
// sent batches must still be drained, the residual discarded, and the part failed
// — so a truncated part is never committed as durable.
func TestReplay_IteratorErrorDrainsAndDiscards(t *testing.T) {
	s, vended := senderReturning(t)
	const n = 2*measureReplayBatchSize + 300 // two full batches sent, 300 buffered, then the iterator errors
	var counter uint64
	rows, err := s.replay(context.TODO(), logger.GetLogger("test-replayer"), "g", "p", &counter,
		&fakeCursor{n: n, err: fmt.Errorf("iterator boom")}, cleanEmit(s))
	require.ErrorContains(t, err, "iterator boom", "an iterator error must fail the part")
	require.Equal(t, n, rows, "all rows read before the iterator error are counted")
	require.Zero(t, counter, "a failed part must not be counted")

	require.Len(t, vended(), 3, "two sent batches plus the staged one")
	require.Equal(t, int32(1), vended()[0].closeCount.Load(), "sent batch 0 must be drained")
	require.Equal(t, int32(1), vended()[1].closeCount.Load(), "sent batch 1 must be drained")
	require.Equal(t, int32(0), vended()[2].publishCount.Load(), "the residual must not be published")
	require.Nil(t, s.take(), "the residual must be discarded")
}

// TestReplay_NodeErrorViaInflightBoundDrivesAbort covers the composition of a
// per-node confirm failure surfacing through the in-flight bound's add-await
// while driven by replay (not raw enqueue): the part aborts mid-scan, in-flight
// batches drain, and the residual is discarded.
func TestReplay_NodeErrorViaInflightBoundDrivesAbort(t *testing.T) {
	failing := &countingBatchPublisher{closeCee: map[string]*common.Error{"node-1": common.NewError("boom")}}
	s, _ := senderReturning(t, failing) // batch 1's confirm fails
	var counter uint64
	rows, err := s.replay(context.TODO(), logger.GetLogger("test-replayer"), "g", "p", &counter,
		&fakeCursor{n: 3 * measureReplayBatchSize}, cleanEmit(s))
	require.Contains(t, nodeErrCee(t, err), "node-1", "the node error must abort the part")
	require.Zero(t, counter, "a failed part must not be counted")
	// Batch 1's failure surfaces only once a later batch's flush awaits it at the
	// in-flight bound, so the part aborts mid-scan: after more than one batch's
	// worth of rows but before consuming all of them.
	require.Greater(t, rows, measureReplayBatchSize, "aborted after more than one batch")
	require.Less(t, rows, 3*measureReplayBatchSize, "aborted mid-scan, not after consuming every row")
	require.Nil(t, s.take(), "the residual must be discarded")
	_, cerr := s.close()
	require.NoError(t, cerr)
}

// TestRecordReplayNodeErrors_ReportsPartialFailure pins the partial-failure
// report: the per-node delivery errors a failed batch carries are recorded into
// the progress report keyed by node (even through fmt wrapping), while a
// global-only failure and a plain non-nodeReplayError add no node entries — so
// the resume report reflects exactly which nodes rejected rows and nothing else.
func TestRecordReplayNodeErrors_ReportsPartialFailure(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	l := logger.GetLogger("test")

	t.Run("per_node_errors_recorded", func(t *testing.T) {
		p := NewProgress(filepath.Join(t.TempDir(), "progress.json"), l)
		err := newNodeReplayError(map[string]*common.Error{
			"node-2": common.NewError("flush timeout"),
			"node-5": common.NewError("shard rejected"),
		}, nil)
		recordReplayNodeErrors(p, "g", err)
		require.Contains(t, p.RowReplayNodeErrors, "g")
		require.Len(t, p.RowReplayNodeErrors["g"], 2, "exactly the failing nodes are reported")
		require.Contains(t, p.RowReplayNodeErrors["g"]["node-2"], "flush timeout")
		require.Contains(t, p.RowReplayNodeErrors["g"]["node-5"], "shard rejected")
		require.NotContains(t, p.RowReplayNodeErrors["g"], "node-1", "a node that did not fail must not be reported")
	})

	t.Run("wrapped_error_still_unwrapped", func(t *testing.T) {
		p := NewProgress(filepath.Join(t.TempDir(), "progress.json"), l)
		inner := newNodeReplayError(map[string]*common.Error{"node-7": common.NewError("boom")}, nil)
		recordReplayNodeErrors(p, "g", fmt.Errorf("row-replay measure part p: %w", inner))
		require.Contains(t, p.RowReplayNodeErrors["g"], "node-7", "errors.As must find node errors through fmt wrapping")
	})

	t.Run("global_only_error_records_nothing", func(t *testing.T) {
		p := NewProgress(filepath.Join(t.TempDir(), "progress.json"), l)
		recordReplayNodeErrors(p, "g", newNodeReplayError(nil, fmt.Errorf("stream broke")))
		require.NotContains(t, p.RowReplayNodeErrors, "g", "a global-only failure must not add node entries")
	})

	t.Run("plain_error_records_nothing", func(t *testing.T) {
		p := NewProgress(filepath.Join(t.TempDir(), "progress.json"), l)
		recordReplayNodeErrors(p, "g", fmt.Errorf("open part failed"))
		require.NotContains(t, p.RowReplayNodeErrors, "g", "a non-nodeReplayError must not be reported as node failures")
	})
}

// TestBatchSender_EachBatchGetsItsOwnTimeoutWindow pins the fix for the original
// migration timeout bug: every batch is published on its OWN publisher opened
// with a fresh copy of the configured timeout, so the window is per-batch. A slow
// gap between two sends can never eat into the next batch's window the way a
// single long-lived stream's lifetime once did. A 5s-timeout sender sends two
// full batches; we assert a fresh NewBatchPublisher(5s) backs each batch (initial
// publisher + one rotation per batch), every call uses the full 5s (not a shrunk
// or shared window), and the two batches ride distinct publishers.
func TestBatchSender_EachBatchGetsItsOwnTimeoutWindow(t *testing.T) {
	const (
		timeout   = 5 * time.Second
		batchSize = 2
	)
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	mockClient := queue.NewMockClient(ctrl)
	var timeouts []time.Duration
	var pubs []*countingBatchPublisher
	mockClient.EXPECT().NewBatchPublisher(gomock.Any()).
		DoAndReturn(func(d time.Duration) queue.BatchPublisher {
			timeouts = append(timeouts, d)
			p := &countingBatchPublisher{}
			pubs = append(pubs, p)
			return p
		}).AnyTimes()

	s := newBatchSender(mockClient, data.TopicMeasureWrite, batchSize, timeout)
	for i := 0; i < 2*batchSize; i++ { // two full batches
		require.NoError(t, s.enqueue(context.TODO(),
			bus.NewBatchMessageWithNode(bus.MessageID(1), "node-1", &measurev1.InternalWriteRequest{})))
	}
	require.NoError(t, s.drain())

	// Initial publisher + one rotation per sent batch = 3 NewBatchPublisher calls,
	// and EVERY one must request the full configured timeout (a fresh 5s window),
	// proving the two sends do not share one stream's single 5s lifetime.
	require.Len(t, timeouts, 3, "each batch must open its own publisher, not share one")
	for i, d := range timeouts {
		require.Equalf(t, timeout, d, "publisher %d must open with the full configured timeout, not a shared/shrunk one", i)
	}
	require.Len(t, pubs, 3)
	require.Equal(t, int32(1), pubs[0].publishCount.Load(), "batch 1 rides its own publisher")
	require.Equal(t, int32(1), pubs[1].publishCount.Load(), "batch 2 rides a different publisher")
	require.Equal(t, int32(0), pubs[2].publishCount.Load(), "the staged publisher holds nothing")
	require.NotSame(t, pubs[0], pubs[1], "the two batches must not share a publisher (nor its timeout window)")

	_, cerr := s.close()
	require.NoError(t, cerr)
}

// TestNodeReplayError_ErrorAndUnwrap pins the error type directly: its two
// message forms (global cause vs per-node count) and that Unwrap exposes the
// global cause so errors.Is/As chains reach it even through fmt wrapping.
func TestNodeReplayError_ErrorAndUnwrap(t *testing.T) {
	// Global-error form: Error() is the wrapped cause, and Unwrap exposes it.
	cause := fmt.Errorf("publish failed")
	globalErr := newNodeReplayError(nil, cause)
	require.EqualError(t, globalErr, "publish failed")
	require.ErrorIs(t, globalErr, cause, "Unwrap must expose the global cause to errors.Is")
	require.ErrorIs(t, fmt.Errorf("row-replay measure part p: %w", globalErr), cause,
		"the global cause must stay reachable through fmt wrapping")

	// Node-error form (cee only, no global err): Error() reports the node count
	// and there is no global cause to unwrap.
	nodeErr := newNodeReplayError(map[string]*common.Error{
		"node-1": common.NewError("boom"),
		"node-2": common.NewError("boom"),
	}, nil)
	require.EqualError(t, nodeErr, "2 node error(s)")
	require.NotErrorIs(t, nodeErr, cause, "a node-only error has no global cause to unwrap")
}

// TestBatchSender_CloseSurfacesDrainedFailure covers close()'s failure branch:
// when an in-flight batch failed, close() drains it and returns that batch's
// per-node errors instead of the idle publisher's clean result.
func TestBatchSender_CloseSurfacesDrainedFailure(t *testing.T) {
	failing := &countingBatchPublisher{closeCee: map[string]*common.Error{"node-9": common.NewError("boom")}}
	s, _ := senderReturning(t, failing)    // the first publisher carries the batch and fails
	enqueueN(t, s, measureReplayBatchSize) // one full batch sent, left in-flight (undrained)
	cee, err := s.close()                  // close() must drain it and surface the node error
	require.NoError(t, err, "a node-only failure carries no global error")
	require.Contains(t, cee, "node-9", "close must return the drained batch's per-node errors")
}

// TestReplay_SurfacesSynchronousPublishErrorPromptly pins that a synchronous
// Publish failure aborts the part on the flush that triggered it — not deferred
// until the in-flight bound or the final drain. With the failing publisher
// carrying the first batch, replay must stop right after that batch instead of
// scanning ~depth more batches: the old code returned the error only via the
// bound, so it would have consumed ~3 batches before aborting.
func TestReplay_SurfacesSynchronousPublishErrorPromptly(t *testing.T) {
	pub := &countingBatchPublisher{publishErr: fmt.Errorf("stream send broke")}
	s, _ := senderReturning(t, pub) // the first batch's Publish fails synchronously
	var counter uint64
	rows, err := s.replay(context.TODO(), logger.GetLogger("test-replayer"), "g", "p", &counter,
		&fakeCursor{n: 3 * measureReplayBatchSize}, cleanEmit(s))
	require.ErrorContains(t, err, "stream send broke", "a synchronous Publish error must abort the part")
	require.Zero(t, counter, "a failed part must not be counted")
	// Row 2000's emit triggers the failing flush and returns its error, so the loop
	// breaks before counting it: exactly one batch worth minus the failing row, far
	// short of the 3 batches the deferred (bound-only) behavior would have scanned.
	require.Equal(t, measureReplayBatchSize-1, rows, "replay must abort on the failing flush, not keep scanning")
	require.Nil(t, s.take(), "the residual must be discarded")
	_, cerr := s.close()
	require.NoError(t, cerr)
}
