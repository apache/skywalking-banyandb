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
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

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
