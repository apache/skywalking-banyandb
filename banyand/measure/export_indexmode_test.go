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

package measure

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

const (
	exportIMGroup        = "im_group"
	exportIMMeasure      = "im_measure"
	exportNormalMeasure  = "normal_measure"
	exportRuleInst       = uint32(11)
	exportRuleCode       = uint32(12)
	exportRuleNormalOnly = uint32(13)
)

// exportIndexModeMeasure has entity tags (svc not indexed, inst indexed), an
// indexed INT tag, non-indexed STRING_ARRAY / INT_ARRAY / BINARY tags, a
// 4-character non-indexed tag name and a second tag family.
func exportIndexModeMeasure() *databasev1.Measure {
	return &databasev1.Measure{
		Metadata:  &commonv1.Metadata{Group: exportIMGroup, Name: exportIMMeasure},
		IndexMode: true,
		Entity:    &databasev1.Entity{TagNames: []string{"svc", "inst"}},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "default", Tags: []*databasev1.TagSpec{
				{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "inst", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "code", Type: databasev1.TagType_TAG_TYPE_INT},
				{Name: "labels", Type: databasev1.TagType_TAG_TYPE_STRING_ARRAY},
				{Name: "ids", Type: databasev1.TagType_TAG_TYPE_INT_ARRAY},
				{Name: "blob", Type: databasev1.TagType_TAG_TYPE_DATA_BINARY},
				{Name: "port", Type: databasev1.TagType_TAG_TYPE_INT},
			}},
			{Name: "extra", Tags: []*databasev1.TagSpec{
				{Name: "region", Type: databasev1.TagType_TAG_TYPE_STRING},
			}},
		},
	}
}

func exportNormalMeasureSchema() *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Group: exportIMGroup, Name: exportNormalMeasure},
		Entity:   &databasev1.Entity{TagNames: []string{"svc"}},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "default", Tags: []*databasev1.TagSpec{
				{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "layer", Type: databasev1.TagType_TAG_TYPE_STRING},
			}},
		},
		Fields: []*databasev1.FieldSpec{{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}
}

func exportIndexRules() []*databasev1.IndexRule {
	return []*databasev1.IndexRule{
		{Metadata: &commonv1.Metadata{Group: exportIMGroup, Name: "inst", Id: exportRuleInst}, Tags: []string{"inst"}, Type: databasev1.IndexRule_TYPE_INVERTED},
		{Metadata: &commonv1.Metadata{Group: exportIMGroup, Name: "code", Id: exportRuleCode}, Tags: []string{"code"}, Type: databasev1.IndexRule_TYPE_INVERTED},
		{Metadata: &commonv1.Metadata{Group: exportIMGroup, Name: "layer", Id: exportRuleNormalOnly}, Tags: []string{"layer"}, Type: databasev1.IndexRule_TYPE_INVERTED},
	}
}

type exportIMPoint struct {
	defaultTags []*modelv1.TagValue
	extraTags   []*modelv1.TagValue
	timestamp   int64
	version     int64
}

// indexModeDocFromWrite builds the series document exactly as processDataPoint
// does for an index-mode measure: handleIndexMode + appendEntityTagsToIndexFields.
func indexModeDocFromWrite(t *testing.T, m *measure, p exportIMPoint) index.Document {
	t.Helper()
	req := &measurev1.WriteRequest{
		Metadata: m.schema.GetMetadata(),
		DataPoint: &measurev1.DataPointValue{
			Version: p.version,
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{Tags: p.defaultTags},
				{Tags: p.extraTags},
			},
		},
	}
	series := &pbv1.Series{Subject: m.schema.GetMetadata().GetName(), EntityValues: p.defaultTags[:2]}
	require.NoError(t, series.Marshal())
	is := m.indexSchema.Load().(indexSchema)
	fields := handleIndexMode(m.schema, req, is.indexRuleLocators, nil)
	fields = appendEntityTagsToIndexFields(fields, m, series)
	return index.Document{
		DocID:        uint64(series.ID),
		EntityValues: append([]byte(nil), series.Buffer...),
		Fields:       fields,
		Version:      p.version,
		Timestamp:    p.timestamp,
	}
}

func newExportTestMeasure(schema *databasev1.Measure, rules []*databasev1.IndexRule) *measure {
	m := &measure{schema: schema}
	m.OnIndexUpdate(rules)
	return m
}

// seedExportSidx writes two index-mode rows plus a normal measure's series
// document (mixed group) into dir and returns the written points.
func seedExportSidx(t *testing.T, dir string) []exportIMPoint {
	t.Helper()
	im := newExportTestMeasure(exportIndexModeMeasure(), exportIndexRules()[:2])
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	points := []exportIMPoint{
		{
			defaultTags: []*modelv1.TagValue{
				strTagValue("svc-a"), strTagValue("inst-1"), int64TagValue(200),
				strArrTagValue([]string{"x", "y", "z"}), int64ArrTagValue([]int64{3, 1, 2}),
				binaryDataTagValue([]byte{0, 1, 2}), int64TagValue(8080),
			},
			extraTags: []*modelv1.TagValue{strTagValue("eu")},
			timestamp: base,
			version:   7,
		},
		{
			// Null code / labels / ids / blob / region and a zero version.
			defaultTags: []*modelv1.TagValue{
				strTagValue("svc-b"), strTagValue("inst-2"), pbv1.NullTagValue,
				pbv1.NullTagValue, pbv1.NullTagValue, pbv1.NullTagValue, int64TagValue(9090),
			},
			extraTags: []*modelv1.TagValue{pbv1.NullTagValue},
			timestamp: base + int64(time.Minute),
		},
	}
	docs := make(index.Documents, 0, len(points)+1)
	for _, p := range points {
		docs = append(docs, indexModeDocFromWrite(t, im, p))
	}
	// The normal measure's series document: no timestamp, only an indexed tag.
	normalSeries := &pbv1.Series{Subject: exportNormalMeasure, EntityValues: []*modelv1.TagValue{strTagValue("svc-a")}}
	require.NoError(t, normalSeries.Marshal())
	layer := index.NewBytesField(index.FieldKey{IndexRuleID: exportRuleNormalOnly}, []byte("general"))
	layer.Index = true
	docs = append(docs, index.Document{
		DocID:        uint64(normalSeries.ID),
		EntityValues: append([]byte(nil), normalSeries.Buffer...),
		Fields:       []index.Field{layer},
	})
	writeIndexModeSourceDocs(t, dir, docs)
	return points
}

func collectIndexModeRows(t *testing.T, dir string, measures []*databasev1.Measure, rules []*databasev1.IndexRule) map[string]*IndexModeRow {
	t.Helper()
	rows := map[string]*IndexModeRow{}
	require.NoError(t, WalkIndexModeRows(context.Background(), dir, measures, rules, func(row *IndexModeRow) error {
		rows[row.Tags["default.svc"].GetStr().GetValue()] = row
		return nil
	}))
	return rows
}

func requireTagValue(t *testing.T, want, got *modelv1.TagValue, key string) {
	t.Helper()
	require.NotNil(t, got, "tag %s missing", key)
	require.True(t, proto.Equal(want, got), "tag %s: want %v, got %v", key, want, got)
}

func TestWalkIndexModeRows(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sidx")
	points := seedExportSidx(t, dir)

	rows := collectIndexModeRows(t, dir, []*databasev1.Measure{exportIndexModeMeasure()}, exportIndexRules())
	require.Len(t, rows, 2, "the normal measure's series document must be skipped")

	a := rows["svc-a"]
	require.NotNil(t, a)
	require.Equal(t, exportIMMeasure, a.Subject)
	require.Equal(t, points[0].timestamp, a.Timestamp)
	require.Equal(t, int64(7), a.Version)
	require.Zero(t, a.UnknownFields)
	require.Len(t, a.Tags, 8)
	requireTagValue(t, strTagValue("svc-a"), a.Tags["default.svc"], "default.svc")
	requireTagValue(t, strTagValue("inst-1"), a.Tags["default.inst"], "default.inst")
	requireTagValue(t, int64TagValue(200), a.Tags["default.code"], "default.code")
	requireTagValue(t, strArrTagValue([]string{"x", "y", "z"}), a.Tags["default.labels"], "default.labels")
	requireTagValue(t, int64ArrTagValue([]int64{3, 1, 2}), a.Tags["default.ids"], "default.ids")
	requireTagValue(t, binaryDataTagValue([]byte{0, 1, 2}), a.Tags["default.blob"], "default.blob")
	requireTagValue(t, int64TagValue(8080), a.Tags["default.port"], "default.port")
	requireTagValue(t, strTagValue("eu"), a.Tags["extra.region"], "extra.region")

	b := rows["svc-b"]
	require.NotNil(t, b)
	require.Equal(t, points[1].timestamp, b.Timestamp)
	require.Zero(t, b.Version)
	require.Len(t, b.Tags, 3, "null tags are not stored")
	requireTagValue(t, strTagValue("inst-2"), b.Tags["default.inst"], "default.inst")
	requireTagValue(t, int64TagValue(9090), b.Tags["default.port"], "default.port")
}

// TestWalkIndexModeRowsKeepsEmptyString verifies that a stored empty string reads back as an
// empty string, not as null, the way a query returns it.
func TestWalkIndexModeRowsKeepsEmptyString(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sidx")
	im := newExportTestMeasure(exportIndexModeMeasure(), exportIndexRules()[:2])
	doc := indexModeDocFromWrite(t, im, exportIMPoint{
		defaultTags: []*modelv1.TagValue{
			strTagValue("svc-a"), strTagValue("inst-1"), pbv1.NullTagValue,
			pbv1.NullTagValue, pbv1.NullTagValue, pbv1.NullTagValue, pbv1.NullTagValue,
		},
		extraTags: []*modelv1.TagValue{strTagValue("")},
		timestamp: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
	})
	writeIndexModeSourceDocs(t, dir, index.Documents{doc})

	rows := collectIndexModeRows(t, dir, []*databasev1.Measure{exportIndexModeMeasure()}, exportIndexRules())
	require.Len(t, rows, 1)
	requireTagValue(t, strTagValue(""), rows["svc-a"].Tags["extra.region"], "extra.region")
}

// TestWalkIndexModeRowsEntityFromSeries verifies entity tags come from the
// series id even when their stored field cannot be resolved (rule missing).
func TestWalkIndexModeRowsEntityFromSeries(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sidx")
	seedExportSidx(t, dir)

	// Without the rules, the indexed inst / code fields cannot be mapped.
	rows := collectIndexModeRows(t, dir, []*databasev1.Measure{exportIndexModeMeasure()}, nil)
	require.Len(t, rows, 2)
	a := rows["svc-a"]
	require.Equal(t, 2, a.UnknownFields)
	requireTagValue(t, strTagValue("inst-1"), a.Tags["default.inst"], "default.inst")
	require.NotContains(t, a.Tags, "default.code")
}

// TestWalkIndexModeRowsOnlyRequestedMeasures verifies a normal measure passed in
// is ignored, so neither its series documents nor other subjects are visited.
func TestWalkIndexModeRowsOnlyRequestedMeasures(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sidx")
	seedExportSidx(t, dir)

	visited := 0
	require.NoError(t, WalkIndexModeRows(context.Background(), dir, []*databasev1.Measure{exportNormalMeasureSchema()}, exportIndexRules(),
		func(*IndexModeRow) error {
			visited++
			return nil
		}))
	require.Zero(t, visited)
}

func TestWalkIndexModeRowsMissingOrEmptyDir(t *testing.T) {
	measures := []*databasev1.Measure{exportIndexModeMeasure()}
	visit := func(*IndexModeRow) error {
		t.Fatal("no row expected")
		return nil
	}
	missing := filepath.Join(t.TempDir(), "absent", "sidx")
	require.NoError(t, WalkIndexModeRows(context.Background(), missing, measures, nil, visit))
	_, err := os.Stat(missing)
	require.ErrorIs(t, err, fs.ErrNotExist, "the walk must not create the directory")

	empty := t.TempDir()
	require.NoError(t, WalkIndexModeRows(context.Background(), empty, measures, nil, visit))
	entries, err := os.ReadDir(empty)
	require.NoError(t, err)
	require.Empty(t, entries, "the walk must not create files")
}

type exportFileStamp struct {
	modTime time.Time
	size    int64
}

func snapshotExportDir(t *testing.T, dir string) map[string]exportFileStamp {
	t.Helper()
	out := map[string]exportFileStamp{}
	require.NoError(t, filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		out[path] = exportFileStamp{modTime: info.ModTime(), size: info.Size()}
		return nil
	}))
	return out
}

func TestWalkIndexModeRowsIsReadOnly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sidx")
	seedExportSidx(t, dir)
	before := snapshotExportDir(t, dir)

	rows := collectIndexModeRows(t, dir, []*databasev1.Measure{exportIndexModeMeasure()}, exportIndexRules())
	require.Len(t, rows, 2)
	require.Equal(t, before, snapshotExportDir(t, dir), "the walk must not create, remove or modify files")
}

func TestWalkIndexModeRowsVisitErrorStops(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sidx")
	seedExportSidx(t, dir)
	stop := context.Canceled
	visited := 0
	err := WalkIndexModeRows(context.Background(), dir, []*databasev1.Measure{exportIndexModeMeasure()}, exportIndexRules(),
		func(*IndexModeRow) error {
			visited++
			return stop
		})
	require.ErrorIs(t, err, stop)
	require.Equal(t, 1, visited)
}

// TestWalkIndexModeRowsSkipsMultiTagRule verifies the values a multi-tag index rule stores
// under its single rule id are not merged into one tag: they count as unknown fields.
func TestWalkIndexModeRowsSkipsMultiTagRule(t *testing.T) {
	rules := []*databasev1.IndexRule{
		exportIndexRules()[0],
		{
			Metadata: &commonv1.Metadata{Group: exportIMGroup, Name: "code_port", Id: 21},
			Tags:     []string{"code", "port"}, Type: databasev1.IndexRule_TYPE_INVERTED,
		},
	}
	im := newExportTestMeasure(exportIndexModeMeasure(), rules)
	point := exportIMPoint{
		defaultTags: []*modelv1.TagValue{
			strTagValue("svc-a"), strTagValue("inst-1"), int64TagValue(200),
			pbv1.NullTagValue, pbv1.NullTagValue, pbv1.NullTagValue, int64TagValue(8080),
		},
		extraTags: []*modelv1.TagValue{strTagValue("eu")},
		timestamp: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
		version:   1,
	}
	dir := filepath.Join(t.TempDir(), "sidx")
	writeIndexModeSourceDocs(t, dir, index.Documents{indexModeDocFromWrite(t, im, point)})

	rows := collectIndexModeRows(t, dir, []*databasev1.Measure{exportIndexModeMeasure()}, rules)
	require.Len(t, rows, 1)
	a := rows["svc-a"]
	require.Zero(t, a.UnknownFields, "code and port are stored under the multi-tag rule id, which the export reports once per measure")
	require.NotContains(t, a.Tags, "default.code")
	require.NotContains(t, a.Tags, "default.port")
	requireTagValue(t, strTagValue("inst-1"), a.Tags["default.inst"], "default.inst")
	requireTagValue(t, strTagValue("eu"), a.Tags["extra.region"], "extra.region")
}
