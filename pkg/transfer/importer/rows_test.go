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

package importer

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
)

// decodeRows writes rows as a CSV file for the artifact layout and reads them back through
// a builder for the target schema.
func decodeRows(t *testing.T, art, target *Schemas, catalog commonv1.Catalog, group string, rows ...cells) ([]row, error) {
	t.Helper()
	layout := layoutOf(t, art, catalog, group)
	dir := t.TempDir()
	f := writeFile(t, dir, "data.csv", csvFile(t, layout, rows...))
	b := newRowBuilder(layout, target, catalog, group)
	var out []row
	err := b.itemRows(context.Background(), dir, &WorkItem{Kind: ShardItem, Sources: []WorkSource{{Files: []manifest.File{f}}}}, func(r row) error {
		out = append(out, r)
		return nil
	})
	return out, err
}

func strTag(s string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s}}}
}

func TestStreamRowFollowsTheTargetSchema(t *testing.T) {
	art := dataSchemas()
	target := dataSchemas()
	// The target drops latency, adds region and declares its tags in another order.
	target.Streams[ResourceKey{"sw_stream", "log"}] = &databasev1.Stream{
		Metadata: md("sw_stream", "log"),
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "region", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
		}}},
	}
	rows, err := decodeRows(t, art, target, commonv1.Catalog_CATALOG_STREAM, "sw_stream",
		cells{"_name": "log", "_element_id": csvfmt.EncodeElementID(42), "_timestamp": cellTime(0), "default.svc": "a", "default.latency": "10"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	r := rows[0].msg.(*transferv1.ImportRowsRequest_StreamRow)
	assert.Equal(t, "log", r.GetName())
	assert.Equal(t, uint64(42), r.GetRawElementId())
	assert.Equal(t, csvfmt.EncodeElementID(42), r.GetElement().GetElementId())
	assert.Equal(t, dataTime, r.GetElement().GetTimestamp().AsTime())
	require.Len(t, r.GetElement().GetTagFamilies(), 1)
	assert.Equal(t, []*modelv1.TagValue{nullTag, strTag("a")}, r.GetElement().GetTagFamilies()[0].GetTags())
	assert.Equal(t, uint64(1), rows[0].record)
	assert.Equal(t, "data.csv", rows[0].file)
}

func TestCorruptRowsAreRefused(t *testing.T) {
	traceRow := func(traceID, spanID string) cells {
		c := cells{"_name": "segment", "_span": csvfmt.EncodeBinary([]byte("x")), "ts": cellTime(0), "span_id": spanID}
		if traceID != "" {
			c["trace_id"] = traceID
		}
		return c
	}
	cases := map[string]struct {
		row     cells
		art     *Schemas
		group   string
		catalog commonv1.Catalog
	}{
		"null trace id": {
			catalog: commonv1.Catalog_CATALOG_TRACE, group: "sw_trace", art: traceSchemas(),
			row: traceRow("", "s1"),
		},
		"empty span id": {
			catalog: commonv1.Catalog_CATALOG_TRACE, group: "sw_trace", art: traceSchemas(),
			row: traceRow("t1", ""),
		},
		"element id 0": {
			catalog: commonv1.Catalog_CATALOG_STREAM, group: "sw_stream",
			row: cells{"_name": "log", "_element_id": csvfmt.EncodeElementID(0), "_timestamp": cellTime(0)},
		},
		"null element id": {
			catalog: commonv1.Catalog_CATALOG_STREAM, group: "sw_stream",
			row: cells{"_name": "log", "_timestamp": cellTime(0)},
		},
		"null version": {
			catalog: commonv1.Catalog_CATALOG_MEASURE, group: "sw_metric",
			row: cells{"_name": "cpm", "_timestamp": cellTime(0), "default.entity_id": "e"},
		},
		"version 0": {
			catalog: commonv1.Catalog_CATALOG_MEASURE, group: "sw_metric",
			row: cells{"_name": "cpm", "_timestamp": cellTime(0), "_version": "0", "default.entity_id": "e"},
		},
		"negative delete time": {
			catalog: commonv1.Catalog_CATALOG_PROPERTY, group: "sw_prop",
			row: cells{"_name": "ui", "_id": "p", "_mod_revision": "1", "_create_revision": "1", "_delete_time": "-1"},
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			art := c.art
			if art == nil {
				art = dataSchemas()
			}
			_, err := decodeRows(t, art, art, c.catalog, c.group, c.row)
			require.Error(t, err)
			assert.True(t, errors.Is(err, errCorrupt), err)
			assert.ErrorContains(t, err, "file data.csv record 1")
		})
	}
}

func TestResourceMissingFromTheTargetIsCorrupt(t *testing.T) {
	art := dataSchemas()
	target := dataSchemas()
	delete(target.Streams, ResourceKey{"sw_stream", "log"})
	_, err := decodeRows(t, art, target, commonv1.Catalog_CATALOG_STREAM, "sw_stream",
		cells{"_name": "log", "_element_id": csvfmt.EncodeElementID(1), "_timestamp": cellTime(0)})
	assert.ErrorIs(t, err, errCorrupt)
	assert.ErrorContains(t, err, "not in the target schema")
}

func TestMeasureRowKeepsVersionAndFieldOrder(t *testing.T) {
	art := dataSchemas()
	rows, err := decodeRows(t, art, art, commonv1.Catalog_CATALOG_MEASURE, "sw_metric",
		cells{"_name": "cpm", "_timestamp": cellTime(0), "_version": "-3", "default.entity_id": "e1", "cpm::_field.value": "9"})
	require.NoError(t, err)
	r := rows[0].msg.(*transferv1.ImportRowsRequest_MeasureRow)
	assert.Equal(t, int64(-3), r.GetPoint().GetVersion(), "a negative version is valid data")
	assert.Equal(t, []*modelv1.TagValue{strTag("e1"), nullTag}, r.GetPoint().GetTagFamilies()[0].GetTags())
	require.Len(t, r.GetPoint().GetFields(), 1)
	assert.Equal(t, int64(9), r.GetPoint().GetFields()[0].GetInt().GetValue())
}

func traceSchemas() *Schemas {
	s := NewSchemas()
	s.Groups["sw_trace"] = testGroup("sw_trace", commonv1.Catalog_CATALOG_TRACE)
	tr := testTrace("sw_trace", "segment")
	tr.Tags = append(tr.Tags, &databasev1.TraceTagSpec{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING})
	s.Traces[ResourceKey{"sw_trace", "segment"}] = tr
	return &s
}

func TestTraceRowPlacesTheIDColumnsInTheirTags(t *testing.T) {
	s := traceSchemas()
	rows, err := decodeRows(t, s, s, commonv1.Catalog_CATALOG_TRACE, "sw_trace",
		cells{"_name": "segment", "trace_id": "t1", "span_id": "s1", "_span": csvfmt.EncodeBinary([]byte("span")), "ts": cellTime(0), "svc": "a"})
	require.NoError(t, err)
	r := rows[0].msg.(*transferv1.ImportRowsRequest_TraceRow)
	require.Len(t, r.GetTags(), 4)
	assert.Equal(t, "t1", r.GetTags()[0].GetStr().GetValue())
	assert.Equal(t, "s1", r.GetTags()[1].GetStr().GetValue())
	assert.Equal(t, dataTime, r.GetTags()[2].GetTimestamp().AsTime())
	assert.Equal(t, "a", r.GetTags()[3].GetStr().GetValue())
	assert.Equal(t, []byte("span"), r.GetSpan())

	_, err = decodeRows(t, s, s, commonv1.Catalog_CATALOG_TRACE, "sw_trace",
		cells{"_name": "segment", "trace_id": "t1", "span_id": "s1", "_span": csvfmt.EncodeBinary(nil)})
	assert.ErrorIs(t, err, errCorrupt, "a null timestamp tag is corrupt")
}

func TestTraceRowFollowsTheTargetTagOrder(t *testing.T) {
	art := traceSchemas()
	target := traceSchemas()
	tr := target.Traces[ResourceKey{"sw_trace", "segment"}]
	byName := map[string]*databasev1.TraceTagSpec{}
	for _, tag := range tr.Tags {
		byName[tag.Name] = tag
	}
	tr.Tags = []*databasev1.TraceTagSpec{
		byName["svc"], byName["ts"], byName["span_id"], {Name: "region", Type: databasev1.TagType_TAG_TYPE_STRING}, byName["trace_id"],
	}
	rows, err := decodeRows(t, art, target, commonv1.Catalog_CATALOG_TRACE, "sw_trace",
		cells{"_name": "segment", "trace_id": "t1", "span_id": "s1", "_span": csvfmt.EncodeBinary([]byte("span")), "ts": cellTime(0), "svc": "a"})
	require.NoError(t, err)
	tags := rows[0].msg.(*transferv1.ImportRowsRequest_TraceRow).GetTags()
	require.Len(t, tags, 5)
	assert.Equal(t, strTag("a"), tags[0])
	assert.Equal(t, dataTime, tags[1].GetTimestamp().AsTime())
	assert.Equal(t, strTag("s1"), tags[2])
	assert.Equal(t, nullTag, tags[3], "a target tag missing from the artifact is null")
	assert.Equal(t, strTag("t1"), tags[4])
}

func TestPropertyRowDecodesDeleteTimeAndOmitsNullTags(t *testing.T) {
	art := dataSchemas()
	rows, err := decodeRows(t, art, art, commonv1.Catalog_CATALOG_PROPERTY, "sw_prop",
		cells{"_name": "ui", "_id": "p1", "_mod_revision": "5", "_create_revision": "1", "_delete_time": "0", "_updated_at": cellTime(0), "content": "x"},
		cells{"_name": "ui", "_id": "p2", "_mod_revision": "6", "_create_revision": "2", "_delete_time": cellTime(time.Hour)},
		cells{"_name": "ui", "_id": "p3", "_mod_revision": "7", "_create_revision": "3"},
	)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	live := rows[0].msg.(*transferv1.ImportRowsRequest_PropertyRow)
	assert.Equal(t, int64(0), live.GetDeleteTime())
	assert.Equal(t, "sw_prop", live.GetProperty().GetMetadata().GetGroup())
	assert.Equal(t, int64(5), live.GetProperty().GetMetadata().GetModRevision())
	assert.Equal(t, int64(1), live.GetProperty().GetMetadata().GetCreateRevision())
	assert.Equal(t, dataTime, live.GetProperty().GetUpdatedAt().AsTime())
	require.Len(t, live.GetProperty().GetTags(), 1)
	assert.Equal(t, "content", live.GetProperty().GetTags()[0].GetKey())

	tomb := rows[1].msg.(*transferv1.ImportRowsRequest_PropertyRow)
	assert.Equal(t, dataTime.Add(time.Hour).UnixNano(), tomb.GetDeleteTime())
	assert.Nil(t, tomb.GetProperty().GetUpdatedAt(), "a null _updated_at stays unset")
	assert.Empty(t, tomb.GetProperty().GetTags(), "a null tag is not written")
	assert.Equal(t, int64(0), rows[2].msg.(*transferv1.ImportRowsRequest_PropertyRow).GetDeleteTime(), "a null _delete_time is live")
}

// itemOf writes one file per source and returns a work item of kind over them.
func itemOf(t *testing.T, s *Schemas, catalog commonv1.Catalog, group string, kind WorkItemKind, sources ...[]cells) (string, *WorkItem) {
	t.Helper()
	dir := t.TempDir()
	layout := layoutOf(t, s, catalog, group)
	item := &WorkItem{Kind: kind, Catalog: catalog, Group: group}
	for i, rows := range sources {
		f := writeFile(t, dir, "src"+string(rune('a'+i))+".csv", csvFile(t, layout, rows...))
		item.Sources = append(item.Sources, WorkSource{Node: "data-" + string(rune('a'+i)), Files: []manifest.File{f}})
	}
	return dir, item
}

func collectRows(t *testing.T, s *Schemas, dir string, item *WorkItem) []row {
	t.Helper()
	layout := layoutOf(t, s, item.Catalog, item.Group)
	var out []row
	require.NoError(t, newRowBuilder(layout, s, item.Catalog, item.Group).itemRows(context.Background(), dir, item, func(r row) error {
		out = append(out, r)
		return nil
	}))
	return out
}

func TestSegmentItemKeepsTheLargestVersionPerSeries(t *testing.T) {
	s := dataSchemas()
	point := func(entity, version string, d time.Duration) cells {
		return cells{"_name": "cpm", "_timestamp": cellTime(d), "_version": version, "default.entity_id": entity}
	}
	dir, item := itemOf(t, s, commonv1.Catalog_CATALOG_MEASURE, "sw_metric", SegmentItem,
		[]cells{point("e1", "1", 0), point("e2", "5", 0), point("e3", "7", 2*time.Minute), point("e4", "4", 0)},
		[]cells{point("e1", "3", 0), point("e2", "2", 0), point("e1", "2", time.Minute), point("e3", "3", time.Minute), point("e4", "4", time.Minute)},
	)
	rows := collectRows(t, s, dir, item)
	var got []string
	for _, r := range rows {
		m := r.msg.(*transferv1.ImportRowsRequest_MeasureRow)
		got = append(got, m.GetPoint().GetTagFamilies()[0].GetTags()[0].GetStr().GetValue()+"@"+
			m.GetPoint().GetTimestamp().AsTime().Format(time.Kitchen)+"/v"+string(rune('0'+m.GetPoint().GetVersion())))
	}
	// The target keeps one document per series, so each series is sent once: its largest
	// version whatever the timestamp (e3 keeps the first source's newer version), the larger
	// timestamp on a version tie (e4).
	assert.Equal(t, []string{"e2@5:00AM/v5", "e3@5:02AM/v7", "e1@5:00AM/v3", "e4@5:01AM/v4"}, got)
}

func TestSegmentItemOfOneSourceIsSentAsRead(t *testing.T) {
	s := dataSchemas()
	point := func(entity, version string) cells {
		return cells{"_name": "cpm", "_timestamp": cellTime(0), "_version": version, "default.entity_id": entity}
	}
	dir, item := itemOf(t, s, commonv1.Catalog_CATALOG_MEASURE, "sw_metric", SegmentItem,
		[]cells{point("e1", "3"), point("e2", "5"), point("e1", "1")})
	rows := collectRows(t, s, dir, item)
	var got []uint64
	for _, r := range rows {
		got = append(got, r.record)
	}
	assert.Equal(t, []uint64{1, 2, 3}, got, "no deduplication without several sources")
}

func TestItemRowsStopOnACanceledContext(t *testing.T) {
	s := dataSchemas()
	measure := func(entity string) cells {
		return cells{"_name": "cpm", "_timestamp": cellTime(0), "_version": "1", "default.entity_id": entity}
	}
	prop := cells{"_name": "ui", "_id": "p1", "_mod_revision": "1", "_create_revision": "1"}
	segDir, seg := itemOf(t, s, commonv1.Catalog_CATALOG_MEASURE, "sw_metric", SegmentItem, []cells{measure("e1")}, []cells{measure("e2")})
	propDir, props := itemOf(t, s, commonv1.Catalog_CATALOG_PROPERTY, "sw_prop", PropertyItem, []cells{prop})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for dir, item := range map[string]*WorkItem{segDir: seg, propDir: props} {
		b := newRowBuilder(layoutOf(t, s, item.Catalog, item.Group), s, item.Catalog, item.Group)
		err := b.itemRows(ctx, dir, item, func(row) error {
			t.Errorf("%s item emitted a row after cancellation", item.Catalog)
			return nil
		})
		assert.ErrorIs(t, err, context.Canceled)
		assert.NotErrorIs(t, err, errCorrupt)
	}
}

// propertyHistory renders property rows as id@mod_revision/delete_time.
func propertyHistory(rows []row) []string {
	var got []string
	for _, r := range rows {
		p := r.msg.(*transferv1.ImportRowsRequest_PropertyRow)
		got = append(got, fmt.Sprintf("%s@%d/%d", p.GetProperty().GetId(), p.GetProperty().GetMetadata().GetModRevision(), p.GetDeleteTime()))
	}
	return got
}

func TestPropertyItemSendsRowsInFileOrder(t *testing.T) {
	s := dataSchemas()
	rev := func(id, mod, deleteTime string) cells {
		return cells{"_name": "ui", "_id": id, "_mod_revision": mod, "_create_revision": "1", "_delete_time": deleteTime}
	}
	layout := layoutOf(t, s, commonv1.Catalog_CATALOG_PROPERTY, "sw_prop")
	dir := t.TempDir()
	file := func(name string, rows ...cells) manifest.File {
		return writeFile(t, dir, name, csvFile(t, layout, rows...))
	}
	item := &WorkItem{Kind: PropertyItem, Catalog: commonv1.Catalog_CATALOG_PROPERTY, Group: "sw_prop", Sources: []WorkSource{
		{Node: "data-a", Files: []manifest.File{file("a1.csv", rev("p1", "9", "0"), rev("p1", "5", "0"), rev("p1", "1", "0")), file("a2.csv", rev("p2", "4", "0"))}},
		{Node: "data-b", Files: []manifest.File{file("b1.csv", rev("p2", "2", "0"), rev("p1", "5", "70")), file("b2.csv", rev("p1", "9", "0"))}},
	}}
	rows := collectRows(t, s, dir, item)
	// Every row is sent once, as read: newest-first revisions and replica copies included.
	assert.Equal(t, []string{"p1@9/0", "p1@5/0", "p1@1/0", "p2@4/0", "p2@2/0", "p1@5/70", "p1@9/0"}, propertyHistory(rows))
	var files []string
	for _, r := range rows {
		files = append(files, fmt.Sprintf("%s:%d", r.file, r.record))
	}
	assert.Equal(t, []string{"a1.csv:1", "a1.csv:2", "a1.csv:3", "a2.csv:1", "b1.csv:1", "b1.csv:2", "b2.csv:1"}, files)
}

func TestPropertyItemRetainsNoRows(t *testing.T) {
	s := dataSchemas()
	const rowCount, rowBytes = 2000, 8 << 10
	big := strings.Repeat("x", rowBytes)
	rows := make([]cells, 0, rowCount)
	for i := range rowCount {
		rows = append(rows, cells{"_name": "ui", "_id": "p" + strconv.Itoa(i%7), "_mod_revision": strconv.Itoa(rowCount - i), "_create_revision": "1", "content": big})
	}
	dir, item := itemOf(t, s, commonv1.Catalog_CATALOG_PROPERTY, "sw_prop", PropertyItem, rows)
	b := newRowBuilder(layoutOf(t, s, item.Catalog, item.Group), s, item.Catalog, item.Group)
	var ms runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms)
	base := ms.HeapAlloc
	var peak uint64
	sent := 0
	require.NoError(t, b.itemRows(context.Background(), dir, item, func(row) error {
		sent++
		if sent%100 == 0 {
			runtime.GC()
			runtime.ReadMemStats(&ms)
			if ms.HeapAlloc > base {
				peak = max(peak, ms.HeapAlloc-base)
			}
		}
		return nil
	}))
	assert.Equal(t, rowCount, sent)
	// The item holds about 16 MiB of tag values; streaming keeps the heap far below that.
	assert.Less(t, peak, uint64(4<<20), "the heap grew by %d bytes while the item was sent", peak)
}

func TestRowsDoNotDependOnTheLocalTimeZone(t *testing.T) {
	s := dataSchemas()
	dir, item := itemOf(t, s, commonv1.Catalog_CATALOG_STREAM, "sw_stream", ShardItem,
		[]cells{{"_name": "log", "_element_id": csvfmt.EncodeElementID(1), "_timestamp": cellTime(0), "default.svc": "a"}})
	local := time.Local
	t.Cleanup(func() { time.Local = local })
	var seen []string
	for _, zone := range []string{"UTC", "Asia/Shanghai", "America/Los_Angeles"} {
		loc, err := time.LoadLocation(zone)
		require.NoError(t, err)
		time.Local = loc
		rows := collectRows(t, s, dir, item)
		seen = append(seen, rows[0].msg.(*transferv1.ImportRowsRequest_StreamRow).String())
	}
	assert.Equal(t, seen[0], seen[1])
	assert.Equal(t, seen[0], seen[2])
}
