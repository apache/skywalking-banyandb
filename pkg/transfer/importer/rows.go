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
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
)

// nullTag and nullField are the shared, read-only null values of every row: a message only
// reads them when it is encoded.
var (
	nullTag   = &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
	nullField = &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
)

// errCorrupt marks a row the artifact cannot have produced: the import stops with exit 3.
var errCorrupt = errors.New("artifact is corrupt")

// corrupt wraps a row problem as an artifact corruption.
func corrupt(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errCorrupt, fmt.Sprintf(format, args...))
}

// row is one assembled import row and where in the artifact it comes from.
type row struct {
	msg    proto.Message
	file   string
	record uint64
}

// rowBuilder assembles the rows of one (catalog, group) from the artifact's cells into the
// target schema's declaration order. Artifact columns the target resource lacks are dropped
// and target tags or fields the artifact lacks are sent as null (the ignore policy); the
// schema comparison already refused differing types. A rowBuilder is read-only once built.
type rowBuilder struct {
	layout    *csvfmt.Layout
	resources map[string]*resourceMap
	group     string
	// fixed is the column of every fixed column kind, -1 when the layout lacks it.
	fixed   [csvfmt.KindTag]int
	catalog commonv1.Catalog
	// targetColumns tells that the layout comes from the target's schema, the artifact
	// carrying none.
	targetColumns bool
}

// resourceMap places the artifact columns of one resource into its target schema order: -1
// marks a target tag or field the artifact does not have.
type resourceMap struct {
	trace *databasev1.Trace
	// families[f][t] is the artifact column of the t-th tag of the f-th target tag family;
	// trace and property use families[0] for their flat tag lists.
	families [][]int
	names    [][]string
	types    [][]csvfmt.ValueType
	fields   []int
	ftypes   []csvfmt.ValueType
	// entity lists the positions (family, tag) of the stream's or measure's entity tags.
	entity [][2]int
	// traceIdx, spanIdx and tsIdx are the positions of a trace's trace id, span id and
	// timestamp tags, -1 for a tag the trace does not declare.
	traceIdx, spanIdx, tsIdx int
}

func newRowBuilder(layout *csvfmt.Layout, target *Schemas, catalog commonv1.Catalog, group string) *rowBuilder {
	b := &rowBuilder{layout: layout, catalog: catalog, group: group, resources: map[string]*resourceMap{}}
	for k := range b.fixed {
		b.fixed[k] = -1
	}
	for i, c := range layout.Columns {
		if c.Kind != csvfmt.KindTag && c.Kind != csvfmt.KindField {
			b.fixed[c.Kind] = i
		}
	}
	switch catalog {
	case commonv1.Catalog_CATALOG_STREAM:
		for k, s := range target.Streams {
			if k.Group == group {
				b.resources[k.Name] = b.familyMap(k.Name, s.GetTagFamilies(), nil, s.GetEntity().GetTagNames())
			}
		}
	case commonv1.Catalog_CATALOG_MEASURE:
		for k, m := range target.Measures {
			if k.Group == group {
				b.resources[k.Name] = b.familyMap(k.Name, m.GetTagFamilies(), m.GetFields(), m.GetEntity().GetTagNames())
			}
		}
	case commonv1.Catalog_CATALOG_TRACE:
		for k, tr := range target.Traces {
			if k.Group == group {
				rm := flatMap(b, k.Name, tr.GetTags())
				rm.trace = tr
				rm.traceIdx, rm.spanIdx, rm.tsIdx = -1, -1, -1
				for i, t := range tr.GetTags() {
					switch t.GetName() {
					case tr.GetTraceIdTagName():
						rm.traceIdx = i
					case tr.GetSpanIdTagName():
						rm.spanIdx = i
					case tr.GetTimestampTagName():
						rm.tsIdx = i
					}
				}
				b.resources[k.Name] = rm
			}
		}
	case commonv1.Catalog_CATALOG_PROPERTY:
		for k, p := range target.Properties {
			if k.Group == group {
				b.resources[k.Name] = flatMap(b, k.Name, p.GetTags())
			}
		}
	}
	return b
}

func (b *rowBuilder) column(resource, family, tag string) (int, csvfmt.ValueType) {
	idx, ok := b.layout.TagIndex(resource, family, tag)
	if !ok {
		return -1, 0
	}
	return idx, b.layout.Columns[idx].Type
}

func (b *rowBuilder) familyMap(resource string, families []*databasev1.TagFamilySpec, fields []*databasev1.FieldSpec, entity []string) *resourceMap {
	rm := &resourceMap{}
	for fi, fam := range families {
		cols := make([]int, len(fam.GetTags()))
		types := make([]csvfmt.ValueType, len(fam.GetTags()))
		names := make([]string, len(fam.GetTags()))
		for ti, tag := range fam.GetTags() {
			cols[ti], types[ti] = b.column(resource, fam.GetName(), tag.GetName())
			names[ti] = tag.GetName()
			if slices.Contains(entity, tag.GetName()) {
				rm.entity = append(rm.entity, [2]int{fi, ti})
			}
		}
		rm.families = append(rm.families, cols)
		rm.types = append(rm.types, types)
		rm.names = append(rm.names, names)
	}
	for _, f := range fields {
		idx, ok := b.layout.FieldIndex(resource, f.GetName())
		if !ok {
			rm.fields = append(rm.fields, -1)
			rm.ftypes = append(rm.ftypes, 0)
			continue
		}
		rm.fields = append(rm.fields, idx)
		rm.ftypes = append(rm.ftypes, b.layout.Columns[idx].Type)
	}
	return rm
}

func flatMap[T interface{ GetName() string }](b *rowBuilder, resource string, tags []T) *resourceMap {
	cols := make([]int, len(tags))
	types := make([]csvfmt.ValueType, len(tags))
	names := make([]string, len(tags))
	for i, t := range tags {
		cols[i], types[i] = b.column(resource, "", t.GetName())
		names[i] = t.GetName()
	}
	return &resourceMap{families: [][]int{cols}, types: [][]csvfmt.ValueType{types}, names: [][]string{names}}
}

// resource checks the row against the layout and returns its resource and target mapping.
func (b *rowBuilder) resource(cells []csvfmt.Value) (string, *resourceMap, error) {
	if err := b.layout.CheckRow(cells); err != nil {
		return "", nil, corrupt("%v", err)
	}
	name := cells[0].Str
	rm, ok := b.resources[name]
	if !ok {
		return "", nil, corrupt("%s %s/%s is not in the target schema", transfer.CatalogName(b.catalog), b.group, name)
	}
	return name, rm, nil
}

// cell returns the cell of a fixed column, or a null cell when the layout lacks it.
func (b *rowBuilder) cell(cells []csvfmt.Value, kind csvfmt.ColumnKind) csvfmt.Value {
	idx := b.fixed[kind]
	if idx < 0 {
		return csvfmt.Value{Null: true}
	}
	return cells[idx]
}

func tagFamilies(cells []csvfmt.Value, rm *resourceMap) []*modelv1.TagFamilyForWrite {
	out := make([]*modelv1.TagFamilyForWrite, len(rm.families))
	for fi, cols := range rm.families {
		tags := make([]*modelv1.TagValue, len(cols))
		for ti, idx := range cols {
			if idx < 0 || cells[idx].Null {
				tags[ti] = nullTag
				continue
			}
			tags[ti] = cells[idx].TagValue(rm.types[fi][ti])
		}
		out[fi] = &modelv1.TagFamilyForWrite{Tags: tags}
	}
	return out
}

// timestamp returns the row's _timestamp in nanoseconds; it must not be null.
func (b *rowBuilder) timestamp(cells []csvfmt.Value) (int64, error) {
	ts := b.cell(cells, csvfmt.KindTimestamp)
	if ts.Null {
		return 0, corrupt("_timestamp is null")
	}
	return ts.Int, nil
}

// versionAndTime returns a measure row's version, which must be present and not 0, and its timestamp.
func (b *rowBuilder) versionAndTime(cells []csvfmt.Value, name string) (version, ts int64, err error) {
	v := b.cell(cells, csvfmt.KindVersion)
	if v.Null || v.Int == 0 {
		return 0, 0, corrupt("_version of measure %s is null or 0", name)
	}
	if ts, err = b.timestamp(cells); err != nil {
		return 0, 0, err
	}
	return v.Int, ts, nil
}

// stream assembles a stream row: the element id cell is both the raw element id and the
// element_id string, so the target stores the source's id.
func (b *rowBuilder) stream(cells []csvfmt.Value) (*transferv1.ImportRowsRequest_StreamRow, error) {
	name, rm, err := b.resource(cells)
	if err != nil {
		return nil, err
	}
	idCell := b.cell(cells, csvfmt.KindElementID)
	if idCell.Null {
		return nil, corrupt("_element_id is null")
	}
	id, err := csvfmt.DecodeElementID(idCell.Str)
	if err != nil {
		return nil, corrupt("%v", err)
	}
	if id == 0 {
		return nil, corrupt("_element_id is 0")
	}
	ts, err := b.timestamp(cells)
	if err != nil {
		return nil, err
	}
	return &transferv1.ImportRowsRequest_StreamRow{
		Name:         name,
		RawElementId: id,
		Element:      &streamv1.ElementValue{ElementId: idCell.Str, Timestamp: timestamppb.New(time.Unix(0, ts)), TagFamilies: tagFamilies(cells, rm)},
	}, nil
}

// measure assembles a measure data point; its version must be present and not 0.
func (b *rowBuilder) measure(cells []csvfmt.Value) (*transferv1.ImportRowsRequest_MeasureRow, error) {
	name, rm, err := b.resource(cells)
	if err != nil {
		return nil, err
	}
	version, ts, err := b.versionAndTime(cells, name)
	if err != nil {
		return nil, err
	}
	fields := make([]*modelv1.FieldValue, len(rm.fields))
	for i, idx := range rm.fields {
		if idx < 0 || cells[idx].Null {
			fields[i] = nullField
			continue
		}
		fields[i] = cells[idx].FieldValue(rm.ftypes[i])
	}
	return &transferv1.ImportRowsRequest_MeasureRow{
		Name:  name,
		Point: &measurev1.DataPointValue{Timestamp: timestamppb.New(time.Unix(0, ts)), TagFamilies: tagFamilies(cells, rm), Fields: fields, Version: version},
	}, nil
}

// measureKey appends to buf the key of a data point for the index-mode dedup (name, entity
// tags) straight from its cells, and returns the point's version and timestamp. The target
// keeps one index-mode document per series in a segment, so the timestamp is not part of the
// key. Every part is length-prefixed or typed, so distinct series never share a key.
func (b *rowBuilder) measureKey(buf []byte, cells []csvfmt.Value) ([]byte, int64, int64, error) {
	name, rm, err := b.resource(cells)
	if err != nil {
		return buf, 0, 0, err
	}
	version, ts, err := b.versionAndTime(cells, name)
	if err != nil {
		return buf, 0, 0, err
	}
	buf = appendKeyString(buf, name)
	for _, pos := range rm.entity {
		idx := rm.families[pos[0]][pos[1]]
		if idx < 0 || cells[idx].Null {
			buf = append(buf, 'n')
			continue
		}
		c := cells[idx]
		switch rm.types[pos[0]][pos[1]] {
		case csvfmt.ValueString:
			buf = appendKeyString(append(buf, 's'), c.Str)
		case csvfmt.ValueInt, csvfmt.ValueTimestamp:
			buf = strconv.AppendInt(append(buf, 'i'), c.Int, 10)
			buf = append(buf, ';')
		case csvfmt.ValueBinary:
			buf = appendKeyString(append(buf, 'b'), string(c.Bytes))
		case csvfmt.ValueStringArray:
			buf = strconv.AppendInt(append(buf, 'S'), int64(len(c.Strs)), 10)
			for _, v := range c.Strs {
				buf = appendKeyString(buf, v)
			}
		case csvfmt.ValueIntArray:
			buf = strconv.AppendInt(append(buf, 'I'), int64(len(c.Ints)), 10)
			for _, v := range c.Ints {
				buf = strconv.AppendInt(append(buf, ';'), v, 10)
			}
			buf = append(buf, ';')
		default:
			// A type a tag cannot hold is sent as null.
			buf = append(buf, 'n')
		}
	}
	return buf, version, ts, nil
}

// appendKeyString appends s with its length prefix.
func appendKeyString(buf []byte, s string) []byte {
	buf = strconv.AppendInt(buf, int64(len(s)), 10)
	buf = append(buf, ':')
	return append(buf, s...)
}

// trace assembles a span: tags in the target order, the trace and span id columns as
// strings in their tags' positions, and a non-null timestamp tag.
func (b *rowBuilder) trace(cells []csvfmt.Value) (*transferv1.ImportRowsRequest_TraceRow, error) {
	name, rm, err := b.resource(cells)
	if err != nil {
		return nil, err
	}
	tags := make([]*modelv1.TagValue, len(rm.families[0]))
	for i, idx := range rm.families[0] {
		var v *modelv1.TagValue
		switch {
		case i == rm.traceIdx:
			v, err = idTag(b.cell(cells, csvfmt.KindTraceID), "trace_id")
		case i == rm.spanIdx:
			v, err = idTag(b.cell(cells, csvfmt.KindSpanID), "span_id")
		case idx < 0 || cells[idx].Null:
			v = nullTag
		default:
			v = cells[idx].TagValue(rm.types[0][i])
		}
		if err != nil {
			return nil, err
		}
		if i == rm.tsIdx {
			if _, isNull := v.GetValue().(*modelv1.TagValue_Null); isNull {
				return nil, corrupt("timestamp tag %s of trace %s is null", rm.names[0][i], name)
			}
		}
		tags[i] = v
	}
	span := b.cell(cells, csvfmt.KindSpan)
	return &transferv1.ImportRowsRequest_TraceRow{Name: name, Tags: tags, Span: span.Bytes}, nil
}

func idTag(c csvfmt.Value, column string) (*modelv1.TagValue, error) {
	if c.Null || c.Str == "" {
		return nil, corrupt("%s is null or empty", column)
	}
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: c.Str}}}, nil
}

// property assembles a property revision: _id must be present and _mod_revision not null;
// _delete_time 0 or null is live, positive a tombstone and negative corrupt; a null
// _updated_at stays unset and null tags are omitted.
func (b *rowBuilder) property(cells []csvfmt.Value) (*transferv1.ImportRowsRequest_PropertyRow, error) {
	name, rm, err := b.resource(cells)
	if err != nil {
		return nil, err
	}
	id := b.cell(cells, csvfmt.KindPropertyID)
	if id.Null || id.Str == "" {
		return nil, corrupt("_id of property %s is null or empty", name)
	}
	modRev := b.cell(cells, csvfmt.KindModRevision)
	if modRev.Null {
		return nil, corrupt("_mod_revision of property %s/%s is null", name, id.Str)
	}
	var deleteTime int64
	if dt := b.cell(cells, csvfmt.KindDeleteTime); !dt.Null {
		if dt.Int < 0 {
			return nil, corrupt("_delete_time of property %s/%s is negative", name, id.Str)
		}
		deleteTime = dt.Int
	}
	p := &propertyv1.Property{
		Metadata: &commonv1.Metadata{Group: b.group, Name: name, ModRevision: modRev.Int, CreateRevision: b.cell(cells, csvfmt.KindCreateRevision).Int},
		Id:       id.Str,
	}
	if ua := b.cell(cells, csvfmt.KindUpdatedAt); !ua.Null {
		p.UpdatedAt = timestamppb.New(time.Unix(0, ua.Int))
	}
	for i, idx := range rm.families[0] {
		if idx < 0 || cells[idx].Null {
			continue
		}
		p.Tags = append(p.Tags, &modelv1.Tag{Key: rm.names[0][i], Value: cells[idx].TagValue(rm.types[0][i])})
	}
	return &transferv1.ImportRowsRequest_PropertyRow{Property: p, DeleteTime: deleteTime}, nil
}

// build assembles one row of the builder's catalog.
func (b *rowBuilder) build(cells []csvfmt.Value) (proto.Message, error) {
	switch b.catalog {
	case commonv1.Catalog_CATALOG_STREAM:
		return b.stream(cells)
	case commonv1.Catalog_CATALOG_MEASURE:
		return b.measure(cells)
	case commonv1.Catalog_CATALOG_TRACE:
		return b.trace(cells)
	case commonv1.Catalog_CATALOG_PROPERTY:
		return b.property(cells)
	default:
		return nil, fmt.Errorf("unsupported catalog %s", b.catalog)
	}
}

// scanFile reads every record of one artifact file and hands its cells to visit. It stops
// with the context's cause once ctx is done.
func (b *rowBuilder) scanFile(ctx context.Context, dir string, f manifest.File, visit func(file string, record uint64, cells []csvfmt.Value) error) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	file, err := os.Open(filepath.Join(dir, filepath.FromSlash(f.Path)))
	if err != nil {
		return fmt.Errorf("open %s: %w", f.Path, err)
	}
	defer file.Close()
	rd, err := csvfmt.NewReader(file, b.layout, csvfmt.ReaderOptions{Gzip: strings.HasSuffix(f.Path, ".gz")})
	if err != nil {
		if b.targetColumns {
			return fmt.Errorf("file %s: its columns differ from the target's schema, which reads an artifact exported without its schema: %w", f.Path, err)
		}
		return fmt.Errorf("%w: file %s: %w", errCorrupt, f.Path, err)
	}
	defer func() { _ = rd.Close() }()
	for {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		cells, err := rd.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%w: file %s: %w", errCorrupt, f.Path, err)
		}
		if err = visit(f.Path, rd.RecordNo(), cells); err != nil {
			return err
		}
	}
}

// scanSources scans every file of every source of item in order.
func (b *rowBuilder) scanSources(ctx context.Context, dir string, item *WorkItem, visit func(file string, record uint64, cells []csvfmt.Value) error) error {
	for _, src := range item.Sources {
		for _, f := range src.Files {
			if err := b.scanFile(ctx, dir, f, visit); err != nil {
				return err
			}
		}
	}
	return nil
}

// buildRow assembles the record at (file, record) and hands it to visit.
func (b *rowBuilder) buildRow(file string, record uint64, cells []csvfmt.Value, visit func(row) error) error {
	msg, err := b.build(cells)
	if err != nil {
		return fmt.Errorf("file %s record %d: %w", file, record, err)
	}
	return visit(row{msg: msg, file: file, record: record})
}

// readSources assembles every record of every source of item in order.
func (b *rowBuilder) readSources(ctx context.Context, dir string, item *WorkItem, visit func(row) error) error {
	return b.scanSources(ctx, dir, item, func(file string, record uint64, cells []csvfmt.Value) error {
		return b.buildRow(file, record, cells, visit)
	})
}

// itemRows streams the rows of a work item to emit: a segment item's index-mode rows
// deduplicated by (name, entity) keeping the largest version; any other item's
// files source by source, each row as it is read. A property item's revisions are sent in
// file order too: the target's repair refuses a revision older than the newest one an entity
// holds and resolves a same-revision conflict deterministically (a tombstone wins, then the
// smaller delete time), so every entity ends at its latest revision whatever the order.
func (b *rowBuilder) itemRows(ctx context.Context, dir string, item *WorkItem, emit func(row) error) error {
	if item.Kind == SegmentItem {
		return b.segmentRows(ctx, dir, item, emit)
	}
	return b.readSources(ctx, dir, item, emit)
}

// rowPos is where a row sits in the artifact.
type rowPos struct {
	file   string
	record uint64
}

// segmentRows sends an index-mode segment item. A single source is sent as read; several
// sources are read twice: the first pass keeps only the position of the largest version
// of each (name, entity), the larger timestamp on a version tie and the first seen on a full
// tie, and the second sends the rows at those positions, so memory holds the max-version
// table rather than the rows. The target keeps one document per series and the last write
// wins, so sending one row per series keeps arrival order from deciding the result.
func (b *rowBuilder) segmentRows(ctx context.Context, dir string, item *WorkItem, emit func(row) error) error {
	if b.catalog != commonv1.Catalog_CATALOG_MEASURE || len(item.Sources) <= 1 {
		return b.readSources(ctx, dir, item, emit)
	}
	type winner struct {
		pos     rowPos
		version int64
		ts      int64
	}
	winners := map[string]winner{}
	var key []byte
	err := b.scanSources(ctx, dir, item, func(file string, record uint64, cells []csvfmt.Value) error {
		var v, ts int64
		var keyErr error
		if key, v, ts, keyErr = b.measureKey(key[:0], cells); keyErr != nil {
			return fmt.Errorf("file %s record %d: %w", file, record, keyErr)
		}
		if w, ok := winners[string(key)]; !ok || v > w.version || (v == w.version && ts > w.ts) {
			winners[string(key)] = winner{pos: rowPos{file: file, record: record}, version: v, ts: ts}
		}
		return nil
	})
	if err != nil {
		return err
	}
	keep := make(map[rowPos]struct{}, len(winners))
	for _, w := range winners {
		keep[w.pos] = struct{}{}
	}
	clear(winners)
	return b.scanSources(ctx, dir, item, func(file string, record uint64, cells []csvfmt.Value) error {
		if _, ok := keep[rowPos{file: file, record: record}]; !ok {
			return nil
		}
		return b.buildRow(file, record, cells, emit)
	})
}
