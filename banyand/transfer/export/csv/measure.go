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

package csv

import (
	"bytes"
	"cmp"
	"container/heap"
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumpmeasure "github.com/apache/skywalking-banyandb/banyand/internal/dump/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// IndexModeReader is implemented by the measure backend. Index-mode measures keep their
// rows only as documents of the segment-level series index; ExportIndexModeRows walks the
// rows of the given index-mode measures under sidxDir read-only and hands each to visit
// with its tags keyed "family.tag" (entity tags included) and the number of stored fields
// the current schema no longer declares. A missing or never-committed index is no rows.
// It lives behind an interface because banyand/measure tests import the export package.
type IndexModeReader interface {
	ExportIndexModeRows(ctx context.Context, sidxDir string, measures []*databasev1.Measure, rules []*databasev1.IndexRule,
		visit func(subject string, timestamp, version int64, tags map[string]*modelv1.TagValue, unknownFields int) error) error
}

type measureResource struct {
	tags    map[string]int
	fields  map[string]int
	indexed map[uint32]dump.IndexedTagSpec
	entity  []entityColumn
}

// measureGroup is everything the measure producer needs about the schemas of a group.
type measureGroup struct {
	layout    *csvfmt.Layout
	resources map[string]*measureResource
	revisions map[string]int64
	// lost holds, per measure, the warnings about tags only a multi-tag rule stores.
	lost      map[string][]string
	indexMode []*databasev1.Measure
	rules     []*databasev1.IndexRule
}

func loadMeasureGroup(run *unitRun, group string) (*measureGroup, error) {
	measures, err := run.meta.MeasureRegistry().ListMeasure(run.ctx, schema.ListOpt{Group: group})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list measures of group %s: %v", group, err)
	}
	rules, err := run.meta.IndexRuleRegistry().ListIndexRule(run.ctx, schema.ListOpt{Group: group})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list index rules of group %s: %v", group, err)
	}
	bindings, err := run.meta.IndexRuleBindingRegistry().ListIndexRuleBinding(run.ctx, schema.ListOpt{Group: group})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list index rule bindings of group %s: %v", group, err)
	}
	kept := make([]*databasev1.Measure, 0, len(measures))
	for _, m := range measures {
		if m.GetMetadata().GetName() == csvfmt.TopNResultMeasure {
			continue
		}
		kept = append(kept, m)
	}
	layout, err := csvfmt.MeasureLayout(kept)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "measure group %s cannot be exported as CSV: %v", group, err)
	}
	g := &measureGroup{
		layout:    layout,
		resources: make(map[string]*measureResource, len(kept)),
		revisions: make(map[string]int64, len(kept)),
		rules:     rules,
		lost:      multiTagRuleWarnings(kept, rules, bindings),
	}
	indexed := ruleToTagByMeasure(kept, rules, bindings)
	for _, m := range kept {
		name := m.GetMetadata().GetName()
		g.revisions[name] = m.GetMetadata().GetModRevision()
		if m.GetIndexMode() {
			g.indexMode = append(g.indexMode, m)
		}
		res := &measureResource{
			tags:    tagColumns(layout, name, m.GetTagFamilies()),
			fields:  map[string]int{},
			indexed: indexed[name],
			entity:  entityColumns(layout, name, m.GetEntity(), m.GetTagFamilies()),
		}
		for _, f := range m.GetFields() {
			if idx, ok := layout.FieldIndex(name, f.GetName()); ok {
				res.fields[f.GetName()] = idx
			}
		}
		g.resources[name] = res
	}
	return g, nil
}

// ruleToTagByMeasure maps, per measure, every IndexRuleID bound to it to the tag the rule
// stores: indexed tags are written to the series index only, never to the
// columns. Only single-tag rules are decodable; a multi-tag rule indexes a composite term.
func ruleToTagByMeasure(measures []*databasev1.Measure, rules []*databasev1.IndexRule,
	bindings []*databasev1.IndexRuleBinding,
) map[string]map[uint32]dump.IndexedTagSpec {
	ruleByName := make(map[string]*databasev1.IndexRule, len(rules))
	for _, r := range rules {
		ruleByName[r.GetMetadata().GetName()] = r
	}
	byName := make(map[string]*databasev1.Measure, len(measures))
	for _, m := range measures {
		byName[m.GetMetadata().GetName()] = m
	}
	out := map[string]map[uint32]dump.IndexedTagSpec{}
	for _, b := range bindings {
		if b.GetSubject().GetCatalog() != commonv1.Catalog_CATALOG_MEASURE {
			continue
		}
		m, ok := byName[b.GetSubject().GetName()]
		if !ok {
			continue
		}
		for _, ruleName := range b.GetRules() {
			rule, found := ruleByName[ruleName]
			if !found || len(rule.GetTags()) != 1 {
				continue
			}
			for _, fam := range m.GetTagFamilies() {
				for _, t := range fam.GetTags() {
					if t.GetName() != rule.GetTags()[0] {
						continue
					}
					if out[m.GetMetadata().GetName()] == nil {
						out[m.GetMetadata().GetName()] = map[uint32]dump.IndexedTagSpec{}
					}
					out[m.GetMetadata().GetName()][rule.GetMetadata().GetId()] = dump.IndexedTagSpec{
						Family: fam.GetName(), Name: t.GetName(), Type: pbv1.TagValueSpecToValueType(t.GetType()),
					}
				}
			}
		}
	}
	return out
}

// multiTagRuleWarnings names, per measure and bound multi-tag rule, the tags whose values
// may not be exported. The write path keeps every tag a bound rule covers out of the columns
// and stores its value under the id of the first bound rule naming it, in an order the
// bindings do not fix; rules naming a tag the schema no longer declares are ignored. The
// values of a multi-tag rule's tags share one id and cannot be told apart, so a tag such a
// rule covers may be lost even when a single-tag rule covers it too. Entity tags are always
// recovered from the series id.
func multiTagRuleWarnings(measures []*databasev1.Measure, rules []*databasev1.IndexRule,
	bindings []*databasev1.IndexRuleBinding,
) map[string][]string {
	ruleByName := make(map[string]*databasev1.IndexRule, len(rules))
	for _, r := range rules {
		ruleByName[r.GetMetadata().GetName()] = r
	}
	boundRules := map[string][]*databasev1.IndexRule{}
	for _, b := range bindings {
		if b.GetSubject().GetCatalog() != commonv1.Catalog_CATALOG_MEASURE {
			continue
		}
		for _, ruleName := range b.GetRules() {
			if rule, found := ruleByName[ruleName]; found {
				boundRules[b.GetSubject().GetName()] = append(boundRules[b.GetSubject().GetName()], rule)
			}
		}
	}
	out := map[string][]string{}
	for _, m := range measures {
		name := m.GetMetadata().GetName()
		declared := map[string]bool{}
		for _, fam := range m.GetTagFamilies() {
			for _, t := range fam.GetTags() {
				declared[t.GetName()] = true
			}
		}
		entity := map[string]bool{}
		for _, t := range m.GetEntity().GetTagNames() {
			entity[t] = true
		}
		for _, rule := range boundRules[name] {
			if len(rule.GetTags()) < 2 || slices.ContainsFunc(rule.GetTags(), func(t string) bool { return !declared[t] }) {
				continue
			}
			var lost []string
			for _, t := range rule.GetTags() {
				if !entity[t] {
					lost = append(lost, t)
				}
			}
			if len(lost) > 0 {
				out[name] = append(out[name], fmt.Sprintf(
					"measure %s: tags %v are indexed by multi-tag rule %s, which may hold their values; such values cannot be exported and their cells are null",
					name, lost, rule.GetMetadata().GetName()))
			}
		}
	}
	return out
}

func produceMeasureCSV(run *unitRun, unit *transferv1.SegmentUnit, segDir string, indexMode IndexModeReader) error {
	g, err := loadMeasureGroup(run, unit.GetGroup())
	if err != nil {
		return err
	}
	if len(unit.GetShardIds()) > 0 {
		resolver, openErr := dump.NewIndexResolver(segDir, dump.DefaultIndexCacheSize, nil)
		if openErr != nil {
			return readError("open the series index of "+segDir, openErr)
		}
		defer resolver.Close()
		for i, shard := range unit.GetShardIds() {
			fw := run.newWriter(g.layout, shardPrefix(unit, shard), shard, uint32(i), false, g.revisions)
			shardDir := filepath.Join(segDir, fmt.Sprintf("%s%d", shardDirPrefix, shard))
			if err = writeMeasureShard(run, fw, shardDir, resolver, g); err != nil {
				return err
			}
			if err = fw.close(); err != nil {
				return readError("finish "+shardDir, err)
			}
		}
	}
	if len(g.indexMode) > 0 {
		if err = writeIndexModeSegment(run, unit, segDir, g, indexMode); err != nil {
			return err
		}
	}
	// Only the measures that wrote rows in this unit are concerned.
	for _, name := range slices.Sorted(maps.Keys(g.lost)) {
		if _, wrote := run.wrote[name]; wrote {
			run.warnings = append(run.warnings, g.lost[name]...)
		}
	}
	return nil
}

// writeMeasureShard writes the data points of every part of a shard, merged in series and
// timestamp order so that only the latest version of each is written. Every part of the shard
// is open at once, with its block metadata and its rows of the current series, so memory grows
// with the number of parts and the length of a series.
func writeMeasureShard(run *unitRun, fw *fileWriter, shardDir string, resolver *dump.IndexResolver, g *measureGroup) error {
	partIDs, err := LivePartIDs(shardDir)
	if err != nil {
		return readError("list parts of "+shardDir, err)
	}
	cursors := &measureCursors{}
	defer cursors.close()
	for _, id := range partIDs {
		rofs := newReadOnlyFS()
		part, openErr := dumpmeasure.OpenPart(id, shardDir, rofs)
		if openErr == nil && rofs.err != nil {
			_ = part.Close()
			openErr = rofs.err
		}
		if openErr != nil {
			return readError(fmt.Sprintf("open part %016x of %s", id, shardDir), openErr)
		}
		// The resolver recovers both the entity values of parts without smeta.bin and the
		// indexed tags, which the write path never stores as columns.
		part.SetIndexResolver(resolver)
		c := &measureCursor{id: id, part: part, it: part.Iterator()}
		ok, nextErr := c.next(shardDir)
		if nextErr != nil {
			return nextErr
		}
		if ok {
			cursors.heads = append(cursors.heads, c)
		}
	}
	heap.Init(cursors)
	cells := g.layout.NewRow()
	fixed := fixedColumns(g.layout)
	var series seriesDecoder
	for cursors.Len() > 0 {
		// Parts are sorted by series and timestamp, so every version of a data point comes off
		// the heap in a row; like a query, the export keeps the highest version only.
		first := cursors.heads[0].row()
		sid, ts := first.SeriesID, first.Timestamp
		var subject string
		var latest int64
		have := false
		for cursors.Len() > 0 {
			c := cursors.heads[0]
			row := c.row()
			if row.SeriesID != sid || row.Timestamp != ts {
				break
			}
			if !have || row.Version > latest {
				subject, have, latest = fillMeasureRow(run, &series, g, cells, fixed, *row), true, row.Version
			}
			ok, nextErr := c.next(shardDir)
			if !ok {
				heap.Pop(cursors) // next released the part
				if nextErr != nil {
					return nextErr
				}
				continue
			}
			heap.Fix(cursors, 0)
		}
		if subject == "" {
			continue
		}
		if err = fw.writeRow(subject, cells); err != nil {
			return readError("write "+subject+" row", err)
		}
	}
	return nil
}

// fillMeasureRow lays one data point of a measure part out in cells and returns its measure,
// or "" for a row that is not exported.
func fillMeasureRow(run *unitRun, series *seriesDecoder, g *measureGroup, cells []string,
	fixed map[csvfmt.ColumnKind]int, row dumpmeasure.Row,
) string {
	subject, entityValues, ok := subjectOf(run, series, "measure", row.EntityValues)
	if !ok {
		return ""
	}
	res, known := g.resources[subject]
	if !known {
		if subject != csvfmt.TopNResultMeasure {
			run.dropRow("measure " + subject + " is not in the schema registry")
		}
		return ""
	}
	resetRow(cells)
	cells[fixed[csvfmt.KindTimestamp]] = csvfmt.EncodeTimestamp(row.Timestamp)
	cells[fixed[csvfmt.KindVersion]] = csvfmt.EncodeInt(row.Version)
	setStoredTags(cells, g.layout, row.Tags, row.TagTypes, res.tags, run, subject)
	setFields(cells, g.layout, row.Fields, row.FieldTypes, res.fields, run, subject)
	fillEntity(cells, res.entity, entityValues)
	fillIndexed(cells, res, row.IndexedTags, run, subject)
	return subject
}

// measureCursor is one part of a shard positioned on its current row. A part orders its blocks
// by series and first timestamp only: a merged block too large to join the next one is written
// as it is, so the blocks of a series may overlap in time. The cursor therefore holds all rows
// of the part's current series, sorted, and serves them in order.
type measureCursor struct {
	part   *dumpmeasure.PartReader
	it     *dump.Iterator[dumpmeasure.Row]
	series []dumpmeasure.Row
	ahead  dumpmeasure.Row
	pos    int
	id     uint64
	more   bool
}

func (c *measureCursor) row() *dumpmeasure.Row { return &c.series[c.pos] }

// measureCursors orders the parts of a shard that still hold rows by their current row:
// series, then timestamp, then descending version.
type measureCursors struct {
	heads []*measureCursor
}

func (h *measureCursors) Len() int { return len(h.heads) }

func (h *measureCursors) Less(i, j int) bool {
	a, b := h.heads[i].row(), h.heads[j].row()
	if a.SeriesID != b.SeriesID {
		return a.SeriesID < b.SeriesID
	}
	if a.Timestamp != b.Timestamp {
		return a.Timestamp < b.Timestamp
	}
	return a.Version > b.Version
}

func (h *measureCursors) Swap(i, j int) { h.heads[i], h.heads[j] = h.heads[j], h.heads[i] }

func (h *measureCursors) Push(x any) { h.heads = append(h.heads, x.(*measureCursor)) }

func (h *measureCursors) Pop() any {
	c := h.heads[len(h.heads)-1]
	h.heads = h.heads[:len(h.heads)-1]
	return c
}

// next moves c to its next row and reports whether there is one; a part without one is
// released.
func (c *measureCursor) next(shardDir string) (bool, error) {
	if c.pos+1 < len(c.series) {
		c.pos++
		return true, nil
	}
	c.series, c.pos = c.series[:0], 0
	if c.more {
		c.series, c.more = append(c.series, c.ahead), false
	} else if c.it.Next() {
		c.series = append(c.series, ownedRow(c.it.Row()))
	}
	for len(c.series) > 0 && c.it.Next() {
		row := c.it.Row()
		if row.SeriesID != c.series[0].SeriesID {
			c.ahead, c.more = ownedRow(row), true
			break
		}
		c.series = append(c.series, ownedRow(row))
	}
	if err := c.it.Err(); err != nil || len(c.series) == 0 {
		_ = c.it.Close()
		_ = c.part.Close()
		if err != nil {
			return false, readError(fmt.Sprintf("read part %016x of %s", c.id, shardDir), err)
		}
		return false, nil
	}
	slices.SortStableFunc(c.series, func(a, b dumpmeasure.Row) int {
		if a.Timestamp != b.Timestamp {
			return cmp.Compare(a.Timestamp, b.Timestamp)
		}
		return cmp.Compare(b.Version, a.Version)
	})
	return true, nil
}

// ownedRow copies the stored values of a row, which alias the part's decode buffer that the
// next block reuses.
func ownedRow(row dumpmeasure.Row) dumpmeasure.Row {
	row.Tags = cloneValues(row.Tags)
	row.Fields = cloneValues(row.Fields)
	return row
}

func cloneValues(values map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(values))
	for k, v := range values {
		out[k] = bytes.Clone(v)
	}
	return out
}

// close releases the parts that still hold rows; the others were released by next.
func (h *measureCursors) close() {
	for _, c := range h.heads {
		_ = c.it.Close()
		_ = c.part.Close()
	}
}

// setFields fills the field columns of a row. A field the schema no longer declares, or one
// stored with another type than its column's, is counted as dropped for resource.
func setFields(cells []string, layout *csvfmt.Layout, fields map[string][]byte, types map[string]pbv1.ValueType,
	columns map[string]int, run *unitRun, resource string,
) {
	for name, raw := range fields {
		if raw == nil {
			continue
		}
		idx, declared := columns[name]
		want, known := columnTypes[types[name]]
		if !declared || !known || want != layout.Columns[idx].Type {
			run.dropTag(resource, "_field."+name)
			continue
		}
		cells[idx] = csvfmt.EncodeFieldValue(dumpmeasure.DecodeFieldValue(types[name], raw))
	}
}

// fillIndexed decodes the indexed tags of the row's series with the rules bound to its
// measure. As in a query, a value the series index stores wins over the part's column; a
// value that does not decode as the tag's current type is dropped and counted.
func fillIndexed(cells []string, res *measureResource, indexed map[uint32][][]byte, run *unitRun, subject string) {
	for ruleID, values := range indexed {
		spec, ok := res.indexed[ruleID]
		if !ok || len(values) == 0 {
			continue
		}
		idx, declared := res.tags[spec.Family+"."+spec.Name]
		if !declared {
			continue
		}
		// Scalar types read values[0], array types read every element.
		if !dump.FitsValueType(spec.Type, values[0], values) {
			run.dropTag(subject, spec.Family+"."+spec.Name)
			continue
		}
		cells[idx] = csvfmt.EncodeTagValue(dump.DecodeTagValue(spec.Type, values[0], values))
	}
}

// writeIndexModeSegment writes the rows of the group's index-mode measures, which live in
// the segment-level series index, as the segment-level file of the unit.
func writeIndexModeSegment(run *unitRun, unit *transferv1.SegmentUnit, segDir string, g *measureGroup, reader IndexModeReader) error {
	if reader == nil {
		return status.Errorf(codes.Internal, "group %s holds index-mode measures but the measure backend cannot read them", unit.GetGroup())
	}
	prefix := fmt.Sprintf("%s/%s/%s%s/%s", transfer.CatalogName(unit.GetCatalog()), csvfmt.EscapePathComponent(unit.GetGroup()),
		segDirPrefix, unit.GetSegmentSuffix(), segmentFileName)
	shards := uint32(len(unit.GetShardIds()))
	fw := run.newWriter(g.layout, prefix, 0, shards, true, g.revisions)
	cells := g.layout.NewRow()
	fixed := fixedColumns(g.layout)
	sidxDir := filepath.Join(segDir, sidxDirName)
	err := reader.ExportIndexModeRows(run.ctx, sidxDir, g.indexMode, g.rules,
		func(subject string, timestamp, version int64, tags map[string]*modelv1.TagValue, unknownFields int) error {
			res, known := g.resources[subject]
			if !known {
				run.dropRow("measure " + subject + " is not in the schema registry")
				return nil
			}
			if unknownFields > 0 {
				run.dropTagN(subject, "(stored fields of removed tags or index rules, or of another type)", uint64(unknownFields))
			}
			resetRow(cells)
			cells[fixed[csvfmt.KindTimestamp]] = csvfmt.EncodeTimestamp(timestamp)
			cells[fixed[csvfmt.KindVersion]] = csvfmt.EncodeInt(version)
			for key, value := range tags {
				if idx, declared := res.tags[key]; declared {
					cells[idx] = csvfmt.EncodeTagValue(value)
				}
			}
			return fw.writeRow(subject, cells)
		})
	if err != nil {
		return readError("read index-mode rows of "+sidxDir, err)
	}
	if err = fw.close(); err != nil {
		return readError("finish "+sidxDir, err)
	}
	return nil
}
