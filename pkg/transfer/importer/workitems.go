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
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
)

// WorkItemKind tells which files of a unit a work item pushes.
type WorkItemKind uint8

// The work item kinds.
const (
	// ShardItem pushes one shard's files of a segment unit.
	ShardItem WorkItemKind = iota
	// SegmentItem pushes the segment-level files of a segment unit (index-mode measures).
	SegmentItem
	// PropertyItem pushes one source shard id's files of a property group.
	PropertyItem
)

// String names the kind for json, yaml and messages.
func (k WorkItemKind) String() string {
	switch k {
	case ShardItem:
		return "shard"
	case SegmentItem:
		return "segment"
	case PropertyItem:
		return "property"
	default:
		return "unknown(" + strconv.Itoa(int(k)) + ")"
	}
}

// MarshalText renders the kind by name.
func (k WorkItemKind) MarshalText() ([]byte, error) { return []byte(k.String()), nil }

// WorkSource is one source node's files of a work item.
type WorkSource struct {
	// RowCounts are the rows per resource of Files.
	RowCounts map[string]uint64 `json:"rowCounts,omitempty"`
	Node      string            `json:"node"`
	// Timezone is the IANA time zone of the node at export time.
	Timezone string `json:"timezone,omitempty"`
	// Files are in artifact path order, which is file sequence order.
	Files        []manifest.File `json:"files"`
	Parts        []manifest.Part `json:"-"`
	Rows         uint64          `json:"rows"`
	MinTimestamp int64           `json:"minTimestamp"`
	MaxTimestamp int64           `json:"maxTimestamp"`
}

// WorkItem is the unit of parallel push: a shard of a segment unit, the segment-level files of
// a segment unit, or a source shard id of a property group.
type WorkItem struct {
	// timeErr is why a segment item's time range could not be resolved; the TTL gate reports it.
	timeErr error
	Group   string `json:"group"`
	// Stage is the source stage from the manifest; "" for property.
	Stage   string `json:"stage"`
	Segment string `json:"segment,omitempty"`
	// Sources are in node name order.
	Sources []WorkSource     `json:"sources"`
	Catalog commonv1.Catalog `json:"-"`
	Shard   uint32           `json:"shard"`
	Kind    WorkItemKind     `json:"kind"`
}

// CatalogName is the lower-case catalog name: stream, measure, trace or property.
func (w *WorkItem) CatalogName() string { return transfer.CatalogName(w.Catalog) }

// Key identifies the item: catalog/group/stage/segment/shard-N, .../segment or property/group/shard-N.
func (w *WorkItem) Key() string {
	switch w.Kind {
	case PropertyItem:
		return fmt.Sprintf("%s/%s/shard-%d", w.CatalogName(), w.Group, w.Shard)
	case SegmentItem:
		return fmt.Sprintf("%s/%s/%s/%s/segment", w.CatalogName(), w.Group, w.Stage, w.Segment)
	default:
		return fmt.Sprintf("%s/%s/%s/%s/shard-%d", w.CatalogName(), w.Group, w.Stage, w.Segment, w.Shard)
	}
}

// position renders where in its unit the item is, for detail lines.
func (w *WorkItem) position() string {
	switch w.Kind {
	case SegmentItem:
		return "seg=" + w.Segment + " segment-level"
	case PropertyItem:
		return fmt.Sprintf("shard=%d", w.Shard)
	default:
		return fmt.Sprintf("seg=%s shard=%d", w.Segment, w.Shard)
	}
}

// Rows is the sum of the sources' rows.
func (w *WorkItem) Rows() uint64 {
	var n uint64
	for _, s := range w.Sources {
		n += s.Rows
	}
	return n
}

// TimeRange is the smallest range holding every source's rows; zero when none has a time.
func (w *WorkItem) TimeRange() (lo, hi int64) {
	for _, s := range w.Sources {
		transfer.MergeRange(&lo, &hi, s.MinTimestamp, s.MaxTimestamp)
	}
	return lo, hi
}

// WithSources returns a copy of the item holding only the named sources.
func (w *WorkItem) WithSources(nodes []string) WorkItem {
	out := *w
	out.Sources = nil
	for _, s := range w.Sources {
		if slices.Contains(nodes, s.Node) {
			out.Sources = append(out.Sources, s)
		}
	}
	return out
}

// SourceInputs returns the sources as SelectSources inputs.
func (w *WorkItem) SourceInputs() []SourceInput {
	in := make([]SourceInput, 0, len(w.Sources))
	for _, s := range w.Sources {
		in = append(in, SourceInput{Node: s.Node, Parts: s.Parts, TotalCount: s.Rows})
	}
	return in
}

// BuildWorkItems splits the manifest's units into work items, sorted by key: a segment unit
// gives one item per shard holding every source's copy of that shard, plus one segment item
// when any source has segment-level files; a property unit gives one item per source shard id,
// holding every source's files of that id. The type of an item follows where its files are, not
// groups[].indexMode, so a group mixing index-mode and regular measures yields both kinds. A
// segment item's sources span the bounds of their segment, the time range of index-mode rows.
// Every source of every item is kept: Decide selects among them.
func BuildWorkItems(m *manifest.Manifest) ([]WorkItem, error) {
	tz := make(map[string]string, len(m.Source.Nodes))
	for _, n := range m.Source.Nodes {
		tz[n.ID] = n.Timezone
	}
	var items []WorkItem
	for _, u := range m.Units {
		catalog, ok := transfer.CatalogByName(strings.ToLower(u.Catalog))
		if !ok {
			return nil, fmt.Errorf("unit %s/%s has unknown catalog %q", u.Group, u.Segment, u.Catalog)
		}
		base := WorkItem{Catalog: catalog, Group: u.Group, Stage: u.Stage, Segment: u.Segment}
		kind := ShardItem
		if catalog == commonv1.Catalog_CATALOG_PROPERTY {
			base.Stage, base.Segment, kind = "", "", PropertyItem
		}
		shards := map[uint32]*WorkItem{}
		var segment *WorkItem
		for _, src := range u.Sources {
			if len(src.SegmentFiles) > 0 {
				if segment == nil {
					seg := base
					seg.Kind = SegmentItem
					segment = &seg
				}
				ws := WorkSource{
					Node: src.Node, Timezone: tz[src.Node], Files: sortedFiles(src.SegmentFiles),
					RowCounts: src.RowCounts, Rows: src.TotalCount,
				}
				var err error
				if ws.MinTimestamp, ws.MaxTimestamp, err = segmentTimeRange(m, &u, ws.Timezone); err != nil && segment.timeErr == nil {
					segment.timeErr = err
				}
				segment.Sources = append(segment.Sources, ws)
			}
			for _, sh := range src.Shards {
				item, ok := shards[sh.Shard]
				if !ok {
					it := base
					it.Kind, it.Shard = kind, sh.Shard
					item = &it
					shards[sh.Shard] = item
				}
				item.Sources = append(item.Sources, WorkSource{
					Node: src.Node, Timezone: tz[src.Node], Files: sortedFiles(sh.Files), Parts: sh.Parts,
					RowCounts: sh.RowCounts, Rows: sh.TotalCount, MinTimestamp: sh.MinTimestamp, MaxTimestamp: sh.MaxTimestamp,
				})
			}
		}
		for _, item := range shards {
			items = append(items, *item)
		}
		if segment != nil {
			items = append(items, *segment)
		}
	}
	for i := range items {
		slices.SortStableFunc(items[i].Sources, func(a, b WorkSource) int { return cmp.Compare(a.Node, b.Node) })
	}
	slices.SortFunc(items, compareItems)
	return items, nil
}

// segmentTimeRange is the time range of a unit's segment-level files: the bounds of the source
// segment in the source node's time zone. Both are zero, with the reason, when the stage or the
// suffix cannot be resolved.
func segmentTimeRange(m *manifest.Manifest, u *manifest.Unit, timezone string) (lo, hi int64, err error) {
	st, ok := sourceStage(m, u.Group, u.Stage)
	if !ok {
		return 0, 0, fmt.Errorf("the manifest has no stage %q for group %s", u.Stage, u.Group)
	}
	start, end, err := segmentBounds(u.Segment, timezone, st.SegmentInterval)
	if err != nil {
		return 0, 0, err
	}
	return start.UnixNano(), end.UnixNano() - 1, nil
}

// compareItems orders items by catalog, group, stage, segment, kind and shard.
func compareItems(a, b WorkItem) int {
	return cmp.Or(
		cmp.Compare(a.CatalogName(), b.CatalogName()), cmp.Compare(a.Group, b.Group), cmp.Compare(a.Stage, b.Stage),
		cmp.Compare(a.Segment, b.Segment), cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Shard, b.Shard),
	)
}

// sortedFiles orders a source's files by path: the zero-padded sequence number makes that the
// sequence order.
func sortedFiles(files []manifest.File) []manifest.File {
	out := slices.Clone(files)
	slices.SortFunc(out, func(a, b manifest.File) int { return cmp.Compare(a.Path, b.Path) })
	return out
}
