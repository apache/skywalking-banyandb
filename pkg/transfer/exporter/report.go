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

package exporter

import (
	"fmt"
	"slices"
	"sort"

	"github.com/apache/skywalking-banyandb/pkg/transfer"
)

// Row is one (node, catalog, group) line of the dry-run table.
type Row struct {
	Node              string `json:"node"`
	Catalog           string `json:"catalog"`
	Group             string `json:"group"`
	Stage             string `json:"stage"`
	Segments          int    `json:"segments"`
	Shards            int    `json:"shards"`
	Parts             int    `json:"parts"`
	EstRows           uint64 `json:"estRows"`
	CompressedBytes   uint64 `json:"compressedBytes"`
	UncompressedBytes uint64 `json:"uncompressedBytes"`
	MinTimestamp      int64  `json:"minTimestamp"`
	MaxTimestamp      int64  `json:"maxTimestamp"`
}

// Source is one node's contribution to a unit that several nodes hold.
type Source struct {
	Node         string `json:"node"`
	Rows         uint64 `json:"rows"`
	MinTimestamp int64  `json:"minTimestamp"`
	MaxTimestamp int64  `json:"maxTimestamp"`
}

// MultiSource is a (catalog, group, stage, segment, shard) key held by more than one node.
// Property groups are left out: their rows are documents, not time-bounded units an importer
// chooses between. Index-mode segment-level units are left out too: they have no shard id,
// so the plan cannot tell replicas of one segment from different shards' slices of it.
type MultiSource struct {
	Key     string   `json:"key"`
	Sources []Source `json:"sources"`
}

// Report is the dry-run summary rendered by Render.
type Report struct {
	Rows        []Row         `json:"rows"`
	MultiSource []MultiSource `json:"multiSource"`
	// DataNodes are the nodes the liaison fanned out to: the answered and the unreachable
	// ones, which may differ from the preflight's registry snapshot.
	DataNodes     []string `json:"dataNodes"`
	AnsweredNodes []string `json:"answeredNodes"`
	// MissingNodes are the nodes the liaison reported unreachable or not holding the
	// session; their data would not be exported (design §2.3 NODES row).
	MissingNodes     []string `json:"missingNodes"`
	UnreachableNodes []string `json:"unreachableNodes"`
	Standalone       bool     `json:"standalone"`
}

// LargestNodeCompressedBytes is the largest per-node compressed subtotal and its node:
// each data node pins its own session snapshot, so that node needs the most space.
func (r *Report) LargestNodeCompressedBytes() (node string, size uint64) {
	perNode := map[string]uint64{}
	for _, row := range r.Rows {
		perNode[row.Node] += row.CompressedBytes
	}
	for n, total := range perNode {
		if total > size || (total == size && (node == "" || n < node)) {
			node, size = n, total
		}
	}
	return node, size
}

// BuildReport folds Plan frames into the table, the multi-source list and the NODES gap.
// Every list is non-nil so the json and yaml renderings print [] rather than null.
func BuildReport(cluster *ClusterInfo, result *PlanResult) *Report {
	rep := &Report{
		Rows:             []Row{},
		MultiSource:      []MultiSource{},
		DataNodes:        nonNil(uniqueSorted(slices.Concat(result.AnsweredNodes, result.UnreachableNodes))),
		MissingNodes:     nonNil(result.UnreachableNodes),
		UnreachableNodes: nonNil(result.UnreachableNodes),
		AnsweredNodes:    nonNil(result.AnsweredNodes),
		Standalone:       cluster.Standalone,
	}
	rows := map[string]*Row{}
	shardIDs := map[string]map[uint32]struct{}{} // row key -> distinct shard ids
	units := map[string][]Source{}
	for _, f := range result.Frames {
		stage := NormalizeStage(f.GetStage())
		for _, u := range f.GetUnits() {
			catalog, group := transfer.CatalogName(unitCatalog(u)), unitGroup(u)
			rowKey := f.GetNodeId() + "\x00" + catalog + "\x00" + group
			row, ok := rows[rowKey]
			if !ok {
				row = &Row{Node: f.GetNodeId(), Catalog: catalog, Group: group, Stage: stage}
				rows[rowKey] = row
				shardIDs[rowKey] = map[uint32]struct{}{}
			}
			if p := u.GetProperty(); p != nil {
				// A property group has no segments, parts or timestamps: its shards are
				// document stores whose on-disk size is both estimates.
				for _, s := range p.GetShards() {
					shardIDs[rowKey][s.GetShardId()] = struct{}{}
					row.EstRows += s.GetDocCount()
					row.CompressedBytes += s.GetEstimatedBytes()
					row.UncompressedBytes += s.GetEstimatedBytes()
				}
				continue
			}
			seg := u.GetSegment()
			row.Segments++
			row.CompressedBytes += seg.GetSegmentLevel().GetEstimatedBytes()
			row.EstRows += seg.GetSegmentLevel().GetDocCount()
			if seg.GetSegmentLevel().GetDocCount() > 0 {
				// The series index is only re-read on import for index-mode groups; elsewhere
				// it is rebuilt from the parts and costs no raw bytes.
				row.UncompressedBytes += seg.GetSegmentLevel().GetEstimatedBytes()
			}
			for _, s := range seg.GetShards() {
				shardIDs[rowKey][s.GetShardId()] = struct{}{}
				row.Parts += int(s.GetPartsCount())
				row.EstRows += s.GetTotalCount()
				row.CompressedBytes += s.GetEstimatedCompressedBytes()
				row.UncompressedBytes += s.GetEstimatedUncompressedBytes()
				mergeRange(&row.MinTimestamp, &row.MaxTimestamp, s.GetMinTimestamp(), s.GetMaxTimestamp())
				unitKey := multiSourceKey(catalog, group, stage, seg.GetUnit().GetSegmentSuffix(), s.GetShardId())
				units[unitKey] = append(units[unitKey], Source{
					Node: f.GetNodeId(), Rows: s.GetTotalCount(), MinTimestamp: s.GetMinTimestamp(), MaxTimestamp: s.GetMaxTimestamp(),
				})
			}
		}
	}
	for key, r := range rows {
		r.Shards = len(shardIDs[key])
		rep.Rows = append(rep.Rows, *r)
	}
	sort.Slice(rep.Rows, func(i, j int) bool {
		a, b := rep.Rows[i], rep.Rows[j]
		if a.Node != b.Node {
			return a.Node < b.Node
		}
		if a.Catalog != b.Catalog {
			return a.Catalog < b.Catalog
		}
		return a.Group < b.Group
	})
	for key, sources := range units {
		if len(sources) > 1 {
			sort.Slice(sources, func(i, j int) bool { return sources[i].Node < sources[j].Node })
			rep.MultiSource = append(rep.MultiSource, MultiSource{Key: key, Sources: sources})
		}
	}
	sort.Slice(rep.MultiSource, func(i, j int) bool { return rep.MultiSource[i].Key < rep.MultiSource[j].Key })
	return rep
}

// nonNil returns s, or an empty slice for nil so json and yaml print [] rather than null.
func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// multiSourceKey names one shard of one segment unit.
func multiSourceKey(catalog, group, stage, segment string, shard uint32) string {
	return fmt.Sprintf("%s/%s/%s/%s/shard-%d", catalog, group, stage, segment, shard)
}

// mergeRange widens [minDst, maxDst] by a source range. A source without a minimum (0) has no
// flushed timestamps and is skipped, so a 0 minDst always means no source was merged yet and
// the result does not depend on the merge order.
func mergeRange(minDst, maxDst *int64, minSrc, maxSrc int64) {
	if minSrc == 0 {
		return
	}
	if *minDst == 0 || minSrc < *minDst {
		*minDst = minSrc
	}
	if maxSrc > *maxDst {
		*maxDst = maxSrc
	}
}
