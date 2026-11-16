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
	"fmt"
	"slices"
	"strings"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
)

// The gates only a native artifact passes through: its part files are placed as they are, so
// the target must lay data out exactly as the source did and be able to read the files.
const (
	// GateTopology refuses a native work item whose target shard number or segment interval
	// differs from the source's.
	GateTopology = "TOPOLOGY"
	// GateTimezone refuses a native work item whose target data nodes run in another time zone
	// than the source node, which would cut segments at other boundaries.
	GateTimezone = "TZ"
	// GateVersion refuses a native work item whose segment file format the target data nodes
	// cannot read.
	GateVersion = "VERSION"
	// GateIndexRule refuses a native work item of a group whose index rule has another id on
	// the target: the index files key the stored tag values by the rule id, so they would be
	// unreadable there.
	GateIndexRule = "INDEX_RULE"
)

// nativeGate checks a native work item against the target: the topology of its target stage,
// the time zone and the readable file formats of its target data nodes. It returns the gate
// and the reason of a refusal, or "" when the item passes. nodes are the item's target data
// nodes; empty for a group the import creates, whose nodes are known only after the schema
// write, so every data node is checked then.
func nativeGate(in Inputs, p *Plan, item *WorkItem, targetStage string, nodes []string) (gate, reason string) {
	if reason = topologyMismatch(in, item, targetStage); reason != "" {
		return GateTopology, reason
	}
	if reason = p.native().ruleMismatch(in, item.Group); reason != "" {
		return GateIndexRule, reason
	}
	targets := targetNodes(in.Target.DataNodes, nodes)
	if nativefmt.Segmented(item.Catalog) {
		for _, src := range item.Sources {
			for _, n := range targets {
				if src.Timezone != "" && n.GetTzName() != "" && src.Timezone != n.GetTzName() {
					return GateTimezone, fmt.Sprintf("source node %s runs in %s, target node %s in %s; segments would be cut at other boundaries",
						src.Node, src.Timezone, n.GetMetadata().GetName(), n.GetTzName())
				}
			}
		}
		v := item.SegmentVersion
		if v == "" {
			return GateVersion, "the artifact records no segment file format version for this unit"
		}
		for _, n := range targets {
			if !readsFormat(n, v) {
				return GateVersion, fmt.Sprintf("target node %s cannot read segment file format %s (it reads %s)",
					n.GetMetadata().GetName(), v, strings.Join(readableFormats(n), ", "))
			}
		}
	}
	return "", ""
}

// topologyMismatch explains how the target stage's layout differs from the source's: both the
// stage's and the group's shard numbers and the stage's segment interval must be equal, or the
// parts would land in shards and segments the target never reads. A property group has no
// stage; only its shard number counts.
func topologyMismatch(in Inputs, item *WorkItem, targetStage string) string {
	m := in.Artifact.Manifest
	ro := in.Target.Groups[item.Group].GetResourceOpts()
	if ro == nil {
		return ""
	}
	srcGroup, ok := sourceGroupDefault(m, item.Group)
	if !ok {
		return fmt.Sprintf("the manifest has no default stage of group %s", item.Group)
	}
	var diffs []string
	if srcGroup.ShardNum != ro.GetShardNum() {
		diffs = append(diffs, fmt.Sprintf("group shard number %d→%d", srcGroup.ShardNum, ro.GetShardNum()))
	}
	if nativefmt.Segmented(item.Catalog) {
		src, found := sourceStage(m, item.Group, item.Stage)
		if normalizeStage(item.Stage) == transfer.DefaultStageName {
			// The target and the route lay out hot with the group's own opts, but the manifest
			// does not tell a default-tier unit from one of a lifecycle stage named hot.
			if found && (src.ShardNum != srcGroup.ShardNum || !strings.EqualFold(src.SegmentInterval, srcGroup.SegmentInterval)) {
				return fmt.Sprintf("group %s has a lifecycle stage named hot whose layout differs from the group default; "+
					"native import cannot tell which layout a hot unit has", item.Group)
			}
			src, found = srcGroup, true
		}
		if !found {
			return fmt.Sprintf("the manifest has no stage %q of group %s", item.Stage, item.Group)
		}
		shardNum, interval := ro.GetShardNum(), output.IntervalLabel(ro.GetSegmentInterval())
		if i := stageIndex(ro, targetStage); i >= 0 && !strings.EqualFold(targetStage, transfer.DefaultStageName) {
			st := ro.GetStages()[i]
			shardNum, interval = st.GetShardNum(), output.IntervalLabel(st.GetSegmentInterval())
		}
		if r, routed := in.Target.Routes[NewRouteKey(item.Group, item.Stage)]; routed && r.GetShardNum() != 0 {
			shardNum = r.GetShardNum()
		}
		if src.ShardNum != shardNum {
			diffs = append(diffs, fmt.Sprintf("stage %s shard number %d→%d", targetStage, src.ShardNum, shardNum))
		}
		if !strings.EqualFold(src.SegmentInterval, interval) {
			diffs = append(diffs, fmt.Sprintf("stage %s segment interval %s→%s", targetStage, src.SegmentInterval, interval))
		}
	}
	if len(diffs) == 0 {
		return ""
	}
	return "native parts need the source topology: " + strings.Join(diffs, ", ")
}

// indexRuleMismatch names the index rules of group whose id on the target differs from the
// artifact's. A rule the target does not have yet is created with the artifact's id.
func indexRuleMismatch(in Inputs, group string) string {
	var diffs []string
	for k, rule := range in.Artifact.Schemas.IndexRules {
		if k.Group != group {
			continue
		}
		target, ok := in.TargetIndexRuleIDs[k]
		if src := rule.GetMetadata().GetId(); ok && src != 0 && target != src {
			diffs = append(diffs, fmt.Sprintf("%s (id %d in the artifact, %d on the target)", k.Name, src, target))
		}
	}
	if len(diffs) == 0 {
		return ""
	}
	slices.Sort(diffs)
	return "native index files key stored tag values by index rule id: " + strings.Join(diffs, ", ")
}

// targetNodes returns the data nodes named in nodes, or every data node when nodes is empty.
func targetNodes(all []*databasev1.Node, nodes []string) []*databasev1.Node {
	if len(nodes) == 0 {
		return all
	}
	var out []*databasev1.Node
	for _, n := range all {
		if slices.Contains(nodes, n.GetMetadata().GetName()) {
			out = append(out, n)
		}
	}
	return out
}

// readsFormat tells whether node reads segment file format version; a node that does not
// report its versions is not refused.
func readsFormat(node *databasev1.Node, version string) bool {
	f, compatible := node.GetVersion().GetFileFormatVersion(), node.GetVersion().GetCompatibleFileFormatVersion()
	return (f == "" && len(compatible) == 0) || f == version || slices.Contains(compatible, version)
}

// readableFormats lists the segment file format versions node reads, for an error message.
func readableFormats(node *databasev1.Node) []string {
	v := node.GetVersion()
	formats := slices.Clone(v.GetCompatibleFileFormatVersion())
	if f := v.GetFileFormatVersion(); f != "" && !slices.Contains(formats, f) {
		formats = append(formats, f)
	}
	return formats
}

// restrictSegmentSources narrows the sources of a native segment item, the series index of a
// segment, to those whose core parts some shard item of the same unit pushes: the index of a
// source that is skipped as a replica is skipped with it. A group with an index-mode measure
// keeps every source: its series index also holds the index-mode rows, which no shard item
// carries, so a source without regular parts can still be their only copy.
func restrictSegmentSources(in Inputs, p *Plan, item *WorkItem) WorkItem {
	if p.native().indexMode(in, item.Group) {
		return *item
	}
	chosen := p.native().shardSources[unitKey(item)]
	if len(chosen) == 0 {
		return *item
	}
	return item.WithSources(transfer.UniqueSorted(chosen))
}

// recordShardSources notes the sources a native shard item pushes, or a gate refuses, for
// restrictSegmentSources.
func (p *Plan) recordShardSources(native bool, item *WorkItem) {
	if !native || item.Kind != ShardItem {
		return
	}
	k := unitKey(item)
	for _, s := range item.Sources {
		p.native().shardSources[k] = append(p.native().shardSources[k], s.Node)
	}
}

// groupHasIndexMode tells whether group holds an index-mode measure, by the manifest, the
// artifact's schema or, for an artifact exported without its schema, the target's.
func groupHasIndexMode(in Inputs, group string) bool {
	for _, g := range in.Artifact.Manifest.Groups {
		if g.Name == group && g.IndexMode {
			return true
		}
	}
	indexMode := func(measures map[ResourceKey]*databasev1.Measure) bool {
		for k, m := range measures {
			if k.Group == group && m.GetIndexMode() {
				return true
			}
		}
		return false
	}
	if indexMode(in.Artifact.Schemas.Measures) {
		return true
	}
	return in.Target != nil && indexMode(in.Target.Schemas.Measures)
}

// nativeDecision caches what the native gates derive per group from the whole schema, so a
// Decide over many work items scans the schema once per group.
type nativeDecision struct {
	indexModes     map[string]bool
	ruleMismatches map[string]string
	// shardSources are the source nodes the shard items of each unit (by unitKey) push.
	shardSources map[string][]string
}

func (p *Plan) native() *nativeDecision {
	if p.nativeCache == nil {
		p.nativeCache = &nativeDecision{indexModes: map[string]bool{}, ruleMismatches: map[string]string{}, shardSources: map[string][]string{}}
	}
	return p.nativeCache
}

func (d *nativeDecision) indexMode(in Inputs, group string) bool {
	return memo(d.indexModes, group, func() bool { return groupHasIndexMode(in, group) })
}

func (d *nativeDecision) ruleMismatch(in Inputs, group string) string {
	return memo(d.ruleMismatches, group, func() string { return indexRuleMismatch(in, group) })
}

// memo returns m[k], computing it with f and storing it the first time k is asked.
func memo[V any](m map[string]V, k string, f func() V) V {
	v, ok := m[k]
	if !ok {
		v = f()
		m[k] = v
	}
	return v
}
