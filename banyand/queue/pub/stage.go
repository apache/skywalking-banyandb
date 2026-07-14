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

package pub

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// ResolveStageResourceOpts resolves the effective ResourceOpts for a data node whose
// lifecycle stage is selected by nodeLabels. It returns a deep clone of ro with
// SegmentInterval, Ttl (cumulative through the matched stage) and ShardNum overridden
// by the matched stage. matchedStage/matchedIdx identify the matched stage and are
// nil/-1 when no stage applies (the group has no stages, the node has no labels, or no
// stage selector matched the node's labels). The returned opts is always a fresh clone,
// so callers never mutate the shared group schema.
//
// This is the single source of truth for the per-stage interval/ttl/shard resolution:
// both the initial OpenDB and any later UpdateOptions must go through it, otherwise a
// group update silently overwrites a stage node's interval/ttl with the group default.
func ResolveStageResourceOpts(ro *commonv1.ResourceOpts, nodeLabels map[string]string) (
	resolved *commonv1.ResourceOpts, matchedStage *commonv1.LifecycleStage, matchedIdx int, err error,
) {
	if ro == nil {
		return nil, nil, -1, nil
	}
	resolved = proto.Clone(ro).(*commonv1.ResourceOpts)
	if len(ro.GetStages()) == 0 || len(nodeLabels) == 0 {
		return resolved, nil, -1, nil
	}
	// Reject malformed schemas up front so a missing interval/ttl returns an error here
	// rather than panicking a data node later in storage.MustToIntervalRule.
	if ro.GetTtl() == nil {
		return nil, nil, -1, fmt.Errorf("group ttl must be set when lifecycle stages are configured")
	}
	var ttlNum uint32
	for i, st := range ro.GetStages() {
		if st.GetTtl() == nil || st.GetSegmentInterval() == nil {
			return nil, nil, -1, fmt.Errorf("lifecycle stage %q must set both ttl and segmentInterval", st.GetName())
		}
		if st.GetTtl().GetUnit() != ro.GetTtl().GetUnit() {
			return nil, nil, -1, fmt.Errorf("ttl unit %s is not consistent with stage %s ttl unit %s",
				ro.GetTtl().GetUnit(), st.GetName(), st.GetTtl().GetUnit())
		}
		selector, parseErr := ParseLabelSelector(st.GetNodeSelector())
		if parseErr != nil {
			return nil, nil, -1, fmt.Errorf("failed to parse node selector %q: %w", st.GetNodeSelector(), parseErr)
		}
		ttlNum += st.GetTtl().GetNum()
		if !selector.Matches(nodeLabels) {
			continue
		}
		resolved.SegmentInterval = proto.Clone(st.GetSegmentInterval()).(*commonv1.IntervalRule)
		resolved.ShardNum = st.GetShardNum()
		// resolved.Ttl is the clone of the (validated non-nil) group ttl, so it keeps the
		// group's unit; only the cumulative number changes: the group base plus every
		// stage ttl up to and including the matched stage.
		resolved.Ttl.Num = ro.GetTtl().GetNum() + ttlNum
		return resolved, st, i, nil
	}
	return resolved, nil, -1, nil
}

// StageResolution is the fully-resolved storage configuration for a data node's tsdb,
// derived from the group ResourceOpts and the node's matched lifecycle stage. It carries
// everything OpenDB needs so the caller does no further stage logic.
//
// Only ResourceOpts is re-applied on a group UpdateOptions; the rotation/retention fields
// are wired once at open time, so adding/removing/retiming a stage needs a node restart.
type StageResolution struct {
	// ResourceOpts holds the effective SegmentInterval/Ttl/ShardNum -- the matched stage's
	// values (cumulative ttl) or the group default. It is always a fresh clone.
	ResourceOpts       *commonv1.ResourceOpts
	SegmentIdleTimeout time.Duration
	// Matched reports whether the node matched a lifecycle stage (a warm/cold tier); the
	// hot/initial tier and unlabeled nodes are not matched.
	Matched          bool
	DisableRetention bool
	DisableRotation  bool
}

// ResolveStage resolves the complete tsdb configuration for this node from the group opts
// and the node's lifecycle stage. It warns when a labeled node whose group has stages
// matches none of them and silently falls back to the group default -- the condition that
// previously produced short segments. This is the single entry point OpenDB uses.
func ResolveStage(l *logger.Logger, groupName string, ro *commonv1.ResourceOpts, nodeLabels map[string]string) (StageResolution, error) {
	resolved, matchedStage, matchedIdx, err := ResolveStageResourceOpts(ro, nodeLabels)
	if err != nil {
		return StageResolution{}, err
	}
	staged := len(ro.GetStages()) > 0 && len(nodeLabels) > 0
	if matchedStage == nil && staged {
		l.Warn().Str("group", groupName).Interface("nodeLabels", nodeLabels).
			Msg("no lifecycle stage matched this data node's labels; using the group-default segment interval/ttl/shardNum")
	}
	// A non-zero idle timeout so the reclaimer ticker starts (storage/rotation.go gates it
	// on >=1s); a Close stage shortens it. Retention is disabled on non-terminal tiers
	// (data migrates onward) and on staged-but-unmatched nodes; rotation is disabled on any
	// staged node.
	res := StageResolution{
		ResourceOpts:       resolved,
		Matched:            matchedStage != nil,
		SegmentIdleTimeout: time.Hour,
	}
	switch {
	case matchedStage != nil:
		if matchedStage.GetClose() {
			res.SegmentIdleTimeout = 5 * time.Minute
		}
		res.DisableRetention = matchedIdx+1 < len(ro.GetStages())
		res.DisableRotation = true
	case staged:
		res.DisableRetention = true
		res.DisableRotation = true
	}
	return res, nil
}

// ResolveResourceOptsForUpdate resolves the ResourceOpts a data node applies when a group
// is updated: the matched stage's interval/ttl/shardNum, or the group default when no
// stage applies. On a resolution error it keeps the group default rather than failing the
// update, and returns nil when the group carries no ResourceOpts. This is the shared body
// of every data supplier's ResolveResourceOpts.
func ResolveResourceOptsForUpdate(l *logger.Logger, groupSchema *commonv1.Group, nodeLabels map[string]string) *commonv1.ResourceOpts {
	ro := groupSchema.GetResourceOpts()
	if ro == nil {
		return nil
	}
	res, err := ResolveStage(l, groupSchema.GetMetadata().GetName(), ro, nodeLabels)
	if err != nil {
		l.Warn().Err(err).Str("group", groupSchema.GetMetadata().GetName()).
			Msg("failed to resolve stage resource opts on update; keeping group-default opts")
		return ro
	}
	return res.ResourceOpts
}
