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
	"fmt"
	"os"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
)

func (l *lifecycleService) getSnapshots(ctx context.Context, groups []*commonv1.Group, p *Progress) (streamDir string, measureDir string, traceDir string, err error) {
	// If we already have snapshot dirs in Progress, reuse them
	if p.SnapshotStreamDir != "" || p.SnapshotMeasureDir != "" || p.SnapshotTraceDir != "" {
		return p.SnapshotStreamDir, p.SnapshotMeasureDir, p.SnapshotTraceDir, nil
	}

	snapshotGroups := make([]*databasev1.SnapshotRequest_Group, 0, len(groups))
	for _, group := range groups {
		snapshotGroups = append(snapshotGroups, &databasev1.SnapshotRequest_Group{
			Group:   group.Metadata.Name,
			Catalog: group.Catalog,
		})
	}
	snn, err := snapshot.Get(ctx, l.gRPCAddr, l.enableTLS, l.insecure, l.cert, snapshotGroups...)
	if err != nil {
		return "", "", "", err
	}
	for _, snp := range snn {
		snapshotDir, errDir := snapshot.Dir(snp, l.streamRoot, l.measureRoot, "", l.traceRoot, "")
		if errDir != nil {
			l.l.Error().Err(errDir).Msgf("Failed to get snapshot directory for %s", snp.Name)
			continue
		}
		if _, err := os.Stat(snapshotDir); os.IsNotExist(err) {
			l.l.Error().Err(err).Msgf("Snapshot directory %s does not exist", snapshotDir)
			continue
		}
		if snp.Catalog == commonv1.Catalog_CATALOG_STREAM {
			streamDir = snapshotDir
		}
		if snp.Catalog == commonv1.Catalog_CATALOG_MEASURE {
			measureDir = snapshotDir
		}
		if snp.Catalog == commonv1.Catalog_CATALOG_TRACE {
			traceDir = snapshotDir
		}
	}
	// Save the new snapshot paths into Progress
	p.SnapshotStreamDir = streamDir
	p.SnapshotMeasureDir = measureDir
	p.SnapshotTraceDir = traceDir
	return streamDir, measureDir, traceDir, nil
}
// deriveSelfIdentity returns the SenderNode and SenderTier the lifecycle
// publisher should stamp on its wire SendRequest so the data-node receiver
// can label its banyandb_queue_sub_* family accordingly.
//
// Inputs (no new CLI flags needed):
//
//   - coLocatedDataNodeAddr: the lifecycle's --grpc-addr, which is the
//     gRPC address of the data node the lifecycle is co-located with
//     (sidecar topology). It is the authoritative match key.
//   - nodeLabels: the lifecycle's own --node-labels, used as a fallback
//     when the co-located data node hasn't synced to the metadata
//     registry yet (cold start of a freshly created lifecycle).
//   - nodes: the cluster's data-node registry (ROLE_DATA), which carries
//     both Metadata.Name (the BanyanDB NodeID) and Labels (e.g. type=hot).
//
// Resolution order:
//
//  1. Match by GrpcAddress. In the standard sidecar layout the lifecycle's
//     --grpc-addr equals the co-located data node's GrpcAddress, and
//     that data node's Metadata.Name is the BanyanDB NodeID the
//     receiver records as remote_node. This is the production path
//     (the live cluster's lifecycle container has no --node-labels).
//  2. Fall back to label matching. If a data node's Labels are a
//     superset of nodeLabels (every key in nodeLabels matches), use
//     that node's Metadata.Name and Labels["type"].
//  3. Fall back to type-only match on nodeLabels["type"].
//  4. If nothing matches, return empty strings — preserves the
//     pre-fix behavior so existing test setups that don't populate
//     these fields keep working.
func deriveSelfIdentity(coLocatedDataNodeAddr string, nodeLabels map[string]string, nodes []*databasev1.Node) (senderNode, senderTier string) {
	// Pass 1: GrpcAddress match (the production sidecar path).
	if coLocatedDataNodeAddr != "" {
		for _, n := range nodes {
			if n.GrpcAddress == coLocatedDataNodeAddr {
				return n.Metadata.Name, n.Labels["type"]
			}
		}
	}
	// Pass 2: every-key label match.
	for _, n := range nodes {
		if n.Labels == nil {
			continue
		}
		if !labelsContain(n.Labels, nodeLabels) {
			continue
		}
		return n.Metadata.Name, n.Labels["type"]
	}
	// Pass 3: type-only label match.
	if wantType := nodeLabels["type"]; wantType != "" {
		for _, n := range nodes {
			if n.Labels == nil {
				continue
			}
			if n.Labels["type"] == wantType {
				return n.Metadata.Name, n.Labels["type"]
			}
		}
	}
	return "", ""
}

// labelsContain reports whether superset has every (k, v) pair in subset.
// An empty subset matches anything.
func labelsContain(superset, subset map[string]string) bool {
	for k, v := range subset {
		if superset[k] != v {
			return false
		}
	}
	return true
}

// GroupConfig encapsulates the parsed lifecycle configuration for a Group.
// It contains all necessary information for migration and deletion operations.
type GroupConfig struct {
	*commonv1.Group
	NodeSelector   node.Selector
	QueueClient    queue.Client
	AccumulatedTTL *commonv1.IntervalRule
	// SegmentInterval is the current stage's segment interval, used to read
	// source segments on this node.
	SegmentInterval *commonv1.IntervalRule
	// TargetSegmentInterval is the next stage's segment interval. It differs
	// from SegmentInterval on 3-stage deployments (e.g. warm->cold), so any
	// computation against the target tier's segment grid must use this field.
	TargetSegmentInterval *commonv1.IntervalRule
	TargetShardNum        uint32
	TargetReplicas        uint32
}

// Close releases resources held by the GroupConfig.
func (gc *GroupConfig) Close() {
	if gc.QueueClient != nil {
		gc.QueueClient.GracefulStop()
	}
}

// cloneIntervalRule returns a deep copy of ir, or nil if ir is nil. proto.Clone
// applied to a typed-nil *commonv1.IntervalRule yields a non-nil zero value
// (Num=0, Unit=UNIT_UNSPECIFIED) that downstream MustToIntervalRule rejects;
// short-circuiting on nil keeps the GroupConfig fallback chain intact.
func cloneIntervalRule(ir *commonv1.IntervalRule) *commonv1.IntervalRule {
	if ir == nil {
		return nil
	}
	return proto.Clone(ir).(*commonv1.IntervalRule)
}

//nolint:contextcheck // health check goroutine uses context.Background()
func parseGroup(
	g *commonv1.Group, nodeLabels map[string]string, nodes []*databasev1.Node,
	l *logger.Logger, metadata metadata.Repo, clusterStateMgr *clusterStateManager,
	omr observability.MetricsRegistry,
	coLocatedDataNodeAddr string,
) (*GroupConfig, error) {
	ro := g.ResourceOpts
	if ro == nil {
		return nil, fmt.Errorf("no resource opts in group %s", g.Metadata.Name)
	}
	if len(ro.Stages) == 0 {
		return nil, fmt.Errorf("no stages in group %s", g.Metadata.Name)
	}
	// Validate IntervalRules up-front so later derefs (incl. Stages[i+1]) are safe.
	if ro.Ttl == nil {
		return nil, fmt.Errorf("group %s: missing ttl", g.Metadata.Name)
	}
	if ro.SegmentInterval == nil {
		return nil, fmt.Errorf("group %s: missing segment_interval", g.Metadata.Name)
	}
	for _, st := range ro.Stages {
		if st.SegmentInterval == nil {
			return nil, fmt.Errorf("group %s stage %s: missing segment_interval", g.Metadata.Name, st.Name)
		}
		if st.Ttl == nil {
			return nil, fmt.Errorf("group %s stage %s: missing ttl", g.Metadata.Name, st.Name)
		}
	}
	ttlTime := proto.Clone(ro.Ttl).(*commonv1.IntervalRule)
	segmentInterval := cloneIntervalRule(ro.SegmentInterval)
	var nst *commonv1.LifecycleStage
	var targetSegmentInterval *commonv1.IntervalRule
	for i, st := range ro.Stages {
		selector, err := pub.ParseLabelSelector(st.NodeSelector)
		if err != nil {
			return nil, errors.WithMessagef(err, "failed to parse node selector %s", st.NodeSelector)
		}
		ttlTime.Num += st.Ttl.Num
		if !selector.Matches(nodeLabels) {
			continue
		}
		if i+1 >= len(ro.Stages) {
			l.Info().Msgf("no next stage for group %s at stage %s", g.Metadata.Name, st.Name)
			return nil, nil
		}
		nst = ro.Stages[i+1]
		// Clone before exposing through GroupConfig so callers cannot mutate
		// the shared proto Stages[*] sub-objects.
		segmentInterval = cloneIntervalRule(st.SegmentInterval)
		targetSegmentInterval = cloneIntervalRule(nst.SegmentInterval)
		l.Info().Msgf("migrating group %s at stage %s to stage %s, source segment interval: %d(%s), target segment interval: %d(%s), total ttl needs: %d(%s)",
			g.Metadata.Name, st.Name, nst.Name,
			segmentInterval.Num, segmentInterval.Unit.String(),
			targetSegmentInterval.Num, targetSegmentInterval.Unit.String(),
			ttlTime.Num, ttlTime.Unit.String())
		break
	}
	if nst == nil {
		nst = ro.Stages[0]
		ttlTime = proto.Clone(ro.Ttl).(*commonv1.IntervalRule)
		targetSegmentInterval = cloneIntervalRule(nst.SegmentInterval)
		l.Info().Msgf("no matching stage for group %s, defaulting to first stage %s segment interval: %d(%s), total ttl needs: %d(%s)",
			g.Metadata.Name, nst.Name, segmentInterval.Num, segmentInterval.Unit.String(), ttlTime.Num, ttlTime.Unit.String())
	}
	nsl, err := pub.ParseLabelSelector(nst.NodeSelector)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to parse node selector %s", nst.NodeSelector)
	}
	nodeSel := node.NewRoundRobinSelector("", metadata)
	if ok, _ := nodeSel.OnInit([]schema.Kind{schema.KindGroup}); !ok {
		return nil, fmt.Errorf("failed to initialize node selector for group %s", g.Metadata.Name)
	}
	client := pub.NewWithoutMetadata(omr) //nolint:contextcheck // health check goroutine uses context.Background()
	// Stamp the lifecycle's self identity onto the publisher so the wire
	// SenderNode / SenderRole / SenderTier fields and the parallel
	// banyandb_lifecycle_migration_* labels are populated. The three
	// values are derived from already-known inputs (the co-located data
	// node's gRPC address and the cluster's data-node registry) so the
	// fix needs no new CLI flags:
	//   - SenderNode  = the data node whose GrpcAddress matches the
	//     lifecycle's --grpc-addr (i.e. the co-located data node). Its
	//     Metadata.Name is the BanyanDB NodeID the receiver records as
	//     remote_node.
	//   - SenderRole  = "lifecycle" (no Role enum entry; matches the
	//     liaison's hard-coded "liaison" pattern in pkg/cmdsetup/liaison.go).
	//   - SenderTier  = the matched data node's `type` label
	//     (hot/warm/cold), which becomes the receiver's remote_tier.
	// Falls back to the lifecycle's own --node-labels when the co-located
	// data node isn't in the registry yet (cold start), and to empty
	// when neither is available — preserving the pre-fix behavior.
	senderNode, senderTier := deriveSelfIdentity(coLocatedDataNodeAddr, nodeLabels, nodes)
	if senderNode != "" || senderTier != "" {
		client.SetSelfNode(senderNode, "lifecycle", senderTier)
	}
	switch g.Catalog {
	case commonv1.Catalog_CATALOG_STREAM:
		_ = grpc.NewClusterNodeRegistry(data.TopicStreamWrite, client, nodeSel)
	case commonv1.Catalog_CATALOG_TRACE:
		_ = grpc.NewClusterNodeRegistry(data.TopicTraceWrite, client, nodeSel)
	case commonv1.Catalog_CATALOG_MEASURE:
		_ = grpc.NewClusterNodeRegistry(data.TopicMeasureWrite, client, nodeSel)
	default:
		return nil, fmt.Errorf("unsupported catalog %s for lifecycle migration of group %s", g.Catalog, g.Metadata.Name)
	}

	var existed bool
	for _, n := range nodes {
		if n.Labels == nil {
			continue
		}
		if nsl.Matches(n.Labels) {
			existed = true
			client.OnAddOrUpdate(schema.Metadata{
				TypeMeta: schema.TypeMeta{
					Kind: schema.KindNode,
				},
				Spec: n,
			})
		}
	}
	if !existed {
		return nil, errors.New("no nodes matched")
	}

	if t := client.GetRouteTable(); t != nil {
		clusterStateMgr.addRouteTable(t)
	}
	return &GroupConfig{
		Group:                 g,
		TargetShardNum:        nst.ShardNum,
		TargetReplicas:        nst.Replicas,
		AccumulatedTTL:        ttlTime,
		SegmentInterval:       segmentInterval,
		TargetSegmentInterval: targetSegmentInterval,
		NodeSelector:          nodeSel,
		QueueClient:           client,
	}, nil
}

type fileInfo struct {
	file fs.File
	name string
}
