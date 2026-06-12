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
	"net"
	"os"
	"strings"

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
	"github.com/apache/skywalking-banyandb/pkg/meter"
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

// resolveSelfIdentity returns the BanyanDB NodeID (Metadata.Name) and
// tier label (Labels["type"]) the lifecycle publisher should stamp on
// its wire SendRequest / SyncMetadata, so the data-node receiver labels
// its banyandb_queue_sub_total_* family with non-empty remote_node and
// remote_tier.
//
// Resolution: the lifecycle sidecar's own pod hostname is the stable
// identifier the receiver will see as the sender. It comes from
// POD_NAME (K8s downward API) and falls back to os.Hostname() — the
// same precedence as nativeNodeContext at service.go:160-165. The
// function then looks the host up directly in the data-node registry,
// matching against the host portion of GrpcAddress and NodeID (the
// registry may carry either an IP, a headless-service FQDN, or a
// loopback alias, depending on which bind address the data pod
// registered with). The first registry entry whose host matches (with
// loopback-alias normalization) is the co-located data pod; its
// Metadata.Name is the SenderNode and its Labels["type"] is the
// SenderTier.
//
// Re-runs on every parseGroup call (no caching) so a data-pod
// restart, re-registration, or new host is picked up by the next
// cycle. Returns ok=false when no registry entry matches.
func resolveSelfIdentity(selfPodHost string, nodes []*databasev1.Node) (senderNode, senderTier string, ok bool) {
	if selfPodHost == "" {
		return "", "", false
	}
	for _, n := range nodes {
		if n == nil || n.Metadata == nil {
			continue
		}
		if hostMatches(n.GrpcAddress, selfPodHost) {
			return n.Metadata.Name, n.Labels["type"], true
		}
	}
	return "", "", false
}

// selfPodHostname returns the lifecycle sidecar's own pod host.
// Precedence matches nativeNodeContext at service.go:160-165:
// POD_NAME first (K8s downward API), then os.Hostname() as a
// fallback. Returns "" only if both lookups fail (very rare; e.g.
// hostname uname syscall returns ENAMETOOLONG).
func selfPodHostname() string {
	if v := os.Getenv("POD_NAME"); v != "" {
		return v
	}
	if h, err := os.Hostname(); err == nil {
		return h
	}
	return ""
}

// hostMatches reports whether aRegistryHost (which may carry a :port
// and may be a loopback alias, an IP, or a headless-service FQDN)
// identifies the same pod as selfPodHost. The host portion is
// extracted via net.SplitHostPort, then reduced to its leftmost
// label -- but only for FQDNs (multi-label hostnames); IP literals
// are kept as-is so a 127.0.0.1 form is not truncated to "127".
// (a FQDN like "data-x.data-x-headless.ns" maps to the pod name
// "data-x".) Loopback aliases (localhost, 127.0.0.1, ::1) are
// treated as equivalent so a registry entry advertised as
// 127.0.0.1:17912 still matches a selfPodHost of "localhost" or
// vice versa.
func hostMatches(aRegistryHost, selfPodHost string) bool {
	if aRegistryHost == "" {
		return false
	}
	if h, _, err := net.SplitHostPort(aRegistryHost); err == nil {
		aRegistryHost = h
	}
	if net.ParseIP(aRegistryHost) == nil {
		if i := strings.Index(aRegistryHost, "."); i >= 0 {
			aRegistryHost = aRegistryHost[:i]
		}
	}
	if aRegistryHost == selfPodHost {
		return true
	}
	if isLoopbackHost(selfPodHost) && isLoopbackHost(aRegistryHost) {
		return true
	}
	return false
}

// grpcAddrEqual reports whether two advertised gRPC addresses identify the
// same endpoint. Three equivalences are honored:
//   - exact string match,
//   - host-portion match after reducing each to its leftmost label
//     (FQDN-only; IP literals are kept as-is) with the same port,
//   - both hosts are loopback aliases (localhost / 127.0.0.1 / ::1)
//     with the same port.
//
// The middle case is what was missing pre-fix: a --grpc-addr of
// 127.0.0.1:17912 and a registry GrpcAddress of
// "<headless-svc>.<ns>:17912" used to be rejected because the
// headless-svc host is not loopback and not a literal string match.
// The new behavior is "same port and same leftmost label" wins
// regardless of the FQDN or loopback status.
func grpcAddrEqual(a, b string) bool {
	if a == b {
		return true
	}
	hostA, portA, errA := net.SplitHostPort(a)
	hostB, portB, errB := net.SplitHostPort(b)
	if errA != nil || errB != nil || portA != portB {
		return false
	}
	if net.ParseIP(hostA) == nil {
		if i := strings.Index(hostA, "."); i >= 0 {
			hostA = hostA[:i]
		}
	}
	if net.ParseIP(hostB) == nil {
		if i := strings.Index(hostB, "."); i >= 0 {
			hostB = hostB[:i]
		}
	}
	if hostA == hostB {
		return true
	}
	return isLoopbackHost(hostA) && isLoopbackHost(hostB)
}

// isLoopbackHost reports whether the host is a loopback alias.
func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
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
	// SourceStage and TargetStage are the migration's source and target stage
	// names (e.g. "hot" -> "warm"), surfaced in the migration report's errors.
	SourceStage    string
	TargetStage    string
	TargetShardNum uint32
	TargetReplicas uint32
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
	resolutionCounter meter.Counter,
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
	var sourceStage string
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
		sourceStage = st.Name
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
		// No stage matched this node (e.g. the initial hot tier is not listed in
		// Stages): the source stage is the running node's own tier label.
		sourceStage = nodeLabels["type"]
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
	// banyandb_lifecycle_migration_* labels are populated. The
	// resolveSelfIdentity algorithm matches the lifecycle's own pod
	// hostname (POD_NAME -> os.Hostname(), same precedence as
	// nativeNodeContext at service.go:160-165) against the
	// data-node registry's GrpcAddress with loopback-alias and
	// port-strip normalization. The first matching registry entry is
	// the co-located data pod; its Metadata.Name is the BanyanDB
	// NodeID the receiver records as remote_node, and its
	// Labels["type"] is the receiver's remote_tier. SenderRole is
	// hard-coded to "lifecycle" to mirror the liaison's
	// "liaison" pattern at pkg/cmdsetup/liaison.go:170-171.
	//
	// The resolution counter (banyandb_lifecycle_self_identity_resolution_total)
	// is incremented with result=ok on a non-empty match and
	// result=empty on a no-match. Pre-fix, 2 of 4 lifecycle pods
	// (hot-0, warm-1) returned empty due to a DNS-name vs loopback
	// mismatch in deriveSelfIdentity's Pass 1; the new
	// resolveSelfIdentity closes that gap.
	selfHost := selfPodHostname()
	senderNode, senderTier, resolvedOK := resolveSelfIdentity(selfHost, nodes)
	if resolutionCounter != nil {
		label := "empty"
		if resolvedOK {
			label = "ok"
		}
		resolutionCounter.Inc(1, label)
	}
	if resolvedOK {
		client.SetSelfNode(senderNode, "lifecycle", senderTier)
		// Info log so operators can see which identity the agent
		// stamped on the wire, and which co-located data pod the
		// registry picked. This is the log line that surfaces the
		// "remote node" the user wants visible at startup.
		l.Info().
			Str("data_pod", selfHost).
			Str("sender_node", senderNode).
			Str("sender_tier", senderTier).
			Msg("lifecycle: stamped sender identity on wire (SenderNode, SenderTier)")
	} else {
		l.Warn().
			Str("data_pod", selfHost).
			Msg("lifecycle: sender identity resolution returned empty; SenderNode on wire will be empty (pre-fix regression)")
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
		SourceStage:           sourceStage,
		TargetStage:           nst.Name,
		NodeSelector:          nodeSel,
		QueueClient:           client,
	}, nil
}

type fileInfo struct {
	file fs.File
	name string
}
