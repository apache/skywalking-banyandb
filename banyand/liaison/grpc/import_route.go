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

package grpc

import (
	"context"
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
)

// importHotStage names the default stage of a group: the nodes that match none of its
// lifecycle stages.
const importHotStage = "hot"

// importRoute is the placement snapshot of one (group, stage) import: shards[s] lists the
// distinct nodes shard s is written to.
type importRoute struct {
	group    string
	stage    string
	shards   [][]string
	shardNum uint32
	copies   uint32
}

// targets returns the nodes shard is written to.
func (r *importRoute) targets(shard uint32) []string {
	if int(shard) >= len(r.shards) {
		return nil
	}
	return r.shards[shard]
}

// nodes returns the sorted union of every shard's targets.
func (r *importRoute) nodes() []string {
	set := make(map[string]struct{})
	for _, ns := range r.shards {
		for _, n := range ns {
			set[n] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func (r *importRoute) toProto() *transferv1.TargetsResponse {
	shards := make([]*transferv1.TargetsResponse_Shard, len(r.shards))
	for i, ns := range r.shards {
		shards[i] = &transferv1.TargetsResponse_Shard{ShardId: uint32(i), NodeIds: ns}
	}
	return &transferv1.TargetsResponse{
		Group:    r.group,
		Stage:    r.stage,
		ShardNum: r.shardNum,
		Copies:   r.copies,
		Shards:   shards,
	}
}

// stageRoute places the shards of a stream, measure or trace group's stage where the cluster
// places them, through the same node selection API. The hot stage (the default stage, a stage
// explicitly named hot, or a stage the group does not define, which falls back to it) goes to
// the nodes the liaison's write queue syncs online writes to: the catalog's data node selector
// of the --data-node-selector nodes, with the group's shard number and replicas. Any other
// stage goes to the nodes a lifecycle migration moves it to: a round-robin selector over the
// nodes matching the stage's node selector, with the stage's shard number and replicas. A
// shard the selector cannot place, such as one past the group's shard number, is refused.
func (s *importService) stageRoute(ctx context.Context, g *commonv1.Group, stage string) (*importRoute, error) {
	group, ro := g.GetMetadata().GetName(), g.GetResourceOpts()
	want := strings.ToLower(stage)
	matched := lookupStage(ro, want)
	if want == "" || want == importHotStage || matched == nil {
		return locateRoute(group, importHotStage, ro.GetShardNum(), ro.GetReplicas(), s.dataNodes[g.GetCatalog()])
	}
	nodes, err := s.stageNodes(ctx, matched)
	if err != nil {
		return nil, err
	}
	return locateRoute(group, want, matched.GetShardNum(), matched.GetReplicas(), nodes)
}

// stageNodes builds the selector a lifecycle migration places a stage with: a round-robin
// selector over the registered nodes matching the stage's node selector, laid out by every
// group the schema registry holds.
func (s *importService) stageNodes(ctx context.Context, st *commonv1.LifecycleStage) (NodeRegistry, error) {
	ls, err := pub.ParseLabelSelector(st.GetNodeSelector())
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "stage %s: %v", st.GetName(), err)
	}
	groups, err := s.listGroups(ctx)
	if err != nil {
		return nil, schemaReadError(err, "list the groups")
	}
	sel := node.NewRoundRobinSelector("", nil)
	sel.SetNodeSelector(ls)
	handler := sel.(schema.EventHandler)
	for _, g := range groups {
		handler.OnAddOrUpdate(schema.Metadata{TypeMeta: schema.TypeMeta{Kind: schema.KindGroup}, Spec: g})
	}
	for _, n := range s.routeTable.GetRouteTable().GetRegistered() {
		sel.AddNode(n)
	}
	return selectorRegistry(sel), nil
}

// locateRoute places every shard of a group's stage on the nodes registry locates for its
// replicas.
func locateRoute(group, stage string, shardNum, replicas uint32, registry NodeRegistry) (*importRoute, error) {
	if shardNum == 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "stage %s of group %s has no shard number", stage, group)
	}
	if registry == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "stage %s of group %s has no data node selector", stage, group)
	}
	copies := replicas + 1
	shards := make([][]string, shardNum)
	for sh := range shards {
		located, err := registry.LocateAll(group, uint32(sh), int(copies))
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "stage %s of group %s has no data node for shard %d: %v", stage, group, sh, err)
		}
		shards[sh] = located
	}
	return &importRoute{group: group, stage: stage, shardNum: shardNum, copies: copies, shards: shards}, nil
}

// lookupStage returns the lifecycle stage of ro named name, case-insensitively, or nil.
func lookupStage(ro *commonv1.ResourceOpts, name string) *commonv1.LifecycleStage {
	for _, st := range ro.GetStages() {
		if strings.EqualFold(st.GetName(), name) {
			return st
		}
	}
	return nil
}

// standaloneRoute places every shard of a group on the standalone process itself, named
// self as in its export plans. A standalone node matches no lifecycle stage, so any stage but
// the default falls back.
func standaloneRoute(group string, ro *commonv1.ResourceOpts, self string) (*importRoute, error) {
	shardNum := ro.GetShardNum()
	if shardNum == 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "group %s has no shard number", group)
	}
	if self == "" {
		return nil, importError(codes.Unavailable, transfer.ImportReasonNotWritten, nil, "this process has not resolved its node id yet")
	}
	shards := make([][]string, shardNum)
	for s := range shards {
		shards[s] = []string{self}
	}
	return &importRoute{group: group, stage: importHotStage, shardNum: shardNum, copies: 1, shards: shards}, nil
}

// propertyRoute places every shard of a property group on the nodes the property node
// registry locates for its replicas, the placement of online property writes. Properties
// have no lifecycle stage.
func propertyRoute(group string, ro *commonv1.ResourceOpts, nodes NodeRegistry) (*importRoute, error) {
	return locateRoute(group, importHotStage, ro.GetShardNum(), ro.GetReplicas(), nodes)
}
