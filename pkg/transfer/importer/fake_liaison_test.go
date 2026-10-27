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
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

// fakeSegment is the segment every row the fake receives lands in.
const fakeSegment = "20261016"

// fakeLiaison is an in-process gRPC server standing in for a target liaison: the schema
// registries (List, Create, Update), ImportService, ExportService.Plan over what it received,
// and SchemaBarrierService. A row of shard s goes to nodes[(s+r) % len(nodes)] for every
// replica r; the shard is the row's position in its batch modulo the shard number.
type fakeLiaison struct {
	transferv1.UnimplementedImportServiceServer
	transferv1.UnimplementedExportServiceServer
	schemav1.UnimplementedSchemaBarrierServiceServer
	// importErr, when set, is asked before every batch with the stream's context; a non-nil
	// error ends the stream.
	importErr func(ctx context.Context, init *transferv1.ImportRowsRequest_Init, seq uint64) error
	// ackGate, when set, is called before the acknowledgement of every batch is sent; the fake
	// keeps receiving and applying batches meanwhile, and acknowledges them in order.
	ackGate func(init *transferv1.ImportRowsRequest_Init, seq uint64)
	// onPlan, when set, is called on every Plan request, under mu.
	onPlan func()
	// awaitAnswer, when set, replaces the barrier answer.
	awaitAnswer *schemav1.AwaitSchemaAppliedResponse
	// targetsHook, when set, may edit every Targets answer; via names the serving fake
	// ("" for the one serve returns, otherwise the serveAs name).
	targetsHook func(via string, resp *transferv1.TargetsResponse)
	// routeErr, when set, is asked for every Targets key; a non-nil error is the answer.
	routeErr func(req *transferv1.TargetsRequest) error
	// revisionAnswer, when set, answers the probe-th AwaitRevisionApplied (1-based).
	revisionAnswer func(probe int) *schemav1.AwaitRevisionAppliedResponse
	// deleting names the groups being deleted.
	deleting map[string]bool
	// inactive names the data nodes the liaison is not connected to.
	inactive []string
	// createErr, when set, is the answer to every Create.
	createErr  error
	schema     Schemas
	counts     map[CountKey]int64
	properties map[CountKey]int64
	// dropNode, when set, names a node that acknowledges rows without storing them.
	dropNode    string
	inits       []*transferv1.ImportRowsRequest_Init
	batches     []*transferv1.ImportRowsRequest_RowBatch
	writes      []string
	awaits      []*schemav1.AwaitSchemaAppliedRequest
	nodes       []string
	unreachable []string
	targetCalls int
	revProbes   int
	planCalls   int
	rev         int64
	mu          sync.Mutex
}

func newFakeLiaison() *fakeLiaison {
	return &fakeLiaison{schema: NewSchemas(), nodes: []string{"data-a", "data-b"}, counts: map[CountKey]int64{}, properties: map[CountKey]int64{}}
}

// importStatus is an ImportRows failure carrying the ErrorInfo detail the liaison attaches.
func importStatus(code codes.Code, reason string, metadata map[string]string, msg string) error {
	st, err := status.New(code, msg).WithDetails(&errdetails.ErrorInfo{Domain: transfer.ImportErrorDomain, Reason: reason, Metadata: metadata})
	if err != nil {
		panic(err)
	}
	return st.Err()
}

// nodeFailed is the NODE_FAILED failure of node for batch seq.
func nodeFailed(code codes.Code, seq, node, msg string) error {
	return importStatus(code, transfer.ImportReasonNodeFailed, map[string]string{transfer.ImportMetadataBatchSeq: seq, transfer.ImportMetadataNode: node},
		"batch_seq="+seq+" node="+node+": "+msg)
}

func (f *fakeLiaison) group(name string) (*commonv1.Group, error) {
	g, ok := f.schema.Groups[name]
	if !ok {
		return nil, importStatus(codes.NotFound, transfer.ImportReasonSchemaNotFound, nil, "group "+name+" not found")
	}
	return g, nil
}

// route places a (group, stage) like the liaison: a stage the group lacks falls back to hot.
func (f *fakeLiaison) route(g *commonv1.Group, stage string) *transferv1.TargetsResponse {
	ro := g.GetResourceOpts()
	shardNum, replicas := ro.GetShardNum(), ro.GetReplicas()
	want := normalizeStage(stage)
	if g.GetCatalog() == commonv1.Catalog_CATALOG_PROPERTY {
		want = transfer.DefaultStageName
	} else if want != transfer.DefaultStageName {
		idx := stageIndex(ro, want)
		if idx < 0 {
			want = transfer.DefaultStageName
		} else {
			shardNum, replicas = ro.GetStages()[idx].GetShardNum(), ro.GetStages()[idx].GetReplicas()
		}
	}
	r := &transferv1.TargetsResponse{Group: g.GetMetadata().GetName(), Stage: want, ShardNum: shardNum, Copies: replicas + 1}
	for s := range shardNum {
		var ids []string
		for rep := range min(int(replicas)+1, len(f.nodes)) {
			ids = append(ids, f.nodes[(int(s)+rep)%len(f.nodes)])
		}
		r.Shards = append(r.Shards, &transferv1.TargetsResponse_Shard{ShardId: s, NodeIds: ids})
	}
	return r
}

// targets answers like the liaison: the key is routed with its stored group.
func (f *fakeLiaison) targets(via string, req *transferv1.TargetsRequest) (*transferv1.TargetsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.targetCalls++
	if f.routeErr != nil {
		if err := f.routeErr(req); err != nil {
			return nil, err
		}
	}
	g, err := f.group(req.GetGroup())
	if err != nil {
		return nil, err
	}
	r := f.route(g, req.GetStage())
	r.GroupDeleting = f.deleting[req.GetGroup()]
	for _, s := range r.GetShards() {
		for _, n := range s.GetNodeIds() {
			if slices.Contains(f.inactive, n) && !slices.Contains(r.InactiveNodeIds, n) {
				r.InactiveNodeIds = append(r.InactiveNodeIds, n)
			}
		}
	}
	if f.targetsHook != nil {
		f.targetsHook(via, r)
	}
	return r, nil
}

func (f *fakeLiaison) ImportRows(stream transferv1.ImportService_ImportRowsServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	init := first.GetInit()
	if init == nil {
		return status.Error(codes.InvalidArgument, "the first frame must be init")
	}
	f.mu.Lock()
	f.inits = append(f.inits, init)
	f.mu.Unlock()
	// A receiver goroutine applies the batches and queues their outcomes; this one sends the
	// acknowledgements in order and ends the stream with the first failure.
	type outcome struct {
		resp *transferv1.ImportRowsResponse
		err  error
	}
	outcomes := make(chan outcome, 64)
	ctx := stream.Context()
	//panicdiag:allow-rawgo test server
	go func() {
		defer close(outcomes)
		put := func(o outcome) bool {
			select {
			case outcomes <- o:
				return o.err == nil
			case <-ctx.Done():
				return false
			}
		}
		for {
			req, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				return
			}
			if recvErr != nil {
				put(outcome{err: recvErr})
				return
			}
			b := req.GetBatch()
			if f.importErr != nil {
				if batchErr := f.importErr(ctx, init, b.GetBatchSeq()); batchErr != nil {
					put(outcome{err: batchErr})
					return
				}
			}
			// Like the liaison, a batch of a canceled stream is not written.
			if ctx.Err() != nil {
				return
			}
			if applyErr := f.apply(init, b); applyErr != nil {
				put(outcome{err: applyErr})
				return
			}
			if !put(outcome{resp: &transferv1.ImportRowsResponse{BatchSeq: b.GetBatchSeq()}}) {
				return
			}
		}
	}()
	for o := range outcomes {
		if o.err != nil {
			return o.err
		}
		if f.ackGate != nil {
			f.ackGate(init, o.resp.GetBatchSeq())
		}
		if err = stream.Send(o.resp); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeLiaison) apply(init *transferv1.ImportRowsRequest_Init, b *transferv1.ImportRowsRequest_RowBatch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	g, err := f.group(init.GetGroup())
	if err != nil {
		return err
	}
	f.batches = append(f.batches, proto.Clone(b).(*transferv1.ImportRowsRequest_RowBatch))
	r := f.route(g, init.GetStage())
	n := batchRows(b)
	for i := range n {
		shard := uint32(i) % r.GetShardNum()
		for _, node := range r.GetShards()[shard].GetNodeIds() {
			if node == f.dropNode {
				continue
			}
			k := CountKey{Catalog: init.GetCatalog(), Group: init.GetGroup(), Stage: r.GetStage(), Node: node, Shard: shard}
			if init.GetCatalog() == commonv1.Catalog_CATALOG_PROPERTY {
				k.Stage = ""
				f.properties[k]++
				continue
			}
			f.counts[k]++
		}
	}
	return nil
}

func (f *fakeLiaison) Plan(req *transferv1.PlanRequest, stream transferv1.ExportService_PlanServer) error {
	f.mu.Lock()
	f.planCalls++
	if f.onPlan != nil {
		f.onPlan()
	}
	frames := map[[2]string]*transferv1.UnitFrame{}
	frame := func(node, stage string) *transferv1.UnitFrame {
		k := [2]string{node, stage}
		if frames[k] == nil {
			frames[k] = &transferv1.UnitFrame{NodeId: node, Stage: stage}
		}
		return frames[k]
	}
	selected := func(c commonv1.Catalog, group string) bool {
		return slices.ContainsFunc(req.GetSelectors(), func(s *transferv1.Selector) bool {
			return s.GetCatalog() == c && (len(s.GetGroups()) == 0 || slices.Contains(s.GetGroups(), group))
		})
	}
	for k, n := range f.counts {
		if !selected(k.Catalog, k.Group) {
			continue
		}
		stage := k.Stage
		if stage == transfer.DefaultStageName {
			stage = ""
		}
		fr := frame(k.Node, stage)
		fr.Units = append(fr.Units, &transferv1.UnitInventory{Kind: &transferv1.UnitInventory_Segment{Segment: &transferv1.SegmentInventory{
			Unit:   &transferv1.SegmentUnit{Catalog: k.Catalog, Group: k.Group, SegmentSuffix: fakeSegment, ShardIds: []uint32{k.Shard}},
			Shards: []*transferv1.ShardStat{{ShardId: k.Shard, TotalCount: uint64(n), PartsCount: 1}},
		}}})
	}
	for k, n := range f.properties {
		if !selected(k.Catalog, k.Group) {
			continue
		}
		fr := frame(k.Node, "")
		fr.Units = append(fr.Units, &transferv1.UnitInventory{Kind: &transferv1.UnitInventory_Property{Property: &transferv1.PropertyInventory{
			Group: k.Group, Shards: []*transferv1.PropertyShardStat{{ShardId: k.Shard, DocCount: uint64(n)}},
		}}})
	}
	var answered []string
	for _, n := range f.nodes {
		if !slices.Contains(f.unreachable, n) {
			answered = append(answered, n)
		}
	}
	unreachable := slices.Clone(f.unreachable)
	f.mu.Unlock()
	for _, fr := range frames {
		if slices.Contains(unreachable, fr.GetNodeId()) {
			continue
		}
		if err := stream.Send(&transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Units{Units: fr}}); err != nil {
			return err
		}
	}
	return stream.Send(&transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Summary{Summary: &transferv1.PlanSummary{
		AnsweredNodes: answered, UnreachableNodes: unreachable,
	}}})
}

func (f *fakeLiaison) AwaitRevisionApplied(_ context.Context, _ *schemav1.AwaitRevisionAppliedRequest) (*schemav1.AwaitRevisionAppliedResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revProbes++
	if f.revisionAnswer != nil {
		return f.revisionAnswer(f.revProbes), nil
	}
	return &schemav1.AwaitRevisionAppliedResponse{Applied: true}, nil
}

func (f *fakeLiaison) AwaitSchemaApplied(_ context.Context, req *schemav1.AwaitSchemaAppliedRequest) (*schemav1.AwaitSchemaAppliedResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.awaits = append(f.awaits, req)
	if f.awaitAnswer != nil {
		return f.awaitAnswer, nil
	}
	return &schemav1.AwaitSchemaAppliedResponse{Applied: true}, nil
}

// put stores a created or updated resource, stamping a revision; like the registries, create
// refuses an existing one (AlreadyExists) and update a missing one (NotFound).
func (f *fakeLiaison) put(kind string, m proto.Message, create bool) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if create && f.createErr != nil {
		return 0, f.createErr
	}
	m = proto.Clone(m)
	md := m.ProtoReflect().Get(m.ProtoReflect().Descriptor().Fields().ByName("metadata")).Message().Interface().(*commonv1.Metadata)
	id := kind + "/" + md.GetGroup() + "/" + md.GetName()
	if kind == schemasync.KindGroup {
		id = kind + "/" + md.GetName()
	}
	switch exists := f.schemaHas(m); {
	case create && exists:
		return 0, status.Errorf(codes.AlreadyExists, "%s already exists", id)
	case !create && !exists:
		return 0, status.Errorf(codes.NotFound, "%s not found", id)
	}
	f.rev++
	rev := f.rev
	md.ModRevision = rev
	if err := f.schema.Add(kind, m); err != nil {
		return 0, err
	}
	verb := "update "
	if create {
		verb = "create "
	}
	f.writes = append(f.writes, verb+id)
	if g, ok := m.(*commonv1.Group); ok && create && g.GetCatalog() == commonv1.Catalog_CATALOG_MEASURE {
		f.schema.Measures[ResourceKey{Group: md.GetName(), Name: csvfmt.TopNResultMeasure}] = testTopNResult(md.GetName())
	}
	return rev, nil
}

// schemaHas tells whether the fake already holds the resource m names; a group is keyed by its name alone.
func (f *fakeLiaison) schemaHas(m proto.Message) bool {
	var ok bool
	switch v := m.(type) {
	case *commonv1.Group:
		_, ok = f.schema.Groups[v.GetMetadata().GetName()]
	case *databasev1.Stream:
		_, ok = f.schema.Streams[keyOf(v.GetMetadata())]
	case *databasev1.Measure:
		_, ok = f.schema.Measures[keyOf(v.GetMetadata())]
	case *databasev1.Trace:
		_, ok = f.schema.Traces[keyOf(v.GetMetadata())]
	case *databasev1.Property:
		_, ok = f.schema.Properties[keyOf(v.GetMetadata())]
	case *databasev1.IndexRule:
		_, ok = f.schema.IndexRules[keyOf(v.GetMetadata())]
	case *databasev1.IndexRuleBinding:
		_, ok = f.schema.IndexRuleBindings[keyOf(v.GetMetadata())]
	case *databasev1.TopNAggregation:
		_, ok = f.schema.TopNs[keyOf(v.GetMetadata())]
	}
	return ok
}

func (f *fakeLiaison) writeLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.writes)
}

func (f *fakeLiaison) receivedBatches() []*transferv1.ImportRowsRequest_RowBatch {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.batches)
}

func inGroup[T any](m map[ResourceKey]T, group string) []T {
	var out []T
	for k, v := range m {
		if k.Group == group {
			out = append(out, v)
		}
	}
	return out
}

type fakeGroupRegistry struct {
	databasev1.UnimplementedGroupRegistryServiceServer
	f *fakeLiaison
}

func (r fakeGroupRegistry) List(context.Context, *databasev1.GroupRegistryServiceListRequest) (*databasev1.GroupRegistryServiceListResponse, error) {
	r.f.mu.Lock()
	defer r.f.mu.Unlock()
	resp := &databasev1.GroupRegistryServiceListResponse{}
	for _, g := range r.f.schema.Groups {
		resp.Group = append(resp.Group, g)
	}
	slices.SortFunc(resp.Group, func(a, b *commonv1.Group) int {
		return strings.Compare(a.GetMetadata().GetName(), b.GetMetadata().GetName())
	})
	return resp, nil
}

func (r fakeGroupRegistry) Create(_ context.Context, req *databasev1.GroupRegistryServiceCreateRequest) (*databasev1.GroupRegistryServiceCreateResponse, error) {
	rev, err := r.f.put(schemasync.KindGroup, req.GetGroup(), true)
	return &databasev1.GroupRegistryServiceCreateResponse{ModRevision: rev}, err
}

func (r fakeGroupRegistry) Update(_ context.Context, req *databasev1.GroupRegistryServiceUpdateRequest) (*databasev1.GroupRegistryServiceUpdateResponse, error) {
	rev, err := r.f.put(schemasync.KindGroup, req.GetGroup(), false)
	return &databasev1.GroupRegistryServiceUpdateResponse{ModRevision: rev}, err
}

type fakeStreamRegistry struct {
	databasev1.UnimplementedStreamRegistryServiceServer
	f *fakeLiaison
}

func (r fakeStreamRegistry) List(_ context.Context, req *databasev1.StreamRegistryServiceListRequest) (*databasev1.StreamRegistryServiceListResponse, error) {
	r.f.mu.Lock()
	defer r.f.mu.Unlock()
	return &databasev1.StreamRegistryServiceListResponse{Stream: inGroup(r.f.schema.Streams, req.GetGroup())}, nil
}

func (r fakeStreamRegistry) Create(_ context.Context, req *databasev1.StreamRegistryServiceCreateRequest) (*databasev1.StreamRegistryServiceCreateResponse, error) {
	rev, err := r.f.put(schemasync.KindStream, req.GetStream(), true)
	return &databasev1.StreamRegistryServiceCreateResponse{ModRevision: rev}, err
}

func (r fakeStreamRegistry) Update(_ context.Context, req *databasev1.StreamRegistryServiceUpdateRequest) (*databasev1.StreamRegistryServiceUpdateResponse, error) {
	rev, err := r.f.put(schemasync.KindStream, req.GetStream(), false)
	return &databasev1.StreamRegistryServiceUpdateResponse{ModRevision: rev}, err
}

type fakeMeasureRegistry struct {
	databasev1.UnimplementedMeasureRegistryServiceServer
	f *fakeLiaison
}

func (r fakeMeasureRegistry) List(_ context.Context, req *databasev1.MeasureRegistryServiceListRequest) (*databasev1.MeasureRegistryServiceListResponse, error) {
	r.f.mu.Lock()
	defer r.f.mu.Unlock()
	return &databasev1.MeasureRegistryServiceListResponse{Measure: inGroup(r.f.schema.Measures, req.GetGroup())}, nil
}

func (r fakeMeasureRegistry) Create(_ context.Context, req *databasev1.MeasureRegistryServiceCreateRequest) (*databasev1.MeasureRegistryServiceCreateResponse, error) {
	rev, err := r.f.put(schemasync.KindMeasure, req.GetMeasure(), true)
	return &databasev1.MeasureRegistryServiceCreateResponse{ModRevision: rev}, err
}

func (r fakeMeasureRegistry) Update(_ context.Context, req *databasev1.MeasureRegistryServiceUpdateRequest) (*databasev1.MeasureRegistryServiceUpdateResponse, error) {
	rev, err := r.f.put(schemasync.KindMeasure, req.GetMeasure(), false)
	return &databasev1.MeasureRegistryServiceUpdateResponse{ModRevision: rev}, err
}

type fakeTraceRegistry struct {
	databasev1.UnimplementedTraceRegistryServiceServer
	f *fakeLiaison
}

func (r fakeTraceRegistry) List(_ context.Context, req *databasev1.TraceRegistryServiceListRequest) (*databasev1.TraceRegistryServiceListResponse, error) {
	r.f.mu.Lock()
	defer r.f.mu.Unlock()
	return &databasev1.TraceRegistryServiceListResponse{Trace: inGroup(r.f.schema.Traces, req.GetGroup())}, nil
}

type fakePropertyRegistry struct {
	databasev1.UnimplementedPropertyRegistryServiceServer
	f *fakeLiaison
}

func (r fakePropertyRegistry) List(_ context.Context, req *databasev1.PropertyRegistryServiceListRequest) (*databasev1.PropertyRegistryServiceListResponse, error) {
	r.f.mu.Lock()
	defer r.f.mu.Unlock()
	return &databasev1.PropertyRegistryServiceListResponse{Properties: inGroup(r.f.schema.Properties, req.GetGroup())}, nil
}

func (r fakePropertyRegistry) Create(_ context.Context, req *databasev1.PropertyRegistryServiceCreateRequest) (*databasev1.PropertyRegistryServiceCreateResponse, error) {
	rev, err := r.f.put(schemasync.KindProperty, req.GetProperty(), true)
	return &databasev1.PropertyRegistryServiceCreateResponse{ModRevision: rev}, err
}

type fakeIndexRuleRegistry struct {
	databasev1.UnimplementedIndexRuleRegistryServiceServer
	f *fakeLiaison
}

func (r fakeIndexRuleRegistry) List(_ context.Context, req *databasev1.IndexRuleRegistryServiceListRequest) (*databasev1.IndexRuleRegistryServiceListResponse, error) {
	r.f.mu.Lock()
	defer r.f.mu.Unlock()
	return &databasev1.IndexRuleRegistryServiceListResponse{IndexRule: inGroup(r.f.schema.IndexRules, req.GetGroup())}, nil
}

type fakeBindingRegistry struct {
	databasev1.UnimplementedIndexRuleBindingRegistryServiceServer
	f *fakeLiaison
}

func (r fakeBindingRegistry) List(_ context.Context, req *databasev1.IndexRuleBindingRegistryServiceListRequest) (
	*databasev1.IndexRuleBindingRegistryServiceListResponse, error,
) {
	r.f.mu.Lock()
	defer r.f.mu.Unlock()
	return &databasev1.IndexRuleBindingRegistryServiceListResponse{IndexRuleBinding: inGroup(r.f.schema.IndexRuleBindings, req.GetGroup())}, nil
}

type fakeTopNRegistry struct {
	databasev1.UnimplementedTopNAggregationRegistryServiceServer
	f *fakeLiaison
}

func (r fakeTopNRegistry) List(_ context.Context, req *databasev1.TopNAggregationRegistryServiceListRequest) (
	*databasev1.TopNAggregationRegistryServiceListResponse, error,
) {
	r.f.mu.Lock()
	defer r.f.mu.Unlock()
	return &databasev1.TopNAggregationRegistryServiceListResponse{TopNAggregation: inGroup(r.f.schema.TopNs, req.GetGroup())}, nil
}

// namedImport serves the fake's ImportService under another liaison's name.
type namedImport struct {
	*fakeLiaison
	name string
}

func (n namedImport) Targets(ctx context.Context, req *transferv1.TargetsRequest) (*transferv1.TargetsResponse, error) {
	return n.targets(n.name, req)
}

// serve starts the fake on 127.0.0.1:0 and returns a connected cluster.Liaison.
func (f *fakeLiaison) serve(t *testing.T) *cluster.Liaison {
	t.Helper()
	return f.serveAs(t, "", true)
}

// serveAs starts another liaison of the same fake cluster: its Targets answers pass name to
// targetsHook, and withImport false leaves ImportService out, like a liaison without it.
func (f *fakeLiaison) serveAs(t *testing.T, name string, withImport bool) *cluster.Liaison {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	if withImport {
		transferv1.RegisterImportServiceServer(srv, namedImport{fakeLiaison: f, name: name})
	}
	transferv1.RegisterExportServiceServer(srv, f)
	schemav1.RegisterSchemaBarrierServiceServer(srv, f)
	databasev1.RegisterGroupRegistryServiceServer(srv, fakeGroupRegistry{f: f})
	databasev1.RegisterStreamRegistryServiceServer(srv, fakeStreamRegistry{f: f})
	databasev1.RegisterMeasureRegistryServiceServer(srv, fakeMeasureRegistry{f: f})
	databasev1.RegisterTraceRegistryServiceServer(srv, fakeTraceRegistry{f: f})
	databasev1.RegisterPropertyRegistryServiceServer(srv, fakePropertyRegistry{f: f})
	databasev1.RegisterIndexRuleRegistryServiceServer(srv, fakeIndexRuleRegistry{f: f})
	databasev1.RegisterIndexRuleBindingRegistryServiceServer(srv, fakeBindingRegistry{f: f})
	databasev1.RegisterTopNAggregationRegistryServiceServer(srv, fakeTopNRegistry{f: f})
	//panicdiag:allow-rawgo test server
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &cluster.Liaison{Addr: lis.Addr().String(), Conn: conn}
}

// clusterOf is the preflight answer for the fake's data nodes.
func clusterOf(f *fakeLiaison, addr string) *cluster.ClusterInfo {
	info := &cluster.ClusterInfo{Liaison: addr}
	for _, n := range f.nodes {
		info.DataNodes = append(info.DataNodes, &databasev1.Node{Metadata: &commonv1.Metadata{Name: n}, Roles: []databasev1.Role{databasev1.Role_ROLE_DATA}})
	}
	return info
}

// batchRows is the number of rows a batch carries, whatever its catalog.
func batchRows(b *transferv1.ImportRowsRequest_RowBatch) int {
	return len(b.GetStream().GetRows()) + len(b.GetMeasure().GetRows()) + len(b.GetTrace().GetRows()) + len(b.GetProperty().GetRows())
}
