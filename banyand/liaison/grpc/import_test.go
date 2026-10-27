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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"go.uber.org/multierr"
	"golang.org/x/time/rate"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	propertydb "github.com/apache/skywalking-banyandb/banyand/property/db"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/ratelimit"
)

const (
	testStreamGroup   = "sg"
	testMeasureGroup  = "mg"
	testTraceGroup    = "tg"
	testPropertyGroup = "pg"
	testStreamName    = "logs"
	testMeasureName   = "cpm"
	testTraceName     = "segment"
	testPropertyName  = "ui"
	testSelfNode      = "standalone-1"
)

func testRO(shardNum, replicas uint32, stages ...*commonv1.LifecycleStage) *commonv1.ResourceOpts {
	day := &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1}
	return &commonv1.ResourceOpts{ShardNum: shardNum, Replicas: replicas, SegmentInterval: day, Ttl: day, Stages: stages}
}

// propertyRO is the resource options of a property group, which has no segment interval nor TTL.
func propertyRO(shardNum, replicas uint32) *commonv1.ResourceOpts {
	return &commonv1.ResourceOpts{ShardNum: shardNum, Replicas: replicas}
}

func testStage(name, selector string, shardNum, replicas uint32) *commonv1.LifecycleStage {
	day := &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1}
	return &commonv1.LifecycleStage{Name: name, ShardNum: shardNum, Replicas: replicas, SegmentInterval: day, Ttl: day, NodeSelector: selector}
}

func nowMs() time.Time { return time.Now().Truncate(time.Millisecond) }

func labeledNode(name string, labels map[string]string) *databasev1.Node {
	return &databasev1.Node{Metadata: &commonv1.Metadata{Name: name}, Labels: labels}
}

func hotLabel() map[string]string  { return map[string]string{"type": "hot"} }
func warmLabel() map[string]string { return map[string]string{"type": "warm"} }

func wantCode(t *testing.T, err error, code codes.Code, contains string) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("got error %v, want code %s", err, code)
	}
	if contains != "" && !strings.Contains(status.Convert(err).Message(), contains) {
		t.Fatalf("error message %q does not contain %q", status.Convert(err).Message(), contains)
	}
}

// wantErrorInfo asserts that err carries the import ErrorInfo detail of reason and metadata.
func wantErrorInfo(t *testing.T, err error, reason string, metadata map[string]string) {
	t.Helper()
	for _, d := range status.Convert(err).Details() {
		info, ok := d.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}
		if info.GetDomain() != transfer.ImportErrorDomain || info.GetReason() != reason {
			t.Fatalf("error info = %v, want domain %s and reason %s", info, transfer.ImportErrorDomain, reason)
		}
		if !maps.Equal(info.GetMetadata(), metadata) {
			t.Fatalf("error info metadata = %v, want %v", info.GetMetadata(), metadata)
		}
		return
	}
	t.Fatalf("error %v carries no ErrorInfo detail", err)
}

// routeOf routes group g of options ro on a liaison started with --data-node-selector
// liaisonSel, the registered nodes and no other group.
func routeOf(t *testing.T, ro *commonv1.ResourceOpts, stage, liaisonSel string, registered []*databasev1.Node) (*importRoute, error) {
	t.Helper()
	g := testGroup("g", commonv1.Catalog_CATALOG_STREAM, ro)
	svc := newImportService(importDeps{
		listGroups: func(context.Context) ([]*commonv1.Group, error) { return []*commonv1.Group{g}, nil },
		routeTable: fakeLabeledRouteTable{nodes: registered},
		dataNodes: map[commonv1.Catalog]NodeRegistry{
			commonv1.Catalog_CATALOG_STREAM:  testDataNodes(t, liaisonSel, registered, g),
			commonv1.Catalog_CATALOG_MEASURE: nil, commonv1.Catalog_CATALOG_TRACE: nil, commonv1.Catalog_CATALOG_PROPERTY: nil,
		},
	}, logger.GetLogger("import-test"))
	return svc.stageRoute(context.Background(), g, stage)
}

func TestStageRoute(t *testing.T) {
	mixed := []*databasev1.Node{
		labeledNode("hot-b", hotLabel()), labeledNode("hot-a", hotLabel()),
		labeledNode("warm-a", warmLabel()), labeledNode("warm-b", warmLabel()), labeledNode("bare", nil),
	}
	tests := []struct {
		ro           *commonv1.ResourceOpts
		name         string
		stage        string
		liaisonSel   string
		wantStage    string
		wantErr      string
		registered   []*databasev1.Node
		wantShards   [][]string
		wantShardNum uint32
		wantCopies   uint32
	}{
		{
			name:       "default stage without stages",
			ro:         testRO(3, 1),
			registered: []*databasev1.Node{labeledNode("c", nil), labeledNode("a", nil), labeledNode("b", nil)},
			wantStage:  "hot", wantShardNum: 3, wantCopies: 2,
			wantShards: [][]string{{"a", "b"}, {"b", "c"}, {"a", "c"}},
		},
		{
			name:       "named stage matched case-insensitively takes the nodes of its selector",
			ro:         testRO(2, 0, testStage("warm", "type=warm", 2, 1)),
			stage:      "WARM",
			registered: mixed,
			wantStage:  "warm", wantShardNum: 2, wantCopies: 2,
			wantShards: [][]string{{"warm-a", "warm-b"}, {"warm-a", "warm-b"}},
		},
		{
			name:       "named stage with more shards than the group is refused like a lifecycle migration",
			ro:         testRO(1, 0, testStage("warm", "type=warm", 4, 1)),
			stage:      "warm",
			registered: mixed,
			wantErr:    "unknown shard",
		},
		{
			name:       "default stage takes every node of a liaison without a selector",
			ro:         testRO(2, 0, testStage("warm", "type=warm", 2, 1)),
			registered: mixed,
			wantStage:  "hot", wantShardNum: 2, wantCopies: 1,
			wantShards: [][]string{{"bare"}, {"hot-a"}},
		},
		{
			name:       "liaison selector narrows the default stage",
			ro:         testRO(2, 0, testStage("warm", "type=warm", 2, 1)),
			liaisonSel: "type=hot",
			stage:      "hot",
			registered: mixed,
			wantStage:  "hot", wantShardNum: 2, wantCopies: 1,
			wantShards: [][]string{{"hot-a"}, {"hot-b"}},
		},
		{
			name:       "explicit hot stage follows the online placement with the group's shard number",
			ro:         testRO(1, 0, testStage("Hot", "type=hot", 3, 0), testStage("warm", "type=warm", 1, 1)),
			liaisonSel: "type=hot",
			registered: mixed,
			wantStage:  "hot", wantShardNum: 1, wantCopies: 1,
			wantShards: [][]string{{"hot-a"}},
		},
		{
			name:       "unknown stage falls back to hot",
			ro:         testRO(1, 0, testStage("warm", "type=warm", 1, 1)),
			stage:      "cold",
			registered: mixed,
			wantStage:  "hot", wantShardNum: 1, wantCopies: 1,
			wantShards: [][]string{{"bare"}},
		},
		{
			name:       "no matching node",
			ro:         testRO(1, 0, testStage("warm", "type=warm", 1, 1)),
			stage:      "warm",
			registered: []*databasev1.Node{labeledNode("hot-a", hotLabel())},
			wantErr:    "no nodes available",
		},
		{
			name:       "fewer nodes than copies writes to every available node",
			ro:         testRO(2, 2),
			registered: []*databasev1.Node{labeledNode("b", nil), labeledNode("a", nil)},
			wantStage:  "hot", wantShardNum: 2, wantCopies: 3,
			wantShards: [][]string{{"a", "b"}, {"a", "b"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, err := routeOf(t, tc.ro, tc.stage, tc.liaisonSel, tc.registered)
			if tc.wantErr != "" {
				wantCode(t, err, codes.FailedPrecondition, tc.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("stageRoute() = %v", err)
			}
			if r.stage != tc.wantStage || r.shardNum != tc.wantShardNum || r.copies != tc.wantCopies {
				t.Fatalf("route = stage %q shardNum %d copies %d, want %q %d %d",
					r.stage, r.shardNum, r.copies, tc.wantStage, tc.wantShardNum, tc.wantCopies)
			}
			if !slices.EqualFunc(r.shards, tc.wantShards, slices.Equal[[]string]) {
				t.Fatalf("shards = %v, want %v", r.shards, tc.wantShards)
			}
		})
	}
}

func TestStageRoute_HotFollowsTheWriteQueuePlacement(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	g := testGroups()[testMeasureGroup]
	r, err := f.svc.routeFor(context.Background(), g, "")
	if err != nil {
		t.Fatal(err)
	}
	queue := f.svc.dataNodes[commonv1.Catalog_CATALOG_MEASURE]
	for sh := range r.shardNum {
		want, err := queue.LocateAll(testMeasureGroup, sh, int(r.copies))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(r.targets(sh), want) {
			t.Fatalf("shard %d goes to %v, want %v where the write queue syncs online writes", sh, r.targets(sh), want)
		}
	}
}

func TestStandaloneRouteFallsBackForNamedStage(t *testing.T) {
	r, err := standaloneRoute("g", testRO(2, 1, testStage("warm", "type=warm", 4, 0)), testSelfNode)
	if err != nil {
		t.Fatal(err)
	}
	if r.stage != "hot" || r.copies != 1 || r.shardNum != 2 {
		t.Fatalf("standalone route = %+v, want hot fallback with 2 shards and 1 copy", r)
	}
	if !slices.EqualFunc(r.shards, [][]string{{testSelfNode}, {testSelfNode}}, slices.Equal[[]string]) {
		t.Fatalf("shards = %v, want every shard on %s", r.shards, testSelfNode)
	}
	_, err = standaloneRoute("g", testRO(2, 1), "")
	wantCode(t, err, codes.Unavailable, "node id")
	wantErrorInfo(t, err, transfer.ImportReasonNotWritten, nil)
}

// fakeImportClient records what the import service publishes and answers with configured
// failures.
type fakeImportClient struct {
	queue.Client
	publishErr map[string]error
	closeErr   map[string]*common.Error
	// onClose, when set, runs when the batch publisher of a node is closed and may fail it.
	onClose   func(node string, payloads []any) *common.Error
	published []importPublished
	// inits are the init frames sent, and timeouts the timeouts of the batch publishers opened.
	inits    []importPublished
	timeouts []time.Duration
	// frames lists, per node batch in the order they were closed, the frames it carried.
	frames []nodeFrames
	mu     sync.Mutex
}

// nodeFrames are the frames one batch publisher sent to one node, in order.
type nodeFrames struct {
	node   string
	frames []*transferv1.ImportWrite
}

type importPublished struct {
	payload any
	node    string
	topic   bus.Topic
}

// record records the row a frame carries and returns it; nil for an init frame, which is
// recorded among the inits.
func (c *fakeImportClient) record(topic bus.Topic, node string, payload any) any {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := payload.(*transferv1.ImportWrite)
	if w.GetInit() != nil {
		c.inits = append(c.inits, importPublished{topic: topic, node: node, payload: w.GetInit()})
	}
	var row any
	switch r := w.GetRow().(type) {
	case *transferv1.ImportWrite_Measure:
		row = r.Measure
	case *transferv1.ImportWrite_Stream:
		row = r.Stream
	case *transferv1.ImportWrite_Trace:
		row = r.Trace
	case *transferv1.ImportWrite_Property:
		row = r.Property
	default:
		return nil
	}
	c.published = append(c.published, importPublished{topic: topic, node: node, payload: row})
	return row
}

func (c *fakeImportClient) byNode() map[string][]importPublished {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string][]importPublished)
	for _, p := range c.published {
		out[p.node] = append(out[p.node], p)
	}
	return out
}

func (c *fakeImportClient) NewBatchPublisher(timeout time.Duration) queue.BatchPublisher {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timeouts = append(c.timeouts, timeout)
	return &fakeImportBatch{c: c, nodes: make(map[string][]any), frames: make(map[string][]*transferv1.ImportWrite)}
}

type fakeImportBatch struct {
	ctx    context.Context
	c      *fakeImportClient
	nodes  map[string][]any
	frames map[string][]*transferv1.ImportWrite
}

func (b *fakeImportBatch) Publish(ctx context.Context, topic bus.Topic, msgs ...bus.Message) (bus.Future, error) {
	// Like pub, a batch whose context is done is not sent.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.ctx = ctx
	for _, m := range msgs {
		if !m.BatchModeEnabled() {
			return nil, errors.New("not a batch message")
		}
		if err := b.c.publishErr[m.Node()]; err != nil {
			return nil, err
		}
		b.frames[m.Node()] = append(b.frames[m.Node()], m.Data().(*transferv1.ImportWrite))
		if row := b.c.record(topic, m.Node(), m.Data()); row != nil {
			b.nodes[m.Node()] = append(b.nodes[m.Node()], row)
		}
	}
	return nil, nil
}

func (b *fakeImportBatch) Close() (map[string]*common.Error, error) {
	b.c.mu.Lock()
	for n, frames := range b.frames {
		b.c.frames = append(b.c.frames, nodeFrames{node: n, frames: frames})
	}
	b.c.mu.Unlock()
	cee := make(map[string]*common.Error)
	for n, payloads := range b.nodes {
		if ce := b.c.closeErr[n]; ce != nil {
			cee[n] = ce
		}
		if b.c.onClose != nil {
			if ce := b.c.onClose(n, payloads); ce != nil {
				cee[n] = ce
			}
		}
	}
	// Like pub, a batch whose stream context is done while the nodes answer is not accepted.
	if b.ctx != nil && b.ctx.Err() != nil {
		return cee, b.ctx.Err()
	}
	return cee, nil
}

type fakePropertyNodes struct{ nodes []string }

func (f fakePropertyNodes) Locate(_, _ string, _, replicaID uint32) (string, error) {
	return f.nodes[int(replicaID)%len(f.nodes)], nil
}

// LocateAll places shard s on the nodes from s on, so that every shard has its own nodes.
func (f fakePropertyNodes) LocateAll(_ string, shard uint32, replicas int) ([]string, error) {
	out := make([]string, min(replicas, len(f.nodes)))
	for r := range out {
		out[r] = f.nodes[(int(shard)+r)%len(f.nodes)]
	}
	return out, nil
}

func (f fakePropertyNodes) String() string { return "fake" }

// fakeLabeledRouteTable registers nodes; every one of them is active except those in inactive.
type fakeLabeledRouteTable struct {
	inactive map[string]bool
	nodes    []*databasev1.Node
}

func (f fakeLabeledRouteTable) GetRouteTable() *databasev1.RouteTable {
	rt := &databasev1.RouteTable{Registered: f.nodes}
	for _, n := range f.nodes {
		if !f.inactive[n.GetMetadata().GetName()] {
			rt.Active = append(rt.Active, n.GetMetadata().GetName())
		}
	}
	return rt
}

func testGroup(name string, catalog commonv1.Catalog, ro *commonv1.ResourceOpts) *commonv1.Group {
	return &commonv1.Group{Metadata: &commonv1.Metadata{Name: name}, Catalog: catalog, ResourceOpts: ro}
}

func testGroups() map[string]*commonv1.Group {
	return map[string]*commonv1.Group{
		testStreamGroup:   testGroup(testStreamGroup, commonv1.Catalog_CATALOG_STREAM, testRO(3, 1, testStage("warm", "type=warm", 2, 0))),
		testMeasureGroup:  testGroup(testMeasureGroup, commonv1.Catalog_CATALOG_MEASURE, testRO(2, 1)),
		testTraceGroup:    testGroup(testTraceGroup, commonv1.Catalog_CATALOG_TRACE, testRO(2, 0)),
		testPropertyGroup: testGroup(testPropertyGroup, commonv1.Catalog_CATALOG_PROPERTY, propertyRO(2, 1)),
	}
}

func strTag(v string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

func tsTag(ts time.Time) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(ts)}}
}

func nullTag() *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
}

type importFixture struct {
	svc    *importService
	client *fakeImportClient
}

func newImportFixture(t testing.TB, nodes []*databasev1.Node) *importFixture {
	t.Helper()
	l := logger.GetLogger("import-test")
	gr := &groupRepo{resourceOpts: make(map[string]*commonv1.ResourceOpts), inflight: make(map[string]*groupInflight), log: l}
	streams := newDiscoveryService(schema.KindStream, nil, nil, gr)
	measures := newDiscoveryService(schema.KindMeasure, nil, nil, gr)
	traces := newDiscoveryService(schema.KindTrace, nil, nil, gr)
	for _, ds := range []*discoveryService{streams, measures, traces} {
		ds.SetLogger(l)
	}
	family := []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
		{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "ep", Type: databasev1.TagType_TAG_TYPE_STRING},
	}}}
	streams.entityRepo.OnAddOrUpdate(schema.Metadata{TypeMeta: schema.TypeMeta{Kind: schema.KindStream}, Spec: &databasev1.Stream{
		Metadata:    &commonv1.Metadata{Group: testStreamGroup, Name: testStreamName},
		TagFamilies: family,
		Entity:      &databasev1.Entity{TagNames: []string{"svc"}},
	}})
	measure := &databasev1.Measure{
		Metadata:    &commonv1.Metadata{Group: testMeasureGroup, Name: testMeasureName},
		TagFamilies: family,
		Entity:      &databasev1.Entity{TagNames: []string{"svc", "ep"}},
		ShardingKey: &databasev1.ShardingKey{TagNames: []string{"svc"}},
	}
	measures.entityRepo.OnAddOrUpdate(schema.Metadata{TypeMeta: schema.TypeMeta{Kind: schema.KindMeasure}, Spec: measure})
	measures.shardingKeyRepo.OnAddOrUpdate(schema.Metadata{TypeMeta: schema.TypeMeta{Kind: schema.KindMeasure}, Spec: measure})
	traces.entityRepo.OnAddOrUpdate(schema.Metadata{TypeMeta: schema.TypeMeta{Kind: schema.KindTrace}, Spec: &databasev1.Trace{
		Metadata: &commonv1.Metadata{Group: testTraceGroup, Name: testTraceName},
		Tags: []*databasev1.TraceTagSpec{
			{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "ts", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
			{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
		},
		TraceIdTagName:   "trace_id",
		SpanIdTagName:    "span_id",
		TimestampTagName: "ts",
	}})
	groups := testGroups()
	client := &fakeImportClient{publishErr: map[string]error{}, closeErr: map[string]*common.Error{}}
	deps := importDeps{
		groups: func(_ context.Context, name string) (*commonv1.Group, error) {
			if g, ok := groups[name]; ok {
				return g, nil
			}
			return nil, schema.ErrGRPCResourceNotFound
		},
		properties: func(_ context.Context, md *commonv1.Metadata) (*databasev1.Property, error) {
			if md.GetGroup() != testPropertyGroup || md.GetName() != testPropertyName {
				return nil, schema.ErrGRPCResourceNotFound
			}
			return &databasev1.Property{
				Metadata: &commonv1.Metadata{Group: testPropertyGroup, Name: testPropertyName},
				Tags:     []*databasev1.TagSpec{{Name: "k", Type: databasev1.TagType_TAG_TYPE_STRING}},
			}, nil
		},
		selfNode:  func() string { return testSelfNode },
		tire2:     client,
		groupRepo: gr,
		streams:   streams,
		measures:  measures,
		traces:    traces,
	}
	deps.listGroups = func(context.Context) ([]*commonv1.Group, error) { return slices.Collect(maps.Values(groups)), nil }
	var dataNodes NodeRegistry
	if nodes != nil {
		deps.routeTable = fakeLabeledRouteTable{nodes: nodes}
		dataNodes = testDataNodes(t, "type=hot", nodes, slices.Collect(maps.Values(groups))...)
	}
	deps.dataNodes = map[commonv1.Catalog]NodeRegistry{
		commonv1.Catalog_CATALOG_STREAM: dataNodes, commonv1.Catalog_CATALOG_MEASURE: dataNodes, commonv1.Catalog_CATALOG_TRACE: dataNodes,
		commonv1.Catalog_CATALOG_PROPERTY: fakePropertyNodes{nodes: []string{"p1", "p2", "p3"}},
	}
	return &importFixture{svc: newImportService(deps, l), client: client}
}

// testDataNodes is the data node selector of a liaison started with --data-node-selector
// liaisonSel ("" selects every node), knowing nodes and groups.
func testDataNodes(t testing.TB, liaisonSel string, nodes []*databasev1.Node, groups ...*commonv1.Group) NodeRegistry {
	t.Helper()
	sel := node.NewRoundRobinSelector("", nil)
	if liaisonSel != "" {
		ls, err := pub.ParseLabelSelector(liaisonSel)
		if err != nil {
			t.Fatal(err)
		}
		sel.SetNodeSelector(ls)
	}
	for _, g := range groups {
		sel.(schema.EventHandler).OnAddOrUpdate(schema.Metadata{TypeMeta: schema.TypeMeta{Kind: schema.KindGroup}, Spec: g})
	}
	for _, n := range nodes {
		sel.AddNode(n)
	}
	return selectorRegistry(sel)
}

func clusterNodes() []*databasev1.Node {
	return []*databasev1.Node{
		labeledNode("hot-a", hotLabel()), labeledNode("hot-b", hotLabel()), labeledNode("hot-c", hotLabel()),
		labeledNode("warm-a", warmLabel()),
	}
}

// fakeImportStream feeds frames to ImportRows and records its answers.
type fakeImportStream struct {
	grpclib.ServerStream
	ctx    context.Context
	onRecv func(i int)
	frames []*transferv1.ImportRowsRequest
	sent   []*transferv1.ImportRowsResponse
	next   int
}

func (f *fakeImportStream) Context() context.Context { return f.ctx }

func (f *fakeImportStream) Recv() (*transferv1.ImportRowsRequest, error) {
	if f.onRecv != nil {
		f.onRecv(f.next)
	}
	if f.next >= len(f.frames) {
		return nil, io.EOF
	}
	f.next++
	return f.frames[f.next-1], nil
}

func (f *fakeImportStream) Send(r *transferv1.ImportRowsResponse) error {
	f.sent = append(f.sent, r)
	return nil
}

func initFrame(catalog commonv1.Catalog, group, stage string) *transferv1.ImportRowsRequest {
	return &transferv1.ImportRowsRequest{Frame: &transferv1.ImportRowsRequest_Init_{Init: &transferv1.ImportRowsRequest_Init{
		Catalog: catalog, Group: group, Stage: stage,
	}}}
}

func batchFrame(b *transferv1.ImportRowsRequest_RowBatch) *transferv1.ImportRowsRequest {
	return &transferv1.ImportRowsRequest{Frame: &transferv1.ImportRowsRequest_Batch{Batch: b}}
}

func runImport(f *importFixture, frames ...*transferv1.ImportRowsRequest) (*fakeImportStream, error) {
	s := &fakeImportStream{ctx: context.Background(), frames: frames}
	return s, f.svc.ImportRows(s)
}

func streamRow(svc string, rawID uint64) *transferv1.ImportRowsRequest_StreamRow {
	return &transferv1.ImportRowsRequest_StreamRow{
		Name: testStreamName,
		Element: &streamv1.ElementValue{
			ElementId:   "00000000000000aa",
			Timestamp:   timestamppb.New(nowMs()),
			TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{strTag(svc), strTag("/ep")}}},
		},
		RawElementId: rawID,
	}
}

func measureRow(version int64) *transferv1.ImportRowsRequest_MeasureRow {
	return &transferv1.ImportRowsRequest_MeasureRow{
		Name: testMeasureName,
		Point: &measurev1.DataPointValue{
			Timestamp:   timestamppb.New(nowMs()),
			TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{strTag("svc"), strTag("/ep")}}},
			Version:     version,
		},
	}
}

func importTraceRow(tags ...*modelv1.TagValue) *transferv1.ImportRowsRequest_TraceRow {
	return &transferv1.ImportRowsRequest_TraceRow{Name: testTraceName, Tags: tags, Span: []byte("span")}
}

func TestImportRows_InitValidation(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	_, err := runImport(f, batchFrame(&transferv1.ImportRowsRequest_RowBatch{}))
	wantCode(t, err, codes.InvalidArgument, "first frame")

	_, err = runImport(f, initFrame(commonv1.Catalog_CATALOG_UNSPECIFIED, testStreamGroup, ""))
	wantCode(t, err, codes.InvalidArgument, "")

	_, err = runImport(f, initFrame(commonv1.Catalog_CATALOG_STREAM, "missing", ""))
	wantCode(t, err, codes.NotFound, "group missing not found")

	_, err = runImport(f, initFrame(commonv1.Catalog_CATALOG_MEASURE, testStreamGroup, ""))
	wantCode(t, err, codes.InvalidArgument, "catalog")

	_, err = runImport(f, initFrame(commonv1.Catalog_CATALOG_STREAM, testStreamGroup, ""), initFrame(commonv1.Catalog_CATALOG_STREAM, testStreamGroup, ""))
	wantCode(t, err, codes.InvalidArgument, "only as the first frame")
}

func TestImportRows_StreamFansOutWithMetadataAndRawElementID(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	rows := []*transferv1.ImportRowsRequest_StreamRow{streamRow("svc-1", 11), streamRow("svc-2", 22), streamRow("svc-3", 33)}
	s, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_STREAM, testStreamGroup, "hot"),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 0, Rows: streamRows(rows...)}),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 1}))
	if err != nil {
		t.Fatalf("ImportRows() = %v", err)
	}
	if got := ackSeqs(s.sent); !slices.Equal(got, []uint64{0, 1}) {
		t.Fatalf("acks = %v, want [0 1]", got)
	}
	route, err := f.svc.routeFor(context.Background(), testGroups()[testStreamGroup], "hot")
	if err != nil {
		t.Fatal(err)
	}
	byNode := f.client.byNode()
	if _, ok := byNode["warm-a"]; ok {
		t.Fatal("a hot stream row reached the warm node")
	}
	var rawIDs []uint64
	for node, msgs := range byNode {
		for _, p := range msgs {
			if p.topic != data.TopicStreamImport {
				t.Fatalf("topic = %s, want %s", p.topic, data.TopicStreamImport)
			}
			iwr := p.payload.(*streamv1.InternalWriteRequest)
			rawIDs = append(rawIDs, iwr.GetRawElementId())
			md := iwr.GetRequest().GetMetadata()
			if md.GetGroup() != testStreamGroup || md.GetName() != testStreamName {
				t.Fatalf("message metadata = %v, want %s/%s on every message", md, testStreamGroup, testStreamName)
			}
			if iwr.GetRequest().GetTagFamilySpec() != nil {
				t.Fatal("an import message carries a tag family spec")
			}
			if iwr.GetRawElementId() == 0 || len(iwr.GetEntityValues()) != 1 {
				t.Fatalf("message = %v, want the raw element id and one entity value", iwr)
			}
			if !slices.Contains(route.targets(iwr.GetShardId()), node) {
				t.Fatalf("shard %d went to %s, want one of %v", iwr.GetShardId(), node, route.targets(iwr.GetShardId()))
			}
		}
	}
	if len(rawIDs) != 6 {
		t.Fatalf("published %d messages, want 3 rows x 2 copies", len(rawIDs))
	}
	for _, id := range []uint64{11, 22, 33} {
		if !slices.Contains(rawIDs, id) {
			t.Fatalf("raw element ids %v miss %d", rawIDs, id)
		}
	}
}

func TestImportRows_StreamStageRoutingUsesStageShardNum(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	_, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_STREAM, testStreamGroup, "Warm"),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{Rows: streamRows(streamRow("svc-1", 1))}))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.client.published) != 1 || f.client.published[0].node != "warm-a" {
		t.Fatalf("published = %v, want one message on warm-a", f.client.published)
	}
	_, want, err := partition.ApplyLocators(testStreamName,
		streamRow("svc-1", 1).GetElement().GetTagFamilies(), mustLocator(t, f.svc.streams, testStreamGroup, testStreamName), nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.client.published[0].payload.(*streamv1.InternalWriteRequest).GetShardId(); got != uint32(want) {
		t.Fatalf("shard = %d, want %d computed with the warm shard number", got, want)
	}
}

func mustLocator(t *testing.T, ds *discoveryService, group, name string) partition.Locator {
	t.Helper()
	l, ok := ds.entityRepo.getLocator(identity{group: group, name: name})
	if !ok {
		t.Fatalf("no locator for %s/%s", group, name)
	}
	return l
}

func TestImportRows_MeasureVersionZeroIsRejected(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	_, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_MEASURE, testMeasureGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 5, Rows: measureRows(measureRow(0))}))
	wantCode(t, err, codes.InvalidArgument, "batch_seq=5: row 0: the version of measure")
	wantErrorInfo(t, err, transfer.ImportReasonRowInvalid,
		map[string]string{transfer.ImportMetadataBatchSeq: "5", transfer.ImportMetadataRow: "0"})
}

func TestImportRows_MeasureKeepsVersion(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	_, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_MEASURE, testMeasureGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{Rows: measureRows(measureRow(-7))}))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.client.published) != 2 {
		t.Fatalf("published %d messages, want 2 copies", len(f.client.published))
	}
	for _, p := range f.client.published {
		// A cluster import sends plain requests: data nodes never compute Top-N.
		iwr, ok := p.payload.(*measurev1.InternalWriteRequest)
		if !ok {
			t.Fatalf("payload = %T, want *measurev1.InternalWriteRequest", p.payload)
		}
		if iwr.GetRequest().GetDataPoint().GetVersion() != -7 || iwr.GetRequest().GetMetadata().GetName() != testMeasureName {
			t.Fatalf("message = %v, want version -7 and metadata", iwr)
		}
		if p.topic != data.TopicMeasureImport {
			t.Fatalf("topic = %s, want %s", p.topic, data.TopicMeasureImport)
		}
	}
}

func TestImportRows_TraceValidation(t *testing.T) {
	now := nowMs()
	tests := []struct {
		name string
		row  *transferv1.ImportRowsRequest_TraceRow
		want string
	}{
		{name: "tag count", row: importTraceRow(strTag("t"), strTag("s"), tsTag(now)), want: "has 4 tags"},
		{name: "empty trace id", row: importTraceRow(strTag(""), strTag("s"), tsTag(now), strTag("svc")), want: "trace id tag"},
		{name: "null span id", row: importTraceRow(strTag("t"), nullTag(), tsTag(now), strTag("svc")), want: "span id tag"},
		{name: "null timestamp", row: importTraceRow(strTag("t"), strTag("s"), nullTag(), strTag("svc")), want: "must not be null"},
		{name: "invalid timestamp", row: importTraceRow(strTag("t"), strTag("s"), tsTag(now.Add(time.Nanosecond)), strTag("svc")), want: "invalid timestamp"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newImportFixture(t, clusterNodes())
			_, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_TRACE, testTraceGroup, ""),
				batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 9, Rows: traceRows(tc.row)}))
			wantCode(t, err, codes.InvalidArgument, "batch_seq=9")
			wantCode(t, err, codes.InvalidArgument, tc.want)
			wantErrorInfo(t, err, transfer.ImportReasonRowInvalid,
				map[string]string{transfer.ImportMetadataBatchSeq: "9", transfer.ImportMetadataRow: "0"})
		})
	}
}

func TestImportRows_TraceWritesVersionOneOnTraceShard(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	_, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_TRACE, testTraceGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{Rows: traceRows(
			importTraceRow(strTag("trace-1"), strTag("span-1"), tsTag(nowMs()), strTag("svc")),
		)}))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.client.published) != 1 {
		t.Fatalf("published %d messages, want 1", len(f.client.published))
	}
	iwr := f.client.published[0].payload.(*tracev1.InternalWriteRequest)
	if iwr.GetRequest().GetVersion() != 1 || iwr.GetRequest().GetMetadata().GetName() != testTraceName || string(iwr.GetRequest().GetSpan()) != "span" {
		t.Fatalf("message = %v, want version 1, metadata and span", iwr)
	}
	if want := uint32(partition.TraceShardID("trace-1", 2)); iwr.GetShardId() != want {
		t.Fatalf("shard = %d, want %d", iwr.GetShardId(), want)
	}
}

func TestImportRows_NodeFailureMapping(t *testing.T) {
	tests := []struct {
		setup func(c *fakeImportClient)
		name  string
		want  codes.Code
	}{
		{
			name: "disk full",
			setup: func(c *fakeImportClient) {
				c.closeErr["hot-b"] = common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "disk full")
			},
			want: codes.ResourceExhausted,
		},
		{
			name:  "node rejects the batch",
			setup: func(c *fakeImportClient) { c.closeErr["hot-b"] = common.NewError("boom") },
			want:  codes.Internal,
		},
		{
			name:  "node unreachable",
			setup: func(c *fakeImportClient) { c.publishErr["hot-b"] = errors.New("failed to get client for node hot-b") },
			want:  codes.Unavailable,
		},
		{
			name: "health check fails before the send",
			setup: func(c *fakeImportClient) {
				c.publishErr["hot-b"] = multierr.Append(nil,
					common.NewErrorWithStatus(modelv1.Status_STATUS_INTERNAL_ERROR, "rpc error: code = Unavailable desc = connection refused"))
			},
			want: codes.Unavailable,
		},
		{
			name: "disk full reported by the health check",
			setup: func(c *fakeImportClient) {
				c.publishErr["hot-b"] = common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "disk full")
			},
			want: codes.ResourceExhausted,
		},
		{
			name: "connection breaks while the node answers",
			setup: func(c *fakeImportClient) {
				c.closeErr["hot-b"] = common.NewErrorWithStatus(modelv1.Status_STATUS_INTERNAL_ERROR, "rpc error: code = Unavailable desc = transport is closing")
			},
			want: codes.Unavailable,
		},
		{
			name: "node answers with an internal error",
			setup: func(c *fakeImportClient) {
				c.closeErr["hot-b"] = common.NewErrorWithStatus(modelv1.Status_STATUS_INTERNAL_ERROR, "write failed")
			},
			want: codes.Internal,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newImportFixture(t, clusterNodes()[:2])
			tc.setup(f.client)
			s, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_MEASURE, testMeasureGroup, ""),
				batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 7, Rows: measureRows(measureRow(1))}))
			wantCode(t, err, tc.want, "batch_seq=7 node=hot-b: ")
			wantErrorInfo(t, err, transfer.ImportReasonNodeFailed,
				map[string]string{transfer.ImportMetadataBatchSeq: "7", transfer.ImportMetadataNode: "hot-b"})
			if len(s.sent) != 0 {
				t.Fatalf("acks = %v, want none for a failed batch", s.sent)
			}
		})
	}
}

func TestImportRows_StandaloneWritesLocally(t *testing.T) {
	f := newImportFixture(t, nil)
	_, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_STREAM, testStreamGroup, "warm"),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{Rows: streamRows(streamRow("svc", 1), streamRow("svc-2", 2))}))
	if err != nil {
		t.Fatal(err)
	}
	if byNode := f.client.byNode(); len(byNode) != 1 || len(byNode[testSelfNode]) != 2 {
		t.Fatalf("published = %v, want both rows on %s", f.client.published, testSelfNode)
	}
	resp := targetRoute(t, f, testStreamGroup, "")
	for _, sh := range resp.GetShards() {
		if !slices.Equal(sh.GetNodeIds(), []string{testSelfNode}) {
			t.Fatalf("standalone shard %v, want it on the process's own node id %s", sh, testSelfNode)
		}
	}
}

func propertyRow(id string, rev int64, deleteTime int64) *transferv1.ImportRowsRequest_PropertyRow {
	return &transferv1.ImportRowsRequest_PropertyRow{
		Property: &propertyv1.Property{
			Metadata: &commonv1.Metadata{Group: testPropertyGroup, Name: testPropertyName, ModRevision: rev},
			Id:       id,
			Tags:     []*modelv1.Tag{{Key: "k", Value: strTag("v")}},
		},
		DeleteTime: deleteTime,
	}
}

func TestImportRows_PropertyGroupMismatchIsRejected(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	row := propertyRow("a", 1, 0)
	row.Property.Metadata.Group = "other"
	_, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_PROPERTY, testPropertyGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 2, Rows: propertyRows(row)}))
	wantCode(t, err, codes.InvalidArgument, "batch_seq=2")
}

func TestImportRows_PropertyRowsKeepBatchOrderOnEveryNode(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	route, err := propertyRoute(testPropertyGroup, testGroups()[testPropertyGroup].GetResourceOpts(), f.svc.dataNodes[commonv1.Catalog_CATALOG_PROPERTY])
	if err != nil {
		t.Fatal(err)
	}
	var rows []*transferv1.ImportRowsRequest_PropertyRow
	for rev := int64(1); rev <= 5; rev++ {
		rows = append(rows, propertyRow("a", rev, rev*10), propertyRow("b", rev, 0))
	}
	s, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_PROPERTY, testPropertyGroup, "warm"),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{Rows: propertyRows(rows...)}))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.sent) != 1 {
		t.Fatalf("acks = %v, want one ack", s.sent)
	}
	if len(f.client.published) != 20 {
		t.Fatalf("published %d repairs, want 10 rows x 2 copies", len(f.client.published))
	}
	// Every node gets the batch once, through a batch publisher: the init frame, then its rows.
	revisions := map[string][]int64{}
	seen := map[string]bool{}
	for _, nf := range f.client.frames {
		if seen[nf.node] {
			t.Fatalf("node %s got more than one batch for one import batch", nf.node)
		}
		seen[nf.node] = true
		if nf.frames[0].GetInit() == nil {
			t.Fatalf("the batch of node %s opens with %v, want the init frame", nf.node, nf.frames[0])
		}
		for _, fr := range nf.frames[1:] {
			req := fr.GetProperty()
			if req == nil {
				t.Fatalf("node %s got frame %v, want property rows after the init", nf.node, fr)
			}
			entity := propertydb.GetEntity(req.GetProperty())
			revisions[nf.node+"/"+entity] = append(revisions[nf.node+"/"+entity], req.GetProperty().GetMetadata().GetModRevision())
		}
	}
	for key, revs := range revisions {
		if !slices.Equal(revs, []int64{1, 2, 3, 4, 5}) {
			t.Fatalf("revisions of %s = %v, want batch order", key, revs)
		}
	}
	shards := map[uint64]bool{}
	for _, p := range f.client.published {
		req := p.payload.(*propertyv1.InternalRepairRequest)
		want, shardErr := partition.ShardID(convert.StringToBytes(propertydb.GetEntity(req.GetProperty())), 2)
		if shardErr != nil {
			t.Fatal(shardErr)
		}
		if req.GetShardId() != uint64(want) {
			t.Fatalf("repair of %s on shard %d, want shard %d", propertydb.GetEntity(req.GetProperty()), req.GetShardId(), want)
		}
		if !slices.Contains(route.targets(uint32(want)), p.node) {
			t.Fatalf("shard %d repaired on %s, want one of %v", want, p.node, route.targets(uint32(want)))
		}
		shards[req.GetShardId()] = true
		if p.topic != data.TopicPropertyImport || !bytes.Equal(req.GetId(), propertydb.GetPropertyID(req.GetProperty())) {
			t.Fatalf("repair = %v on %s, want the property id on %s", req, p.topic, data.TopicPropertyImport)
		}
		if req.GetProperty().GetId() == "a" && req.GetDeleteTime() != req.GetProperty().GetMetadata().GetModRevision()*10 {
			t.Fatalf("delete time = %d, want the row's delete time", req.GetDeleteTime())
		}
	}
	if len(shards) != 2 {
		t.Fatalf("repaired shards = %v, want the two entities on both shards", shards)
	}
}

func TestImportRows_PropertyRepairFailure(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	f.client.closeErr["p2"] = common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "disk full")
	s, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_PROPERTY, testPropertyGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 4, Rows: propertyRows(propertyRow("a", 1, 0))}))
	wantCode(t, err, codes.ResourceExhausted, "batch_seq=4 node=p2: ")
	wantErrorInfo(t, err, transfer.ImportReasonNodeFailed,
		map[string]string{transfer.ImportMetadataBatchSeq: "4", transfer.ImportMetadataNode: "p2"})
	if len(s.sent) != 0 {
		t.Fatalf("acks = %v, want none for a failed repair", ackSeqs(s.sent))
	}
}

func TestImportRows_PropertyNodeUnreachable(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	// Entity a lives on shard 1, placed on [p2 p3]; p3 cannot be handed the batch.
	f.client.publishErr["p3"] = errors.New("failed to get client for node p3")
	_, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_PROPERTY, testPropertyGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 5, Rows: propertyRows(
			propertyRow("a", 1, 0), propertyRow("a", 2, 0), propertyRow("b", 1, 0), propertyRow("c", 1, 0),
		)}))
	wantCode(t, err, codes.Unavailable, "batch_seq=5 node=p3: ")
	if _, ok := f.client.byNode()["p3"]; ok {
		t.Fatal("rows reached the node that could not be handed the batch")
	}
}

func TestImportTargets(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	warm := targetRoute(t, f, testStreamGroup, "warm")
	cold := targetRoute(t, f, testStreamGroup, "cold")
	prop := targetRoute(t, f, testPropertyGroup, "")
	if warm.GetStage() != "warm" || warm.GetShardNum() != 2 || warm.GetCopies() != 1 || len(warm.GetShards()) != 2 ||
		!slices.Equal(warm.GetShards()[1].GetNodeIds(), []string{"warm-a"}) || warm.GetShards()[1].GetShardId() != 1 {
		t.Fatalf("warm route = %v", warm)
	}
	hot := targetRoute(t, f, testStreamGroup, "hot")
	if cold.GetStage() != "hot" || cold.GetCopies() != 2 || cold.GetShardNum() != 3 ||
		!slices.EqualFunc(cold.GetShards(), hot.GetShards(), func(a, b *transferv1.TargetsResponse_Shard) bool { return proto.Equal(a, b) }) {
		t.Fatalf("cold route = %v, want the hot route %v", cold, hot)
	}
	if prop.GetShardNum() != 2 || prop.GetCopies() != 2 || !slices.Equal(prop.GetShards()[0].GetNodeIds(), []string{"p1", "p2"}) ||
		!slices.Equal(prop.GetShards()[1].GetNodeIds(), []string{"p2", "p3"}) {
		t.Fatalf("property route = %v", prop)
	}

	_, err := f.svc.Targets(context.Background(), &transferv1.TargetsRequest{Group: "missing"})
	wantCode(t, err, codes.NotFound, "missing")
	_, err = f.svc.Targets(context.Background(), &transferv1.TargetsRequest{})
	wantCode(t, err, codes.InvalidArgument, "")
}

func targetRoute(t *testing.T, f *importFixture, group, stage string) *transferv1.TargetsResponse {
	t.Helper()
	resp, err := f.svc.Targets(context.Background(), &transferv1.TargetsRequest{Group: group, Stage: stage})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestImportTargets_InactiveNodesAndDeletingGroup(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	f.svc.routeTable = fakeLabeledRouteTable{nodes: clusterNodes(), inactive: map[string]bool{"hot-c": true, "hot-b": true, "warm-a": true}}
	f.svc.groupRepo.waitInflightRequests(testMeasureGroup)
	measure := targetRoute(t, f, testMeasureGroup, "")
	if !slices.Equal(measure.GetInactiveNodeIds(), []string{"hot-b", "hot-c"}) || !measure.GetGroupDeleting() {
		t.Fatalf("measure route = %v, want hot-b and hot-c inactive and the group deleting", measure)
	}
	if stream := targetRoute(t, f, testStreamGroup, ""); stream.GetGroupDeleting() {
		t.Fatalf("stream route = %v, want the group not deleting", stream)
	}
	// The write path refuses the deleting group with the same condition.
	s, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_MEASURE, testMeasureGroup, ""), measureBatch(3))
	wantCode(t, err, codes.FailedPrecondition, "batch_seq=3: group mg is pending deletion")
	if len(s.sent) != 0 || len(f.client.published) != 0 {
		t.Fatalf("acks = %v, published = %v, want nothing written to a group pending deletion", ackSeqs(s.sent), f.client.published)
	}
}

func TestImportTargets_LiaisonSelector(t *testing.T) {
	nodes := append(clusterNodes(), labeledNode("bare", nil))
	f := newImportFixture(t, nodes)
	resp := targetRoute(t, f, testMeasureGroup, "")
	for _, sh := range resp.GetShards() {
		if slices.Contains(sh.GetNodeIds(), "bare") || slices.Contains(sh.GetNodeIds(), "warm-a") {
			t.Fatalf("shard %v includes a node outside the liaison selector", sh)
		}
	}
}

func TestImportRows_LimiterAppliesAndPaysEachBatch(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	init := initFrame(commonv1.Catalog_CATALOG_STREAM, testStreamGroup, "")
	init.GetInit().LiaisonBytesPerSecond = 2000
	// Each batch fits in the 2000 byte burst; only paying both of them makes the stream wait.
	frames := []*transferv1.ImportRowsRequest{init}
	for seq := uint64(0); seq < 2; seq++ {
		frames = append(frames, batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: seq, Rows: streamRows(streamRow(strings.Repeat("x", 1100), 1))}))
	}
	var limitDuringStream rate.Limit
	s := &fakeImportStream{
		ctx:    context.Background(),
		frames: frames,
		onRecv: func(i int) {
			if i == 1 {
				limitDuringStream = f.svc.rate.Limit()
			}
		},
	}
	start := time.Now()
	if err := f.svc.ImportRows(s); err != nil {
		t.Fatal(err)
	}
	if limitDuringStream != 2000 {
		t.Fatalf("limit during the stream = %v, want 2000", limitDuringStream)
	}
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("two batches of over 1100 bytes each at 2000 B/s took %v, want every batch paid", elapsed)
	}
	if f.svc.rate.Limit() != rate.Inf {
		t.Fatal("the import limiter stays limited after its last stream ended")
	}
}

func TestImportRows_LimiterHonoursCancel(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	release := f.svc.rate.Open(1000)
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.svc.rate.Pay(ctx, 5000); status.Code(err) != codes.Canceled {
		t.Fatalf("pay() on a canceled context = %v, want Canceled", err)
	}
}

func TestImportRows_SchemaNotFoundDetail(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	_, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_STREAM, "missing", ""))
	wantCode(t, err, codes.NotFound, "group missing not found")
	wantErrorInfo(t, err, transfer.ImportReasonSchemaNotFound, nil)

	row := streamRow("svc", 1)
	row.Name = "absent"
	_, err = runImport(f, initFrame(commonv1.Catalog_CATALOG_STREAM, testStreamGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 4, Rows: streamRows(streamRow("svc", 2), row)}))
	wantCode(t, err, codes.NotFound, "batch_seq=4: stream sg/absent not found")
	wantErrorInfo(t, err, transfer.ImportReasonSchemaNotFound, map[string]string{transfer.ImportMetadataBatchSeq: "4"})

	mrow := measureRow(1)
	mrow.Name = "absent"
	_, err = runImport(f, initFrame(commonv1.Catalog_CATALOG_MEASURE, testMeasureGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 6, Rows: measureRows(mrow)}))
	wantCode(t, err, codes.NotFound, "batch_seq=6: measure mg/absent not found")
	wantErrorInfo(t, err, transfer.ImportReasonSchemaNotFound, map[string]string{transfer.ImportMetadataBatchSeq: "6"})
}

func TestImportRows_RowInvalidNamesTheRow(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	bad := propertyRow("b", 1, -1)
	_, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_PROPERTY, testPropertyGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 8, Rows: propertyRows(propertyRow("a", 1, 0), bad)}))
	wantCode(t, err, codes.InvalidArgument, "batch_seq=8: row 1: delete time must not be negative")
	wantErrorInfo(t, err, transfer.ImportReasonRowInvalid,
		map[string]string{transfer.ImportMetadataBatchSeq: "8", transfer.ImportMetadataRow: "1"})
}

// tooLargeStream answers the init frame and then fails the next Recv as gRPC does for a frame
// over the server's receive message size.
type tooLargeStream struct {
	fakeImportStream
}

func (f *tooLargeStream) Recv() (*transferv1.ImportRowsRequest, error) {
	if f.next >= len(f.frames) {
		return nil, status.Error(codes.ResourceExhausted, "grpc: received message larger than max (9 vs. 8)")
	}
	return f.fakeImportStream.Recv()
}

func TestImportRows_MessageTooLargeDetail(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	s := &tooLargeStream{fakeImportStream{ctx: context.Background(), frames: []*transferv1.ImportRowsRequest{
		initFrame(commonv1.Catalog_CATALOG_STREAM, testStreamGroup, ""),
	}}}
	err := f.svc.ImportRows(s)
	wantCode(t, err, codes.ResourceExhausted, "grpc: received message larger than max (9 vs. 8)")
	wantErrorInfo(t, err, transfer.ImportReasonMessageTooLarge, nil)

	other := status.Error(codes.ResourceExhausted, "too many in-flight requests")
	if got := withTooLargeDetail(other); !errors.Is(got, other) {
		t.Fatalf("withTooLargeDetail(%v) = %v, want it unchanged", other, got)
	}
}

func TestImportRows_RowsOfOneResourceShareMetadata(t *testing.T) {
	f := newImportFixture(t, clusterNodes()[:1])
	now := nowMs()
	_, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_TRACE, testTraceGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{Rows: traceRows(
			importTraceRow(strTag("trace-1"), strTag("span-1"), tsTag(now), strTag("svc")),
			importTraceRow(strTag("trace-2"), strTag("span-2"), tsTag(now), strTag("svc")),
		)}))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.client.published) != 2 {
		t.Fatalf("published %d messages, want 2", len(f.client.published))
	}
	first := f.client.published[0].payload.(*tracev1.InternalWriteRequest).GetRequest().GetMetadata()
	second := f.client.published[1].payload.(*tracev1.InternalWriteRequest).GetRequest().GetMetadata()
	if first != second || first.GetGroup() != testTraceGroup || first.GetName() != testTraceName {
		t.Fatalf("metadata %v and %v, want one shared %s/%s", first, second, testTraceGroup, testTraceName)
	}
}

func TestImportService_RateUnlimitedOnlyWithoutLimitedStream(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	if !f.svc.rate.Unlimited() {
		t.Fatal("the import rate is limited before any stream opened")
	}
	release := f.svc.rate.Open(1000)
	if f.svc.rate.Unlimited() {
		t.Fatal("the import rate is unlimited while a limited stream is open")
	}
	release()
	if !f.svc.rate.Unlimited() {
		t.Fatal("the import rate stays limited after the limited stream ended")
	}
}

// measureBatch is a measure batch whose single row carries its seq as the version, so a fake
// node can tell the batches apart.
func measureBatch(seq uint64) *transferv1.ImportRowsRequest {
	return batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: seq, Rows: measureRows(measureRow(int64(seq)))})
}

// measureFrames is a measure import of n batches, from measureBatch(1) to measureBatch(n).
func measureFrames(n uint64) []*transferv1.ImportRowsRequest {
	frames := []*transferv1.ImportRowsRequest{initFrame(commonv1.Catalog_CATALOG_MEASURE, testMeasureGroup, "")}
	for seq := uint64(1); seq <= n; seq++ {
		frames = append(frames, measureBatch(seq))
	}
	return frames
}

func batchVersion(payloads []any) uint64 {
	return uint64(payloads[0].(*measurev1.InternalWriteRequest).GetRequest().GetDataPoint().GetVersion())
}

func ackSeqs(sent []*transferv1.ImportRowsResponse) []uint64 {
	seqs := make([]uint64, 0, len(sent))
	for _, r := range sent {
		seqs = append(seqs, r.GetBatchSeq())
	}
	return seqs
}

func TestImportRows_PipelinedBatchesAreAckedInOrder(t *testing.T) {
	f := newImportFixture(t, clusterNodes()[:2])
	var laterClosed atomic.Int32
	laterDone := make(chan struct{})
	overtaken := atomic.Bool{}
	f.client.onClose = func(_ string, payloads []any) *common.Error {
		switch batchVersion(payloads) {
		case 1:
			// Batch 1 answers only after batch 2 was fully answered by both nodes.
			select {
			case <-laterDone:
				overtaken.Store(true)
			case <-time.After(5 * time.Second):
			}
		case 2:
			if laterClosed.Add(1) == 2 {
				close(laterDone)
			}
		}
		return nil
	}
	s, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_MEASURE, testMeasureGroup, ""), measureBatch(1), measureBatch(2), measureBatch(3))
	if err != nil {
		t.Fatal(err)
	}
	if !overtaken.Load() {
		t.Fatal("batch 2 was not written while batch 1 was in flight")
	}
	if got := ackSeqs(s.sent); !slices.Equal(got, []uint64{1, 2, 3}) {
		t.Fatalf("acks = %v, want [1 2 3]", got)
	}
}

func TestImportRows_PipelineKeepsAtMostDepthBatchesInFlight(t *testing.T) {
	f := newImportFixture(t, clusterNodes()[:2])
	var (
		mu       sync.Mutex
		inflight = map[uint64]int{}
		peak     int
	)
	f.client.onClose = func(_ string, payloads []any) *common.Error {
		v := batchVersion(payloads)
		mu.Lock()
		inflight[v]++
		peak = max(peak, len(inflight))
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		inflight[v]--
		if inflight[v] == 0 {
			delete(inflight, v)
		}
		mu.Unlock()
		return nil
	}
	s, err := runImport(f, measureFrames(3*importPipelineDepth)...)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.sent) != 3*importPipelineDepth {
		t.Fatalf("acks = %v, want one per batch", ackSeqs(s.sent))
	}
	if peak > importPipelineDepth || peak < 2 {
		t.Fatalf("peak batches in flight = %d, want between 2 and %d", peak, importPipelineDepth)
	}
}

func TestImportRows_PipelineFailureEndsStreamAtTheFailedBatch(t *testing.T) {
	goods := gleak.Goroutines()
	f := newImportFixture(t, clusterNodes()[:2])
	f.client.onClose = func(node string, payloads []any) *common.Error {
		switch batchVersion(payloads) {
		case 1:
			// Batch 1 is still in flight when batch 2 fails; it is acknowledged first.
			time.Sleep(50 * time.Millisecond)
		case 2:
			if node == "hot-b" {
				return common.NewErrorWithStatus(modelv1.Status_STATUS_INTERNAL_ERROR, "write failed")
			}
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &fakeImportStream{
		ctx:    ctx,
		frames: []*transferv1.ImportRowsRequest{initFrame(commonv1.Catalog_CATALOG_MEASURE, testMeasureGroup, ""), measureBatch(1), measureBatch(2), measureBatch(3)},
		onRecv: func(i int) {
			// The client waits for acknowledgements before it sends batch 3, so the handler must
			// end the stream on the failure without receiving again.
			if i == 3 {
				<-ctx.Done()
			}
		},
	}
	err := f.svc.ImportRows(s)
	wantCode(t, err, codes.Internal, "batch_seq=2 node=hot-b: ")
	wantErrorInfo(t, err, transfer.ImportReasonNodeFailed,
		map[string]string{transfer.ImportMetadataBatchSeq: "2", transfer.ImportMetadataNode: "hot-b"})
	if got := ackSeqs(s.sent); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("acks = %v, want only batch 1", got)
	}
	// gRPC cancels the stream once the handler returned, which ends the pending Recv.
	cancel()
	gomega.NewWithT(t).Eventually(gleak.Goroutines, 5*time.Second).ShouldNot(gleak.HaveLeaked(goods))
}

func TestImportPipeline_StartsNoBatchAfterAFailedWrite(t *testing.T) {
	p := newImportPipeline(context.Background(), importPipelineDepth)
	defer p.cancel()
	close(p.failed)
	if p.start(&importStep{done: make(chan struct{})}, nil, measureBatch(5).GetBatch()) {
		t.Fatal("a batch started writing after an earlier write failed")
	}
}

func TestImportRows_PipelineCancelDrainsWrites(t *testing.T) {
	goods := gleak.Goroutines()
	f := newImportFixture(t, clusterNodes()[:2])
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var once sync.Once
	f.client.onClose = func(_ string, payloads []any) *common.Error {
		if batchVersion(payloads) == 1 {
			once.Do(cancel)
		}
		// The nodes answer only once the stream is canceled.
		<-ctx.Done()
		return nil
	}
	s := &fakeImportStream{ctx: ctx, frames: measureFrames(2 * importPipelineDepth)}
	if err := f.svc.ImportRows(s); err == nil {
		t.Fatal("ImportRows on a canceled stream succeeded")
	}
	if len(s.sent) != 0 {
		t.Fatalf("acks = %v, want none on a canceled stream", ackSeqs(s.sent))
	}
	gomega.NewWithT(t).Eventually(gleak.Goroutines, 5*time.Second).ShouldNot(gleak.HaveLeaked(goods))
}

func TestImportRows_PropertyBatchesStaySequential(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	var (
		mu       sync.Mutex
		inflight = map[string]int{}
		overlap  bool
	)
	f.client.onClose = func(_ string, payloads []any) *common.Error {
		id := payloads[0].(*propertyv1.InternalRepairRequest).GetProperty().GetId()
		mu.Lock()
		for other, n := range inflight {
			if other != id && n > 0 {
				overlap = true
			}
		}
		inflight[id]++
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		inflight[id]--
		mu.Unlock()
		return nil
	}
	s, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_PROPERTY, testPropertyGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 1, Rows: propertyRows(propertyRow("a", 1, 0))}),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 2, Rows: propertyRows(propertyRow("b", 1, 0))}),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 3, Rows: propertyRows(propertyRow("c", 1, 0))}))
	if err != nil {
		t.Fatal(err)
	}
	if overlap {
		t.Fatal("repairs of two property batches were in flight at once")
	}
	if got := ackSeqs(s.sent); !slices.Equal(got, []uint64{1, 2, 3}) {
		t.Fatalf("acks = %v, want [1 2 3]", got)
	}
}

func streamRows(rows ...*transferv1.ImportRowsRequest_StreamRow) *transferv1.ImportRowsRequest_RowBatch_Stream {
	return &transferv1.ImportRowsRequest_RowBatch_Stream{Stream: &transferv1.ImportRowsRequest_StreamRows{Rows: rows}}
}

func measureRows(rows ...*transferv1.ImportRowsRequest_MeasureRow) *transferv1.ImportRowsRequest_RowBatch_Measure {
	return &transferv1.ImportRowsRequest_RowBatch_Measure{Measure: &transferv1.ImportRowsRequest_MeasureRows{Rows: rows}}
}

func traceRows(rows ...*transferv1.ImportRowsRequest_TraceRow) *transferv1.ImportRowsRequest_RowBatch_Trace {
	return &transferv1.ImportRowsRequest_RowBatch_Trace{Trace: &transferv1.ImportRowsRequest_TraceRows{Rows: rows}}
}

func propertyRows(rows ...*transferv1.ImportRowsRequest_PropertyRow) *transferv1.ImportRowsRequest_RowBatch_Property {
	return &transferv1.ImportRowsRequest_RowBatch_Property{Property: &transferv1.ImportRowsRequest_PropertyRows{Rows: rows}}
}

func TestImportRows_RowsOfAnotherCatalogAreRefused(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	_, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_STREAM, testStreamGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 2, Rows: measureRows(measureRow(1))}))
	wantCode(t, err, codes.InvalidArgument, "batch_seq=2: 1 rows of catalog CATALOG_MEASURE in a CATALOG_STREAM import stream")
	if len(f.client.published) != 0 {
		t.Fatalf("published %d messages, want none", len(f.client.published))
	}
}

func TestStageRoute_FallbackMatchesExplicitHotStage(t *testing.T) {
	ro := testRO(2, 0, testStage("hot", "type=hot", 4, 0), testStage("warm", "type=warm", 1, 0))
	nodes := append(clusterNodes(), labeledNode("bare", nil))
	hot, err := routeOf(t, ro, "hot", "type=hot", nodes)
	if err != nil {
		t.Fatal(err)
	}
	cold, err := routeOf(t, ro, "cold", "type=hot", nodes)
	if err != nil {
		t.Fatal(err)
	}
	if cold.stage != "hot" || cold.shardNum != hot.shardNum || cold.copies != hot.copies ||
		!slices.EqualFunc(cold.shards, hot.shards, slices.Equal[[]string]) {
		t.Fatalf("fallback route = %+v, want the explicit hot route %+v", cold, hot)
	}
	if slices.Contains(cold.nodes(), "bare") {
		t.Fatalf("fallback route %v reaches a node outside the liaison selector", cold.nodes())
	}
}

func TestImportRows_StandalonePropertyRepairFailure(t *testing.T) {
	f := newImportFixture(t, nil)
	f.svc.dataNodes[commonv1.Catalog_CATALOG_PROPERTY] = fakePropertyNodes{nodes: []string{testSelfNode}}
	// The local batch publisher answers a failed repair with the error of its reply.
	f.client.closeErr[testSelfNode] = common.NewError("fail to delete property: boom")
	s, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_PROPERTY, testPropertyGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 3, Rows: propertyRows(propertyRow("a", 1, 0))}))
	wantCode(t, err, codes.Internal, "batch_seq=3 node="+testSelfNode+": ")
	wantErrorInfo(t, err, transfer.ImportReasonNodeFailed,
		map[string]string{transfer.ImportMetadataBatchSeq: "3", transfer.ImportMetadataNode: testSelfNode})
	if len(s.sent) != 0 {
		t.Fatalf("acks = %v, want none for a failed repair", ackSeqs(s.sent))
	}
}

// refuseBusyTwice retries quickly and makes the first two closes on node answer server busy.
func refuseBusyTwice(f *importFixture, node string) {
	f.svc.writeConfig = &writeSessionConfig{pressureTimeout: 5 * time.Second, retryBackoff: time.Millisecond, retryBackoffMax: 2 * time.Millisecond}
	var refusals atomic.Int32
	f.client.onClose = func(closed string, _ []any) *common.Error {
		if closed == node && refusals.Add(1) <= 2 {
			return common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, "memory pressure")
		}
		return nil
	}
}

func TestImportRows_BusyNodeIsRetried(t *testing.T) {
	busy := func() *common.Error {
		return common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, "memory pressure")
	}
	t.Run("refused at close until the node recovers", func(t *testing.T) {
		f := newImportFixture(t, clusterNodes()[:2])
		refuseBusyTwice(f, "hot-b")
		s, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_MEASURE, testMeasureGroup, ""), measureBatch(1))
		if err != nil {
			t.Fatal(err)
		}
		if got := ackSeqs(s.sent); !slices.Equal(got, []uint64{1}) {
			t.Fatalf("acks = %v, want [1]", got)
		}
		if byNode := f.client.byNode(); len(byNode["hot-a"]) != 1 || len(byNode["hot-b"]) != 3 {
			t.Fatalf("published per node = %v, want hot-a once and hot-b on each of its 3 attempts", byNode)
		}
	})
	t.Run("refused by the health check until the pressure timeout", func(t *testing.T) {
		f := newImportFixture(t, clusterNodes()[:2])
		f.svc.writeConfig = &writeSessionConfig{pressureTimeout: 50 * time.Millisecond, retryBackoff: time.Millisecond, retryBackoffMax: 5 * time.Millisecond}
		f.client.publishErr["hot-b"] = busy()
		s, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_MEASURE, testMeasureGroup, ""), measureBatch(7))
		wantCode(t, err, codes.ResourceExhausted, "batch_seq=7 node=hot-b: the data node stayed busy under transient load")
		wantErrorInfo(t, err, transfer.ImportReasonNodeFailed,
			map[string]string{transfer.ImportMetadataBatchSeq: "7", transfer.ImportMetadataNode: "hot-b"})
		if len(s.sent) != 0 {
			t.Fatalf("acks = %v, want none", ackSeqs(s.sent))
		}
	})
	t.Run("stops when the stream is canceled", func(t *testing.T) {
		f := newImportFixture(t, clusterNodes()[:2])
		f.svc.writeConfig = &writeSessionConfig{pressureTimeout: time.Minute, retryBackoff: time.Millisecond, retryBackoffMax: 5 * time.Millisecond}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f.client.onClose = func(node string, _ []any) *common.Error {
			if node == "hot-b" {
				cancel()
				return busy()
			}
			return nil
		}
		s := &fakeImportStream{ctx: ctx, frames: []*transferv1.ImportRowsRequest{
			initFrame(commonv1.Catalog_CATALOG_MEASURE, testMeasureGroup, ""), measureBatch(1),
		}}
		if err := f.svc.ImportRows(s); status.Code(err) != codes.Canceled {
			t.Fatalf("ImportRows() = %v, want Canceled", err)
		}
	})
}

func TestImportRows_PropertySchemaIsChecked(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	missing := propertyRow("a", 1, 0)
	missing.Property.Metadata.Name = "absent"
	_, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_PROPERTY, testPropertyGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 2, Rows: propertyRows(propertyRow("b", 1, 0), missing)}))
	wantCode(t, err, codes.NotFound, "batch_seq=2: property pg/absent not found")
	wantErrorInfo(t, err, transfer.ImportReasonSchemaNotFound, map[string]string{transfer.ImportMetadataBatchSeq: "2"})

	unknown := propertyRow("a", 1, 0)
	unknown.Property.Tags = append(unknown.Property.Tags, &modelv1.Tag{Key: "other", Value: strTag("v")})
	_, err = runImport(f, initFrame(commonv1.Catalog_CATALOG_PROPERTY, testPropertyGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 3, Rows: propertyRows(unknown)}))
	wantCode(t, err, codes.InvalidArgument, "batch_seq=3: row 0: property ui tag other not found")

	mistyped := propertyRow("a", 1, 0)
	mistyped.Property.Tags = []*modelv1.Tag{{Key: "k", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 1}}}}}
	_, err = runImport(f, initFrame(commonv1.Catalog_CATALOG_PROPERTY, testPropertyGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 4, Rows: propertyRows(mistyped)}))
	wantCode(t, err, codes.InvalidArgument, "batch_seq=4: row 0: property ui tag k type mismatch")
	if len(f.client.published) != 0 {
		t.Fatalf("published %d repairs, want none for a refused batch", len(f.client.published))
	}

	// A deleted row keeps the tags of its last revision and is not checked against the schema.
	stale := propertyRow("a", 1, 10)
	stale.Property.Tags = append(stale.Property.Tags, &modelv1.Tag{Key: "dropped", Value: strTag("v")})
	if _, err = runImport(f, initFrame(commonv1.Catalog_CATALOG_PROPERTY, testPropertyGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{Rows: propertyRows(stale)})); err != nil {
		t.Fatal(err)
	}
}

func TestImportRows_RefusedBatchNamesItsSeq(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	_, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_STREAM, testStreamGroup, ""),
		batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 6, Rows: streamRows(streamRow("svc", 0))}))
	wantCode(t, err, codes.InvalidArgument, "batch_seq=6: ")
}

func TestImportService_RefusesInternalGroups(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	_, err := f.svc.Targets(context.Background(), &transferv1.TargetsRequest{Group: common.DeletionTaskGroup})
	wantCode(t, err, codes.InvalidArgument, "internal to the server")
	_, err = runImport(f, initFrame(commonv1.Catalog_CATALOG_PROPERTY, common.DeletionTaskGroup, ""))
	wantCode(t, err, codes.InvalidArgument, "internal to the server")
}

func TestImportService_GroupReadFailureIsRetryable(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	f.svc.groups = func(context.Context, string) (*commonv1.Group, error) {
		return nil, errors.New("no active schema server")
	}
	_, err := f.svc.Targets(context.Background(), &transferv1.TargetsRequest{Group: testMeasureGroup})
	wantCode(t, err, codes.Unavailable, "get group mg: no active schema server")
	wantErrorInfo(t, err, transfer.ImportReasonNotWritten, nil)

	f.svc.groups = func(context.Context, string) (*commonv1.Group, error) { return nil, context.DeadlineExceeded }
	_, err = f.svc.Targets(context.Background(), &transferv1.TargetsRequest{Group: testMeasureGroup})
	wantCode(t, err, codes.DeadlineExceeded, "")
}

// rateLimitedInit is the init frame of an import limited to 10 rows per second.
func rateLimitedInit(catalog commonv1.Catalog, group, stage string) (*transferv1.ImportRowsRequest, *transferv1.RateLimit) {
	rl := &transferv1.RateLimit{RowsPerSecond: 10}
	init := initFrame(catalog, group, stage)
	init.GetInit().RateLimit = rl
	return init, rl
}

func TestImportRows_EveryNodeStreamOpensWithTheRateLimit(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	init, rl := rateLimitedInit(commonv1.Catalog_CATALOG_STREAM, testStreamGroup, "hot")
	rows := []*transferv1.ImportRowsRequest_StreamRow{streamRow("svc-1", 11), streamRow("svc-2", 22), streamRow("svc-3", 33)}
	if _, err := runImport(f, init, batchFrame(&transferv1.ImportRowsRequest_RowBatch{BatchSeq: 0, Rows: streamRows(rows...)})); err != nil {
		t.Fatal(err)
	}
	byNode := f.client.byNode()
	if len(byNode) < 2 {
		t.Fatalf("rows reached %d node(s), want the replicas of several nodes", len(byNode))
	}
	initsOf := make(map[string]int)
	for _, p := range f.client.inits {
		if !proto.Equal(p.payload.(*transferv1.ImportWriteInit).GetRateLimit(), rl) || p.topic != data.TopicStreamImport {
			t.Fatalf("init %v on %s, want rate limit %v on %s", p.payload, p.topic, rl, data.TopicStreamImport)
		}
		initsOf[p.node]++
	}
	for node := range byNode {
		if initsOf[node] != 1 {
			t.Fatalf("node %s got %d init frame(s) for one batch, want 1", node, initsOf[node])
		}
	}
}

func TestImportRows_BatchTimeoutCoversTheRateLimit(t *testing.T) {
	f := newImportFixture(t, clusterNodes()[:1])
	init, _ := rateLimitedInit(commonv1.Catalog_CATALOG_MEASURE, testMeasureGroup, "")
	if _, err := runImport(f, init, measureBatch(1)); err != nil {
		t.Fatal(err)
	}
	// One row at 10 rows/s: the node may hold the batch for its 100ms share plus the slack.
	if want := importBatchTimeout + 100*time.Millisecond + ratelimit.NodeSlack; len(f.client.timeouts) != 1 || f.client.timeouts[0] != want {
		t.Fatalf("batch timeouts = %v, want [%v]", f.client.timeouts, want)
	}
}

func TestImportWriter_BatchTimeout(t *testing.T) {
	payloads := make([]*transferv1.ImportWrite, 20)
	size := 0
	for i := range payloads {
		payloads[i] = &transferv1.ImportWrite{Row: &transferv1.ImportWrite_Stream{Stream: &streamv1.InternalWriteRequest{ShardId: uint32(i)}}}
		size += proto.Size(payloads[i])
	}
	for name, tc := range map[string]struct {
		limit *transferv1.RateLimit
		want  time.Duration
	}{
		"no rate limit":         {limit: nil, want: importBatchTimeout},
		"an empty rate limit":   {limit: &transferv1.RateLimit{}, want: importBatchTimeout},
		"rows":                  {limit: &transferv1.RateLimit{RowsPerSecond: 4}, want: importBatchTimeout + 5*time.Second + ratelimit.NodeSlack},
		"bytes":                 {limit: &transferv1.RateLimit{BytesPerSecond: uint64(size)}, want: importBatchTimeout + time.Second + ratelimit.NodeSlack},
		"the slower of the two": {limit: &transferv1.RateLimit{BytesPerSecond: uint64(size), RowsPerSecond: 4}, want: importBatchTimeout + 5*time.Second + ratelimit.NodeSlack},
	} {
		t.Run(name, func(t *testing.T) {
			w := &importWriter{init: &transferv1.ImportRowsRequest_Init{RateLimit: tc.limit}}
			if got := w.batchTimeout(payloads); got != tc.want {
				t.Fatalf("batchTimeout = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestImportRows_RateBusyNodeIsRetriedPastThePressureTimeout(t *testing.T) {
	f := newImportFixture(t, clusterNodes()[:2])
	f.svc.writeConfig = &writeSessionConfig{pressureTimeout: 10 * time.Millisecond, retryBackoff: time.Millisecond, retryBackoffMax: 5 * time.Millisecond}
	var refusals atomic.Int32
	f.client.onClose = func(node string, _ []any) *common.Error {
		// hot-b's rate limit holds the batch back for longer than any memory pressure may last.
		if node == "hot-b" && refusals.Add(1) <= 10 {
			return common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, transfer.ImportRateBusy+"the node is admitting other imports")
		}
		return nil
	}
	s, err := runImport(f, initFrame(commonv1.Catalog_CATALOG_MEASURE, testMeasureGroup, ""), measureBatch(1))
	if err != nil {
		t.Fatal(err)
	}
	if got := ackSeqs(s.sent); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("acks = %v, want [1]", got)
	}
	if byNode := f.client.byNode(); len(byNode["hot-a"]) != 1 || len(byNode["hot-b"]) != 11 {
		t.Fatalf("published per node = %v, want hot-a once and hot-b on each of its 11 attempts", byNode)
	}
}

func TestImportRows_RateBusyRetryStopsWhenTheStreamIsCanceled(t *testing.T) {
	f := newImportFixture(t, clusterNodes()[:2])
	f.svc.writeConfig = &writeSessionConfig{pressureTimeout: time.Millisecond, retryBackoff: time.Millisecond, retryBackoffMax: 5 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var refusals atomic.Int32
	f.client.onClose = func(node string, _ []any) *common.Error {
		if node != "hot-b" {
			return nil
		}
		if refusals.Add(1) == 3 {
			cancel()
		}
		return common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, transfer.ImportRateBusy+"the node is admitting other imports")
	}
	s := &fakeImportStream{ctx: ctx, frames: []*transferv1.ImportRowsRequest{
		initFrame(commonv1.Catalog_CATALOG_MEASURE, testMeasureGroup, ""), measureBatch(1),
	}}
	if err := f.svc.ImportRows(s); status.Code(err) != codes.Canceled {
		t.Fatalf("ImportRows() = %v, want Canceled", err)
	}
}

func TestBusyOf(t *testing.T) {
	if got := busyOf(common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, transfer.ImportRateBusy+"x")); got != rateBusy {
		t.Fatalf("busyOf(rate limit) = %v, want rateBusy", got)
	}
	// pub rebuilds a remote node's reply around the text of the node's own error.
	relayed := common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, transfer.ImportRateBusy+"x")
	if got := busyOf(common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, relayed.Error())); got != rateBusy {
		t.Fatalf("busyOf(relayed rate limit) = %v, want rateBusy", got)
	}
	if got := busyOf(common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, "memory pressure")); got != pressureBusy {
		t.Fatalf("busyOf(memory pressure) = %v, want pressureBusy", got)
	}
}

func TestImportRows_EveryNodeBatchOpensWithTheInitFrame(t *testing.T) {
	for name, tc := range map[string]struct {
		init  *transferv1.ImportRowsRequest
		batch *transferv1.ImportRowsRequest
	}{
		"stream": {
			init:  initFrame(commonv1.Catalog_CATALOG_STREAM, testStreamGroup, "hot"),
			batch: batchFrame(&transferv1.ImportRowsRequest_RowBatch{Rows: streamRows(streamRow("svc-1", 11), streamRow("svc-2", 22), streamRow("svc-3", 33))}),
		},
		"measure": {init: initFrame(commonv1.Catalog_CATALOG_MEASURE, testMeasureGroup, ""), batch: measureBatch(1)},
		"property": {
			init:  initFrame(commonv1.Catalog_CATALOG_PROPERTY, testPropertyGroup, ""),
			batch: batchFrame(&transferv1.ImportRowsRequest_RowBatch{Rows: propertyRows(propertyRow("a", 1, 0), propertyRow("b", 1, 0))}),
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newImportFixture(t, clusterNodes())
			if _, err := runImport(f, tc.init, tc.batch); err != nil {
				t.Fatal(err)
			}
			if len(f.client.frames) == 0 {
				t.Fatal("no batch was sent")
			}
			for _, nf := range f.client.frames {
				if len(nf.frames) < 2 || nf.frames[0].GetInit() == nil {
					t.Fatalf("the batch of node %s = %v, want the init frame first and then rows", nf.node, nf.frames)
				}
				for _, fr := range nf.frames[1:] {
					if fr.GetInit() != nil || fr.GetRow() == nil {
						t.Fatalf("the batch of node %s carries %v after its init, want only rows", nf.node, fr)
					}
				}
			}
		})
	}
}

func TestImportRows_EveryPropertyBatchCarriesTheRateLimit(t *testing.T) {
	f := newImportFixture(t, clusterNodes())
	init, rl := rateLimitedInit(commonv1.Catalog_CATALOG_PROPERTY, testPropertyGroup, "")
	batch := &transferv1.ImportRowsRequest_RowBatch{BatchSeq: 0, Rows: propertyRows(propertyRow("p-1", 1, 0), propertyRow("p-2", 1, 0))}
	if _, err := runImport(f, init, batchFrame(batch)); err != nil {
		t.Fatal(err)
	}
	byNode := f.client.byNode()
	if len(byNode) == 0 || len(f.client.inits) != len(byNode) {
		t.Fatalf("%d node(s) got %d init(s), want one each", len(byNode), len(f.client.inits))
	}
	for _, p := range f.client.inits {
		if !proto.Equal(p.payload.(*transferv1.ImportWriteInit).GetRateLimit(), rl) || p.topic != data.TopicPropertyImport {
			t.Fatalf("init %v on %s, want rate limit %v on %s", p.payload, p.topic, rl, data.TopicPropertyImport)
		}
	}
}

// benchImportClient stands in for the tier-2 publisher of a cluster liaison: like pub, it
// marshals every proto payload it is handed and takes a []byte payload as it is.
type benchImportClient struct {
	queue.Client
}

func (benchImportClient) NewBatchPublisher(time.Duration) queue.BatchPublisher {
	return benchImportBatch{}
}

type benchImportBatch struct{}

func (benchImportBatch) Publish(_ context.Context, _ bus.Topic, msgs ...bus.Message) (bus.Future, error) {
	for _, m := range msgs {
		if p, ok := m.Data().(proto.Message); ok {
			if _, err := proto.Marshal(p); err != nil {
				return nil, err
			}
		}
	}
	return nil, nil
}

func (benchImportBatch) Close() (map[string]*common.Error, error) { return nil, nil }

const benchBatchRows = 1000

func benchStreamBatch() *transferv1.ImportRowsRequest_RowBatch {
	rows := make([]*transferv1.ImportRowsRequest_StreamRow, benchBatchRows)
	for i := range rows {
		rows[i] = &transferv1.ImportRowsRequest_StreamRow{
			Name: testStreamName,
			Element: &streamv1.ElementValue{
				ElementId: fmt.Sprintf("%016x", i),
				Timestamp: timestamppb.New(time.Unix(1700000000, int64(i)*int64(time.Millisecond))),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
					strTag(fmt.Sprintf("svc-%d", i%50)), strTag(fmt.Sprintf("/endpoint/%d", i)),
				}}},
			},
			RawElementId: uint64(i + 1),
		}
	}
	return &transferv1.ImportRowsRequest_RowBatch{Rows: streamRows(rows...)}
}

func benchMeasureBatch() *transferv1.ImportRowsRequest_RowBatch {
	rows := make([]*transferv1.ImportRowsRequest_MeasureRow, benchBatchRows)
	for i := range rows {
		rows[i] = &transferv1.ImportRowsRequest_MeasureRow{
			Name: testMeasureName,
			Point: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(time.Unix(1700000000, int64(i)*int64(time.Millisecond))),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
					strTag(fmt.Sprintf("svc-%d", i%50)), strTag(fmt.Sprintf("/endpoint/%d", i)),
				}}},
				Version: int64(i + 1),
			},
		}
	}
	return &transferv1.ImportRowsRequest_RowBatch{Rows: measureRows(rows...)}
}

// BenchmarkImportWrite measures the liaison's work for one batch of 1000 rows written to a
// group of one replica on three nodes: building the internal write requests and handing them,
// marshaled, to every replica.
func BenchmarkImportWrite(b *testing.B) {
	cases := []struct {
		batch   func() *transferv1.ImportRowsRequest_RowBatch
		name    string
		group   string
		catalog commonv1.Catalog
	}{
		{name: "stream", group: testStreamGroup, catalog: commonv1.Catalog_CATALOG_STREAM, batch: benchStreamBatch},
		{name: "measure", group: testMeasureGroup, catalog: commonv1.Catalog_CATALOG_MEASURE, batch: benchMeasureBatch},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			f := newImportFixture(b, clusterNodes())
			f.svc.tire2 = benchImportClient{}
			g := testGroups()[c.group]
			r, err := f.svc.routeFor(context.Background(), g, "")
			if err != nil {
				b.Fatal(err)
			}
			w := &importWriter{s: f.svc, init: &transferv1.ImportRowsRequest_Init{Catalog: c.catalog, Group: c.group}, route: r}
			batch := c.batch()
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err = w.write(ctx, batch); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.N*benchBatchRows)/b.Elapsed().Seconds(), "rows/s")
		})
	}
}
