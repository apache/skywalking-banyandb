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

package data_export_test

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/transfer/export"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	test_property "github.com/apache/skywalking-banyandb/pkg/test/property"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	test_trace "github.com/apache/skywalking-banyandb/pkg/test/trace"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exporter"
	casesmeasuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	casespropertydata "github.com/apache/skywalking-banyandb/test/cases/property/data"
	casesstreamdata "github.com/apache/skywalking-banyandb/test/cases/stream/data"
	casestracedata "github.com/apache/skywalking-banyandb/test/cases/trace/data"
)

var (
	stopFunc    func()
	liaisonAddr string
	config      *setup.ClusterConfig
	// Per data node, in start order: gRPC address, data directory, node id as the liaison
	// names it (also the address it registered under), and the close function.
	dataAddrs      []string
	dataDirs       []string
	nodeNames      []string
	closeDataNodes []func()
	conn           *grpc.ClientConn
)

var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())
	// The specs expire leases on purpose; the periodic sweep must not reclaim them mid-spec.
	export.SweepInterval = time.Hour
	tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
	gomega.Expect(tmpErr).NotTo(gomega.HaveOccurred())
	config = setup.PropertyClusterConfig(setup.NewDiscoveryFileWriter(tmpDir))
	// A stopped node stays in the discovery file, so it stays registered and the liaison has
	// to report it as unreachable, which one case relies on.
	config.NodeDiscovery.KeepStoppedNodes = true
	for i := 0; i < 2; i++ {
		ginkgo.By("Starting data node")
		addr, dir, _, closeFn := setup.DataNodeWithAddrAndDir(config,
			"--stream-flush-timeout=500ms", "--measure-flush-timeout=500ms", "--trace-flush-timeout=500ms")
		dataAddrs = append(dataAddrs, addr)
		dataDirs = append(dataDirs, dir)
		closeDataNodes = append(closeDataNodes, closeFn)
		nodeNames = append(nodeNames, currentNodeName(addr))
	}
	ginkgo.By("Loading schema via property")
	setup.PreloadSchemaViaProperty(config, test_stream.PreloadSchema, test_measure.PreloadSchema, test_trace.PreloadSchema, test_property.PreloadSchema)
	config.AddLoadedKinds(schema.KindStream, schema.KindMeasure, schema.KindTrace)
	ginkgo.By("Starting liaison node")
	var closeLiaison func()
	liaisonAddr, closeLiaison = setup.LiaisonNode(config)
	stopFunc = func() {
		closeLiaison()
		for _, closeFn := range closeDataNodes {
			closeFn()
		}
		tmpDirCleanup()
	}
	var err error
	conn, err = grpchelper.Conn(liaisonAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("Writing stream, measure, trace and property data")
	casesstreamdata.Write(conn, "sw", timestamp.NowMilli(), 500*time.Millisecond)
	ns := timestamp.NowMilli().UnixNano()
	casesmeasuredata.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json",
		time.Unix(0, ns-ns%int64(time.Minute)), 500*time.Millisecond)
	casestracedata.Write(conn, "sw", timestamp.NowMilli(), 500*time.Millisecond)
	casespropertydata.Write(conn, "sw1")
	return []byte(liaisonAddr)
}, func(_ []byte) {})

var _ = ginkgo.SynchronizedAfterSuite(func() {
	if conn != nil {
		gomega.Expect(conn.Close()).To(gomega.Succeed())
	}
	if stopFunc != nil {
		stopFunc()
	}
}, func() {})

// dialDataNode opens a direct connection to a data node, bypassing the liaison.
func dialDataNode(addr string) *grpc.ClientConn {
	c, err := grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	return c
}

// currentNodeName asks a data node for the node id the liaison registers it under.
func currentNodeName(addr string) string {
	c := dialDataNode(addr)
	defer func() { gomega.Expect(c.Close()).To(gomega.Succeed()) }()
	cur, err := databasev1.NewNodeQueryServiceClient(c).GetCurrentNode(context.Background(), &databasev1.GetCurrentNodeRequest{})
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	return cur.GetNode().GetMetadata().GetName()
}

// heartbeat runs ACTION_HEARTBEAT and returns the error text every data node answers with,
// "" for a node that answered done. Any other outcome fails the test.
func heartbeat(id string) map[string]string {
	out := map[string]string{}
	for _, f := range transfertest.Heartbeat(conn, id) {
		switch o := f.GetOutcome().(type) {
		case *transferv1.SessionsResponse_Done:
			out[f.GetNodeId()] = ""
		case *transferv1.SessionsResponse_Error:
			out[f.GetNodeId()] = o.Error
		default:
			ginkgo.Fail("a heartbeat frame is done or error, got " + f.String())
		}
	}
	return out
}

// sessionsByNode returns the session id every data node lists, "" for a node answering none.
// Any other outcome fails the test.
func sessionsByNode() map[string]string {
	out := map[string]string{}
	for _, f := range transfertest.List(conn) {
		switch o := f.GetOutcome().(type) {
		case *transferv1.SessionsResponse_Session:
			out[f.GetNodeId()] = o.Session.GetSessionId()
		case *transferv1.SessionsResponse_None:
			out[f.GetNodeId()] = ""
		default:
			ginkgo.Fail("a list frame is session or none, got " + f.String())
		}
	}
	return out
}

func registeredDataNodes() []string {
	state, err := databasev1.NewClusterStateServiceClient(conn).GetClusterState(context.Background(), &databasev1.GetClusterStateRequest{})
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	var names []string
	for _, n := range state.GetRouteTables()["tire2"].GetRegistered() {
		names = append(names, n.GetMetadata().GetName())
	}
	sort.Strings(names)
	return names
}

func streamLease(dataDir, id string) string { return transfertest.LeaseFile(dataDir, "stream", id) }

// unitNodes returns the distinct node ids stamped on the unit frames.
func unitNodes(frames []*transferv1.PlanResponse) []string {
	seen := map[string]struct{}{}
	for _, f := range transfertest.UnitFrames(frames) {
		seen[f.GetNodeId()] = struct{}{}
	}
	var out []string
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// catalogParts counts the parts of one catalog and group across every node's units.
func catalogParts(frames []*transferv1.PlanResponse, catalog commonv1.Catalog, group string) uint64 {
	var parts uint64
	for _, f := range transfertest.UnitFrames(frames) {
		for _, u := range f.GetUnits() {
			seg := u.GetSegment()
			if seg.GetUnit().GetCatalog() != catalog || seg.GetUnit().GetGroup() != group {
				continue
			}
			for _, sh := range seg.GetShards() {
				parts += uint64(sh.GetPartsCount())
			}
		}
	}
	return parts
}

// propertyDocs sums the documents of one property group across every node's units.
func propertyDocs(frames []*transferv1.PlanResponse, group string) uint64 {
	var docs uint64
	for _, f := range transfertest.UnitFrames(frames) {
		for _, u := range f.GetUnits() {
			if p := u.GetProperty(); p != nil && p.GetGroup() == group {
				for _, sh := range p.GetShards() {
					docs += sh.GetDocCount()
				}
			}
		}
	}
	return docs
}

// expectEveryCatalogInventoried asserts that the frames list the data the suite wrote in each
// of the four catalogs.
func expectEveryCatalogInventoried(frames []*transferv1.PlanResponse) {
	gomega.ExpectWithOffset(1, catalogParts(frames, commonv1.Catalog_CATALOG_STREAM, "default")).To(gomega.BeNumerically(">", 0), "stream")
	gomega.ExpectWithOffset(1, catalogParts(frames, commonv1.Catalog_CATALOG_MEASURE, "sw_metric")).To(gomega.BeNumerically(">", 0), "measure")
	gomega.ExpectWithOffset(1, catalogParts(frames, commonv1.Catalog_CATALOG_TRACE, "test-trace-group")).To(gomega.BeNumerically(">", 0), "trace")
	gomega.ExpectWithOffset(1, propertyDocs(frames, "sw")).To(gomega.BeNumerically(">", 0), "property")
}

func totalRows(frames []*transferv1.PlanResponse) uint64 {
	var total uint64
	for _, f := range transfertest.UnitFrames(frames) {
		for _, u := range f.GetUnits() {
			for _, s := range u.GetSegment().GetShards() {
				total += s.GetTotalCount()
			}
		}
	}
	return total
}

// defaultShardSources maps every stream shard key of the default group to the rows each node
// reports for it, built independently of BuildReport.
func defaultShardSources(frames []*transferv1.PlanResponse) map[string]map[string]uint64 {
	out := map[string]map[string]uint64{}
	for _, f := range transfertest.UnitFrames(frames) {
		for _, u := range f.GetUnits() {
			if u.GetSegment().GetUnit().GetGroup() != "default" {
				continue
			}
			for _, sh := range u.GetSegment().GetShards() {
				key := "stream/default/" + exporter.NormalizeStage(f.GetStage()) + "/" + u.GetSegment().GetUnit().GetSegmentSuffix() +
					"/shard-" + strconv.FormatUint(uint64(sh.GetShardId()), 10)
				if out[key] == nil {
					out[key] = map[string]uint64{}
				}
				out[key][f.GetNodeId()] = sh.GetTotalCount()
			}
		}
	}
	return out
}

var _ = ginkgo.Describe("ExportService", ginkgo.Ordered, func() {
	streamSelector := []*transferv1.Selector{{Catalog: commonv1.Catalog_CATALOG_STREAM}}

	ginkgo.BeforeAll(func() {
		ginkgo.By("Waiting for the written data to be flushed on both nodes")
		gomega.Eventually(func(g gomega.Gomega) {
			frames, err := transfertest.Plan(conn, &transferv1.PlanRequest{Selectors: streamSelector})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(unitNodes(frames)).To(gomega.HaveLen(2))
			g.Expect(totalRows(frames)).To(gomega.BeNumerically(">", 0))
			sources := defaultShardSources(frames)
			g.Expect(sources).NotTo(gomega.BeEmpty())
			for key, nodes := range sources {
				g.Expect(nodes).To(gomega.HaveLen(2), "shard %s must be flushed on both replicas", key)
			}
		}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())
		sorted := append([]string(nil), nodeNames...)
		sort.Strings(sorted)
		gomega.Expect(registeredDataNodes()).To(gomega.Equal(sorted), "the liaison names the nodes the way they name themselves")
	})

	ginkgo.It("stamps node_id on every frame, never spans groups and reports both nodes as answered", func() {
		frames := transfertest.MustPlan(conn, &transferv1.PlanRequest{Selectors: streamSelector})
		summary := transfertest.Summary(frames)
		gomega.Expect(summary.GetUnreachableNodes()).To(gomega.BeEmpty())
		registered := registeredDataNodes()
		gomega.Expect(summary.GetAnsweredNodes()).To(gomega.Equal(registered))
		units := transfertest.UnitFrames(frames)
		gomega.Expect(units).To(gomega.HaveLen(len(frames)-1), "a dry run streams units frames and one summary")
		for _, f := range units {
			gomega.Expect(registered).To(gomega.ContainElement(f.GetNodeId()))
			groups := map[string]struct{}{}
			for _, u := range f.GetUnits() {
				groups[u.GetSegment().GetUnit().GetGroup()] = struct{}{}
				gomega.Expect(u.GetSegment().GetSegmentVersion()).NotTo(gomega.BeEmpty())
			}
			gomega.Expect(groups).To(gomega.HaveLen(1))
		}
	})

	ginkgo.It("reports every source of a replicated shard", func() {
		defaultSelector := []*transferv1.Selector{{Catalog: commonv1.Catalog_CATALOG_STREAM, Groups: []string{"default"}}}
		frames := transfertest.MustPlan(conn, &transferv1.PlanRequest{Selectors: defaultSelector})
		want := defaultShardSources(frames)
		gomega.Expect(want).NotTo(gomega.BeEmpty())
		summary := transfertest.Summary(frames)
		rep := exporter.BuildReport(&exporter.ClusterInfo{}, &exporter.PlanResult{
			Frames:           transfertest.UnitFrames(frames),
			AnsweredNodes:    summary.GetAnsweredNodes(),
			UnreachableNodes: summary.GetUnreachableNodes(),
		})
		got := map[string]map[string]uint64{}
		for _, m := range rep.MultiSource {
			got[m.Key] = map[string]uint64{}
			nodes := make([]string, 0, len(m.Sources))
			for _, src := range m.Sources {
				got[m.Key][src.Node] = src.Rows
				nodes = append(nodes, src.Node)
			}
			gomega.Expect(sort.StringsAreSorted(nodes)).To(gomega.BeTrue(), "sources are listed in node order: %v", nodes)
		}
		gomega.Expect(got).To(gomega.Equal(want), "every replicated shard is a multi-source unit listing both replicas")
		for key, nodes := range got {
			gomega.Expect(nodes).To(gomega.HaveLen(2), "unit %s must list both replicas", key)
		}
	})

	ginkgo.It("keeps the live data directories byte-identical across a live plan", func() {
		liveDir := filepath.Join(dataDirs[0], "stream", "data")
		before := transfertest.StableTree(liveDir)
		_ = transfertest.MustPlan(conn, &transferv1.PlanRequest{})
		gomega.Expect(transfertest.TreeDiff(before, transfertest.HashTree(liveDir))).To(gomega.BeEmpty())
	})

	var sessionID string

	ginkgo.It("creates a session on every data node and announces it in the first frame", func() {
		frames := transfertest.MustPlan(conn, transfertest.CreateRequest(streamSelector, false))
		sessionID = frames[0].GetCreated().GetSessionId()
		gomega.Expect(sessionID).NotTo(gomega.BeEmpty())
		created := &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Created{Created: &transferv1.SessionCreated{SessionId: sessionID}}}
		gomega.Expect(proto.Equal(frames[0], created)).To(gomega.BeTrue(), "the first frame is the created frame carrying nothing but the session id")
		for _, f := range frames[1 : len(frames)-1] {
			gomega.Expect(f.GetUnits()).NotTo(gomega.BeNil(), "every frame between created and summary carries units: %+v", f)
		}
		for _, dir := range dataDirs {
			gomega.Expect(streamLease(dir, sessionID)).To(gomega.BeAnExistingFile())
			for _, catalog := range transfertest.Catalogs {
				if catalog != "stream" {
					gomega.Expect(transfertest.SessionDir(dir, catalog, sessionID)).NotTo(gomega.BeADirectory(), "unselected catalogs get no session dir")
				}
			}
		}
		gomega.Expect(transfertest.UnitFrames(frames)).NotTo(gomega.BeEmpty())
		gomega.Expect(transfertest.Summary(frames).GetAnsweredNodes()).To(gomega.HaveLen(2))
		gomega.Expect(transfertest.Summary(frames).GetPreemptedSessionIds()).To(gomega.BeEmpty())
	})

	ginkgo.It("plans on the snapshot and renews; a heartbeat rewrites lastHeartbeatAt with one done frame per node", func() {
		before := transfertest.ReadLastHeartbeat(streamLease(dataDirs[0], sessionID))
		time.Sleep(10 * time.Millisecond)
		frames := transfertest.MustPlan(conn, transfertest.ReadRequest(sessionID))
		gomega.Expect(transfertest.UnitFrames(frames)).NotTo(gomega.BeEmpty())
		afterPlan := transfertest.ReadLastHeartbeat(streamLease(dataDirs[0], sessionID))
		gomega.Expect(afterPlan).To(gomega.BeNumerically(">", before))
		time.Sleep(10 * time.Millisecond)
		gomega.Expect(heartbeat(sessionID)).To(gomega.Equal(map[string]string{nodeNames[0]: "", nodeNames[1]: ""}))
		for _, dir := range dataDirs {
			gomega.Expect(transfertest.ReadLastHeartbeat(streamLease(dir, sessionID))).To(gomega.BeNumerically(">", afterPlan))
		}
	})

	ginkgo.It("lists one frame per node carrying the live lease", func() {
		frames := transfertest.List(conn)
		gomega.Expect(frames).To(gomega.HaveLen(2))
		for _, f := range frames {
			gomega.Expect(f.GetNodeId()).NotTo(gomega.BeEmpty())
			gomega.Expect(f.GetSession()).NotTo(gomega.BeNil())
			gomega.Expect(f.GetSession().GetSessionId()).To(gomega.Equal(sessionID))
			gomega.Expect(f.GetSession().GetCatalogs()).To(gomega.ConsistOf(commonv1.Catalog_CATALOG_STREAM))
			gomega.Expect(f.GetSession().GetExpiresAt()).To(gomega.BeNumerically(">", time.Now().UnixNano()))
		}
	})

	ginkgo.It("refuses a second session and rolls back, then preempts on request", func() {
		_, err := transfertest.Plan(conn, transfertest.CreateRequest(streamSelector, false))
		gomega.Expect(status.Code(err)).To(gomega.Equal(codes.AlreadyExists))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring(sessionID))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("aborted, snapshots released"))
		gomega.Expect(status.Convert(err).Message()).NotTo(gomega.ContainSubstring("rpc error"), "the node's message is unwrapped, not nested")
		listed := transfertest.List(conn)
		gomega.Expect(listed).To(gomega.HaveLen(2))
		for _, f := range listed {
			gomega.Expect(f.GetSession()).NotTo(gomega.BeNil(), "the loser must leave no trace")
			gomega.Expect(f.GetSession().GetSessionId()).To(gomega.Equal(sessionID))
		}
		for _, dir := range dataDirs {
			gomega.Expect(streamLease(dir, sessionID)).To(gomega.BeAnExistingFile())
			gomega.Expect(transfertest.ExportEntries(dir, "stream")).To(gomega.Equal([]string{sessionID}),
				"the refused session's snapshot is gone, the winner's stays")
		}
		frames := transfertest.MustPlan(conn, transfertest.CreateRequest(streamSelector, true))
		gomega.Expect(transfertest.Summary(frames).GetPreemptedSessionIds()).To(gomega.ConsistOf(sessionID))
		gomega.Expect(transfertest.UnitFrames(frames)).NotTo(gomega.BeEmpty())
		gomega.Expect(frames[0].GetCreated().GetSessionId()).NotTo(gomega.BeEmpty())
		for _, dir := range dataDirs {
			gomega.Expect(transfertest.SessionDir(dir, "stream", sessionID)).NotTo(gomega.BeADirectory())
			gomega.Expect(streamLease(dir, frames[0].GetCreated().GetSessionId())).To(gomega.BeAnExistingFile())
		}
		sessionID = frames[0].GetCreated().GetSessionId()
	})

	ginkgo.It("releases idempotently and then degrades every node that no longer holds the session", func() {
		// The first release deletes the session on both nodes (done); the second finds nothing
		// to delete and says so (none), yet still succeeds.
		first := transfertest.ReleaseOutcome(conn, sessionID)
		gomega.Expect(first.Failed).To(gomega.BeEmpty())
		gomega.Expect(first.NotHeld).To(gomega.BeEmpty())
		gomega.Expect(first.Released).To(gomega.ConsistOf(registeredDataNodes()))
		second := transfertest.ReleaseOutcome(conn, sessionID)
		gomega.Expect(second.Failed).To(gomega.BeEmpty())
		gomega.Expect(second.Released).To(gomega.BeEmpty())
		gomega.Expect(second.NotHeld).To(gomega.ConsistOf(registeredDataNodes()))
		for _, dir := range dataDirs {
			gomega.Expect(streamLease(dir, sessionID)).NotTo(gomega.BeAnExistingFile())
		}
		listed := transfertest.List(conn)
		gomega.Expect(listed).To(gomega.HaveLen(2))
		for _, f := range listed {
			gomega.Expect(f.GetNone()).NotTo(gomega.BeNil(), "a released node lists none: %+v", f)
		}
		// A node that does not hold the session reports that in its renew frame, and a
		// ReadSession plan degrades it to unreachable_nodes: the call succeeds with nobody answering.
		renewed := heartbeat(sessionID)
		gomega.Expect(renewed).To(gomega.HaveLen(len(registeredDataNodes())))
		for node, errText := range renewed {
			gomega.Expect(registeredDataNodes()).To(gomega.ContainElement(node))
			gomega.Expect(errText).To(gomega.ContainSubstring("not found on this node"), node)
		}
		frames := transfertest.MustPlan(conn, transfertest.ReadRequest(sessionID))
		gomega.Expect(frames).To(gomega.HaveLen(1), "nothing but the summary frame")
		summary := transfertest.Summary(frames)
		gomega.Expect(summary.GetAnsweredNodes()).To(gomega.BeEmpty())
		gomega.Expect(summary.GetUnreachableNodes()).To(gomega.Equal(registeredDataNodes()))
	})

	ginkgo.It("reports an expired session distinctly and keeps it", func() {
		frames := transfertest.MustPlan(conn, transfertest.CreateRequest(streamSelector, false))
		id := frames[0].GetCreated().GetSessionId()
		// The lease length is a data-node constant, so expiry is forced by rewriting the lease
		// files: the stamp lies a little ahead so the rewrite itself is never racing the node.
		expiresAt := time.Now().Add(300 * time.Millisecond)
		for _, dir := range dataDirs {
			transfertest.RewriteLease(streamLease(dir, id), func(l *export.Lease) { l.ExpiresAt = expiresAt.UnixNano() })
		}
		time.Sleep(time.Until(expiresAt) + 50*time.Millisecond)
		_, err := transfertest.Plan(conn, transfertest.ReadRequest(id))
		gomega.Expect(status.Code(err)).To(gomega.Equal(codes.FailedPrecondition))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("expired"))
		for _, dir := range dataDirs {
			gomega.Expect(streamLease(dir, id)).To(gomega.BeAnExistingFile(), "a failed session-mode plan never releases")
		}
		gomega.Expect(transfertest.Release(conn, id)).To(gomega.BeEmpty())
	})

	ginkgo.It("keeps the live data directories byte-identical through the session lifecycle", func() {
		liveDir := filepath.Join(dataDirs[1], "stream", "data")
		before := transfertest.StableTree(liveDir)
		frames := transfertest.MustPlan(conn, transfertest.CreateRequest(streamSelector, false))
		_ = transfertest.MustPlan(conn, transfertest.ReadRequest(frames[0].GetCreated().GetSessionId()))
		gomega.Expect(transfertest.Release(conn, frames[0].GetCreated().GetSessionId())).To(gomega.BeEmpty())
		gomega.Expect(transfertest.TreeDiff(before, transfertest.HashTree(liveDir))).To(gomega.BeEmpty())
	})

	ginkgo.It("degrades a node that lost the session to unreachable_nodes", func() {
		frames := transfertest.MustPlan(conn, transfertest.CreateRequest(streamSelector, false))
		id := frames[0].GetCreated().GetSessionId()
		// Node 1 loses its copy on disk, as if an operator wiped its snapshots.
		for _, catalog := range transfertest.Catalogs {
			gomega.Expect(os.RemoveAll(transfertest.SessionDir(dataDirs[1], catalog, id))).To(gomega.Succeed())
		}
		// The heartbeat reports the loss in that node's frame while the other node renews.
		renewed := heartbeat(id)
		gomega.Expect(renewed).To(gomega.HaveLen(2))
		gomega.Expect(renewed[nodeNames[0]]).To(gomega.BeEmpty())
		gomega.Expect(renewed[nodeNames[1]]).To(gomega.ContainSubstring("not found on this node"))
		frames = transfertest.MustPlan(conn, transfertest.ReadRequest(id))
		summary := transfertest.Summary(frames)
		gomega.Expect(summary.GetUnreachableNodes()).To(gomega.Equal([]string{nodeNames[1]}))
		gomega.Expect(summary.GetAnsweredNodes()).To(gomega.Equal([]string{nodeNames[0]}))
		gomega.Expect(unitNodes(frames)).To(gomega.Equal([]string{nodeNames[0]}))
		gomega.Expect(transfertest.Release(conn, id)).To(gomega.BeEmpty())
	})

	ginkgo.It("rolls back a half-created session when one node is already occupied", func() {
		const occupant = "0cc0a7ed"
		direct := dialDataNode(dataAddrs[0])
		defer func() { gomega.Expect(direct.Close()).To(gomega.Succeed()) }()
		// Occupy node 0 alone, the way a session created through another liaison would.
		occupy := &transferv1.PlanRequest{
			Selectors: streamSelector,
			Session:   &transferv1.PlanRequest_Create{Create: &transferv1.CreateSession{Id: occupant}},
		}
		_, err := transfertest.Plan(direct, occupy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(streamLease(dataDirs[0], occupant)).To(gomega.BeAnExistingFile())

		_, err = transfertest.Plan(conn, transfertest.CreateRequest(streamSelector, false))
		gomega.Expect(status.Code(err)).To(gomega.Equal(codes.AlreadyExists))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("aborted, snapshots released: data node " + nodeNames[0] + ": "))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring(occupant))
		gomega.Expect(status.Convert(err).Message()).NotTo(gomega.ContainSubstring("rpc error"), "the node's message is unwrapped, not nested")
		// Node 1 accepted the request and snapshotted (or was cut short); the rollback must have
		// removed every trace of it, while node 0 keeps its occupant.
		gomega.Expect(transfertest.ExportEntries(dataDirs[1], "stream")).To(gomega.BeEmpty())
		gomega.Expect(sessionsByNode()).To(gomega.Equal(map[string]string{nodeNames[0]: occupant, nodeNames[1]: ""}))

		gomega.Expect(transfertest.Release(direct, occupant)).To(gomega.BeEmpty())
		gomega.Expect(transfertest.ExportEntries(dataDirs[0], "stream")).To(gomega.BeEmpty())
	})

	ginkgo.It("snapshots and inventories every catalog on every data node", func() {
		ginkgo.By("Waiting for the measure and trace data to be flushed")
		gomega.Eventually(func(g gomega.Gomega) {
			frames, err := transfertest.Plan(conn, &transferv1.PlanRequest{Selectors: []*transferv1.Selector{
				{Catalog: commonv1.Catalog_CATALOG_MEASURE, Groups: []string{"sw_metric"}},
				{Catalog: commonv1.Catalog_CATALOG_TRACE, Groups: []string{"test-trace-group"}},
			}})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(catalogParts(frames, commonv1.Catalog_CATALOG_MEASURE, "sw_metric")).To(gomega.BeNumerically(">", 0))
			g.Expect(catalogParts(frames, commonv1.Catalog_CATALOG_TRACE, "test-trace-group")).To(gomega.BeNumerically(">", 0))
		}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())

		frames := transfertest.MustPlan(conn, transfertest.CreateRequest(nil, false))
		id := frames[0].GetCreated().GetSessionId()
		gomega.Expect(id).NotTo(gomega.BeEmpty())
		for _, dir := range dataDirs {
			for _, catalog := range transfertest.Catalogs {
				gomega.Expect(transfertest.LeaseFile(dir, catalog, id)).To(gomega.BeAnExistingFile(), "%s on %s", catalog, dir)
			}
		}
		// Both the create plan and a later ReadSession plan read the session snapshots, so each
		// lists the data the suite wrote in every catalog.
		expectEveryCatalogInventoried(frames)
		expectEveryCatalogInventoried(transfertest.MustPlan(conn, transfertest.ReadRequest(id)))
		gomega.Expect(transfertest.Release(conn, id)).To(gomega.BeEmpty())
		for _, dir := range dataDirs {
			for _, catalog := range transfertest.Catalogs {
				gomega.Expect(transfertest.ExportEntries(dir, catalog)).To(gomega.BeEmpty(), "%s on %s", catalog, dir)
			}
		}
	})

	ginkgo.It("keeps serving the surviving node after a data node dies", func() {
		survivor, dead := nodeNames[0], nodeNames[1]
		// KeepStoppedNodes leaves the dead node in the discovery file, so the liaison keeps it
		// registered and has to account for it as unreachable on every plan.
		closeDataNodes[1]()
		closeDataNodes[1] = func() {}
		gomega.Eventually(func(g gomega.Gomega) {
			frames, err := transfertest.Plan(conn, &transferv1.PlanRequest{Selectors: streamSelector})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			summary := transfertest.Summary(frames)
			g.Expect(summary.GetUnreachableNodes()).To(gomega.Equal([]string{dead}))
			g.Expect(summary.GetAnsweredNodes()).To(gomega.Equal([]string{survivor}))
			g.Expect(unitNodes(frames)).To(gomega.Equal([]string{survivor}), "only the surviving node's units are reported")
			g.Expect(registeredDataNodes()).To(gomega.ContainElement(dead), "the registry still lists the dead node")
		}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())
		// LIST is strict about a silent node; HEARTBEAT and RELEASE report it in its own frame.
		_, err := transfertest.Sessions(conn, &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_LIST})
		gomega.Expect(status.Code(err)).To(gomega.Equal(codes.Unavailable), "LIST fails while a node does not answer: %v", err)
		gomega.Expect(err.Error()).To(gomega.ContainSubstring(dead))
		const absent = "abad1dea"
		renewed := heartbeat(absent)
		gomega.Expect(renewed).To(gomega.HaveLen(2))
		gomega.Expect(renewed[dead]).NotTo(gomega.BeEmpty(), "the dead node answers an error frame")
		gomega.Expect(renewed[survivor]).To(gomega.ContainSubstring("not found on this node"))
		released := transfertest.ReleaseOutcome(conn, absent)
		gomega.Expect(released.Released).To(gomega.BeEmpty(), "no node held the absent id")
		gomega.Expect(released.NotHeld).To(gomega.Equal([]string{survivor}), "the survivor answers none for an absent id")
		var failed []string
		for _, f := range released.Failed {
			failed = append(failed, f.GetNodeId())
		}
		gomega.Expect(failed).To(gomega.Equal([]string{dead}), "the dead node answers an error frame")
		// A create in strict mode cannot skip the dead node.
		_, err = transfertest.Plan(conn, transfertest.CreateRequest(streamSelector, false))
		gomega.Expect(status.Code(err)).To(gomega.Equal(codes.Unavailable))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("aborted, snapshots released: data node " + dead + ": "))
		gomega.Expect(transfertest.ExportEntries(dataDirs[0], "stream")).To(gomega.BeEmpty(), "the survivor's half of the session is rolled back")

		// Once the operator drops the dead node from discovery (a data node registers under its
		// own name) the registry converges on the survivor and a session is created there alone.
		config.NodeDiscovery.FileWriter.RemoveNode(dead)
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(registeredDataNodes()).To(gomega.Equal([]string{survivor}))
			frames, err := transfertest.Plan(conn, &transferv1.PlanRequest{Selectors: streamSelector})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(transfertest.Summary(frames).GetUnreachableNodes()).To(gomega.BeEmpty())
			g.Expect(transfertest.Summary(frames).GetAnsweredNodes()).To(gomega.Equal([]string{survivor}))
		}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())
		frames := transfertest.MustPlan(conn, transfertest.CreateRequest(streamSelector, false))
		gomega.Expect(transfertest.Summary(frames).GetAnsweredNodes()).To(gomega.Equal([]string{survivor}))
		gomega.Expect(transfertest.UnitFrames(frames)).NotTo(gomega.BeEmpty())
		gomega.Expect(transfertest.Release(conn, frames[0].GetCreated().GetSessionId())).To(gomega.BeEmpty())
	})
})
