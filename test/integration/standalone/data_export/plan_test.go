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
	"github.com/apache/skywalking-banyandb/banyand/transfer/export"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	casesmeasuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	casespropertydata "github.com/apache/skywalking-banyandb/test/cases/property/data"
	casesstreamdata "github.com/apache/skywalking-banyandb/test/cases/stream/data"
	casestracedata "github.com/apache/skywalking-banyandb/test/cases/trace/data"
)

const (
	traceGroup    = "test-trace-group"
	propertyGroup = "sw"
)

var (
	dataDir  string
	conn     *grpc.ClientConn
	stopFunc func()
)

var _ = ginkgo.BeforeSuite(func() {
	gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())
	// The specs expire leases on purpose; the periodic sweep must not reclaim them mid-spec.
	export.SweepInterval = time.Hour
	var deferFn func()
	var err error
	dataDir, deferFn, err = test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ports, err := test.AllocateFreePorts(5)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	grpcAddr, _, closeFn := setup.ClosableStandalone(nil, dataDir, ports, "--stream-flush-timeout=500ms", "--measure-flush-timeout=500ms",
		"--trace-flush-timeout=500ms")
	stopFunc = func() {
		closeFn()
		deferFn()
	}
	conn, err = grpchelper.Conn(grpcAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	casesstreamdata.Write(conn, "sw", timestamp.NowMilli(), 500*time.Millisecond)
	ns := timestamp.NowMilli().UnixNano()
	casesmeasuredata.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json",
		time.Unix(0, ns-ns%int64(time.Minute)), 500*time.Millisecond)
	casestracedata.Write(conn, "sw", timestamp.NowMilli(), 500*time.Millisecond)
	casespropertydata.Write(conn, "sw1")
})

var _ = ginkgo.AfterSuite(func() {
	if conn != nil {
		gomega.Expect(conn.Close()).To(gomega.Succeed())
	}
	if stopFunc != nil {
		stopFunc()
	}
})

// list runs ACTION_LIST and returns the single frame a standalone process answers with.
func list() *transferv1.SessionsResponse {
	frames := transfertest.List(conn)
	gomega.ExpectWithOffset(1, frames).To(gomega.HaveLen(1), "a standalone process answers exactly one list frame")
	return frames[0]
}

// heartbeat runs ACTION_HEARTBEAT and returns the single frame a standalone process answers with.
func heartbeat(id string) *transferv1.SessionsResponse {
	frames := transfertest.Heartbeat(conn, id)
	gomega.ExpectWithOffset(1, frames).To(gomega.HaveLen(1), "a standalone process answers exactly one heartbeat frame")
	return frames[0]
}

// heartbeatErr runs ACTION_HEARTBEAT and returns the call's error.
func heartbeatErr(id string) error {
	_, err := transfertest.Sessions(conn, &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_HEARTBEAT, SessionId: id})
	return err
}

// shardsWithParts counts the shards with at least one part across the units of one catalog and group.
func shardsWithParts(frames []*transferv1.PlanResponse, catalog commonv1.Catalog, group string) int {
	var n int
	for _, f := range transfertest.UnitFrames(frames) {
		for _, u := range f.GetUnits() {
			seg := u.GetSegment()
			if seg.GetUnit().GetCatalog() != catalog || seg.GetUnit().GetGroup() != group {
				continue
			}
			for _, sh := range seg.GetShards() {
				if sh.GetPartsCount() > 0 {
					n++
				}
			}
		}
	}
	return n
}

// propertyUnit returns the property inventory of one group, or nil.
func propertyUnit(frames []*transferv1.PlanResponse, group string) *transferv1.PropertyInventory {
	for _, f := range transfertest.UnitFrames(frames) {
		for _, u := range f.GetUnits() {
			if p := u.GetProperty(); p != nil && p.GetGroup() == group {
				return p
			}
		}
	}
	return nil
}

// expectEveryCatalogInventoried asserts that the frames list the data the suite wrote in each
// of the four catalogs.
func expectEveryCatalogInventoried(frames []*transferv1.PlanResponse) {
	gomega.ExpectWithOffset(1, shardsWithParts(frames, commonv1.Catalog_CATALOG_STREAM, "default")).To(gomega.BeNumerically(">", 0), "stream")
	gomega.ExpectWithOffset(1, shardsWithParts(frames, commonv1.Catalog_CATALOG_MEASURE, "sw_metric")).To(gomega.BeNumerically(">", 0), "measure")
	gomega.ExpectWithOffset(1, shardsWithParts(frames, commonv1.Catalog_CATALOG_TRACE, traceGroup)).To(gomega.BeNumerically(">", 0), "trace")
	prop := propertyUnit(frames, propertyGroup)
	gomega.ExpectWithOffset(1, prop).NotTo(gomega.BeNil(), "property")
	var docs, bytes uint64
	for _, sh := range prop.GetShards() {
		docs += sh.GetDocCount()
		bytes += sh.GetEstimatedBytes()
	}
	gomega.ExpectWithOffset(1, docs).To(gomega.BeNumerically(">", 0), "property documents")
	gomega.ExpectWithOffset(1, bytes).To(gomega.BeNumerically(">", 0), "property bytes")
}

// expectSingleSession asserts that the process holds exactly one session, with the given id.
func expectSingleSession(id string) {
	frame := list()
	gomega.ExpectWithOffset(1, frame.GetSession()).NotTo(gomega.BeNil())
	gomega.ExpectWithOffset(1, frame.GetSession().GetSessionId()).To(gomega.Equal(id))
}

var _ = ginkgo.Describe("ExportService on a standalone process", ginkgo.Ordered, func() {
	var self string
	streamSelector := []*transferv1.Selector{{Catalog: commonv1.Catalog_CATALOG_STREAM}}
	measureSelector := []*transferv1.Selector{{Catalog: commonv1.Catalog_CATALOG_MEASURE, Groups: []string{"sw_metric"}}}
	streamLease := func(id string) string { return transfertest.LeaseFile(dataDir, "stream", id) }

	ginkgo.BeforeAll(func() {
		state, err := databasev1.NewClusterStateServiceClient(conn).GetClusterState(context.Background(), &databasev1.GetClusterStateRequest{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(state.GetRouteTables()["tire2"].GetRegistered()).To(gomega.BeEmpty(), "standalone has no tire2 table; the fallback rule must apply")
		cur, err := databasev1.NewNodeQueryServiceClient(conn).GetCurrentNode(context.Background(), &databasev1.GetCurrentNodeRequest{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(cur.GetNode().GetRoles()).To(gomega.ContainElements(databasev1.Role_ROLE_LIAISON, databasev1.Role_ROLE_DATA))
		self = cur.GetNode().GetMetadata().GetName()
		gomega.Eventually(func(g gomega.Gomega) {
			selectors := append(append([]*transferv1.Selector{}, streamSelector...), measureSelector...)
			selectors = append(selectors, &transferv1.Selector{Catalog: commonv1.Catalog_CATALOG_TRACE, Groups: []string{traceGroup}})
			frames, planErr := transfertest.Plan(conn, &transferv1.PlanRequest{Selectors: selectors})
			g.Expect(planErr).NotTo(gomega.HaveOccurred())
			g.Expect(shardsWithParts(frames, commonv1.Catalog_CATALOG_STREAM, "default")).To(gomega.BeNumerically(">", 0))
			g.Expect(shardsWithParts(frames, commonv1.Catalog_CATALOG_MEASURE, "sw_metric")).To(gomega.BeNumerically(">", 0))
			g.Expect(shardsWithParts(frames, commonv1.Catalog_CATALOG_TRACE, traceGroup)).To(gomega.BeNumerically(">", 0))
		}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("answers a dry run from the live directories without creating or changing anything", func() {
		liveDir := filepath.Join(dataDir, "stream", "data")
		before := transfertest.StableTree(liveDir)
		gomega.Expect(before).NotTo(gomega.BeEmpty())
		frames := transfertest.MustPlan(conn, &transferv1.PlanRequest{Selectors: streamSelector})
		units := transfertest.UnitFrames(frames)
		gomega.Expect(units).To(gomega.HaveLen(len(frames)-1), "a dry run streams units frames and one summary, never a created frame")
		// The "sw" stream written by the suite lives in the "default" group.
		var defaultUnits int
		for _, f := range units {
			gomega.Expect(f.GetNodeId()).To(gomega.Equal(self))
			for _, u := range f.GetUnits() {
				if u.GetSegment().GetUnit().GetGroup() == "default" {
					defaultUnits++
					gomega.Expect(u.GetSegment().GetSegmentVersion()).NotTo(gomega.BeEmpty())
				}
			}
		}
		gomega.Expect(defaultUnits).To(gomega.BeNumerically(">", 0))
		gomega.Expect(shardsWithParts(frames, commonv1.Catalog_CATALOG_STREAM, "default")).To(gomega.BeNumerically(">", 0))
		summary := transfertest.Summary(frames)
		gomega.Expect(summary.GetAnsweredNodes()).To(gomega.ConsistOf(self))
		gomega.Expect(summary.GetUnreachableNodes()).To(gomega.BeEmpty())
		for _, catalog := range transfertest.Catalogs {
			gomega.Expect(transfertest.ExportEntries(dataDir, catalog)).To(gomega.BeEmpty(), "a dry run snapshots nothing: %s", catalog)
		}
		gomega.Expect(transfertest.TreeDiff(before, transfertest.HashTree(liveDir))).To(gomega.BeEmpty())
	})

	ginkgo.It("inventories the measure catalog", func() {
		frames := transfertest.MustPlan(conn, &transferv1.PlanRequest{Selectors: measureSelector})
		gomega.Expect(shardsWithParts(frames, commonv1.Catalog_CATALOG_MEASURE, "sw_metric")).To(gomega.BeNumerically(">", 0))
		var rows uint64
		for _, f := range transfertest.UnitFrames(frames) {
			for _, u := range f.GetUnits() {
				gomega.Expect(u.GetSegment().GetUnit().GetCatalog()).To(gomega.Equal(commonv1.Catalog_CATALOG_MEASURE))
				gomega.Expect(u.GetSegment().GetUnit().GetGroup()).To(gomega.Equal("sw_metric"))
				for _, sh := range u.GetSegment().GetShards() {
					rows += sh.GetTotalCount()
				}
			}
		}
		gomega.Expect(rows).To(gomega.BeNumerically(">", 0))
	})

	ginkgo.It("inventories the property catalog from the live directories", func() {
		// The property index may not have committed the suite's writes yet, so poll.
		gomega.Eventually(func(g gomega.Gomega) {
			frames, err := transfertest.Plan(conn, &transferv1.PlanRequest{Selectors: []*transferv1.Selector{{Catalog: commonv1.Catalog_CATALOG_PROPERTY}}})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			prop := propertyUnit(frames, propertyGroup)
			g.Expect(prop).NotTo(gomega.BeNil(), "a dry run lists the property group")
			var bytes uint64
			for _, sh := range prop.GetShards() {
				bytes += sh.GetEstimatedBytes()
			}
			g.Expect(bytes).To(gomega.BeNumerically(">", 0), "property bytes")
		}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())
		for _, catalog := range transfertest.Catalogs {
			gomega.Expect(transfertest.ExportEntries(dataDir, catalog)).To(gomega.BeEmpty(), "a dry run snapshots nothing: %s", catalog)
		}
	})

	ginkgo.It("runs the session lifecycle in-process", func() {
		frames := transfertest.MustPlan(conn, transfertest.CreateRequest(streamSelector, false))
		id := frames[0].GetCreated().GetSessionId()
		gomega.Expect(id).NotTo(gomega.BeEmpty())
		gomega.Expect(streamLease(id)).To(gomega.BeAnExistingFile())
		gomega.Expect(transfertest.UnitFrames(frames)).NotTo(gomega.BeEmpty())

		first := list()
		gomega.Expect(first.GetNodeId()).To(gomega.Equal(self))
		gomega.Expect(first.GetSession()).NotTo(gomega.BeNil())
		gomega.Expect(first.GetSession().GetSessionId()).To(gomega.Equal(id))

		for _, catalog := range transfertest.Catalogs {
			if catalog != "stream" {
				gomega.Expect(transfertest.SessionDir(dataDir, catalog, id)).NotTo(gomega.BeADirectory(), "unselected catalogs get no session dir")
			}
		}
		renewedBefore := transfertest.ReadLastHeartbeat(streamLease(id))
		time.Sleep(10 * time.Millisecond)
		again := transfertest.MustPlan(conn, transfertest.ReadRequest(id))
		gomega.Expect(transfertest.UnitFrames(again)).NotTo(gomega.BeEmpty())
		gomega.Expect(transfertest.ReadLastHeartbeat(streamLease(id))).To(gomega.BeNumerically(">", renewedBefore), "a ReadSession plan renews the lease")

		_, err := transfertest.Plan(conn, transfertest.CreateRequest(streamSelector, false))
		gomega.Expect(status.Code(err)).To(gomega.Equal(codes.AlreadyExists))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring(id))
		// The refused attempt must leave the occupant exactly as it was.
		expectSingleSession(id)
		gomega.Expect(streamLease(id)).To(gomega.BeAnExistingFile())

		released := transfertest.ReleaseOutcome(conn, id)
		gomega.Expect(released.Failed).To(gomega.BeEmpty())
		gomega.Expect(released.Released).To(gomega.ConsistOf(self), "the process held the session")
		gomega.Expect(transfertest.SessionDir(dataDir, "stream", id)).NotTo(gomega.BeADirectory())
		releasedAgain := transfertest.ReleaseOutcome(conn, id)
		gomega.Expect(releasedAgain.Failed).To(gomega.BeEmpty())
		gomega.Expect(releasedAgain.NotHeld).To(gomega.ConsistOf(self), "a second release finds nothing to delete")
		// The released session is NotFound on the process's own export service. A renew reports that in
		// the liaison's own frame; a ReadSession plan degrades the single node to unreachable
		// exactly like the fan-out does outside create mode.
		gomega.Expect(list().GetNone()).NotTo(gomega.BeNil(), "a released session is listed as none")
		lost := heartbeat(id)
		gomega.Expect(lost.GetNodeId()).To(gomega.Equal(self))
		gomega.Expect(lost.GetDone()).To(gomega.BeNil())
		gomega.Expect(lost.GetError()).To(gomega.ContainSubstring("not found on this node"))
		gone := transfertest.MustPlan(conn, transfertest.ReadRequest(id))
		gomega.Expect(gone).To(gomega.HaveLen(1), "nothing but the summary frame")
		summary := transfertest.Summary(gone)
		gomega.Expect(summary.GetAnsweredNodes()).To(gomega.BeEmpty())
		gomega.Expect(summary.GetUnreachableNodes()).To(gomega.Equal([]string{self}))
	})

	var sessionID string

	ginkgo.It("snapshots every catalog when no selector is given", func() {
		frames := transfertest.MustPlan(conn, transfertest.CreateRequest(nil, false))
		sessionID = frames[0].GetCreated().GetSessionId()
		created := &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Created{Created: &transferv1.SessionCreated{SessionId: sessionID}}}
		gomega.Expect(proto.Equal(frames[0], created)).To(gomega.BeTrue(), "the first frame is the created frame carrying nothing but the session id")
		gomega.Expect(sessionID).NotTo(gomega.BeEmpty())
		for _, catalog := range transfertest.Catalogs {
			gomega.Expect(transfertest.LeaseFile(dataDir, catalog, sessionID)).To(gomega.BeAnExistingFile(), catalog)
		}
		frame := list()
		gomega.Expect(frame.GetSession()).NotTo(gomega.BeNil())
		gomega.Expect(frame.GetSession().GetSessionId()).To(gomega.Equal(sessionID))
		gomega.Expect(frame.GetSession().GetCatalogs()).To(gomega.ConsistOf(
			commonv1.Catalog_CATALOG_STREAM, commonv1.Catalog_CATALOG_MEASURE, commonv1.Catalog_CATALOG_TRACE, commonv1.Catalog_CATALOG_PROPERTY))
		for _, catalog := range transfertest.Catalogs {
			entries, readErr := os.ReadDir(transfertest.SessionDir(dataDir, catalog, sessionID))
			gomega.Expect(readErr).NotTo(gomega.HaveOccurred(), catalog)
			gomega.Expect(len(entries)).To(gomega.BeNumerically(">", 1), "%s holds a snapshot next to its lease", catalog)
		}
		// Both the create plan and a later ReadSession plan read the session snapshots, so each
		// lists the data the suite wrote in every catalog.
		expectEveryCatalogInventoried(frames)
		expectEveryCatalogInventoried(transfertest.MustPlan(conn, transfertest.ReadRequest(sessionID)))
	})

	ginkgo.It("renews every catalog copy of the lease with a heartbeat and answers done", func() {
		before := map[string]int64{}
		for _, catalog := range transfertest.Catalogs {
			before[catalog] = transfertest.ReadLastHeartbeat(transfertest.LeaseFile(dataDir, catalog, sessionID))
		}
		time.Sleep(10 * time.Millisecond)
		frame := heartbeat(sessionID)
		gomega.Expect(frame.GetNodeId()).To(gomega.Equal(self))
		gomega.Expect(frame.GetDone()).NotTo(gomega.BeNil(), "a renewed session answers done: %+v", frame)
		gomega.Expect(frame.GetError()).To(gomega.BeEmpty())
		for _, catalog := range transfertest.Catalogs {
			gomega.Expect(transfertest.ReadLastHeartbeat(transfertest.LeaseFile(dataDir, catalog, sessionID))).To(gomega.BeNumerically(">", before[catalog]), catalog)
		}
	})

	ginkgo.It("refuses to replace a live occupant unless preempt is set", func() {
		_, err := transfertest.Plan(conn, transfertest.CreateRequest(streamSelector, false))
		gomega.Expect(status.Code(err)).To(gomega.Equal(codes.AlreadyExists))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring(sessionID))
		expectSingleSession(sessionID)
		for _, catalog := range transfertest.Catalogs {
			gomega.Expect(transfertest.LeaseFile(dataDir, catalog, sessionID)).To(gomega.BeAnExistingFile(), catalog)
		}

		frames := transfertest.MustPlan(conn, transfertest.CreateRequest(streamSelector, true))
		gomega.Expect(transfertest.Summary(frames).GetPreemptedSessionIds()).To(gomega.ConsistOf(sessionID))
		gomega.Expect(transfertest.UnitFrames(frames)).NotTo(gomega.BeEmpty())
		for _, catalog := range transfertest.Catalogs {
			gomega.Expect(transfertest.SessionDir(dataDir, catalog, sessionID)).NotTo(gomega.BeADirectory(), catalog)
		}
		sessionID = frames[0].GetCreated().GetSessionId()
		gomega.Expect(streamLease(sessionID)).To(gomega.BeAnExistingFile())
		expectSingleSession(sessionID)
	})

	ginkgo.It("refuses an expired session without releasing it", func() {
		transfertest.ExpireLease(streamLease(sessionID))
		_, err := transfertest.Plan(conn, transfertest.ReadRequest(sessionID))
		gomega.Expect(status.Code(err)).To(gomega.Equal(codes.FailedPrecondition))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("expired"))
		gomega.Expect(transfertest.SessionDir(dataDir, "stream", sessionID)).To(gomega.BeADirectory(), "a failed session-mode plan never releases")
		gomega.Expect(streamLease(sessionID)).To(gomega.BeAnExistingFile())
		err = heartbeatErr(sessionID)
		gomega.Expect(status.Code(err)).To(gomega.Equal(codes.FailedPrecondition), "an expired session stops the heartbeat: %v", err)
		gomega.Expect(streamLease(sessionID)).To(gomega.BeAnExistingFile())
		gomega.Expect(status.Code(heartbeatErr(""))).To(gomega.Equal(codes.InvalidArgument), "a heartbeat needs the session id")

		gomega.Expect(transfertest.Release(conn, sessionID)).To(gomega.BeEmpty())
		gomega.Expect(transfertest.SessionDir(dataDir, "stream", sessionID)).NotTo(gomega.BeADirectory())
	})

	ginkgo.It("refuses a long-silent occupant without preempt and removes it with preempt", func() {
		frames := transfertest.MustPlan(conn, transfertest.CreateRequest(streamSelector, false))
		silent := frames[0].GetCreated().GetSessionId()
		// The owner stopped heartbeating long ago while its lease is still far from expiring:
		// create takes no liveness decision, so the occupant is refused all the same.
		transfertest.RewriteLease(streamLease(silent), func(l *export.Lease) {
			l.LastHeartbeatAt = time.Now().Add(-time.Hour).UnixNano()
		})
		gomega.Expect(transfertest.ReadLease(streamLease(silent)).ExpiresAt).To(gomega.BeNumerically(">", time.Now().UnixNano()))
		_, err := transfertest.Plan(conn, transfertest.CreateRequest(streamSelector, false))
		gomega.Expect(status.Code(err)).To(gomega.Equal(codes.AlreadyExists))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring(silent))
		expectSingleSession(silent)

		frames = transfertest.MustPlan(conn, transfertest.CreateRequest(streamSelector, true))
		gomega.Expect(transfertest.Summary(frames).GetPreemptedSessionIds()).To(gomega.ConsistOf(silent))
		gomega.Expect(transfertest.UnitFrames(frames)).NotTo(gomega.BeEmpty())
		gomega.Expect(transfertest.SessionDir(dataDir, "stream", silent)).NotTo(gomega.BeADirectory())
		sessionID = frames[0].GetCreated().GetSessionId()
		gomega.Expect(streamLease(sessionID)).To(gomega.BeAnExistingFile())
		expectSingleSession(sessionID)

		gomega.Expect(transfertest.Release(conn, sessionID)).To(gomega.BeEmpty())
		gomega.Expect(transfertest.ExportEntries(dataDir, "stream")).To(gomega.BeEmpty(), "no export snapshot survives the release")
	})
})
