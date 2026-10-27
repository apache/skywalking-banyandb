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

package data_import_test

import (
	"context"
	"os"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
	"github.com/apache/skywalking-banyandb/pkg/test/transfer/topncheck"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

var flushFlags = []string{"--stream-flush-timeout=500ms", "--measure-flush-timeout=500ms", "--trace-flush-timeout=500ms"}

// instance is one standalone process of the suite.
type instance struct {
	conn *grpc.ClientConn
	stop func()
	addr string
}

var (
	source *instance
	// artifactA is the source's export every import of the suite reads.
	artifactA string
	// workDir holds every export of the suite, so an export outlives the spec that made it.
	workDir string
)

// startStandalone starts a standalone process on free ports, preloaded with the test schema
// or empty, and connects to it.
func startStandalone(preload bool) *instance {
	dataDir, deferFn, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ports, err := test.AllocateFreePorts(5)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	start := setup.EmptyClosableStandalone
	if preload {
		start = setup.ClosableStandalone
	}
	addr, _, closeFn := start(nil, dataDir, ports, flushFlags...)
	conn, err := grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return &instance{addr: addr, conn: conn, stop: func() {
		_ = conn.Close()
		closeFn()
		deferFn()
	}}
}

// startTarget starts an empty standalone target that is stopped when the calling node's
// container finishes.
func startTarget() *instance {
	t := startStandalone(false)
	ginkgo.DeferCleanup(t.stop)
	return t
}

var _ = ginkgo.BeforeSuite(func() {
	gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())
	var err error
	workDir, err = os.MkdirTemp("", "data-import-")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	source = startStandalone(true)
	conn := source.conn
	transfertest.WriteSuiteData(conn)

	ginkgo.By("Waiting for every catalog of the source to be flushed")
	gomega.Eventually(func(g gomega.Gomega) {
		frames, planErr := transfertest.Plan(conn, &transferv1.PlanRequest{})
		g.Expect(planErr).NotTo(gomega.HaveOccurred())
		g.Expect(transfertest.GroupRows(frames, commonv1.Catalog_CATALOG_STREAM, transfertest.StreamGroup)).To(gomega.BeNumerically(">", 0))
		// The written points and at least one _top_n_result row the Top-N processor wrote.
		g.Expect(transfertest.GroupRows(frames, commonv1.Catalog_CATALOG_MEASURE, transfertest.MeasureGroup)).
			To(gomega.BeNumerically(">", transfertest.MeasureRows+transfertest.TopNSourcePoints), "Top-N results")
		g.Expect(transfertest.GroupRows(frames, commonv1.Catalog_CATALOG_TRACE, transfertest.TraceGroup)).To(gomega.BeNumerically(">", 0))
		g.Expect(transfertest.IndexModeDocs(frames, transfertest.IndexModeGroup)).To(gomega.BeNumerically(">=", 3), "index-mode rows")
		// The property index commits on its own schedule: wait for the live properties and the tombstone.
		g.Expect(transfertest.PropertyDocs(frames, transfertest.PropertyGroup)).To(gomega.BeNumerically(">=", 2))
	}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())

	artifactA = transfertest.ExportFrom(source.addr, workDir)
	a := transfertest.ReadArtifact(artifactA, false)
	a.VerifyManifest()
	topNRows := topncheck.ExpectTopNExported(a)
	gomega.Expect(topNRows).To(gomega.BeNumerically(">", 0), "the source artifact holds Top-N results")
	gomega.Expect(a.LiveProperty()).NotTo(gomega.BeNil(), "the source artifact holds a live property")
	tomb := latestTombstone(a)
	gomega.Expect(tomb).NotTo(gomega.BeNil(), "the source artifact holds the tombstone")
	gomega.Expect(transfertest.IsLiveProperty(tomb)).To(gomega.BeFalse(), "the tombstone's latest revision is deleted")
})

var _ = ginkgo.AfterSuite(func() {
	if source != nil {
		source.stop()
	}
	if workDir != "" {
		gomega.Expect(os.RemoveAll(workDir)).To(gomega.Succeed())
	}
})

// expectRoundTrip re-exports the target at addr until it holds the logical rows of artifactA
// in the given groups (every group of artifactA when none is given) and returns the export.
func expectRoundTrip(addr string, groups ...string) string {
	return transfertest.ExportEventually(addr, workDir, func(g gomega.Gomega, dir string) {
		transfertest.ExpectRoundTrip(g, artifactA, dir, groups...)
	})
}

// runImport imports artifactA into the target at addr and returns the run.
func runImport(addr string, mutate func(o *config.Import)) transfertest.ImportRun {
	opts := transfertest.DefaultImportOptions(artifactA)
	if mutate != nil {
		mutate(&opts)
	}
	return transfertest.RunImport(context.Background(), addr, opts, nil)
}

// expectImported asserts that an import succeeded and its verify passed.
func expectImported(run transfertest.ImportRun) {
	gomega.ExpectWithOffset(1, run.Err).NotTo(gomega.HaveOccurred(), run.Log)
	gomega.ExpectWithOffset(1, run.Result.Verify).NotTo(gomega.BeNil(), "verify ran")
	gomega.ExpectWithOffset(1, run.Result.Verify.Failed()).To(gomega.BeFalse(), run.Log)
}

// schemaSnapshot hashes every schema resource of the instance at addr, revisions included.
func schemaSnapshot(conn *grpc.ClientConn) map[string]string {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	groups, err := cluster.FetchGroups(ctx, conn)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	dir := ginkgo.GinkgoT().TempDir()
	_, err = schemasync.Export(ctx, conn, groups, dir)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	return transfertest.HashTree(dir)
}

// totalData sums every row and document a plan of the instance reports.
func totalData(conn *grpc.ClientConn) uint64 {
	frames := transfertest.MustPlan(conn, &transferv1.PlanRequest{})
	var n uint64
	for _, f := range transfertest.UnitFrames(frames) {
		for _, u := range f.GetUnits() {
			for _, sh := range u.GetSegment().GetShards() {
				n += sh.GetTotalCount() + uint64(sh.GetPartsCount())
			}
			n += u.GetSegment().GetSegmentLevel().GetDocCount()
			for _, sh := range u.GetProperty().GetShards() {
				n += sh.GetDocCount()
			}
		}
	}
	return n
}

// latestTombstone returns the latest revision of the tombstone the suite wrote, or nil.
func latestTombstone(a *transfertest.Artifact) map[string]string {
	return a.LatestProperties()[transfertest.TombstoneProperty().GetId()]
}

// resourceRows counts the rows of one resource of a catalog and group, across nodes.
func resourceRows(a *transfertest.Artifact, catalog, group, name string) int {
	var n int
	for _, rows := range a.RowsOf(catalog, group, name) {
		n += len(rows)
	}
	return n
}
