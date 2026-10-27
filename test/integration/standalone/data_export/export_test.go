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
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
	"github.com/apache/skywalking-banyandb/pkg/test/transfer/topncheck"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	casesstreamdata "github.com/apache/skywalking-banyandb/test/cases/stream/data"
)

// streamRows sums the total_count of the stream group's shards a dry-run plan reports.
func streamRows() uint64 {
	frames := transfertest.MustPlan(conn, &transferv1.PlanRequest{Selectors: []*transferv1.Selector{
		{Catalog: commonv1.Catalog_CATALOG_STREAM, Groups: []string{transfertest.StreamGroup}},
	}})
	return transfertest.GroupRows(frames, commonv1.Catalog_CATALOG_STREAM, transfertest.StreamGroup)
}

// expectNoSession asserts that the process holds no export session and no snapshot.
func expectNoSession() {
	gomega.ExpectWithOffset(1, list().GetNone()).NotTo(gomega.BeNil(), "no session is listed")
	for _, catalog := range transfertest.Catalogs {
		gomega.ExpectWithOffset(1, transfertest.ExportEntries(dataDir, catalog)).To(gomega.BeEmpty(), "no export snapshot is left: %s", catalog)
	}
}

var _ = ginkgo.Describe("CSV export on a standalone process", ginkgo.Ordered, func() {
	var self string
	suiteData := func() transfertest.SuiteData {
		return transfertest.SuiteData{StreamBase: streamBase, MeasureBase: measureBase, TraceBase: traceBase, Interval: transfertest.WriteInterval}
	}
	allOnOneNode := func(string) bool { return true }

	ginkgo.BeforeAll(func() {
		cur, err := databasev1.NewNodeQueryServiceClient(conn).GetCurrentNode(context.Background(), &databasev1.GetCurrentNodeRequest{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		self = cur.GetNode().GetMetadata().GetName()
		ginkgo.By("Waiting for every catalog to be flushed")
		gomega.Eventually(func(g gomega.Gomega) {
			frames, planErr := transfertest.Plan(conn, &transferv1.PlanRequest{})
			g.Expect(planErr).NotTo(gomega.HaveOccurred())
			g.Expect(shardsWithParts(frames, commonv1.Catalog_CATALOG_STREAM, transfertest.StreamGroup)).To(gomega.BeNumerically(">", 0))
			g.Expect(shardsWithParts(frames, commonv1.Catalog_CATALOG_MEASURE, transfertest.MeasureGroup)).To(gomega.BeNumerically(">", 0))
			// The written points and at least one _top_n_result row the Top-N processor wrote.
			g.Expect(transfertest.GroupRows(frames, commonv1.Catalog_CATALOG_MEASURE, transfertest.MeasureGroup)).
				To(gomega.BeNumerically(">", transfertest.MeasureRows+transfertest.TopNSourcePoints), "Top-N results")
			g.Expect(shardsWithParts(frames, commonv1.Catalog_CATALOG_TRACE, transfertest.TraceGroup)).To(gomega.BeNumerically(">", 0))
			g.Expect(transfertest.IndexModeDocs(frames, transfertest.IndexModeGroup)).To(gomega.BeNumerically(">=", 3), "index-mode rows")
			prop := propertyUnit(frames, transfertest.PropertyGroup)
			g.Expect(prop).NotTo(gomega.BeNil())
			var docs uint64
			for _, sh := range prop.GetShards() {
				docs += sh.GetDocCount()
			}
			// The property index commits on its own schedule: wait for the live property and the tombstone.
			g.Expect(docs).To(gomega.BeNumerically(">=", 2))
		}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())
		expectNoSession()
	})

	ginkgo.It("exports every catalog into CSV files holding exactly the written rows", func() {
		out := filepath.Join(ginkgo.GinkgoT().TempDir(), "export")
		opts := transfertest.DefaultExportOptions(out)
		opts.CSVAnnotations = true
		run := transfertest.RunExport(context.Background(), grpcAddr, opts, nil)
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		expectNoSession()

		a := transfertest.ReadArtifact(out, true)
		a.VerifyManifest()
		gomega.Expect(a.Manifest.Compress).To(gomega.Equal("gzip"))
		gomega.Expect(a.Manifest.Source.Nodes).To(gomega.HaveLen(1))
		gomega.Expect(a.Manifest.Source.Nodes[0].ID).To(gomega.Equal(self))
		gomega.Expect(a.NodeDirs()).To(gomega.Equal([]string{manifest.NodeDir(self)}))
		for _, f := range a.Files {
			gomega.Expect(f.Path).To(gomega.HaveSuffix(".csv.gz"))
		}
		a.VerifySuiteData(suiteData(), allOnOneNode)
		gomega.Expect(topncheck.ExpectTopNExported(a)).To(gomega.BeNumerically(">", 0), "the Top-N results are exported")
	})

	ginkgo.It("writes plain CSV and rolls small files with the header repeated", func() {
		out := filepath.Join(ginkgo.GinkgoT().TempDir(), "export")
		opts := transfertest.DefaultExportOptions(out)
		opts.Compress = transferv1.Compress_COMPRESS_NONE
		opts.MaxFileSize = 1
		run := transfertest.RunExport(context.Background(), grpcAddr, opts, nil)
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		expectNoSession()
		a := transfertest.ReadArtifact(out, false)
		a.VerifyManifest()
		gomega.Expect(a.Manifest.Compress).To(gomega.Equal("none"))
		transfertest.ExpectRolledFiles(a)
		a.VerifySuiteData(suiteData(), allOnOneNode)
	})

	ginkgo.It("writes only the schema and the manifest with --schema-only and creates no session", func() {
		out := filepath.Join(ginkgo.GinkgoT().TempDir(), "export")
		opts := transfertest.DefaultExportOptions(out)
		opts.SchemaOnly = true
		run := transfertest.RunExport(context.Background(), grpcAddr, opts, nil)
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		gomega.Expect(run.SessionID()).To(gomega.BeEmpty())
		expectNoSession()
		a := transfertest.ReadArtifact(out, false)
		gomega.Expect(a.Manifest.SchemaOnly).To(gomega.BeTrue())
		gomega.Expect(a.Manifest.Units).To(gomega.BeEmpty())
		gomega.Expect(a.Files).To(gomega.BeEmpty())
		gomega.Expect(filepath.Join(out, "nodes")).NotTo(gomega.BeADirectory())
		a.VerifyManifest()
	})

	ginkgo.It("refuses a non-empty output directory", func() {
		out := ginkgo.GinkgoT().TempDir()
		gomega.Expect(os.WriteFile(filepath.Join(out, "leftover"), []byte("x"), 0o600)).To(gomega.Succeed())
		run := transfertest.RunExport(context.Background(), grpcAddr, transfertest.DefaultExportOptions(out), nil)
		gomega.Expect(run.ExitCode()).To(gomega.Equal(exitcode.ExitUsage), run.Log)
		expectNoSession()
	})

	ginkgo.It("exports the session snapshot and leaves out data written after the session was created", func() {
		out := filepath.Join(ginkgo.GinkgoT().TempDir(), "export")
		before := streamRows()
		var wrote bool
		run := transfertest.RunExport(context.Background(), grpcAddr, transfertest.DefaultExportOptions(out), func(line string) {
			// The plan and its snapshots are complete once the workers start.
			if !strings.HasPrefix(line, "exporting ") || wrote {
				return
			}
			wrote = true
			casesstreamdata.Write(conn, transfertest.StreamName, timestamp.NowMilli(), transfertest.WriteInterval)
			gomega.Eventually(streamRows, flags.EventuallyTimeout, time.Second).Should(gomega.BeNumerically(">", before),
				"the late rows reach the live parts while the export runs")
		})
		gomega.Expect(wrote).To(gomega.BeTrue())
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		a := transfertest.ReadArtifact(out, false)
		a.VerifyManifest()
		// The late elements reuse the element ids with later timestamps; any of them would
		// show up as a duplicate or a mismatching timestamp.
		a.VerifySuiteData(suiteData(), allOnOneNode)
		expectNoSession()
	})

	ginkgo.It("refuses a second export while the first holds its session and lets --preempt take over", func() {
		firstOut := filepath.Join(ginkgo.GinkgoT().TempDir(), "first")
		slow := transfertest.DefaultExportOptions(firstOut)
		slow.NodeRowRateLimit = 1
		started := make(chan struct{})
		firstDone := make(chan transfertest.ExportRun, 1)
		//panicdiag:allow-rawgo the first export runs while the spec starts a second one
		go func() {
			defer ginkgo.GinkgoRecover()
			var once sync.Once
			firstDone <- transfertest.RunExport(context.Background(), grpcAddr, slow, func(line string) {
				if strings.Contains(line, "] ") && strings.Contains(line, " started: ") {
					once.Do(func() { close(started) })
				}
			})
		}()
		gomega.Eventually(started, flags.EventuallyTimeout).Should(gomega.BeClosed())
		holder := list().GetSession().GetSessionId()
		gomega.Expect(holder).NotTo(gomega.BeEmpty())

		refused := transfertest.RunExport(context.Background(), grpcAddr, transfertest.DefaultExportOptions(filepath.Join(ginkgo.GinkgoT().TempDir(), "second")), nil)
		gomega.Expect(refused.ExitCode()).To(gomega.Equal(exitcode.ExitPreflight), refused.Log)
		gomega.Expect(refused.Err.Error()).To(gomega.ContainSubstring(holder))
		expectSingleSession(holder)

		preemptOut := filepath.Join(ginkgo.GinkgoT().TempDir(), "preempt")
		preempt := transfertest.DefaultExportOptions(preemptOut)
		preempt.Preempt = true
		taken := transfertest.RunExport(context.Background(), grpcAddr, preempt, nil)
		gomega.Expect(taken.Err).NotTo(gomega.HaveOccurred(), taken.Log)
		gomega.Expect(taken.Log).To(gomega.ContainSubstring(holder), "the preempting export names the session it preempted")
		transfertest.ReadArtifact(preemptOut, false).VerifyManifest()

		var first transfertest.ExportRun
		gomega.Eventually(firstDone, flags.EventuallyTimeout).Should(gomega.Receive(&first))
		gomega.Expect(first.ExitCode()).To(gomega.Equal(exitcode.ExitRuntime), first.Log)
		gomega.Expect(first.SessionID()).To(gomega.Equal(holder))
		transfertest.ExpectAbortedOutput(firstOut)
		expectNoSession()
	})

	ginkgo.It("keeps the session when the export is canceled", func() {
		out := filepath.Join(ginkgo.GinkgoT().TempDir(), "export")
		opts := transfertest.DefaultExportOptions(out)
		opts.NodeRowRateLimit = 1
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var once sync.Once
		run := transfertest.RunExport(ctx, grpcAddr, opts, func(line string) {
			if strings.Contains(line, "] ") && strings.Contains(line, " started: ") {
				once.Do(func() { time.AfterFunc(2*time.Second, cancel) })
			}
		})
		gomega.Expect(run.ExitCode()).To(gomega.Equal(exitcode.ExitRuntime), run.Log)
		id := run.SessionID()
		gomega.Expect(id).NotTo(gomega.BeEmpty())
		gomega.Expect(run.Log).To(gomega.ContainSubstring("bydbctl data release-session --id " + id))
		expectSingleSession(id)
		for _, catalog := range transfertest.Catalogs {
			gomega.Expect(transfertest.LeaseFile(dataDir, catalog, id)).To(gomega.BeAnExistingFile(), catalog)
		}
		transfertest.ExpectAbortedOutput(out)
		gomega.Expect(transfertest.Release(conn, id)).To(gomega.BeEmpty())
		expectNoSession()
	})
})
