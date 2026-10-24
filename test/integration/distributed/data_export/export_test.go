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
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
	"github.com/apache/skywalking-banyandb/pkg/test/transfer/topncheck"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exporter"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	casesstreamdata "github.com/apache/skywalking-banyandb/test/cases/stream/data"
)

// rowsByNode sums, per node, the total_count of one catalog and group's shards in a dry-run plan.
func rowsByNode(frames []*transferv1.PlanResponse, catalog commonv1.Catalog, group string) map[string]uint64 {
	out := map[string]uint64{}
	for _, f := range transfertest.UnitFrames(frames) {
		for _, u := range f.GetUnits() {
			seg := u.GetSegment()
			if seg.GetUnit().GetCatalog() != catalog || seg.GetUnit().GetGroup() != group {
				continue
			}
			for _, sh := range seg.GetShards() {
				out[f.GetNodeId()] += sh.GetTotalCount()
			}
		}
	}
	return out
}

// liveStreamRows sums the rows of the stream group a dry-run plan reports across the nodes.
func liveStreamRows() uint64 {
	frames := transfertest.MustPlan(conn, &transferv1.PlanRequest{Selectors: []*transferv1.Selector{
		{Catalog: commonv1.Catalog_CATALOG_STREAM, Groups: []string{transfertest.StreamGroup}},
	}})
	var n uint64
	for _, rows := range rowsByNode(frames, commonv1.Catalog_CATALOG_STREAM, transfertest.StreamGroup) {
		n += rows
	}
	return n
}

// expectNoExportSession asserts that no data node holds an export session or a snapshot.
func expectNoExportSession() {
	for node, id := range sessionsByNode() {
		gomega.ExpectWithOffset(1, id).To(gomega.BeEmpty(), "%s holds no session", node)
	}
	for _, dir := range dataDirs {
		for _, catalog := range transfertest.Catalogs {
			gomega.ExpectWithOffset(1, transfertest.ExportEntries(dir, catalog)).To(gomega.BeEmpty(), "no export snapshot is left: %s on %s", catalog, dir)
		}
	}
}

// expectExportSessionEverywhere asserts that every data node holds exactly the session id.
func expectExportSessionEverywhere(id string) {
	byNode := sessionsByNode()
	gomega.ExpectWithOffset(1, byNode).To(gomega.HaveLen(len(nodeNames)))
	for node, held := range byNode {
		gomega.ExpectWithOffset(1, held).To(gomega.Equal(id), "%s holds the session", node)
	}
}

// replicatedOnEveryNode tells which "<catalog>/<group>" keys the suite's groups replicate to
// both data nodes (one replica on a two-node cluster); the others split their shards.
func replicatedOnEveryNode(key string) bool {
	switch key {
	case "stream/" + transfertest.StreamGroup, "measure/" + transfertest.MeasureGroup, "property/" + transfertest.PropertyGroup:
		return true
	default:
		return false
	}
}

func expectedNodeDirs() []string {
	dirs := make([]string, 0, len(nodeNames))
	for _, n := range nodeNames {
		dirs = append(dirs, manifest.NodeDir(n))
	}
	sort.Strings(dirs)
	return dirs
}

func exportSpecs() {
	ginkgo.Describe("CSV export", ginkgo.Ordered, csvExportSpecs)
}

func csvExportSpecs() {
	suiteData := func() transfertest.SuiteData {
		return transfertest.SuiteData{StreamBase: streamBase, MeasureBase: measureBase, TraceBase: traceBase, Interval: writeInterval}
	}

	ginkgo.BeforeAll(func() {
		ginkgo.By("Waiting for every catalog to be flushed on both nodes")
		gomega.Eventually(func(g gomega.Gomega) {
			frames, err := transfertest.Plan(conn, &transferv1.PlanRequest{})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			for _, n := range nodeNames {
				g.Expect(rowsByNode(frames, commonv1.Catalog_CATALOG_STREAM, transfertest.StreamGroup)).To(gomega.HaveKeyWithValue(n, uint64(5)), "stream on %s", n)
				// The written points and at least one _top_n_result row on every replica.
				g.Expect(rowsByNode(frames, commonv1.Catalog_CATALOG_MEASURE, transfertest.MeasureGroup)).
					To(gomega.HaveKeyWithValue(n, gomega.BeNumerically(">", measureRows+topNSourcePoints)), "measure and Top-N results on %s", n)
			}
			var spans uint64
			for _, n := range rowsByNode(frames, commonv1.Catalog_CATALOG_TRACE, transfertest.TraceGroup) {
				spans += n
			}
			g.Expect(spans).To(gomega.Equal(uint64(13)), "trace")
			g.Expect(transfertest.IndexModeDocs(frames, transfertest.IndexModeGroup)).To(gomega.BeNumerically(">=", 3), "index-mode rows")
			// The live property and the tombstone on both replicas.
			g.Expect(propertyDocs(frames, transfertest.PropertyGroup)).To(gomega.BeNumerically(">=", 4), "property")
		}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())
		expectNoExportSession()
	})

	ginkgo.It("exports every data node into its own subtree holding exactly the written rows", func() {
		out := filepath.Join(ginkgo.GinkgoT().TempDir(), "export")
		opts := transfertest.DefaultExportOptions(out)
		opts.CSVAnnotations = true
		run := transfertest.RunExport(context.Background(), liaisonAddr, opts, nil)
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		expectNoExportSession()

		a := transfertest.ReadArtifact(out, true)
		a.VerifyManifest()
		var ids []string
		for _, n := range a.Manifest.Source.Nodes {
			ids = append(ids, n.ID)
		}
		gomega.Expect(ids).To(gomega.ConsistOf(nodeNames))
		gomega.Expect(a.NodeDirs()).To(gomega.Equal(expectedNodeDirs()), "each data node gets its own subtree")
		a.VerifySuiteData(suiteData(), replicatedOnEveryNode)
		gomega.Expect(a.ExpectReplicasEqual()).To(gomega.BeNumerically(">", 0), "replicated shards appear under every source")
		for _, u := range a.Manifest.Units {
			if u.Catalog == "STREAM" && u.Group == transfertest.StreamGroup {
				gomega.Expect(u.Sources).To(gomega.HaveLen(len(nodeNames)), "a replicated stream unit lists both sources")
			}
		}
		gomega.Expect(topncheck.ExpectTopNExported(a)).To(gomega.BeNumerically(">", 0), "the Top-N results are exported")
	})

	ginkgo.It("writes plain CSV and rolls small files with the header repeated", func() {
		out := filepath.Join(ginkgo.GinkgoT().TempDir(), "export")
		opts := transfertest.DefaultExportOptions(out)
		opts.Compress = transferv1.Compress_COMPRESS_NONE
		opts.MaxFileSize = 1
		run := transfertest.RunExport(context.Background(), liaisonAddr, opts, nil)
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		expectNoExportSession()
		a := transfertest.ReadArtifact(out, false)
		a.VerifyManifest()
		gomega.Expect(a.Manifest.Compress).To(gomega.Equal("none"))
		transfertest.ExpectRolledFiles(a)
		a.VerifySuiteData(suiteData(), replicatedOnEveryNode)
		a.ExpectReplicasEqual()
	})

	ginkgo.It("writes only the schema and the manifest with --schema-only and creates no session", func() {
		out := filepath.Join(ginkgo.GinkgoT().TempDir(), "export")
		opts := transfertest.DefaultExportOptions(out)
		opts.SchemaOnly = true
		run := transfertest.RunExport(context.Background(), liaisonAddr, opts, nil)
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		gomega.Expect(run.SessionID()).To(gomega.BeEmpty())
		expectNoExportSession()
		a := transfertest.ReadArtifact(out, false)
		gomega.Expect(a.Manifest.SchemaOnly).To(gomega.BeTrue())
		gomega.Expect(a.Manifest.Units).To(gomega.BeEmpty())
		gomega.Expect(a.Files).To(gomega.BeEmpty())
		gomega.Expect(filepath.Join(out, "nodes")).NotTo(gomega.BeADirectory())
		a.VerifyManifest()
	})

	ginkgo.It("exports the session snapshot and leaves out data written after the session was created", func() {
		out := filepath.Join(ginkgo.GinkgoT().TempDir(), "export")
		before := liveStreamRows()
		var wrote bool
		run := transfertest.RunExport(context.Background(), liaisonAddr, transfertest.DefaultExportOptions(out), func(line string) {
			// The plan and its snapshots are complete once the workers start.
			if !strings.HasPrefix(line, "exporting ") || wrote {
				return
			}
			wrote = true
			casesstreamdata.Write(conn, transfertest.StreamName, timestamp.NowMilli(), writeInterval)
			gomega.Eventually(liveStreamRows, flags.EventuallyTimeout, time.Second).Should(gomega.BeNumerically(">", before),
				"the late rows reach the live parts while the export runs")
		})
		gomega.Expect(wrote).To(gomega.BeTrue())
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		a := transfertest.ReadArtifact(out, false)
		a.VerifyManifest()
		// The late elements reuse the element ids with later timestamps; any of them would
		// show up as a duplicate or a mismatching timestamp.
		a.VerifySuiteData(suiteData(), replicatedOnEveryNode)
		expectNoExportSession()
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
			firstDone <- transfertest.RunExport(context.Background(), liaisonAddr, slow, func(line string) {
				if strings.Contains(line, "] ") && strings.Contains(line, " started: ") {
					once.Do(func() { close(started) })
				}
			})
		}()
		gomega.Eventually(started, flags.EventuallyTimeout).Should(gomega.BeClosed())
		var holder string
		for _, id := range sessionsByNode() {
			holder = id
		}
		gomega.Expect(holder).NotTo(gomega.BeEmpty())
		expectExportSessionEverywhere(holder)

		refused := transfertest.RunExport(context.Background(), liaisonAddr, transfertest.DefaultExportOptions(filepath.Join(ginkgo.GinkgoT().TempDir(), "second")), nil)
		gomega.Expect(refused.ExitCode()).To(gomega.Equal(exporter.ExitPreflight), refused.Log)
		gomega.Expect(refused.Err.Error()).To(gomega.ContainSubstring(holder))
		expectExportSessionEverywhere(holder)

		preemptOut := filepath.Join(ginkgo.GinkgoT().TempDir(), "preempt")
		preempt := transfertest.DefaultExportOptions(preemptOut)
		preempt.Preempt = true
		taken := transfertest.RunExport(context.Background(), liaisonAddr, preempt, nil)
		gomega.Expect(taken.Err).NotTo(gomega.HaveOccurred(), taken.Log)
		gomega.Expect(taken.Log).To(gomega.ContainSubstring(holder), "the preempting export names the session it preempted")
		transfertest.ReadArtifact(preemptOut, false).VerifyManifest()

		var first transfertest.ExportRun
		gomega.Eventually(firstDone, flags.EventuallyTimeout).Should(gomega.Receive(&first))
		gomega.Expect(first.ExitCode()).To(gomega.Equal(exporter.ExitRuntime), first.Log)
		gomega.Expect(first.SessionID()).To(gomega.Equal(holder))
		transfertest.ExpectAbortedOutput(firstOut)
		expectNoExportSession()
	})

	ginkgo.It("keeps the session on every node when the export is canceled", func() {
		out := filepath.Join(ginkgo.GinkgoT().TempDir(), "export")
		opts := transfertest.DefaultExportOptions(out)
		opts.NodeRowRateLimit = 1
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var once sync.Once
		run := transfertest.RunExport(ctx, liaisonAddr, opts, func(line string) {
			if strings.Contains(line, "] ") && strings.Contains(line, " started: ") {
				once.Do(func() { time.AfterFunc(2*time.Second, cancel) })
			}
		})
		gomega.Expect(run.ExitCode()).To(gomega.Equal(exporter.ExitRuntime), run.Log)
		id := run.SessionID()
		gomega.Expect(id).NotTo(gomega.BeEmpty())
		gomega.Expect(run.Log).To(gomega.ContainSubstring("bydbctl data release-session --id " + id))
		expectExportSessionEverywhere(id)
		for _, dir := range dataDirs {
			for _, catalog := range transfertest.Catalogs {
				gomega.Expect(transfertest.LeaseFile(dir, catalog, id)).To(gomega.BeAnExistingFile(), "%s on %s", catalog, dir)
			}
		}
		transfertest.ExpectAbortedOutput(out)
		gomega.Expect(transfertest.Release(conn, id)).To(gomega.BeEmpty())
		expectNoExportSession()
	})

	ginkgo.It("refuses a non-empty output directory", func() {
		out := ginkgo.GinkgoT().TempDir()
		gomega.Expect(os.WriteFile(filepath.Join(out, "leftover"), []byte("x"), 0o600)).To(gomega.Succeed())
		run := transfertest.RunExport(context.Background(), liaisonAddr, transfertest.DefaultExportOptions(out), nil)
		gomega.Expect(run.ExitCode()).To(gomega.Equal(exporter.ExitUsage), run.Log)
		expectNoExportSession()
	})
}
