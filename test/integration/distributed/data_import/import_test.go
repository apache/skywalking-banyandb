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
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
	"github.com/apache/skywalking-banyandb/pkg/test/transfer/topncheck"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/importer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/rowdiff"
)

const (
	// verifyTimeout bounds --verify: its polls wait for the property flush (5s) between samples.
	verifyTimeout = 2 * time.Minute
)

var _ = ginkgo.Describe("CSV import between two clusters", ginkgo.Ordered, func() {
	// artifactA is the source export, artifactB the export of target1 after the import.
	var artifactA, artifactB string
	var reportB *rowdiff.Report
	// topNRowsA counts the _top_n_result rows of artifactA across nodes.
	var topNRowsA int
	// firstB is the export of target2 after its first import.
	var firstB string

	ginkgo.BeforeAll(func() {
		artifactA = transfertest.ExportFrom(source.liaisonAddr, exportRoot)
		a := transfertest.ReadArtifact(artifactA, false)
		var stages []string
		for _, u := range a.Manifest.Units {
			if u.Group == stagedGroup {
				stages = append(stages, u.Stage)
			}
		}
		gomega.Expect(stages).To(gomega.ContainElements("hot", "warm"), "the artifact holds both stages of %s", stagedGroup)
		topNRowsA = topncheck.ExpectTopNExported(a)
		gomega.Expect(topNRowsA).To(gomega.BeNumerically(">", 0), "the source artifact holds Top-N results")
	})

	ginkgo.It("refuses a target group of another shape and writes nothing", func() {
		run := transfertest.RunImport(context.Background(), target1.liaisonAddr, importOptionsWith(artifactA, nil), nil)
		gomega.Expect(run.ExitCode()).To(gomega.Equal(exitcode.ExitPreflight), run.Log)
		gomega.Expect(run.Result).NotTo(gomega.BeNil())
		var conflict bool
		for _, a := range run.Result.Plan.Schema {
			if a.Disposition == importer.DispositionConflict && a.ID() == "group/"+transfertest.StreamGroup {
				conflict = true
			}
		}
		gomega.Expect(conflict).To(gomega.BeTrue(), "the stream group is a conflict: %s", run.Out)
		expectUntouched(target1)
	})

	ginkgo.It("prints the plan of a dry run and writes nothing", func() {
		opts := importOptions(artifactA)
		opts.DryRun = true
		run := transfertest.RunImport(context.Background(), target1.liaisonAddr, opts, nil)
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		gomega.Expect(run.Result.Plan.Entries).NotTo(gomega.BeEmpty())
		gomega.Expect(run.Result.Plan.Rejected).To(gomega.BeEmpty())
		gomega.Expect(run.Result.Push).To(gomega.BeNil())
		expectUntouched(target1)
	})

	ginkgo.It("leaves a created group unrouted at dry run, so its stage is checked only after the schema write", func() {
		dir := transfertest.CopyWithGroup(artifactA, filepath.Join(ginkgo.GinkgoT().TempDir(), "artifact"), stagedGroup, func(g *commonv1.Group) {
			for _, st := range g.GetResourceOpts().GetStages() {
				st.NodeSelector = "type=cold"
			}
		})
		opts := importOptions(dir)
		opts.DryRun = true
		run := transfertest.RunImport(context.Background(), target1.liaisonAddr, opts, nil)
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Out)
		for _, r := range run.Result.Plan.Rejected {
			gomega.Expect(r.Gate).NotTo(gomega.Equal(importer.GateRoute), "a created group has no route to refuse: %s", run.Out)
		}
		expectUntouched(target1)
	})

	ginkgo.It("imports into a target with more nodes and another shard number, stage by stage", func() {
		run := transfertest.RunImport(context.Background(), target1.liaisonAddr, importOptions(artifactA), nil)
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		gomega.Expect(run.Result.Verify).NotTo(gomega.BeNil())
		gomega.Expect(run.Result.Verify.Failed()).To(gomega.BeFalse(), run.Log)
		gomega.Expect(run.Log).To(gomega.MatchRegexp(`parallelism: ([2-9]|\d{2,}) `), "parallelism max")

		ginkgo.By("Routing the warm units to the warm node and the replicated groups to two nodes")
		var stages []string
		for _, e := range run.Result.Recheck.Entries {
			switch e.Item.Group {
			case stagedGroup:
				stages = append(stages, e.TargetStage)
				if e.TargetStage == "warm" {
					gomega.Expect(e.TargetNodes).To(gomega.ConsistOf(target1.namesOf(warmLabel)), e.Item.Key())
				} else {
					gomega.Expect(target1.namesOf(hotLabel)).To(gomega.ContainElements(e.TargetNodes), e.Item.Key())
				}
			case transfertest.StreamGroup, transfertest.MeasureGroup, transfertest.PropertyGroup:
				gomega.Expect(e.Copies).To(gomega.Equal(uint32(2)), "%s has one replica", e.Item.Key())
			}
		}
		gomega.Expect(stages).To(gomega.ContainElements("hot", "warm"))
		frames := transfertest.MustPlan(target1.conn, &transferv1.PlanRequest{Selectors: []*transferv1.Selector{
			{Catalog: commonv1.Catalog_CATALOG_MEASURE, Groups: []string{stagedGroup}},
		}})
		byStage := transfertest.RowsByStageNode(frames, commonv1.Catalog_CATALOG_MEASURE, stagedGroup)
		gomega.Expect(keys(byStage["warm"])).To(gomega.ConsistOf(target1.namesOf(warmLabel)))
		gomega.Expect(sum(byStage["warm"])).To(gomega.Equal(uint64(stagedRows)))
		gomega.Expect(target1.namesOf(hotLabel)).To(gomega.ContainElements(keys(byStage[""])))
		gomega.Expect(sum(byStage[""])).To(gomega.Equal(uint64(stagedRows)))
		gomega.Expect(groupShardNum(target1, transfertest.StreamGroup)).To(gomega.Equal(uint32(targetStreamShards)), "ignore keeps the target group")

		ginkgo.By("Exporting the target and comparing it with the source artifact")
		artifactB, reportB = exportRoundTrip(target1, artifactA)
		gomega.Expect(reportB.StageCounts).To(gomega.HaveKeyWithValue("measure/"+stagedGroup+"/warm", [2]int{stagedRows, stagedRows}))
		gomega.Expect(reportB.StageCounts).To(gomega.HaveKeyWithValue("measure/"+stagedGroup+"/hot", [2]int{stagedRows, stagedRows}))
		// The import never recomputes Top-N: the target holds exactly the source's results.
		gomega.Expect(topncheck.ExpectTopNExported(transfertest.ReadArtifact(artifactB, false))).To(gomega.Equal(topNRowsA), "_top_n_result rows")

		ginkgo.By("Running the same queries on the source and the target")
		gomega.Eventually(func(g gomega.Gomega) {
			transfertest.ExpectSameQueries(g, source.conn, target1.conn)
		}, transfertest.RoundTripTimeout, 3*time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("imports the same rows with parallelism 1 into a fresh target, at a node row rate far below the batches in flight", func() {
		opts := importOptions(artifactA)
		opts.Parallelism = 1
		dry := opts
		dry.DryRun = true
		plan := transfertest.RunImport(context.Background(), target2.liaisonAddr, dry, nil)
		gomega.Expect(plan.Err).NotTo(gomega.HaveOccurred(), plan.Log)
		// Every copy of a row is paid by the node holding it, at that node's own rate. The groups
		// are not routed before they are created, so the busiest node is known only to pay at
		// least the average share of the target's data nodes.
		var copies uint64
		for _, e := range plan.Result.Plan.Entries {
			copies += e.Item.Rows() * uint64(max(e.Copies, 1))
		}
		paid := copies / uint64(len(target2.nodes))
		gomega.Expect(paid).To(gomega.BeNumerically(">", 0))
		// The rate admits a node's share in about 20s, and a batch costs about 4s of it:
		// with up to 8 batches in flight on a stream, later batches queue past the node's slack,
		// are answered busy and resent, instead of timing out.
		rate := max(paid/20, 1)
		opts.NodeRowRateLimit = float64(rate)
		opts.WriteBatchSize = int(min(max(4*rate, 1), 10000))
		opts.WriteInflightBatches = 8
		start := time.Now()
		run := transfertest.RunImport(context.Background(), target2.liaisonAddr, opts, nil)
		elapsed := time.Since(start)
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		gomega.Expect(run.Log).To(gomega.ContainSubstring("parallelism: 1 "))
		// The first second's worth passes at once; every other row of the busiest node waits for its rate.
		gomega.Expect(elapsed).To(gomega.BeNumerically(">=", time.Duration(float64(paid)/float64(rate)*float64(time.Second))-2*time.Second),
			"a node's share of %d rows at %d rows/s took %s", paid, rate, elapsed)
		var report *rowdiff.Report
		firstB, report = exportRoundTrip(target2, artifactA)
		gomega.Expect(report.StageCounts).To(gomega.Equal(reportB.StageCounts), "parallelism 1 and max import the same rows per stage")
	})

	ginkgo.It("keeps measures, index-mode measures, Top-N results and properties unchanged on a replay", func() {
		run := transfertest.RunImport(context.Background(), target2.liaisonAddr, importOptions(artifactA), nil)
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		first := transfertest.ReadArtifact(firstB, false)
		streamRows := transfertest.RowsCount(first, "stream", transfertest.StreamGroup)
		traceRows := transfertest.RowsCount(first, "trace", transfertest.TraceGroup)
		gomega.Expect(streamRows).To(gomega.BeNumerically(">", 0))
		gomega.Expect(traceRows).To(gomega.BeNumerically(">", 0))
		transfertest.ExportEventually(target2.liaisonAddr, exportRoot, func(g gomega.Gomega, dir string) {
			// A replay changes no measure, Top-N result or property row.
			transfertest.ExpectRoundTrip(g, firstB, dir, transfertest.MeasureGroup, transfertest.IndexModeGroup, transfertest.PropertyGroup)
			b := transfertest.ReadArtifact(dir, false)
			// Stream and trace rows are replayed, not deduplicated (a known limitation).
			g.Expect(transfertest.RowsCount(b, "stream", transfertest.StreamGroup)).To(gomega.Equal(2*streamRows), "stream rows double")
			g.Expect(transfertest.RowsCount(b, "trace", transfertest.TraceGroup)).To(gomega.Equal(2*traceRows), "trace rows double")
		})
	})

	ginkgo.It("does not revive a property the target deleted when the artifact holds it live", func() {
		live := transfertest.ReadArtifact(artifactA, false).LiveProperty()
		gomega.Expect(live).NotTo(gomega.BeNil(), "the source artifact holds a live property")
		id := live["_id"]
		client := propertyv1.NewPropertyServiceClient(target2.conn)
		query := &propertyv1.QueryRequest{Groups: []string{transfertest.PropertyGroup}, Name: transfertest.PropertyName, Ids: []string{id}}
		queryProperties := func(g gomega.Gomega) []*propertyv1.Property {
			ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
			defer cancel()
			resp, err := client.Query(ctx, query)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			return resp.GetProperties()
		}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(queryProperties(g)).To(gomega.HaveLen(1), "the imported property is live")
		}, flags.EventuallyTimeout).Should(gomega.Succeed())
		// Deleting marks the imported revision deleted in place on every replica, keeping its mod_revision.
		ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
		defer cancel()
		resp, err := client.Delete(ctx, &propertyv1.DeleteRequest{Group: transfertest.PropertyGroup, Name: transfertest.PropertyName, Id: id})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(resp.GetDeleted()).To(gomega.BeTrue())

		run := transfertest.RunImport(context.Background(), target2.liaisonAddr, importOptions(artifactA), nil)
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		// The replicas repair one another in the background, so a revived revision may show up late.
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(queryProperties(g)).To(gomega.BeEmpty(), "the replayed live revision does not revive the deleted one")
		}, 15*time.Second, time.Second).Should(gomega.Succeed())
		transfertest.ExportEventually(target2.liaisonAddr, exportRoot, func(g gomega.Gomega, dir string) {
			art := transfertest.ReadArtifact(dir, false)
			latest := art.LatestProperties()[id]
			g.Expect(latest).NotTo(gomega.BeNil(), "the deleted property is exported")
			g.Expect(latest["_mod_revision"]).To(gomega.Equal(live["_mod_revision"]), "the latest revision is the deleted one")
			// Every replica's copy of the latest revision is deleted; older revisions are not checked.
			for node, rows := range art.RowsOf("property", transfertest.PropertyGroup, transfertest.PropertyName) {
				for _, r := range rows {
					if r["_id"] == id && r["_mod_revision"] == live["_mod_revision"] {
						g.Expect(transfertest.IsLiveProperty(r)).To(gomega.BeFalse(), "the latest revision stays deleted on %s", node)
					}
				}
			}
		})
	})

	ginkgo.It("exits 3 when a target data node stops in the middle of the import", func() {
		opts := importOptions(artifactA)
		opts.WriteBatchSize = 1
		opts.WriteRateLimit = 10
		opts.Verify = false
		victim := target2.nodes[0]
		var once sync.Once
		var stopped bool
		run := transfertest.RunImport(context.Background(), target2.liaisonAddr, opts, func(event string) {
			if strings.HasPrefix(event, "batch ") {
				once.Do(func() {
					victim.stop()
					stopped = true
				})
			}
		})
		gomega.Expect(stopped).To(gomega.BeTrue(), "a batch was acknowledged before the node stopped")
		gomega.Expect(run.ExitCode()).To(gomega.Equal(exitcode.ExitRuntime), run.Log)
		gomega.Expect(run.Log).To(gomega.ContainSubstring("work items not done"))
	})

	ginkgo.It("exits 4 when a target data node stops before the verification", func() {
		opts := importOptions(artifactA)
		opts.VerifyTimeout = 30 * time.Second
		victim := target1.nodes[0]
		var once sync.Once
		var stopped bool
		run := transfertest.RunImport(context.Background(), target1.liaisonAddr, opts, func(event string) {
			if strings.HasPrefix(event, "IMPORTED ") {
				once.Do(func() {
					victim.stop()
					stopped = true
				})
			}
		})
		gomega.Expect(stopped).To(gomega.BeTrue(), "the push finished")
		gomega.Expect(run.ExitCode()).To(gomega.Equal(exitcode.ExitVerify), run.Log)
		gomega.Expect(run.Result.Verify.Incomplete).To(gomega.ContainElement(
			gomega.ContainSubstring("target node "+victim.name+" did not answer the Plan")), run.Log)
	})
})

// importOptions are the defaults of an import of input with the target's stream group,
// pre-created with another shard number, kept by policy ignore.
func importOptions(input string) config.Import {
	return importOptionsWith(input, []config.ConflictRule{{Match: "group/" + transfertest.StreamGroup, Policy: config.ConflictPolicyIgnore}})
}

func importOptionsWith(input string, rules []config.ConflictRule) config.Import {
	opts := transfertest.DefaultImportOptions(input)
	opts.VerifyTimeout = verifyTimeout
	opts.ConflictRules = rules
	return opts
}

// exportRoundTrip re-exports c until it holds the logical rows of artifactA and returns the
// export and its comparison report.
func exportRoundTrip(c *cluster, artifactA string) (string, *rowdiff.Report) {
	var report *rowdiff.Report
	dir := transfertest.ExportEventually(c.liaisonAddr, exportRoot, func(g gomega.Gomega, dir string) {
		report = transfertest.ExpectRoundTrip(g, artifactA, dir)
	})
	return dir, report
}

// expectUntouched asserts that c holds only what it was started with: the pre-created stream
// group, unchanged, no other suite group, and no row.
func expectUntouched(c *cluster) {
	gomega.ExpectWithOffset(1, groupShardNum(c, transfertest.StreamGroup)).To(gomega.Equal(uint32(targetStreamShards)))
	for _, g := range []string{transfertest.MeasureGroup, transfertest.IndexModeGroup, transfertest.TraceGroup, transfertest.PropertyGroup, stagedGroup} {
		_, err := databasev1.NewGroupRegistryServiceClient(c.conn).Get(context.Background(), &databasev1.GroupRegistryServiceGetRequest{Group: g})
		gomega.ExpectWithOffset(1, status.Code(err)).To(gomega.Equal(codes.NotFound), "group %s is not created: %v", g, err)
	}
	frames := transfertest.MustPlan(c.conn, &transferv1.PlanRequest{})
	for _, f := range transfertest.UnitFrames(frames) {
		for _, u := range f.GetUnits() {
			for _, sh := range u.GetSegment().GetShards() {
				gomega.ExpectWithOffset(1, sh.GetTotalCount()).To(gomega.BeZero(), "no row is written: %s", u.String())
			}
			gomega.ExpectWithOffset(1, u.GetProperty().GetShards()).To(gomega.BeEmpty(), "no property is written")
		}
	}
}

func groupShardNum(c *cluster, group string) uint32 {
	resp, err := databasev1.NewGroupRegistryServiceClient(c.conn).Get(context.Background(), &databasev1.GroupRegistryServiceGetRequest{Group: group})
	gomega.ExpectWithOffset(2, err).NotTo(gomega.HaveOccurred())
	return resp.GetGroup().GetResourceOpts().GetShardNum()
}

func keys(m map[string]uint64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func sum(m map[string]uint64) uint64 {
	var n uint64
	for _, v := range m {
		n += v
	}
	return n
}
