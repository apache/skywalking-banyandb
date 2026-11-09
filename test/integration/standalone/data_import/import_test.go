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
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/importer"
)

const topNResult = "_top_n_result"

// artifactGroup returns the group schema of the source artifact.
func artifactGroup(name string) *commonv1.Group {
	for _, g := range transfertest.Schemas(artifactA, "group", func() *commonv1.Group { return &commonv1.Group{} }) {
		if g.GetMetadata().GetName() == name {
			return importer.PrepareForWrite(g).(*commonv1.Group)
		}
	}
	ginkgo.Fail("the artifact has no group " + name)
	return nil
}

// createGroup creates a group on the instance.
func createGroup(t *instance, g *commonv1.Group) {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	_, err := databasev1.NewGroupRegistryServiceClient(t.conn).Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{Group: g})
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
}

var _ = ginkgo.Describe("CSV import into an empty standalone target", ginkgo.Ordered, func() {
	var target *instance
	var dry, realRun transfertest.ImportRun

	ginkgo.BeforeAll(func() {
		target = startTarget()
	})

	ginkgo.It("plans a dry run without changing the target", func() {
		before := schemaSnapshot(target.conn)
		dry = runImport(target.addr, func(o *config.Import) { o.DryRun = true })
		gomega.Expect(dry.ExitCode()).To(gomega.Equal(0), dry.Log)
		gomega.Expect(dry.Out).To(gomega.ContainSubstring("CATALOG"), "the dry run prints the plan table")
		gomega.Expect(dry.Result.Push).To(gomega.BeNil(), "a dry run pushes nothing")
		gomega.Expect(schemaSnapshot(target.conn)).To(gomega.Equal(before), "a dry run leaves the target schema unchanged")
		gomega.Expect(totalData(target.conn)).To(gomega.BeZero(), "a dry run writes no data")
	})

	ginkgo.It("imports every catalog and prints the plan of the dry run byte for byte", func() {
		realRun = runImport(target.addr, nil)
		expectImported(realRun)
		gomega.Expect(realRun.Out).To(gomega.Equal(dry.Out), "the real run prints the dry run's plan")
	})

	ginkgo.It("re-exports the source rows, Top-N results and the latest property revisions unchanged", func() {
		b := expectRoundTrip(target.addr)
		artB := transfertest.ReadArtifact(b, false)
		artA := transfertest.ReadArtifact(artifactA, false)
		// The import never recomputes Top-N: the target holds exactly the source's results.
		gomega.Expect(resourceRows(artB, "measure", transfertest.MeasureGroup, topNResult)).
			To(gomega.Equal(resourceRows(artA, "measure", transfertest.MeasureGroup, topNResult)))
		tombB := latestTombstone(artB)
		gomega.Expect(tombB).NotTo(gomega.BeNil(), "the tombstone travels")
		gomega.Expect(transfertest.IsLiveProperty(tombB)).To(gomega.BeFalse(), "the tombstone stays deleted")
		gomega.Expect(tombB["_mod_revision"]).To(gomega.Equal(latestTombstone(artA)["_mod_revision"]))
	})

	ginkgo.It("answers the same queries as the source", func() {
		gomega.Eventually(func(g gomega.Gomega) {
			transfertest.ExpectSameQueries(g, source.conn, target.conn)
		}, transfertest.RoundTripTimeout, 3*time.Second).Should(gomega.Succeed())
	})
})

var _ = ginkgo.Describe("CSV import with a parallelism of one and replayed", ginkgo.Ordered, func() {
	var target *instance
	var first string

	ginkgo.BeforeAll(func() {
		target = startTarget()
	})

	ginkgo.It("imports the same rows as with the maximal parallelism", func() {
		run := runImport(target.addr, func(o *config.Import) { o.Parallelism = 1 })
		expectImported(run)
		gomega.Expect(run.Log).To(gomega.ContainSubstring("parallelism: 1 "))
		first = expectRoundTrip(target.addr)
	})

	ginkgo.It("keeps measures, index-mode measures, Top-N results and properties unchanged on a replay", func() {
		run := runImport(target.addr, func(o *config.Import) { o.Parallelism = 1 })
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		a := transfertest.ReadArtifact(artifactA, false)
		streamRows := transfertest.RowsCount(a, "stream", transfertest.StreamGroup)
		traceRows := transfertest.RowsCount(a, "trace", transfertest.TraceGroup)
		gomega.Expect(streamRows).To(gomega.BeNumerically(">", 0))
		gomega.Expect(traceRows).To(gomega.BeNumerically(">", 0))
		transfertest.ExportEventually(target.addr, workDir, func(g gomega.Gomega, dir string) {
			// A replay changes no measure, Top-N result or property row.
			transfertest.ExpectRoundTrip(g, first, dir, transfertest.MeasureGroup, transfertest.IndexModeGroup, transfertest.PropertyGroup)
			g.Expect(decodedTopNResults(g, dir, transfertest.MeasureGroup)).
				To(gomega.Equal(decodedTopNResults(g, first, transfertest.MeasureGroup)), "a replay changes no decoded Top-N result")
			b := transfertest.ReadArtifact(dir, false)
			// Stream and trace rows are replayed, not deduplicated (a known limitation).
			g.Expect(transfertest.RowsCount(b, "stream", transfertest.StreamGroup)).To(gomega.Equal(2*streamRows), "stream rows double")
			g.Expect(transfertest.RowsCount(b, "trace", transfertest.TraceGroup)).To(gomega.Equal(2*traceRows), "trace rows double")
		})
	})
})

var _ = ginkgo.Describe("CSV import of a property the target deleted", ginkgo.Ordered, func() {
	ginkgo.It("does not revive a property the target deleted at the revision the artifact holds live", func() {
		target := startTarget()
		expectImported(runImport(target.addr, nil))
		live := transfertest.ReadArtifact(artifactA, false).LiveProperty()
		id := live["_id"]
		client := propertyv1.NewPropertyServiceClient(target.conn)
		query := &propertyv1.QueryRequest{Groups: []string{transfertest.PropertyGroup}, Name: transfertest.PropertyName, Ids: []string{id}}
		gomega.Eventually(func(g gomega.Gomega) {
			ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
			defer cancel()
			resp, err := client.Query(ctx, query)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(resp.GetProperties()).To(gomega.HaveLen(1), "the imported property is live")
		}, flags.EventuallyTimeout).Should(gomega.Succeed())
		// Deleting marks the imported revision deleted in place, keeping its mod_revision.
		ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
		defer cancel()
		resp, err := client.Delete(ctx, &propertyv1.DeleteRequest{Group: transfertest.PropertyGroup, Name: transfertest.PropertyName, Id: id})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(resp.GetDeleted()).To(gomega.BeTrue())

		run := runImport(target.addr, nil)
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		queryCtx, queryCancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
		defer queryCancel()
		queried, err := client.Query(queryCtx, query)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(queried.GetProperties()).To(gomega.BeEmpty(), "the replayed live revision does not revive the deleted one")
		transfertest.ExportEventually(target.addr, workDir, func(g gomega.Gomega, dir string) {
			latest := transfertest.ReadArtifact(dir, false).LatestProperties()[id]
			g.Expect(latest).NotTo(gomega.BeNil(), "the deleted property is exported")
			g.Expect(latest["_mod_revision"]).To(gomega.Equal(live["_mod_revision"]), "the latest revision is the deleted one")
			g.Expect(transfertest.IsLiveProperty(latest)).To(gomega.BeFalse(), "the latest revision stays deleted")
		})
	})
})

var _ = ginkgo.Describe("CSV import against a conflicting target schema", func() {
	ginkgo.It("refuses the import and leaves the target untouched", func() {
		target := startTarget()
		createGroup(target, artifactGroup(transfertest.StreamGroup))
		var stream *databasev1.Stream
		for _, s := range transfertest.StreamsOf(artifactA, transfertest.StreamGroup) {
			if s.GetMetadata().GetName() == transfertest.StreamName {
				stream = importer.PrepareForWrite(s).(*databasev1.Stream)
			}
		}
		gomega.Expect(stream).NotTo(gomega.BeNil())
		family := stream.GetTagFamilies()[0]
		family.Tags = append(family.Tags, &databasev1.TagSpec{Name: "import_extra_tag", Type: databasev1.TagType_TAG_TYPE_STRING})
		ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
		defer cancel()
		_, err := databasev1.NewStreamRegistryServiceClient(target.conn).Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{Stream: stream})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		before := schemaSnapshot(target.conn)
		run := runImport(target.addr, nil)
		gomega.Expect(run.ExitCode()).To(gomega.Equal(exitcode.ExitPreflight), run.Log)
		gomega.Expect(run.Out).To(gomega.ContainSubstring("stream/" + transfertest.StreamGroup + "/" + transfertest.StreamName))
		gomega.Expect(schemaSnapshot(target.conn)).To(gomega.Equal(before), "a refused import writes no schema")
		gomega.Expect(totalData(target.conn)).To(gomega.BeZero(), "a refused import writes no data")
	})
})

var _ = ginkgo.Describe("CSV import into a group with another shard number", func() {
	ginkgo.It("routes the rows by the target's shard number and keeps them equal", func() {
		target := startTarget()
		g := artifactGroup(transfertest.StreamGroup)
		srcShards := g.GetResourceOpts().GetShardNum()
		// Under srcShards+2 shards some of the suite's stream series land in a shard id the
		// source does not have; under srcShards+1 they do not.
		g.ResourceOpts.ShardNum += 2
		createGroup(target, g)
		run := runImport(target.addr, func(o *config.Import) {
			o.ConflictRules = []config.ConflictRule{{Match: "group/" + transfertest.StreamGroup, Policy: config.ConflictPolicyIgnore}}
		})
		expectImported(run)
		gomega.Expect(run.Result.Plan.Schema).To(gomega.ContainElement(gomega.SatisfyAll(
			gomega.HaveField("Disposition", importer.DispositionIgnore),
			gomega.WithTransform(func(a importer.SchemaAction) string { return a.ID() }, gomega.Equal("group/"+transfertest.StreamGroup)),
		)), run.Out)
		gomega.Expect(run.Out).To(gomega.MatchRegexp(`(?m)^\s+`+transfertest.StreamGroup+`\s+\d+\s+.*1 ignore`), run.Out)
		expectRoundTrip(target.addr, transfertest.StreamGroup)

		ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
		defer cancel()
		resp, err := databasev1.NewGroupRegistryServiceClient(target.conn).Get(ctx, &databasev1.GroupRegistryServiceGetRequest{Group: transfertest.StreamGroup})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(resp.GetGroup().GetResourceOpts().GetShardNum()).To(gomega.Equal(g.GetResourceOpts().GetShardNum()), "ignore keeps the target group")
		frames := transfertest.MustPlan(target.conn, &transferv1.PlanRequest{Selectors: []*transferv1.Selector{
			{Catalog: commonv1.Catalog_CATALOG_STREAM, Groups: []string{transfertest.StreamGroup}},
		}})
		var shards []uint32
		for _, f := range transfertest.UnitFrames(frames) {
			for _, u := range f.GetUnits() {
				for _, sh := range u.GetSegment().GetShards() {
					if sh.GetTotalCount() > 0 {
						shards = append(shards, sh.GetShardId())
					}
				}
			}
		}
		gomega.Expect(shards).NotTo(gomega.BeEmpty())
		gomega.Expect(shards).To(gomega.HaveEach(gomega.BeNumerically("<", g.GetResourceOpts().GetShardNum())))
		gomega.Expect(shards).To(gomega.ContainElement(gomega.BeNumerically(">=", srcShards)),
			"some rows land in a shard only the target's shard number has: %v", shards)
	})
})

var _ = ginkgo.Describe("CSV import of an artifact exported without its schema", func() {
	ginkgo.It("reads the CSV columns with the target's schema and imports every row", func() {
		target := startTarget()
		ginkgo.By("Giving the target the source's schema")
		schemaRun := runImport(target.addr, func(o *config.Import) { o.SchemaOnly = true })
		gomega.Expect(schemaRun.Err).NotTo(gomega.HaveOccurred(), schemaRun.Log)

		ginkgo.By("Exporting the source without its schema")
		out := transfertest.NewExportDir(workDir)
		exportOpts := transfertest.DefaultExportOptions(out)
		exportOpts.IncludeSchema = false
		exportRun := transfertest.RunExport(context.Background(), source.addr, exportOpts, nil)
		gomega.Expect(exportRun.Err).NotTo(gomega.HaveOccurred(), exportRun.Log)

		run := transfertest.RunImport(context.Background(), target.addr, transfertest.DefaultImportOptions(out), nil)
		expectImported(run)
		gomega.Expect(run.Log).To(gomega.ContainSubstring("carries no schema; its CSV columns are read with the target's schema"))
		expectRoundTrip(target.addr)
	})
})
