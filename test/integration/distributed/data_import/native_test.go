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
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/importer"
)

var _ = ginkgo.Describe("Native import between two clusters", ginkgo.Ordered, func() {
	// nativeA is the native export of the source.
	var nativeA string
	// target3 is the native import target: empty, with the source's layout (2 hot nodes and 1
	// warm node), since native parts need the source topology.
	var target3 *cluster

	ginkgo.BeforeAll(func() {
		nativeA = transfertest.NewExportDir(exportRoot)
		opts := transfertest.DefaultExportOptions(nativeA)
		opts.Format = transferv1.Format_FORMAT_NATIVE
		opts.Compress = transferv1.Compress_COMPRESS_NONE
		run := transfertest.RunExport(context.Background(), source.liaisonAddr, opts, nil)
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		ginkgo.By("Starting the native target cluster")
		target3 = startCluster([]string{hotLabel, hotLabel, warmLabel}, true)
	})

	ginkgo.AfterAll(func() {
		if target3 != nil {
			target3.stop()
		}
	})

	ginkgo.It("imports every catalog into a target of the source's topology, stage by stage", func() {
		run := transfertest.RunImport(context.Background(), target3.liaisonAddr, importOptionsWith(nativeA, nil), nil)
		gomega.Expect(run.Err).NotTo(gomega.HaveOccurred(), run.Log)
		gomega.Expect(run.Result.Verify).NotTo(gomega.BeNil())
		gomega.Expect(run.Result.Verify.Failed()).To(gomega.BeFalse(), run.Log)

		ginkgo.By("Placing the replicated groups on two nodes")
		gomega.Expect(run.Result.Recheck).NotTo(gomega.BeNil())
		replicated := map[string]bool{}
		for _, e := range run.Result.Recheck.Entries {
			switch e.Item.Group {
			case transfertest.StreamGroup, transfertest.MeasureGroup, transfertest.PropertyGroup:
				gomega.Expect(e.Copies).To(gomega.Equal(uint32(2)), "%s has one replica", e.Item.Key())
				replicated[e.Item.Group] = true
			}
		}
		gomega.Expect(replicated).To(gomega.HaveLen(3), "every replicated group is imported")
		sourceStreamRows := transfertest.GroupRows(transfertest.MustPlan(source.conn, &transferv1.PlanRequest{}),
			commonv1.Catalog_CATALOG_STREAM, transfertest.StreamGroup)
		gomega.Eventually(func(g gomega.Gomega) {
			frames, err := transfertest.Plan(target3.conn, &transferv1.PlanRequest{})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(transfertest.GroupRows(frames, commonv1.Catalog_CATALOG_STREAM, transfertest.StreamGroup)).
				To(gomega.Equal(sourceStreamRows), "stream rows on both replicas, as on the source")
		}, time.Minute, time.Second).Should(gomega.Succeed())

		ginkgo.By("Placing the warm parts on the warm node")
		frames := transfertest.MustPlan(target3.conn, &transferv1.PlanRequest{Selectors: []*transferv1.Selector{
			{Catalog: commonv1.Catalog_CATALOG_MEASURE, Groups: []string{stagedGroup}},
		}})
		byStage := transfertest.RowsByStageNode(frames, commonv1.Catalog_CATALOG_MEASURE, stagedGroup)
		gomega.Expect(keys(byStage["warm"])).To(gomega.ConsistOf(target3.namesOf(warmLabel)))
		gomega.Expect(sum(byStage["warm"])).To(gomega.Equal(uint64(stagedRows)))
		gomega.Expect(sum(byStage[""])).To(gomega.Equal(uint64(stagedRows)))
	})

	ginkgo.It("refuses a target group of another shard number by the topology gate", func() {
		// A copy of nativeA whose manifest doubles the stream group's shard number, dry-run
		// against target3, which now holds the group with the source's shard number.
		resharded := reshardedCopy(nativeA, transfertest.StreamGroup)
		shards := streamGroupShardNum(target3)
		opts := importOptionsWith(resharded, nil)
		opts.DryRun, opts.IncludeSchema = true, false
		run := transfertest.RunImport(context.Background(), target3.liaisonAddr, opts, nil)
		gomega.Expect(run.ExitCode()).To(gomega.Equal(exitcode.ExitPreflight), run.Log)
		gomega.Expect(run.Result.Plan.Rejected).To(gomega.ContainElement(gomega.SatisfyAll(
			gomega.HaveField("Gate", importer.GateTopology),
			gomega.HaveField("Item.Group", transfertest.StreamGroup),
		)), run.Out)
		gomega.Expect(run.Result.Push).To(gomega.BeNil())
		gomega.Expect(streamGroupShardNum(target3)).To(gomega.Equal(shards))
	})

	ginkgo.It("re-exports the source rows unchanged", func() {
		exportRoundTrip(target3, csvArtifact)
	})

	ginkgo.It("answers the same queries as the source", func() {
		gomega.Eventually(func(g gomega.Gomega) {
			transfertest.ExpectSameQueries(g, source.conn, target3.conn)
		}, transfertest.RoundTripTimeout, 3*time.Second).Should(gomega.Succeed())
	})
})

// reshardedCopy links the files of the native artifact dir into a new directory under
// exportRoot and writes its manifest with every stage of group at twice its shard number.
func reshardedCopy(dir, group string) string {
	out := transfertest.NewExportDir(exportRoot)
	gomega.Expect(filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(out, rel), 0o755)
		}
		if rel == manifest.FileName {
			return nil
		}
		return os.Link(p, filepath.Join(out, rel))
	})).To(gomega.Succeed())
	m, err := manifest.Read(filepath.Join(dir, manifest.FileName))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for gi := range m.Groups {
		if m.Groups[gi].Name != group {
			continue
		}
		for si := range m.Groups[gi].Stages {
			m.Groups[gi].Stages[si].ShardNum *= 2
		}
	}
	gomega.Expect(manifest.Write(filepath.Join(out, manifest.FileName), m)).To(gomega.Succeed())
	return out
}

// clusterData sums every row and document a plan of c reports.
func clusterData(c *cluster) uint64 {
	var n uint64
	for _, f := range transfertest.UnitFrames(transfertest.MustPlan(c.conn, &transferv1.PlanRequest{})) {
		for _, u := range f.GetUnits() {
			for _, sh := range u.GetSegment().GetShards() {
				n += sh.GetTotalCount()
			}
			n += u.GetSegment().GetSegmentLevel().GetDocCount()
			for _, sh := range u.GetProperty().GetShards() {
				n += sh.GetDocCount()
			}
		}
	}
	return n
}
