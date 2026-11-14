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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/importer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

// nativeArtifactA is the source exported in the native format, right after artifactA, so
// both hold the same data and a native round trip compares with artifactA exactly.
var nativeArtifactA string

// exportNative exports the source at addr in the native format and returns the export.
func exportNative(addr string) string {
	out := transfertest.NewExportDir(workDir)
	opts := transfertest.DefaultExportOptions(out)
	opts.Format = transferv1.Format_FORMAT_NATIVE
	opts.Compress = transferv1.Compress_COMPRESS_NONE
	run := transfertest.RunExport(context.Background(), addr, opts, nil)
	gomega.ExpectWithOffset(1, run.Err).NotTo(gomega.HaveOccurred(), run.Log)
	return out
}

// runNativeImport imports the native artifact of the source into the target at addr.
func runNativeImport(addr string, mutate func(o *config.Import)) transfertest.ImportRun {
	gomega.ExpectWithOffset(1, nativeArtifactA).NotTo(gomega.BeEmpty(), "the suite exported the native artifact")
	opts := transfertest.DefaultImportOptions(nativeArtifactA)
	if mutate != nil {
		mutate(&opts)
	}
	return transfertest.RunImport(context.Background(), addr, opts, nil)
}

var _ = ginkgo.Describe("Native import into an empty standalone target", ginkgo.Ordered, func() {
	var target *instance
	var dry transfertest.ImportRun

	ginkgo.BeforeAll(func() {
		target = startTarget()
	})

	ginkgo.It("plans a dry run without changing the target", func() {
		before := schemaSnapshot(target.conn)
		dry = runNativeImport(target.addr, func(o *config.Import) { o.DryRun = true })
		gomega.Expect(dry.ExitCode()).To(gomega.Equal(0), dry.Log)
		gomega.Expect(dry.Result.Plan.Header.Format).To(gomega.Equal(manifest.FormatNative))
		gomega.Expect(dry.Result.Plan.RowCheck).To(gomega.BeNil(), "native parts have no rows to check")
		gomega.Expect(dry.Result.Push).To(gomega.BeNil(), "a dry run pushes nothing")
		gomega.Expect(schemaSnapshot(target.conn)).To(gomega.Equal(before), "a dry run leaves the target schema unchanged")
		gomega.Expect(totalData(target.conn)).To(gomega.BeZero(), "a dry run writes no data")
	})

	ginkgo.It("imports every catalog and prints the plan of the dry run byte for byte", func() {
		run := runNativeImport(target.addr, nil)
		expectImported(run)
		gomega.Expect(run.Out).To(gomega.Equal(dry.Out), "the real run prints the dry run's plan")
		gomega.Expect(run.Result.Push.Done).NotTo(gomega.BeEmpty())
	})

	ginkgo.It("re-exports the source rows, Top-N results and the latest property revisions unchanged", func() {
		b := expectRoundTrip(target.addr)
		artB := transfertest.ReadArtifact(b, false)
		artA := transfertest.ReadArtifact(artifactA, false)
		gomega.Expect(topNResultRows(artB)).
			To(gomega.Equal(topNResultRows(artA)))
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

// expectRefusedBy asserts that run was refused by gate before it wrote anything to target.
func expectRefusedBy(target *instance, run transfertest.ImportRun, gate string) {
	gomega.ExpectWithOffset(1, run.ExitCode()).To(gomega.Equal(exitcode.ExitPreflight), run.Log)
	gomega.ExpectWithOffset(1, run.Result.Plan.Rejected).To(gomega.ContainElement(gomega.HaveField("Gate", gate)), run.Out)
	gomega.ExpectWithOffset(1, run.Result.Push).To(gomega.BeNil(), "a refused import pushes nothing")
	gomega.ExpectWithOffset(1, totalData(target.conn)).To(gomega.BeZero(), "a refused import writes no data")
}

// editedNativeArtifact copies the native artifact of the source and changes the copy's
// manifest with edit. The manifest registers no checksum of itself, so the copy still opens.
func editedNativeArtifact(edit func(m *manifest.Manifest)) string {
	dst := filepath.Join(ginkgo.GinkgoT().TempDir(), "artifact")
	gomega.ExpectWithOffset(1, filepath.WalkDir(nativeArtifactA, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(nativeArtifactA, p)
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), raw, 0o600)
	})).To(gomega.Succeed())
	path := filepath.Join(dst, manifest.FileName)
	m, err := manifest.Read(path)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	edit(m)
	gomega.ExpectWithOffset(1, manifest.Write(path, m)).To(gomega.Succeed())
	return dst
}

var _ = ginkgo.Describe("Native import into a group with another shard number", func() {
	ginkgo.It("refuses the group's parts by the topology gate and writes nothing", func() {
		target := startTarget()
		g := artifactGroup(transfertest.StreamGroup)
		g.ResourceOpts.ShardNum += 2
		createGroup(target, g)
		run := runNativeImport(target.addr, func(o *config.Import) {
			o.ConflictRules = []config.ConflictRule{{Match: "group/" + transfertest.StreamGroup, Policy: config.ConflictPolicyIgnore}}
		})
		expectRefusedBy(target, run, importer.GateTopology)
	})
})

var _ = ginkgo.Describe("Native import into a group whose index rule has another id", func() {
	ginkgo.It("refuses the group's parts by the index rule gate and writes nothing", func() {
		var rule *databasev1.IndexRule
		for _, r := range transfertest.Schemas(nativeArtifactA, schemasync.KindIndexRule, func() *databasev1.IndexRule { return &databasev1.IndexRule{} }) {
			if r.GetMetadata().GetGroup() == transfertest.StreamGroup {
				rule = r
				break
			}
		}
		gomega.Expect(rule).NotTo(gomega.BeNil(), "the artifact has an index rule of group %s", transfertest.StreamGroup)
		gomega.Expect(rule.GetMetadata().GetId()).NotTo(gomega.BeZero(), "the artifact records the rule's id")
		target := startTarget()
		createGroup(target, artifactGroup(transfertest.StreamGroup))
		onTarget := importer.PrepareForWrite(rule).(*databasev1.IndexRule)
		onTarget.Metadata.Id = rule.GetMetadata().GetId() + 1
		ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
		defer cancel()
		_, err := databasev1.NewIndexRuleRegistryServiceClient(target.conn).Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{IndexRule: onTarget})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		run := runNativeImport(target.addr, nil)
		expectRefusedBy(target, run, importer.GateIndexRule)
	})
})

var _ = ginkgo.Describe("Native import of an artifact exported in another time zone", func() {
	ginkgo.It("refuses the segment parts by the time zone gate and writes nothing", func() {
		target := startTarget()
		dir := editedNativeArtifact(func(m *manifest.Manifest) {
			gomega.Expect(m.Source.Nodes).NotTo(gomega.BeEmpty())
			for i := range m.Source.Nodes {
				n := &m.Source.Nodes[i]
				gomega.Expect(n.Timezone).NotTo(gomega.BeEmpty(), "the export records the source node's time zone")
				// The source and the target run on this host, so any other zone differs from the target's.
				if n.Timezone == "Pacific/Chatham" {
					n.Timezone = "Asia/Kathmandu"
				} else {
					n.Timezone = "Pacific/Chatham"
				}
			}
		})
		run := runNativeImport(target.addr, func(o *config.Import) { o.Input = dir })
		expectRefusedBy(target, run, importer.GateTimezone)
	})
})

var _ = ginkgo.Describe("Native import of segments in a file format the target cannot read", func() {
	ginkgo.It("refuses the segment parts by the version gate and writes nothing", func() {
		target := startTarget()
		dir := editedNativeArtifact(func(m *manifest.Manifest) {
			for i := range m.Units {
				if m.Units[i].SegmentVersion != "" {
					m.Units[i].SegmentVersion = "0.0.1"
				}
			}
		})
		run := runNativeImport(target.addr, func(o *config.Import) { o.Input = dir })
		expectRefusedBy(target, run, importer.GateVersion)
	})
})
