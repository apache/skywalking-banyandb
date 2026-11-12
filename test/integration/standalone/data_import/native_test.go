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

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/importer"
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

var _ = ginkgo.Describe("Native import into a group with another shard number", func() {
	ginkgo.It("refuses the group's parts by the topology gate and writes nothing", func() {
		target := startTarget()
		g := artifactGroup(transfertest.StreamGroup)
		g.ResourceOpts.ShardNum += 2
		createGroup(target, g)
		run := runNativeImport(target.addr, func(o *config.Import) {
			o.ConflictRules = []config.ConflictRule{{Match: "group/" + transfertest.StreamGroup, Policy: config.ConflictPolicyIgnore}}
		})
		gomega.Expect(run.ExitCode()).To(gomega.Equal(exitcode.ExitPreflight), run.Log)
		gomega.Expect(run.Result.Plan.Rejected).To(gomega.ContainElement(gomega.HaveField("Gate", importer.GateTopology)), run.Out)
		gomega.Expect(run.Result.Push).To(gomega.BeNil(), "a refused import pushes nothing")
		gomega.Expect(totalData(target.conn)).To(gomega.BeZero(), "a refused import writes no data")
	})
})
