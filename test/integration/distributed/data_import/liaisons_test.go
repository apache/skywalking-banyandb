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
	"strings"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
	"github.com/apache/skywalking-banyandb/pkg/test/transfer/progress"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
)

// An import is bound to its target cluster, not to the liaison it went through: one interrupted
// through a liaison resumes through another liaison of the same cluster.
var _ = ginkgo.Describe("Import resumed through another liaison", ginkgo.Ordered, func() {
	var target *cluster
	var firstAddr, secondAddr string
	var liaisonNames []string

	ginkgo.BeforeAll(func() {
		target = startCluster([]string{hotLabel, hotLabel, warmLabel}, true, loadTargetStreamGroup)
		ginkgo.DeferCleanup(target.stop)
		ginkgo.By("Starting a second liaison")
		var closeSecond func()
		secondAddr, closeSecond = setup.LiaisonNode(target.config, "--data-node-selector", hotLabel)
		ginkgo.DeferCleanup(closeSecond)
		firstAddr = target.liaisonAddr
		liaisonNames = []string{transfertest.CurrentNodeName(firstAddr), transfertest.CurrentNodeName(secondAddr)}
		gomega.Expect(liaisonNames[0]).NotTo(gomega.Equal(liaisonNames[1]))
		ginkgo.By("Waiting until the first liaison registers both liaisons")
		gomega.Eventually(func(g gomega.Gomega) {
			state, err := databasev1.NewClusterStateServiceClient(target.conn).GetClusterState(context.Background(), &databasev1.GetClusterStateRequest{})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			var names []string
			for _, n := range state.GetRouteTables()["tire1"].GetRegistered() {
				names = append(names, n.GetMetadata().GetName())
			}
			g.Expect(names).To(gomega.ContainElements(liaisonNames))
		}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("records both liaisons, resumes through the second one and is then already imported", func() {
		opts := importOptions(csvArtifact)
		opts.Parallelism, opts.WriteInflightBatches, opts.WriteBatchSize = 1, 1, 5
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var seen atomic.Int32
		interrupted := transfertest.RunImport(ctx, firstAddr, opts, func(event string) {
			if strings.HasPrefix(event, "batch ") && seen.Add(1) == 3 {
				cancel()
			}
		})
		gomega.Expect(interrupted.ExitCode()).To(gomega.Equal(exitcode.ExitRuntime), interrupted.Log)
		p, err := progress.ReadImport(opts.Checkpoint)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(p).NotTo(gomega.BeNil())
		gomega.Expect(p.FinishedAt).To(gomega.BeNil())
		gomega.Expect(p.Liaisons).To(gomega.ContainElements(liaisonNames), "the progress file records every liaison of the cluster")
		gomega.Expect(p.TargetNodes).To(gomega.ContainElements(target.namesOf(hotLabel)))

		ginkgo.By("Resuming through the second liaison")
		opts.Resume = true
		resumed := transfertest.RunImport(context.Background(), secondAddr, opts, nil)
		gomega.Expect(resumed.Err).NotTo(gomega.HaveOccurred(), resumed.Log)
		gomega.Expect(resumed.Log).To(gomega.ContainSubstring("RESUME   " + opts.Checkpoint))
		gomega.Expect(resumed.Result.Verify).NotTo(gomega.BeNil())
		gomega.Expect(resumed.Result.Verify.Failed()).To(gomega.BeFalse(), resumed.Log)
		exportRoundTrip(target, csvArtifact)

		ginkgo.By("Running the import again through either liaison")
		opts.Resume = false
		for _, addr := range []string{secondAddr, firstAddr} {
			again := transfertest.RunImport(context.Background(), addr, opts, nil)
			gomega.Expect(again.Err).NotTo(gomega.HaveOccurred(), again.Log)
			gomega.Expect(again.Log).To(gomega.ContainSubstring("already imported into this cluster at "))
			gomega.Expect(again.Log).NotTo(gomega.ContainSubstring("IMPORTED "))
			// The target pre-creates the stream group with another shard number than the source's.
			gomega.Expect(again.Log).To(gomega.ContainSubstring("NOTE the data check of stream " + transfertest.StreamGroup +
				" stage hot is per (group, stage), not per shard"))
		}
	})
})
