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
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
	"github.com/apache/skywalking-banyandb/pkg/test/transfer/progress"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
)

// A real target usually answers a completion before the stop reaches the worker, so this spec does
// not depend on the stop landing between a completion and its answer; the wait for an answer
// held back is proven deterministically by the importer's
// TestPushParts_ControlledStopWaitsForTheAnswerOfASentCompletion, whose liaison holds the answer
// until the push waits for it.
var _ = ginkgo.Describe("Native import stopped while payloads are in flight", func() {
	ginkgo.It("leaves no payload in flight after a controlled stop, so a resume finds nothing ambiguous and stores every part once", func() {
		target := startTarget()
		opts := transfertest.DefaultImportOptions(nativeArtifactA)
		opts.Parallelism = math.MaxInt
		// Stop at the first shard item done: the other workers are in the middle of their payloads,
		// some of them past the completion.
		ctx, cancel := context.WithCancel(context.Background())
		ginkgo.DeferCleanup(cancel)
		var seen atomic.Int32
		stopped := transfertest.RunImport(ctx, target.addr, opts, func(event string) {
			if strings.HasPrefix(event, "item ") && strings.Contains(event, "/shard-") && seen.Add(1) == 1 {
				cancel()
			}
		})
		gomega.Expect(stopped.ExitCode()).To(gomega.Equal(exitcode.ExitRuntime), stopped.Log)
		p, err := progress.ReadImport(opts.Checkpoint)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(p.Inflight).To(gomega.BeEmpty(), "every payload sent was answered and recorded")

		opts.Resume = true
		resumed := transfertest.RunImport(context.Background(), target.addr, opts, nil)
		expectImported(resumed)
		gomega.Expect(resumed.Log).NotTo(gomega.ContainSubstring("AMBIGUOUS"))
		expectStoredOnce(opts)
		expectRoundTrip(target.addr)
	})
})

var _ = ginkgo.Describe("Import plan side file", func() {
	ginkgo.It("holds the plan the progress file names by digest, and is kept aside by --restart", func() {
		target := startTarget()
		opts := transfertest.DefaultImportOptions(artifactA)
		expectImported(transfertest.RunImport(context.Background(), target.addr, opts, nil))
		data, err := os.ReadFile(opts.Checkpoint)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		raw := map[string]json.RawMessage{}
		gomega.Expect(json.Unmarshal(data, &raw)).To(gomega.Succeed())
		gomega.Expect(raw).NotTo(gomega.HaveKey("plan"), "the progress file does not hold the plan")
		gomega.Expect(raw).To(gomega.HaveKey("planDigest"))
		gomega.Expect(progress.PlanPath(opts.Checkpoint)).To(gomega.BeAnExistingFile())
		p, err := progress.ReadImport(opts.Checkpoint)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(p.Plan).NotTo(gomega.BeEmpty())
		gomega.Expect(p.Complete()).To(gomega.Equal(len(p.Plan)), "every planned item is complete")

		opts.Restart, opts.Verify = true, false
		restarted := transfertest.RunImport(context.Background(), target.addr, opts, nil)
		gomega.Expect(restarted.Err).NotTo(gomega.HaveOccurred(), restarted.Log)
		gomega.Expect(restarted.Log).To(gomega.ContainSubstring("already imported are stored twice"))
		baks, err := filepath.Glob(progress.PlanPath(opts.Checkpoint) + ".2*.bak")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(baks).To(gomega.HaveLen(1), "the plan side file is kept with the progress file")
		gomega.Expect(progress.PlanPath(opts.Checkpoint)).To(gomega.BeAnExistingFile(), "the fresh import has a plan of its own")
	})
})

var _ = ginkgo.Describe("Items once refused by the TTL gate", func() {
	ginkgo.It("are imported by a later run with --force-ttl, which appends them to the plan of the finished import", func() {
		target := startTarget()
		later := func() time.Time { return time.Now().AddDate(0, 3, 0) }
		opts := transfertest.DefaultImportOptions(artifactA)
		opts.Verify = false
		first := transfertest.RunImportAt(context.Background(), target.addr, opts, later, nil)
		gomega.Expect(first.Err).NotTo(gomega.HaveOccurred(), first.Log)
		gomega.Expect(first.Result.Plan.Rejected).NotTo(gomega.BeEmpty(), "the TTL gate refused the segment items")
		before, err := progress.ReadImport(opts.Checkpoint)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(before.FinishedAt).NotTo(gomega.BeNil())
		refused := len(first.Result.Plan.Rejected)

		ginkgo.By("Previewing the run with --force-ttl")
		opts.ForceTTL = true
		dry := opts
		dry.DryRun = true
		preview := transfertest.RunImportAt(context.Background(), target.addr, dry, later, nil)
		gomega.Expect(preview.Err).NotTo(gomega.HaveOccurred(), preview.Log)
		gomega.Expect(preview.Log).To(gomega.ContainSubstring("a gate refused when the plan was recorded are accepted now"))
		gomega.Expect(preview.Log).NotTo(gomega.ContainSubstring("already imported"))

		ginkgo.By("Importing them")
		again := transfertest.RunImportAt(context.Background(), target.addr, opts, later, nil)
		gomega.Expect(again.Err).NotTo(gomega.HaveOccurred(), again.Log)
		gomega.Expect(again.Log).NotTo(gomega.ContainSubstring("already imported"))
		gomega.Expect(again.Log).To(gomega.ContainSubstring("a gate refused when the plan was recorded are added to it"))
		gomega.Expect(again.Result.Push.Done).To(gomega.HaveLen(refused), "only the items once refused are pushed")
		for _, k := range again.Result.Push.Done {
			gomega.Expect(before.Done).NotTo(gomega.HaveKey(k), "%s was not pushed by the first run", k)
		}
		after, err := progress.ReadImport(opts.Checkpoint)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(after.FinishedAt).NotTo(gomega.BeNil())
		gomega.Expect(after.Plan).To(gomega.HaveLen(len(before.Plan) + refused))
		expectRoundTrip(target.addr)

		ginkgo.By("Running it once more")
		third := transfertest.RunImportAt(context.Background(), target.addr, opts, later, nil)
		gomega.Expect(third.Err).NotTo(gomega.HaveOccurred(), third.Log)
		gomega.Expect(third.Log).To(gomega.ContainSubstring("already imported into this cluster at "))
	})
})
