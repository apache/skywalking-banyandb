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
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
	"github.com/apache/skywalking-banyandb/pkg/test/transfer/progress"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/importer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

// cancelOn returns a hook that cancels the import at the n-th event starting with prefix (and
// containing contains), and the context it cancels.
func cancelOn(prefix, contains string, n int32) (context.Context, func(string)) {
	ctx, cancel := context.WithCancel(context.Background())
	ginkgo.DeferCleanup(cancel)
	var seen atomic.Int32
	return ctx, func(event string) {
		if strings.HasPrefix(event, prefix) && strings.Contains(event, contains) && seen.Add(1) == n {
			cancel()
		}
	}
}

// interruptedImport imports input into the target at addr one item at a time and cancels it at
// the n-th event of prefix; it returns the options a resume reuses.
func interruptedImport(addr, input, prefix, contains string, n int32, mutate func(o *config.Import)) config.Import {
	opts := transfertest.DefaultImportOptions(input)
	opts.Parallelism, opts.WriteInflightBatches = 1, 1
	if mutate != nil {
		mutate(&opts)
	}
	ctx, hook := cancelOn(prefix, contains, n)
	run := transfertest.RunImport(ctx, addr, opts, hook)
	gomega.ExpectWithOffset(1, run.ExitCode()).To(gomega.Equal(exitcode.ExitRuntime), run.Log)
	gomega.ExpectWithOffset(1, opts.Checkpoint).To(gomega.BeAnExistingFile(), "the interrupted import left its progress file")
	return opts
}

// resumeImport resumes the import of opts and expects it to finish with verify passing.
func resumeImport(addr string, opts config.Import) transfertest.ImportRun {
	opts.Resume = true
	run := transfertest.RunImport(context.Background(), addr, opts, nil)
	gomega.ExpectWithOffset(1, run.Log).To(gomega.ContainSubstring("RESUME   " + opts.Checkpoint))
	expectImported(run)
	return run
}

// completeItems lists the work items the progress file at path records as complete.
func completeItems(path string) []string {
	p, err := progress.ReadImport(path)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	gomega.ExpectWithOffset(1, p).NotTo(gomega.BeNil(), "a progress file at %s", path)
	var keys []string
	for k, it := range p.Done {
		if it.Complete {
			keys = append(keys, k)
		}
	}
	return keys
}

// expectNothingDoneAgain asserts that run, a resume, pushed none of the work items done before it.
func expectNothingDoneAgain(run transfertest.ImportRun, doneBefore []string) {
	gomega.ExpectWithOffset(1, doneBefore).NotTo(gomega.BeEmpty(), "the interrupted run finished some work item")
	for _, k := range doneBefore {
		gomega.ExpectWithOffset(1, run.Result.Push.Done).NotTo(gomega.ContainElement(k), "the resume pushed %s again", k)
	}
}

// expectStoredOnce asserts, from the progress file of opts, that the stream and trace rows every run
// pushed are those of one uninterrupted import of the plan, above them by no more than the rows
// recorded as possible duplicates, per (catalog, group) (of a CSV artifact, exactly those
// of one import): a round trip compares distinct rows and cannot tell.
func expectStoredOnce(opts config.Import) {
	p, err := progress.ReadImport(opts.Checkpoint)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	gomega.ExpectWithOffset(1, p).NotTo(gomega.BeNil())
	m, err := progress.ReadManifest(opts.Input)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	once, pushed, dup := p.StreamTraceRowsOnce(m), p.StreamTracePushed(), p.StreamTraceDuplicates()
	gomega.ExpectWithOffset(1, once).To(gomega.BeNumerically(">", 0), "the artifact holds stream and trace rows")
	gomega.ExpectWithOffset(1, pushed).To(gomega.BeNumerically(">=", once), "every stream and trace row was pushed")
	gomega.ExpectWithOffset(1, pushed-once).To(gomega.BeNumerically("<=", dup),
		"stream and trace rows pushed %d, one import %d: more than the %d recorded as possible duplicates", pushed, once, dup)
	gomega.ExpectWithOffset(1, p.StreamTraceBoundViolations(m)).To(gomega.BeEmpty(), "per group")
}

// editProgress rewrites the progress file at path through edit, on its JSON.
func editProgress(path string, edit func(p map[string]any)) {
	data, err := os.ReadFile(path)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	var p map[string]any
	gomega.ExpectWithOffset(1, json.Unmarshal(data, &p)).To(gomega.Succeed())
	edit(p)
	data, err = json.Marshal(p)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	gomega.ExpectWithOffset(1, os.WriteFile(path, data, 0o600)).To(gomega.Succeed())
	gomega.ExpectWithOffset(1, os.Remove(resume.SidePath(path, "cursor"))).To(gomega.Or(gomega.Succeed(), gomega.MatchError(os.ErrNotExist)))
}

// streamShardItem is the first stream shard work item of the native artifact: its key, its
// stage, its source node and its first part (id and rows).
func streamShardItem() (key, stage, node string, partID, rows uint64) {
	m, err := manifest.Read(filepath.Join(nativeArtifactA, manifest.FileName))
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	for _, u := range m.Units {
		if !strings.EqualFold(u.Catalog, "stream") || u.Group != transfertest.StreamGroup {
			continue
		}
		for _, src := range u.Sources {
			for _, sh := range src.Shards {
				if len(sh.Parts) > 0 {
					return fmt.Sprintf("stream/%s/%s/%s/shard-%d", u.Group, u.Stage, u.Segment, sh.Shard), u.Stage, src.Node, sh.Parts[0].ID, sh.Parts[0].TotalCount
				}
			}
		}
	}
	ginkgo.Fail("the native artifact holds no stream part")
	return "", "", "", 0, 0
}

// shardParts are the parts, by id with their rows, that node holds of the stream shard item key
// of the native artifact.
func shardParts(key, node string) map[uint64]uint64 {
	m, err := manifest.Read(filepath.Join(nativeArtifactA, manifest.FileName))
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	out := map[uint64]uint64{}
	for _, u := range m.Units {
		if !strings.EqualFold(u.Catalog, "stream") || u.Group != transfertest.StreamGroup {
			continue
		}
		for _, src := range u.Sources {
			for _, sh := range src.Shards {
				if src.Node == node && fmt.Sprintf("stream/%s/%s/%s/shard-%d", u.Group, u.Stage, u.Segment, sh.Shard) == key {
					for _, pt := range sh.Parts {
						out[pt.ID] = pt.TotalCount
					}
				}
			}
		}
	}
	return out
}

// plannedNodes are the target nodes the plan side file of the progress file at path records for key.
func plannedNodes(path, key string) []any {
	p, err := progress.ReadImport(path)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	for _, item := range p.Plan {
		if item.Key == key {
			out := make([]any, len(item.TargetNodes))
			for i, n := range item.TargetNodes {
				out[i] = n
			}
			return out
		}
	}
	ginkgo.Fail("the progress file plans no " + key)
	return nil
}

var _ = ginkgo.Describe("Resumed CSV import", func() {
	ginkgo.It("stores every row of the artifact after an interruption, pushing nothing done again", func() {
		target := startTarget()
		opts := interruptedImport(target.addr, artifactA, "batch ", "", 3, func(o *config.Import) { o.WriteBatchSize = 5 })
		gomega.Expect(totalData(target.conn)).To(gomega.BeNumerically(">", 0), "the first run imported something")
		doneBefore := completeItems(opts.Checkpoint)
		run := resumeImport(target.addr, opts)
		gomega.Expect(run.Log).To(gomega.ContainSubstring(fmt.Sprintf("RESUMED   %d of ", len(doneBefore))))
		expectNothingDoneAgain(run, doneBefore)
		expectStoredOnce(opts)
		expectRoundTrip(target.addr)

		expectFinished(target.addr, opts)
	})
})

var _ = ginkgo.Describe("Resumed native import", func() {
	ginkgo.It("stores every part of the artifact once after an interruption among the shard items", func() {
		target := startTarget()
		// The series indexes go first, in a round of their own: stop after the first stream shard
		// item of the round of parts. The hook runs once that item is complete, so the resume skips
		// the whole item and pushes the items left; the specs below cover the parts of an item
		// that is not complete, which reconciliation decides one by one.
		opts := interruptedImport(target.addr, nativeArtifactA, "item stream/", "/shard-", 1, nil)
		doneBefore := completeItems(opts.Checkpoint)
		gomega.Expect(doneBefore).To(gomega.ContainElement(gomega.MatchRegexp(`^stream/.+/shard-\d+$`)), "a stream shard item was done")
		run := resumeImport(target.addr, opts)
		gomega.Expect(run.Log).To(gomega.ContainSubstring(fmt.Sprintf("RESUMED   %d of ", len(doneBefore))))
		expectNothingDoneAgain(run, doneBefore)
		gomega.Expect(run.Result.Push.Done).To(gomega.ContainElement(gomega.MatchRegexp(`/shard-\d+$`)), "the resume pushed the shard items left")
		expectStoredOnce(opts)
		expectRoundTrip(target.addr)
	})

	ginkgo.It("reconciles a stream part that landed and is not pushed again", func() {
		target := startTarget()
		key, stage, node, partID, rows := streamShardItem()
		opts := interruptedImport(target.addr, nativeArtifactA, "item "+key+" ", "", 1, nil)
		// The answer of the part was lost: the progress file has it in flight, not done. A complete
		// item keeps only its landed totals, so its other payloads are recorded again as landed.
		editProgress(opts.Checkpoint, func(p map[string]any) {
			item := p["done"].(map[string]any)[key].(map[string]any)
			gomega.Expect(item["complete"]).To(gomega.BeTrue())
			nodes := plannedNodes(opts.Checkpoint, key)
			parts := map[string]any{}
			for id, n := range shardParts(key, node) {
				if id != partID {
					parts[fmt.Sprintf("%s/%d", node, id)] = map[string]any{"nodes": nodes, "rows": n}
				}
			}
			p["done"].(map[string]any)[key] = map[string]any{
				"complete": false, "parts": parts,
				"blobs": map[string]any{node + "/ELEMENT_INDEX": map[string]any{"nodes": nodes}},
			}
			// A part whose answer was lost was never counted as pushed.
			for _, c := range p["pushed"].([]any) {
				if c := c.(map[string]any); c["catalog"] == "stream" && c["group"] == transfertest.StreamGroup && c["stage"] == stage && c["kind"] == "shard" {
					c["rows"] = c["rows"].(float64) - float64(rows)
				}
			}
			p["inflight"] = map[string]any{"0": map[string]any{"key": key, "source": node, "kind": "PART", "partID": partID, "rows": rows, "nodes": nodes}}
		})
		run := resumeImport(target.addr, opts)
		gomega.Expect(run.Log).To(gomega.MatchRegexp(fmt.Sprintf(`reconcile %s part %d of %s on \S+: landed`, regexp.QuoteMeta(key), partID, regexp.QuoteMeta(node))))
		// Not pushed again: a second copy would pass the round trip, which compares distinct rows.
		p, err := progress.ReadImport(opts.Checkpoint)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(p.StreamTraceDuplicates()).To(gomega.BeZero(), "nothing was pushed again")
		expectStoredOnce(opts)
		expectRoundTrip(target.addr)
	})

	ginkgo.It("reconciles a stream part that did not land and pushes it", func() {
		target := startTarget()
		key, _, node, partID, rows := streamShardItem()
		// The series indexes go first: cancel after the first, before any part is sent.
		opts := interruptedImport(target.addr, nativeArtifactA, "item ", "/segment", 1, nil)
		editProgress(opts.Checkpoint, func(p map[string]any) {
			nodes := plannedNodes(opts.Checkpoint, key)
			gomega.Expect(p["done"]).NotTo(gomega.HaveKey(key))
			p["inflight"] = map[string]any{"0": map[string]any{"key": key, "source": node, "kind": "PART", "partID": partID, "rows": rows, "nodes": nodes}}
		})
		run := resumeImport(target.addr, opts)
		gomega.Expect(run.Log).To(gomega.MatchRegexp(fmt.Sprintf(`reconcile %s part %d of %s on \S+: not landed`, regexp.QuoteMeta(key), partID, regexp.QuoteMeta(node))))
		expectRoundTrip(target.addr)
	})
})

var _ = ginkgo.Describe("Import progress file", func() {
	ginkgo.It("is kept next to the artifact by default under a fixed name, and a rerun pushes nothing", func() {
		target := startTarget()
		// A copy of the artifact, so the progress file does not land in the one every spec reads.
		input := filepath.Join(ginkgo.GinkgoT().TempDir(), "artifact")
		gomega.Expect(os.CopyFS(input, os.DirFS(artifactA))).To(gomega.Succeed())
		opts := transfertest.DefaultImportOptions(input)
		opts.Checkpoint = ""
		run := transfertest.RunImport(context.Background(), target.addr, opts, nil)
		expectImported(run)
		path := filepath.Join(input, importer.ProgressFileName)
		gomega.Expect(path).To(gomega.BeAnExistingFile())
		gomega.Expect(path+".lock").NotTo(gomega.BeAnExistingFile(), "the lock is released")
		opts.Checkpoint = path
		expectFinished(target.addr, opts)

		// --restart keeps the file aside and imports again.
		opts.Restart = true
		restarted := transfertest.RunImport(context.Background(), target.addr, opts, nil)
		gomega.Expect(restarted.Err).NotTo(gomega.HaveOccurred(), restarted.Log)
		gomega.Expect(restarted.Log).To(gomega.ContainSubstring("WARN --restart: " + path + " is kept as "))
		baks, err := filepath.Glob(path + ".2*.bak")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(baks).To(gomega.HaveLen(1))
		gomega.Expect(restarted.Result.Push.Done).NotTo(gomega.BeEmpty())
	})
})

// expectFinished runs the finished import of opts again: it prints the FINISHED summary (no LEFT
// line), asks nothing, pushes nothing and exits 0.
func expectFinished(addr string, opts config.Import) {
	again := transfertest.RunImport(context.Background(), addr, opts, nil)
	gomega.ExpectWithOffset(1, again.Err).NotTo(gomega.HaveOccurred(), again.Log)
	gomega.ExpectWithOffset(1, again.Log).To(gomega.ContainSubstring("FINISHED " + opts.Checkpoint + "  (finished at "))
	gomega.ExpectWithOffset(1, again.Log).To(gomega.MatchRegexp(`(?m)^DONE +\d+ group\(s\), \d+ work item\(s\)`))
	gomega.ExpectWithOffset(1, again.Log).NotTo(gomega.ContainSubstring("RESUME "))
	gomega.ExpectWithOffset(1, again.Log).NotTo(gomega.ContainSubstring("LEFT "))
	gomega.ExpectWithOffset(1, again.Log).To(gomega.ContainSubstring("already imported into this cluster at "))
	gomega.ExpectWithOffset(1, again.Result.Push).To(gomega.BeNil(), "a finished import pushes nothing again")
}
