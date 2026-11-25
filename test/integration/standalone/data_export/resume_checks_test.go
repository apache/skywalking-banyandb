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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exporter"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

// tree maps every file below dir to its content.
func tree(dir string) map[string]string {
	out := map[string]string{}
	gomega.ExpectWithOffset(1, filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		body, readErr := os.ReadFile(p)
		out[p] = string(body)
		return readErr
	})).To(gomega.Succeed())
	return out
}

// interruptAfterFirstUnit runs opts until its first unit is done and returns the session id and
// the units the progress file records as done.
func interruptAfterFirstUnit(opts config.Export) (string, []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run := transfertest.RunExport(ctx, grpcAddr, opts, func(line string) {
		if strings.HasPrefix(line, "✓ ") && strings.Contains(line, "] ") {
			cancel()
		}
	})
	gomega.ExpectWithOffset(1, run.ExitCode()).To(gomega.Equal(exitcode.ExitRuntime), run.Log)
	id := run.SessionID()
	gomega.ExpectWithOffset(1, id).NotTo(gomega.BeEmpty())
	ginkgo.DeferCleanup(func() {
		_, _ = transfertest.Sessions(conn, &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_RELEASE, SessionId: id})
	})
	data, err := os.ReadFile(exporter.ProgressPath(opts))
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	var p struct {
		Done []struct {
			Unit string `json:"unit"`
		} `json:"done"`
	}
	gomega.ExpectWithOffset(1, json.Unmarshal(data, &p)).To(gomega.Succeed())
	var done []string
	for _, d := range p.Done {
		done = append(done, d.Unit)
	}
	gomega.ExpectWithOffset(1, done).NotTo(gomega.BeEmpty(), "the canceled run finished a unit")
	return id, done
}

// addCopy registers a copy of the group's first stream or property named name and returns
// what deletes it again.
func addCopy(catalog commonv1.Catalog, group, name string) func() {
	ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
	defer cancel()
	switch catalog {
	case commonv1.Catalog_CATALOG_STREAM:
		c := databasev1.NewStreamRegistryServiceClient(conn)
		resp, err := c.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{Metadata: &commonv1.Metadata{Group: group, Name: transfertest.StreamName}})
		gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
		s := proto.Clone(resp.GetStream()).(*databasev1.Stream)
		s.Metadata = &commonv1.Metadata{Group: group, Name: name}
		_, err = c.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{Stream: s})
		gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
		return func() {
			delCtx, delCancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
			defer delCancel()
			_, delErr := c.Delete(delCtx, &databasev1.StreamRegistryServiceDeleteRequest{Metadata: &commonv1.Metadata{Group: group, Name: name}})
			gomega.Expect(delErr).NotTo(gomega.HaveOccurred())
		}
	default:
		c := databasev1.NewPropertyRegistryServiceClient(conn)
		resp, err := c.Get(ctx, &databasev1.PropertyRegistryServiceGetRequest{Metadata: &commonv1.Metadata{Group: group, Name: transfertest.PropertyName}})
		gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
		p := proto.Clone(resp.GetProperty()).(*databasev1.Property)
		p.Metadata = &commonv1.Metadata{Group: group, Name: name}
		_, err = c.Create(ctx, &databasev1.PropertyRegistryServiceCreateRequest{Property: p})
		gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
		return func() {
			delCtx, delCancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
			defer delCancel()
			_, delErr := c.Delete(delCtx, &databasev1.PropertyRegistryServiceDeleteRequest{Metadata: &commonv1.Metadata{Group: group, Name: name}})
			gomega.Expect(delErr).NotTo(gomega.HaveOccurred())
		}
	}
}

var _ = ginkgo.Describe("Export lock and resume checks on a standalone process", ginkgo.Ordered, func() {
	ginkgo.BeforeAll(func() {
		waitForFlushedData()
		expectNoSession()
	})

	ginkgo.It("refuses a second export of the same output with exit 1 and removes the lock when the first is canceled", func() {
		out := filepath.Join(ginkgo.GinkgoT().TempDir(), "export")
		opts := transfertest.DefaultExportOptions(out)
		opts.NodeRowRateLimit = 1
		lockFile := resume.LockPath(exporter.ProgressPath(opts))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		started := make(chan struct{})
		firstDone := make(chan transfertest.ExportRun, 1)
		//panicdiag:allow-rawgo the first export runs while the spec starts a second one
		go func() {
			defer ginkgo.GinkgoRecover()
			var once sync.Once
			firstDone <- transfertest.RunExport(ctx, grpcAddr, opts, func(line string) {
				if strings.Contains(line, "] ") && strings.Contains(line, " started: ") {
					once.Do(func() { close(started) })
				}
			})
		}()
		gomega.Eventually(started, flags.EventuallyTimeout).Should(gomega.BeClosed())
		gomega.Expect(lockFile).To(gomega.BeAnExistingFile())

		second := opts
		second.NodeRowRateLimit = 0
		second.Resume = true
		refused := transfertest.RunExport(context.Background(), grpcAddr, second, nil)
		gomega.Expect(refused.ExitCode()).To(gomega.Equal(exitcode.ExitUsage), refused.Log)
		gomega.Expect(refused.Err.Error()).To(gomega.ContainSubstring("another bydbctl is using " + exporter.ProgressPath(opts)))
		gomega.Expect(refused.Err.Error()).To(gomega.ContainSubstring(fmt.Sprintf("held by pid %d on ", os.Getpid())))
		gomega.Expect(lockFile).To(gomega.BeAnExistingFile(), "the refused run leaves the holder's lock file")

		cancel()
		var first transfertest.ExportRun
		gomega.Eventually(firstDone, flags.EventuallyTimeout).Should(gomega.Receive(&first))
		gomega.Expect(first.ExitCode()).To(gomega.Equal(exitcode.ExitRuntime), first.Log)
		gomega.Expect(lockFile).NotTo(gomega.BeAnExistingFile(), "an interrupted export removes its lock file")

		resumed := transfertest.RunExport(context.Background(), grpcAddr, second, nil)
		gomega.Expect(resumed.Err).NotTo(gomega.HaveOccurred(), resumed.Log)
		gomega.Expect(resumed.Log).NotTo(gomega.ContainSubstring("taking over a stale lock"))
		gomega.Expect(lockFile).NotTo(gomega.BeAnExistingFile())
		transfertest.ReadArtifact(out, false).VerifyManifest()
		expectNoSession()
	})

	ginkgo.It("refuses a CSV resume after a resource was added to a group with units left, before reading the session", func() {
		out := filepath.Join(ginkgo.GinkgoT().TempDir(), "export")
		opts := transfertest.DefaultExportOptions(out)
		opts.Parallelism = 1
		opts.Selectors = []*transferv1.Selector{
			{Catalog: commonv1.Catalog_CATALOG_STREAM, Groups: []string{transfertest.StreamGroup}},
			{Catalog: commonv1.Catalog_CATALOG_PROPERTY, Groups: []string{transfertest.PropertyGroup}},
		}
		id, done := interruptAfterFirstUnit(opts)
		// The group of the done unit may be finished; the other one has units left.
		catalog, group, kind := commonv1.Catalog_CATALOG_PROPERTY, transfertest.PropertyGroup, "property"
		if strings.HasPrefix(strings.ToLower(done[0]), "property") {
			catalog, group, kind = commonv1.Catalog_CATALOG_STREAM, transfertest.StreamGroup, "stream"
		}
		remove := addCopy(catalog, group, "resume_check_added")
		removed := false
		defer func() {
			if !removed {
				remove()
			}
		}()

		before := tree(out)
		opts.Resume = true
		refused := transfertest.RunExport(context.Background(), grpcAddr, opts, nil)
		gomega.Expect(refused.ExitCode()).To(gomega.Equal(exitcode.ExitResume), refused.Log)
		gomega.Expect(refused.Err.Error()).To(gomega.ContainSubstring("the schema changed after 1 unit(s) were exported with the first one, " +
			"so this export cannot be resumed; nothing was deleted"))
		gomega.Expect(refused.Err.Error()).To(gomega.ContainSubstring(kind + " " + group + "/resume_check_added: added"))
		gomega.Expect(refused.Err.Error()).To(gomega.ContainSubstring("to start over: release the session with: bydbctl data release-session --id " + id))
		gomega.Expect(tree(out)).To(gomega.Equal(before), "nothing is deleted or written, the progress file included")
		expectSingleSession(id)

		ginkgo.By("Resuming once the schema is back as it was")
		remove()
		removed = true
		resumed := transfertest.RunExport(context.Background(), grpcAddr, opts, nil)
		gomega.Expect(resumed.Err).NotTo(gomega.HaveOccurred(), resumed.Log)
		transfertest.ReadArtifact(out, false).VerifyManifest()
		expectNoSession()
	})

	ginkgo.It("resumes a native export after a resource was added to a group, with a NOTE", func() {
		out := filepath.Join(ginkgo.GinkgoT().TempDir(), "export")
		opts := transfertest.DefaultNativeExportOptions(out)
		opts.Parallelism = 1
		opts.Selectors = []*transferv1.Selector{
			{Catalog: commonv1.Catalog_CATALOG_STREAM, Groups: []string{transfertest.StreamGroup}},
			{Catalog: commonv1.Catalog_CATALOG_PROPERTY, Groups: []string{transfertest.PropertyGroup}},
		}
		interruptAfterFirstUnit(opts)
		defer addCopy(commonv1.Catalog_CATALOG_STREAM, transfertest.StreamGroup, "resume_check_native")()

		opts.Resume = true
		resumed := transfertest.RunExport(context.Background(), grpcAddr, opts, nil)
		gomega.Expect(resumed.Err).NotTo(gomega.HaveOccurred(), resumed.Log)
		gomega.Expect(resumed.Log).To(gomega.ContainSubstring("NOTE resource(s) added to the schema since the first run are not part of this export: " +
			"stream " + transfertest.StreamGroup + "/resume_check_native"))
		a := transfertest.ReadNativeArtifact(out)
		for _, u := range a.Manifest.Units {
			gomega.Expect(u.Group).To(gomega.BeElementOf(transfertest.StreamGroup, transfertest.PropertyGroup))
		}
		expectNoSession()
	})
})
