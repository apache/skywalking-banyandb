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

package cmd_test

import (
	"context"
	"strings"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/liaison/http"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/query"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/cmd"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/ghodss/yaml"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"
	"github.com/zenizh/go-capturer"

	database_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

var _ = Describe("Stream", func() {
	var path string
	var gracefulStop, deferFunc func()
	var listenClientURL, listenPeerURL string
	var rootCmd *cobra.Command
	BeforeEach(func() {
		var err error
		path, deferFunc, err = test.NewSpace()
		Expect(err).NotTo(HaveOccurred())
		listenClientURL, listenPeerURL, err = test.NewEtcdListenUrls()
		Expect(err).NotTo(HaveOccurred())
		flags := []string{
			"--stream-root-path=" + path, "--measure-root-path=" + path, "--metadata-root-path=" + path,
			"--etcd-listen-client-url=" + listenClientURL, "--etcd-listen-peer-url=" + listenPeerURL,
		}
		gracefulStop = setup(false, flags)
		Eventually(helpers.HTTPHealthCheck("localhost:17913"), 10*time.Second).Should(Succeed())
		time.Sleep(1 * time.Second)
		// extracting the operation of creating stream schema
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
		rootCmd.SetArgs([]string{"group", "create", "-r", "{\"group\":{\"metadata\":{\"group\":\"\",\"name\":\"group1\"}}}"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(Equal("{}\n"))
		rootCmd.SetArgs([]string{"stream", "create", "-r", "{\"stream\":{\"metadata\":{\"group\":\"group1\",\"name\":\"name1\"}}}"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(Equal("{}\n"))
	})

	It("get stream schema", func() {
		rootCmd.SetArgs([]string{"stream", "get", "-r", "{\"metadata\":{\"group\":\"group1\",\"name\":\"name1\"}}"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		GinkgoWriter.Write([]byte(out))
		resp := new(database_v1.StreamRegistryServiceGetResponse)
		Expect(yaml.Unmarshal([]byte(out), resp)).To(Succeed())
		GinkgoWriter.Write([]byte(resp.String()))
		Expect(resp.Stream.Metadata.Name).To(Equal("name1"))
		Expect(resp.Stream.Metadata.Group).To(Equal("group1"))
	})

	It("update stream schema", func() {
		rootCmd.SetArgs([]string{"stream", "update", "-r", "{\"stream\":{\"metadata\":{\"group\":\"group1\",\"name\":\"name1\"},\"entity\":{\"tag_names\":[\"tag1\"]}}}"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(Equal("{}\n"))

		rootCmd.SetArgs([]string{"stream", "get", "-r", "{\"metadata\":{\"group\":\"group1\",\"name\":\"name1\"}}"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		s := strings.Split(out, "\n")
		Expect(s[0]).To(Equal("stream:"))
		Expect(s[1]).To(Equal("  entity:"))
		Expect(s[2]).To(Equal("    tagNames:"))
		Expect(s[3]).To(Equal("    - tag1"))
		Expect(s[4]).To(Equal("  metadata:"))
		Expect(s[6]).To(Equal("    group: group1"))
		Expect(s[9]).To(Equal("    name: name1"))
	})

	It("delete stream schema", func() {
		// get
		rootCmd.SetArgs([]string{"stream", "get", "-r", "{\"metadata\":{\"group\":\"group1\",\"name\":\"name1\"}}"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		s := strings.Split(out, "\n")
		Expect(s[0]).To(Equal("stream:"))
		Expect(s[2]).To(Equal("  metadata:"))
		Expect(s[4]).To(Equal("    group: group1"))
		Expect(s[7]).To(Equal("    name: name1"))
		// delete
		rootCmd.SetArgs([]string{"stream", "delete", "-r", "{\"metadata\":{\"group\":\"group1\",\"name\":\"name1\"}}"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(Equal("deleted: true\n"))
		// get again
		rootCmd.SetArgs([]string{"stream", "get", "-r", "{\"metadata\":{\"group\":\"group1\",\"name\":\"name1\"}}"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		s = strings.Split(out, "\n")
		Expect(s[2]).To(Equal("message: 'banyandb: resource not found'"))
	})

	It("list stream schema", func() {
		// create another stream schema for list operation
		rootCmd.SetArgs([]string{"stream", "create", "-r", "{\"stream\":{\"metadata\":{\"group\":\"group1\",\"name\":\"name2\"}}}"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(Equal("{}\n"))
		// list
		rootCmd.SetArgs([]string{"stream", "list", "-r", "{\"group\":\"group1\"}"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		s := strings.Split(out, "\n")
		Expect(s[0]).To(Equal("stream:"))
		Expect(s[2]).To(Equal("  metadata:"))
		Expect(s[4]).To(Equal("    group: group1"))
		Expect(s[7]).To(Equal("    name: name1"))
		Expect(s[11]).To(Equal("  metadata:"))
		Expect(s[13]).To(Equal("    group: group1"))
		Expect(s[16]).To(Equal("    name: name2"))
	})

	AfterEach(func() {
		gracefulStop()
		deferFunc()
	})
})

func setup(loadMetadata bool, flags []string) func() {
	// Init `Discovery` module
	repo, err := discovery.NewServiceRepo(context.Background())
	Expect(err).NotTo(HaveOccurred())
	// Init `Queue` module
	pipeline, err := queue.NewQueue(context.TODO(), repo)
	Expect(err).NotTo(HaveOccurred())
	// Init `Metadata` module
	metaSvc, err := metadata.NewService(context.TODO())
	Expect(err).NotTo(HaveOccurred())
	// Init `Stream` module
	streamSvc, err := stream.NewService(context.TODO(), metaSvc, repo, pipeline)
	Expect(err).NotTo(HaveOccurred())
	// Init `Measure` module
	measureSvc, err := measure.NewService(context.TODO(), metaSvc, repo, pipeline)
	Expect(err).NotTo(HaveOccurred())
	// Init `Query` module
	q, err := query.NewExecutor(context.TODO(), streamSvc, measureSvc, metaSvc, repo, pipeline)
	Expect(err).NotTo(HaveOccurred())
	tcp := grpc.NewServer(context.TODO(), pipeline, repo, metaSvc)

	httpServer := http.NewService()
	if loadMetadata {
		return test.SetUpModules(
			flags,
			repo,
			pipeline,
			metaSvc,
			&preloadStreamService{metaSvc: metaSvc},
			&preloadMeasureService{metaSvc: metaSvc},
			streamSvc,
			measureSvc,
			q,
			tcp,
			httpServer,
		)
	}
	return test.SetUpModules(
		flags,
		repo,
		pipeline,
		metaSvc,
		streamSvc,
		measureSvc,
		q,
		tcp,
		httpServer,
	)
}

type preloadStreamService struct {
	metaSvc metadata.Service
}

type preloadMeasureService struct {
	metaSvc metadata.Service
}

func (p *preloadStreamService) Name() string {
	return "preload-stream"
}

func (p *preloadMeasureService) Name() string {
	return "preload-measure"
}
