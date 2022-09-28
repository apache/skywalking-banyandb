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
	stream_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
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
		rootCmd.SetArgs([]string{"group", "create", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: group1`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("group group1 is created"))
		rootCmd.SetArgs([]string{"stream", "create", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name1
  group: group1
tagFamilies:
  - name: searchable
    tags: 
      - name: trace_id
        type: TAG_TYPE_STRING`))
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("stream group1.name1 is created"))
	})

	It("get stream schema", func() {
		rootCmd.SetArgs([]string{"stream", "get", "-g", "group1", "-n", "name1"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(database_v1.StreamRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Stream.Metadata.Group).To(Equal("group1"))
		Expect(resp.Stream.Metadata.Name).To(Equal("name1"))
	})

	It("update stream schema", func() {
		rootCmd.SetArgs([]string{"stream", "update", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name1
  group: group1
entity:
  tagNames: ["tag1"]`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("stream group1.name1 is updated"))
		rootCmd.SetArgs([]string{"stream", "get", "-g", "group1", "-n", "name1"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(database_v1.StreamRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Stream.Metadata.Group).To(Equal("group1"))
		Expect(resp.Stream.Metadata.Name).To(Equal("name1"))
		Expect(resp.Stream.Entity.TagNames[0]).To(Equal("tag1"))
	})

	It("delete stream schema", func() {
		// delete
		rootCmd.SetArgs([]string{"stream", "delete", "-g", "group1", "-n", "name1"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("stream group1.name1 is deleted"))
		// get again
		rootCmd.SetArgs([]string{"stream", "get", "-g", "group1", "-n", "name1"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("resource not found"))
	})

	It("list stream schema", func() {
		// create another stream schema for list operation
		rootCmd.SetArgs([]string{"stream", "create", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name2
  group: group1`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("stream group1.name2 is created"))
		// list
		rootCmd.SetArgs([]string{"stream", "list", "-g", "group1"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(database_v1.StreamRegistryServiceListResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Stream).To(HaveLen(2))
	})

	It("query stream data", func() {
		rootCmd.SetArgs([]string{"stream", "query", "-g", "group1", "-n", "name1"})
		rootCmd.SetIn(strings.NewReader(`
	metadata:
	 name: name1
	 group: group1
	timeRange:
	 begin: 2022-09-27T00:00:00Z
	 end: 2022-09-27T00:00:30Z
	projection:
	 tagFamilies:
	   - name: searchable
	     tags:
	       - trace_id`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		GinkgoWriter.Println(out) // todo
		resp := new(stream_v1.QueryResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		GinkgoWriter.Println(resp)
		Expect(resp.Elements).To(HaveLen(1))
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
