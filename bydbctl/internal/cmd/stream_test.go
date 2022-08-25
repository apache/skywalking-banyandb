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
	"fmt"
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
	"github.com/zenizh/go-capturer"
)

var _ = Describe("Stream", func() {
	var path string
	var gracefulStop, deferFunc func()
	var listenClientURL, listenPeerURL string
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
		gracefulStop = setup(true, flags)
		Eventually(helpers.HTTPHealthCheck("localhost:17913"), 10*time.Second).Should(Succeed())
	})

	FIt("create stream schema", func() {
		time.Sleep(1 * time.Second)
		rootCmd := cmd.NewRoot()
		rootCmd.SetArgs([]string{"stream", "create", "-j", "{\"stream\":{\"metadata\":{\"group\":\"group1\",\"name\":\"name1\"}}}"})
		out := capturer.CaptureOutput(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		fmt.Println("out:\n" + out)
	})

	It("get stream schema", func() {
		rootCmd := cmd.NewRoot()
		rootCmd.SetArgs([]string{"stream", "create", "-j", "{\"stream\":{\"metadata\":{\"group\":\"group1\",\"name\":\"name1\"}}}"})
		err := rootCmd.Execute()
		Expect(err).NotTo(HaveOccurred())
		rootCmd.SetArgs([]string{"stream", "get", "-j", "{\"metadata\":{\"group\":\"group1\",\"name\":\"name1\"}}"})
		out := capturer.CaptureOutput(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		fmt.Println("out:\n" + out)
	})

	//It("update stream schema", func() {
	//	// create
	//	// get
	//	// update
	//	// get
	//})
	//
	//It("delete stream schema", func() {
	//	// create
	//	// get
	//	// delete
	//	// get
	//})
	//
	//It("list stream schema", func() {
	//	// create * 2 or pre-load stream
	//	// list
	//})
	//
	//It("query stream data", func() {
	//	// create
	//	// insert data
	//	// query
	//})

	AfterEach(func() {
		gracefulStop()
		deferFunc()
	})
})

func setup(loadMetadata bool, flags []string) func() { // start banyand-server
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
	return "preload-stream" // query才需要，预加载schema
}

func (p *preloadMeasureService) Name() string {
	return "preload-measure"
}
