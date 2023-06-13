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

// Package setup implements a real env in which to run tests.
package setup

import (
	"context"
	"fmt"

	"github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/liaison/http"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/query"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
)

const host = "127.0.0.1"

// Common wires common modules to build a testing ready runtime.
func Common(flags ...string) (string, string, func()) {
	return CommonWithSchemaLoaders([]SchemaLoader{
		&preloadService{name: "stream"},
		&preloadService{name: "measure"},
	}, flags...)
}

// CommonWithSchemaLoaders wires common modules to build a testing ready runtime. It also allows to preload schema.
func CommonWithSchemaLoaders(schemaLoaders []SchemaLoader, flags ...string) (string, string, func()) {
	path, _, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var ports []int
	ports, err = test.AllocateFreePorts(4)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr := fmt.Sprintf("%s:%d", host, ports[0])
	httpAddr := fmt.Sprintf("%s:%d", host, ports[1])
	ff := []string{
		"--addr=" + addr,
		"--http-addr=" + httpAddr,
		"--http-grpc-addr=" + addr,
		"--stream-root-path=" + path,
		"--measure-root-path=" + path,
		"--metadata-root-path=" + path,
		fmt.Sprintf("--etcd-listen-client-url=http://%s:%d", host, ports[2]), fmt.Sprintf("--etcd-listen-peer-url=http://%s:%d", host, ports[3]),
	}
	if len(flags) > 0 {
		ff = append(ff, flags...)
	}
	gracefulStop := modules(schemaLoaders, ff)
	return addr, httpAddr, func() {
		gracefulStop()
		// deferFn()
	}
}

func modules(schemaLoaders []SchemaLoader, flags []string) func() {
	// Init `Discovery` module
	repo, err := discovery.NewServiceRepo(context.Background())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// Init `Queue` module
	pipeline, err := queue.NewQueue(context.TODO(), repo)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// Init `Metadata` module
	metaSvc, err := metadata.NewService(context.TODO())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// Init `Stream` module
	streamSvc, err := stream.NewService(context.TODO(), metaSvc, repo, pipeline)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// Init `Measure` module
	measureSvc, err := measure.NewService(context.TODO(), metaSvc, repo, pipeline)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// Init `Query` module
	q, err := query.NewService(context.TODO(), streamSvc, measureSvc, metaSvc, repo, pipeline)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	tcp := grpc.NewServer(context.TODO(), pipeline, repo, metaSvc)
	httpServer := http.NewService()

	units := []run.Unit{
		repo,
		pipeline,
		metaSvc,
	}
	for _, sl := range schemaLoaders {
		sl.SetMeta(metaSvc)
		units = append(units, sl)
	}
	units = append(units, streamSvc, measureSvc, q, tcp, httpServer)

	return test.SetupModules(
		flags,
		units...,
	)
}

// SchemaLoader is a service that can preload schema.
type SchemaLoader interface {
	run.Unit
	SetMeta(meta metadata.Service)
}

type preloadService struct {
	metaSvc metadata.Service
	name    string
}

func (p *preloadService) Name() string {
	return "preload-" + p.name
}

func (p *preloadService) PreRun() error {
	if p.name == "stream" {
		return test_stream.PreloadSchema(p.metaSvc.SchemaRegistry())
	}
	return test_measure.PreloadSchema(p.metaSvc.SchemaRegistry())
}

func (p *preloadService) SetMeta(meta metadata.Service) {
	p.metaSvc = meta
}
