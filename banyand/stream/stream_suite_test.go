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

package stream

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	teststream "github.com/apache/skywalking-banyandb/pkg/test/stream"
)

func TestStream(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Stream Suite")
}

var _ = BeforeSuite(func() {
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(Succeed())
})

type preloadStreamService struct {
	metaSvc metadata.Service
}

func (p *preloadStreamService) Name() string {
	return "preload-stream"
}

func (p *preloadStreamService) PreRun(ctx context.Context) error {
	return teststream.PreloadSchema(ctx, p.metaSvc.SchemaRegistry())
}

type services struct {
	stream          *service
	metadataService metadata.Service
}

func setUp() (*services, func()) {
	ctrl := gomock.NewController(GinkgoT())
	Expect(ctrl).ShouldNot(BeNil())
	// Init Pipeline
	pipeline, err := queue.NewQueue(context.TODO())
	Expect(err).NotTo(HaveOccurred())

	// Init Metadata Service
	metadataService, err := metadata.NewService(context.TODO())
	Expect(err).NotTo(HaveOccurred())
	Expect(err).NotTo(HaveOccurred())

	// Init Stream Service
	streamService, err := NewService(context.TODO(), metadataService, pipeline)
	Expect(err).NotTo(HaveOccurred())
	preloadStreamSvc := &preloadStreamService{metaSvc: metadataService}
	var flags []string
	metaPath, metaDeferFunc, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	flags = append(flags, "--metadata-root-path="+metaPath)
	rootPath, deferFunc, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	flags = append(flags, "--stream-root-path="+rootPath)
	listenClientURL, listenPeerURL, err := test.NewEtcdListenUrls()
	Expect(err).NotTo(HaveOccurred())
	flags = append(flags, "--etcd-listen-client-url="+listenClientURL, "--etcd-listen-peer-url="+listenPeerURL)
	moduleDeferFunc := test.SetupModules(
		flags,
		pipeline,
		metadataService,
		preloadStreamSvc,
		streamService,
	)
	return &services{
			stream:          streamService.(*service),
			metadataService: metadataService,
		}, func() {
			moduleDeferFunc()
			metaDeferFunc()
			deferFunc()
		}
}
