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

package measure_test

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	testmeasure "github.com/apache/skywalking-banyandb/pkg/test/measure"
)

func TestMeasure(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Measure Suite")
}

var _ = ginkgo.BeforeSuite(func() {
	gomega.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gomega.Succeed())
})

type preloadMeasureService struct {
	metaSvc metadata.Service
}

func (p *preloadMeasureService) Name() string {
	return "preload-measure"
}

func (p *preloadMeasureService) PreRun(ctx context.Context) error {
	return testmeasure.PreloadSchema(ctx, p.metaSvc.SchemaRegistry())
}

type services struct {
	measure         measure.Service
	metadataService metadata.Service
	pipeline        queue.Queue
}

func setUp() (*services, func()) {
	ctrl := gomock.NewController(ginkgo.GinkgoT())
	gomega.Expect(ctrl).ShouldNot(gomega.BeNil())

	// Init Pipeline
	pipeline := queue.Local()

	// Init Metadata Service
	metadataService, err := metadata.NewService(context.TODO())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Init Measure Service
	measureService, err := measure.NewService(context.TODO(), metadataService, pipeline)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	preloadMeasureSvc := &preloadMeasureService{metaSvc: metadataService}
	var flags []string
	metaPath, metaDeferFunc, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	flags = append(flags, "--metadata-root-path="+metaPath)
	rootPath, deferFunc, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	flags = append(flags, "--measure-root-path="+rootPath)
	listenClientURL, listenPeerURL, err := test.NewEtcdListenUrls()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	flags = append(flags, "--etcd-listen-client-url="+listenClientURL, "--etcd-listen-peer-url="+listenPeerURL)
	moduleDeferFunc := test.SetupModules(
		flags,
		pipeline,
		metadataService,
		preloadMeasureSvc,
		measureService,
	)
	return &services{
			measure:         measureService,
			metadataService: metadataService,
			pipeline:        pipeline,
		}, func() {
			moduleDeferFunc()
			metaDeferFunc()
			deferFunc()
		}
}
