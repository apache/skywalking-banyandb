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

	"github.com/golang/mock/gomock"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/api/event"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
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

// BeforeSuite - Init logger
var _ = ginkgo.BeforeSuite(func() {
	gomega.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gomega.Succeed())
})

// service to preload measure
type preloadMeasureService struct {
	metaSvc metadata.Service
}

func (p *preloadMeasureService) Name() string {
	return "preload-measure"
}

func (p *preloadMeasureService) PreRun() error {
	return testmeasure.PreloadSchema(p.metaSvc.SchemaRegistry())
}

type services struct {
	measure         measure.Service
	metadataService metadata.Service
	repo            *discovery.MockServiceRepo
	pipeline        queue.Queue
}

func setUp() (*services, func()) {
	ctrl := gomock.NewController(ginkgo.GinkgoT())
	gomega.Expect(ctrl).ShouldNot(gomega.BeNil())
	// Init Discovery
	repo := discovery.NewMockServiceRepo(ctrl)
	repo.EXPECT().NodeID().AnyTimes()
	// Both PreRun and Serve phases send events
	repo.EXPECT().Publish(event.MeasureTopicEntityEvent, test.NewEntityEventMatcher(databasev1.Action_ACTION_PUT)).Times(2 * 8)
	repo.EXPECT().Publish(event.MeasureTopicShardEvent, test.NewShardEventMatcher(databasev1.Action_ACTION_PUT)).Times(2 * 2)

	// Init Pipeline
	pipeline, err := queue.NewQueue(context.TODO(), repo)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Init Metadata Service
	metadataService, err := metadata.NewService(context.TODO())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Init Measure Service
	measureService, err := measure.NewService(context.TODO(), metadataService, repo, pipeline)
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
	moduleDeferFunc := test.SetUpModules(
		flags,
		repo,
		pipeline,
		metadataService,
		preloadMeasureSvc,
		measureService,
	)
	return &services{
			measure:         measureService,
			metadataService: metadataService,
			repo:            repo,
			pipeline:        pipeline,
		}, func() {
			moduleDeferFunc()
			metaDeferFunc()
			deferFunc()
		}
}
