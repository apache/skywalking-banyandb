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
	"fmt"
	"sync"
	"testing"

	"github.com/apache/skywalking-banyandb/api/event"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	teststream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestStream(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Stream Suite")
}

// BeforeSuite - Init logger
var _ = BeforeSuite(func() {
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "info",
	})).To(Succeed())
})

var (
	_ gomock.Matcher = (*shardEventMatcher)(nil)
	_ gomock.Matcher = (*entityEventMatcher)(nil)
)

type shardEventMatcher struct {
	action databasev1.Action
}

func (s *shardEventMatcher) Matches(x interface{}) bool {
	if m, messageOk := x.(bus.Message); messageOk {
		if evt, dataOk := m.Data().(*databasev1.ShardEvent); dataOk {
			return evt.Action == s.action
		}
	}

	return false
}

func (s *shardEventMatcher) String() string {
	return fmt.Sprintf("shard-event-matcher(%s)", databasev1.Action_name[int32(s.action)])
}

type entityEventMatcher struct {
	action databasev1.Action
}

func (s *entityEventMatcher) Matches(x interface{}) bool {
	if m, messageOk := x.(bus.Message); messageOk {
		if evt, dataOk := m.Data().(*databasev1.EntityEvent); dataOk {
			return evt.Action == s.action
		}
	}

	return false
}

func (s *entityEventMatcher) String() string {
	return fmt.Sprintf("entity-event-matcher(%s)", databasev1.Action_name[int32(s.action)])
}

// service to preload stream
type preloadStreamService struct {
	metaSvc metadata.Service
}

func (p *preloadStreamService) Name() string {
	return "preload-stream"
}

func (p *preloadStreamService) PreRun() error {
	return teststream.PreloadSchema(p.metaSvc.SchemaRegistry())
}

type services struct {
	stream          *service
	metadataService metadata.Service
	repo            *discovery.MockServiceRepo
}

func setUp() (*services, func()) {
	ctrl := gomock.NewController(GinkgoT())
	Expect(ctrl).ShouldNot(BeNil())
	var flags []string
	// Init Discovery
	repo := discovery.NewMockServiceRepo(ctrl)
	repo.EXPECT().NodeID().AnyTimes()
	// Both PreRun and Serve phases send events
	repo.EXPECT().Publish(event.StreamTopicEntityEvent, &entityEventMatcher{action: databasev1.Action_ACTION_PUT}).Times(2 * 1)
	repo.EXPECT().Publish(event.StreamTopicShardEvent, &shardEventMatcher{action: databasev1.Action_ACTION_PUT}).Times(2 * 2)

	// Init Pipeline
	pipeline, err := queue.NewQueue(context.TODO(), repo)
	Expect(err).NotTo(HaveOccurred())

	// Init Metadata Service
	metadataService, err := metadata.NewService(context.TODO())
	Expect(err).NotTo(HaveOccurred())
	metaPath, metaDeferFunc, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	flags = append(flags, "--metadata-root-path="+metaPath)

	// Init Stream Service
	streamService, err := NewService(context.TODO(), metadataService, repo, pipeline)
	Expect(err).NotTo(HaveOccurred())
	rootPath, deferFunc, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())

	flags = append(flags, "--root-path="+rootPath)

	closer := run.NewTester("closer")

	g := run.Group{Name: "standalone"}
	preloadStreamSvc := &preloadStreamService{metaSvc: metadataService}
	g.Register(
		closer,
		repo,
		pipeline,
		metadataService,
		preloadStreamSvc,
		streamService,
	)

	err = g.RegisterFlags().Parse(flags)
	Expect(err).NotTo(HaveOccurred())

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer func() {
			metaDeferFunc()
			deferFunc()
			wg.Done()
		}()
		errRun := g.Run()
		Expect(errRun).ShouldNot(HaveOccurred())
	}()
	g.WaitTillReady()
	return &services{
			stream:          streamService.(*service),
			metadataService: metadataService,
			repo:            repo,
		}, func() {
			closer.GracefulStop()
			wg.Wait()
		}
}
