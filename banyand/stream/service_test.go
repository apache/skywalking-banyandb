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
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"

	"github.com/apache/skywalking-banyandb/api/event"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	teststream "github.com/apache/skywalking-banyandb/pkg/test/stream"
)

var _ bus.MessageListener = (*mockedMessageListener)(nil)

type mockedMessageListener struct {
	mock.Mock
}

func (m *mockedMessageListener) Rev(message bus.Message) bus.Message {
	m.Mock.Called(message)

	return bus.NewMessage(0, nil)
}

type streamEventSubscriber struct {
	repo                 discovery.ServiceRepo
	shardEventSubscriber bus.MessageListener
	entityEventListener  bus.MessageListener
}

func (ses *streamEventSubscriber) Name() string {
	return "stream-event-subscriber"
}

func (ses *streamEventSubscriber) PreRun() error {
	components := []struct {
		shardEvent  bus.Topic
		entityEvent bus.Topic
	}{
		{
			shardEvent:  event.StreamTopicShardEvent,
			entityEvent: event.StreamTopicEntityEvent,
		},
	}
	for _, c := range components {
		err := ses.repo.Subscribe(c.shardEvent, ses.shardEventSubscriber)
		if err != nil {
			return err
		}
		err = ses.repo.Subscribe(c.entityEvent, ses.entityEventListener)
		if err != nil {
			return err
		}
	}
	return nil
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

// BeforeSuite - Init logger
var _ = BeforeSuite(func() {
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "info",
	})).To(Succeed())
})

var _ = Describe("Stream Service", func() {
	var closer run.Service
	var streamService Service
	var metadataService metadata.Service
	var shardEventListener *mockedMessageListener
	var entityEventListener *mockedMessageListener

	BeforeEach(func() {
		var flags []string

		// Init Discovery
		repo, err := discovery.NewServiceRepo(context.TODO())
		Expect(err).NotTo(HaveOccurred())

		// Init Pipeline
		pipeline, err := queue.NewQueue(context.TODO(), repo)
		Expect(err).NotTo(HaveOccurred())

		// Init Metadata Service
		metadataService, err = metadata.NewService(context.TODO())
		Expect(err).NotTo(HaveOccurred())
		etcdRootDir := teststream.RandomTempDir()
		flags = append(flags, "--metadata-root-path="+etcdRootDir)
		DeferCleanup(func() {
			_ = os.RemoveAll(etcdRootDir)
		})

		// Init Stream Service
		streamService, err = NewService(context.TODO(), metadataService, repo, pipeline)
		Expect(err).NotTo(HaveOccurred())
		rootPath, deferFunc, err := test.NewSpace()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			deferFunc()
		})

		flags = append(flags, "--root-path="+rootPath)

		closer = run.NewTester("closer")
		startListener := run.NewTester("started-listener")

		shardEventListener = new(mockedMessageListener)
		shardEventListener.On("Rev", mock.Anything).Return(bus.Message{})

		entityEventListener = new(mockedMessageListener)
		entityEventListener.On("Rev", mock.Anything).Return(bus.Message{})

		eventSubscribers := &streamEventSubscriber{
			repo:                 repo,
			shardEventSubscriber: shardEventListener,
			entityEventListener:  entityEventListener,
		}

		g := run.Group{Name: "standalone"}
		preloadStreamSvc := &preloadStreamService{metaSvc: metadataService}
		g.Register(
			closer,
			repo,
			pipeline,
			metadataService,
			eventSubscribers,
			preloadStreamSvc,
			streamService,
			startListener,
		)

		err = g.RegisterFlags().Parse(flags)
		Expect(err).NotTo(HaveOccurred())

		go func() {
			Expect(g.Run()).Should(Succeed())
		}()
		Expect(startListener.WaitUntilStarted()).Should(Succeed())
	})

	AfterEach(func() {
		closer.GracefulStop()
	})

	It("should pass smoke test", func() {
		Eventually(func() bool {
			_, ok := streamService.(*service).schemaMap.Load(formatStreamID("sw", "default"))
			return ok
		}).WithTimeout(10 * time.Second).Should(BeTrue())
	})

	Context("Delete a stream", func() {
		BeforeEach(func() {
			deleted, err := metadataService.StreamRegistry().DeleteStream(context.TODO(), &commonv1.Metadata{
				Name:  "sw",
				Group: "default",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(deleted).Should(BeTrue())
		})

		It("should close the stream", func() {
			Eventually(func() bool {
				_, ok := streamService.(*service).schemaMap.Load(formatStreamID("sw", "default"))
				return ok
			}).WithTimeout(10 * time.Second).Should(BeFalse())
			entityEventListener.AssertNumberOfCalls(GinkgoT(), "Rev", 2)
			shardEventListener.AssertNumberOfCalls(GinkgoT(), "Rev", 4)
		})
	})
})
