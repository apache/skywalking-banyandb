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

package schema

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

type mockedHandler struct {
	data                 map[string]Metadata
	addOrUpdateCalledNum *atomic.Int32
	deleteCalledNum      *atomic.Int32
	sync.RWMutex
}

func newMockedHandler() *mockedHandler {
	return &mockedHandler{
		data:                 make(map[string]Metadata),
		addOrUpdateCalledNum: &atomic.Int32{},
		deleteCalledNum:      &atomic.Int32{},
	}
}

func (m *mockedHandler) OnAddOrUpdate(obj Metadata) {
	m.Lock()
	defer m.Unlock()
	m.data[obj.Name] = obj
	m.addOrUpdateCalledNum.Add(1)
}

func (m *mockedHandler) OnDelete(obj Metadata) {
	m.Lock()
	defer m.Unlock()
	delete(m.data, obj.Name)
	m.deleteCalledNum.Add(1)
}

func (m *mockedHandler) Data() map[string]any {
	m.RLock()
	defer m.RUnlock()
	ret := make(map[string]any)
	for k, v := range m.data {
		ret[k] = v
	}
	return ret
}

var _ = ginkgo.Describe("Watcher", func() {
	var (
		mockedObj *mockedHandler
		watcher   *watcher
		server    embeddedetcd.Server
		registry  *etcdSchemaRegistry
	)

	ginkgo.BeforeEach(func() {
		mockedObj = newMockedHandler()
		gomega.Expect(logger.Init(logger.Logging{
			Env:   "dev",
			Level: flags.LogLevel,
		})).To(gomega.Succeed())
		ports, err := test.AllocateFreePorts(2)
		if err != nil {
			panic("fail to find free ports")
		}
		endpoints := []string{fmt.Sprintf("http://127.0.0.1:%d", ports[0])}
		server, err = embeddedetcd.NewServer(
			embeddedetcd.ConfigureListener(endpoints, []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}),
			embeddedetcd.RootDir(randomTempDir()))
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		<-server.ReadyNotify()
		schemaRegistry, err := NewEtcdSchemaRegistry(
			Namespace("test"),
			ConfigureServerEndpoints(endpoints),
		)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		registry = schemaRegistry.(*etcdSchemaRegistry)
	})
	ginkgo.AfterEach(func() {
		registry.Close()
		server.Close()
		<-server.StopNotify()
	})

	ginkgo.It("should handle all existing key-value pairs on initial load", func() {
		// Insert some key-value pairs
		err := registry.CreateGroup(context.Background(), &commonv1.Group{
			Metadata: &commonv1.Metadata{
				Name: "testgroup-measure",
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var modRevision int64
		for i := 0; i < 2; i++ {
			modRevision, err = registry.CreateMeasure(context.Background(), &databasev1.Measure{
				Metadata: &commonv1.Metadata{
					Name:  fmt.Sprintf("testkey%d", i+1),
					Group: "testgroup-measure",
				},
			})
			gomega.Expect(modRevision).ShouldNot(gomega.BeZero())
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		// Start the watcher
		watcher = registry.newWatcher("test", KindMeasure, mockedObj)
		ginkgo.DeferCleanup(func() {
			gomega.Expect(watcher.Close()).ShouldNot(gomega.HaveOccurred())
		})
		gomega.Eventually(func() bool {
			_, ok := mockedObj.Data()["testkey1"]
			if !ok {
				return false
			}
			_, ok = mockedObj.Data()["testkey2"]
			if !ok {
				return false
			}
			return mockedObj.addOrUpdateCalledNum.Load() == 2
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
	})
	ginkgo.It("should handle watch events", func() {
		watcher = registry.newWatcher("test", KindStream, mockedObj)
		ginkgo.DeferCleanup(func() {
			gomega.Expect(watcher.Close()).ShouldNot(gomega.HaveOccurred())
		})
		err := registry.CreateGroup(context.Background(), &commonv1.Group{
			Metadata: &commonv1.Metadata{
				Name: "testgroup-stream",
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var modRevision int64
		modRevision, err = registry.CreateStream(context.Background(), &databasev1.Stream{
			Metadata: &commonv1.Metadata{
				Name:  "testkey",
				Group: "testgroup-stream",
			},
		})
		gomega.Expect(modRevision).ShouldNot(gomega.BeZero())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ok, err := registry.DeleteStream(context.Background(), &commonv1.Metadata{
			Name:  "testkey",
			Group: "testgroup-stream",
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(ok).To(gomega.BeTrue())

		gomega.Eventually(func() bool {
			return mockedObj.addOrUpdateCalledNum.Load() == 1 &&
				mockedObj.deleteCalledNum.Load() == 1 &&
				len(mockedObj.Data()) == 0
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
	})
})
