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

package schema_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

type mockedHandler struct {
	data                 map[string]schema.Metadata
	addOrUpdateCalledNum *atomic.Int32
	deleteCalledNum      *atomic.Int32
	sync.RWMutex
}

func newMockedHandler() *mockedHandler {
	return &mockedHandler{
		data:                 make(map[string]schema.Metadata),
		addOrUpdateCalledNum: &atomic.Int32{},
		deleteCalledNum:      &atomic.Int32{},
	}
}

func (m *mockedHandler) OnAddOrUpdate(obj schema.Metadata) {
	m.Lock()
	defer m.Unlock()
	m.data[obj.Name] = obj
	m.addOrUpdateCalledNum.Add(1)
}

func (m *mockedHandler) OnDelete(obj schema.Metadata) {
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
		server    embeddedetcd.Server
		registry  schema.Registry
		defFn     func()
		endpoints []string
	)

	ginkgo.BeforeEach(func() {
		mockedObj = newMockedHandler()
		gomega.Expect(logger.Init(logger.Logging{
			Env:   "dev",
			Level: flags.LogLevel,
		})).To(gomega.Succeed())
		var path string
		var err error
		path, defFn, err = test.NewSpace()
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

		ports, err := test.AllocateFreePorts(2)
		if err != nil {
			panic("fail to find free ports")
		}
		endpoints = []string{fmt.Sprintf("http://127.0.0.1:%d", ports[0])}
		server, err = embeddedetcd.NewServer(
			embeddedetcd.ConfigureListener(endpoints, []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}),
			embeddedetcd.RootDir(path),
			embeddedetcd.AutoCompactionMode("periodic"),
			embeddedetcd.AutoCompactionRetention("1h"),
			embeddedetcd.QuotaBackendBytes(2*1024*1024*1024),
		)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		<-server.ReadyNotify()
		registry, err = schema.NewEtcdSchemaRegistry(
			schema.Namespace("test"),
			schema.ConfigureServerEndpoints(endpoints),
		)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		registry.Close()
		server.Close()
		<-server.StopNotify()
		defFn()
	})

	ginkgo.It("should handle all existing key-value pairs on initial load", func() {
		// Insert some key-value pairs
		err := registry.CreateGroup(context.Background(), &commonv1.Group{
			Metadata: &commonv1.Metadata{
				Name: "testgroup-measure",
			},
			Catalog: commonv1.Catalog_CATALOG_MEASURE,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 1,
				SegmentInterval: &commonv1.IntervalRule{
					Num:  1,
					Unit: commonv1.IntervalRule_UNIT_DAY,
				},
				Ttl: &commonv1.IntervalRule{
					Num:  3,
					Unit: commonv1.IntervalRule_UNIT_DAY,
				},
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
				Entity: &databasev1.Entity{
					TagNames: []string{"testtag"},
				},
				TagFamilies: []*databasev1.TagFamilySpec{
					{
						Name: "testtagfamily",
						Tags: []*databasev1.TagSpec{
							{
								Name: "testtag",
								Type: databasev1.TagType_TAG_TYPE_STRING,
							},
						},
					},
				},
				Fields: []*databasev1.FieldSpec{
					{
						Name:              "testfield",
						FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
						EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
						CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
					},
				},
			})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(modRevision).ShouldNot(gomega.BeZero())
		}

		// Start the watcher
		watcher := registry.NewWatcher("test", schema.KindMeasure, 0)
		watcher.AddHandler(mockedObj)
		watcher.Start()
		ginkgo.DeferCleanup(func() {
			watcher.Close()
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
		watcher := registry.NewWatcher("test", schema.KindStream, 0)
		watcher.AddHandler(mockedObj)
		watcher.Start()
		ginkgo.DeferCleanup(func() {
			watcher.Close()
		})
		err := registry.CreateGroup(context.Background(), &commonv1.Group{
			Metadata: &commonv1.Metadata{
				Name: "testgroup-stream",
			},
			Catalog: commonv1.Catalog_CATALOG_STREAM,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 1,
				SegmentInterval: &commonv1.IntervalRule{
					Num:  1,
					Unit: commonv1.IntervalRule_UNIT_DAY,
				},
				Ttl: &commonv1.IntervalRule{
					Num:  3,
					Unit: commonv1.IntervalRule_UNIT_DAY,
				},
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		var modRevision int64
		modRevision, err = registry.CreateStream(context.Background(), &databasev1.Stream{
			Metadata: &commonv1.Metadata{
				Name:  "testkey",
				Group: "testgroup-stream",
			},
			Entity: &databasev1.Entity{
				TagNames: []string{"testtag"},
			},
			TagFamilies: []*databasev1.TagFamilySpec{
				{
					Name: "testtagfamily",
					Tags: []*databasev1.TagSpec{
						{
							Name: "testtag",
							Type: databasev1.TagType_TAG_TYPE_STRING,
						},
					},
				},
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
	ginkgo.It("should load initial state and track revisions", func() {
		groupName := "testgroup-initial"
		err := registry.CreateGroup(context.Background(), &commonv1.Group{
			Metadata: &commonv1.Metadata{
				Name: groupName,
			},
			Catalog: commonv1.Catalog_CATALOG_MEASURE,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 1,
				SegmentInterval: &commonv1.IntervalRule{
					Num:  1,
					Unit: commonv1.IntervalRule_UNIT_DAY,
				},
				Ttl: &commonv1.IntervalRule{
					Num:  3,
					Unit: commonv1.IntervalRule_UNIT_DAY,
				},
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = registry.CreateMeasure(context.Background(), &databasev1.Measure{
			Metadata: &commonv1.Metadata{
				Name:  "initial-key1",
				Group: groupName,
			},
			Entity: &databasev1.Entity{
				TagNames: []string{"testtag"},
			},
			TagFamilies: []*databasev1.TagFamilySpec{
				{
					Name: "testtagfamily",
					Tags: []*databasev1.TagSpec{
						{
							Name: "testtag",
							Type: databasev1.TagType_TAG_TYPE_STRING,
						},
					},
				},
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = registry.CreateMeasure(context.Background(), &databasev1.Measure{
			Metadata: &commonv1.Metadata{
				Name:  "initial-key2",
				Group: groupName,
			},
			Entity: &databasev1.Entity{
				TagNames: []string{"testtag"},
			},
			TagFamilies: []*databasev1.TagFamilySpec{
				{
					Name: "testtagfamily",
					Tags: []*databasev1.TagSpec{
						{
							Name: "testtag",
							Type: databasev1.TagType_TAG_TYPE_STRING,
						},
					},
				},
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		watcher := registry.NewWatcher("test", schema.KindMeasure, 0, schema.CheckInterval(1*time.Second))
		watcher.AddHandler(mockedObj)
		watcher.Start()
		ginkgo.DeferCleanup(func() {
			watcher.Close()
		})

		gomega.Eventually(func() int {
			return len(mockedObj.Data())
		}, flags.EventuallyTimeout).Should(gomega.Equal(2))

		gomega.Expect(mockedObj.addOrUpdateCalledNum.Load()).To(gomega.Equal(int32(2)))
	})

	ginkgo.It("should detect deletions", func() {
		watcher := registry.NewWatcher("test", schema.KindMeasure, 0, schema.CheckInterval(1*time.Second))
		watcher.AddHandler(mockedObj)
		watcher.Start()
		ginkgo.DeferCleanup(func() {
			watcher.Close()
		})

		groupName := "testgroup-delete"
		measureName := "delete-key"
		err := registry.CreateGroup(context.Background(), &commonv1.Group{
			Metadata: &commonv1.Metadata{
				Name: groupName,
			},
			Catalog: commonv1.Catalog_CATALOG_MEASURE,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 1,
				SegmentInterval: &commonv1.IntervalRule{
					Num:  1,
					Unit: commonv1.IntervalRule_UNIT_DAY,
				},
				Ttl: &commonv1.IntervalRule{
					Num:  3,
					Unit: commonv1.IntervalRule_UNIT_DAY,
				},
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = registry.CreateMeasure(context.Background(), &databasev1.Measure{
			Metadata: &commonv1.Metadata{
				Name:  measureName,
				Group: groupName,
			},
			Entity: &databasev1.Entity{
				TagNames: []string{"testtag"},
			},
			TagFamilies: []*databasev1.TagFamilySpec{
				{
					Name: "testtagfamily",
					Tags: []*databasev1.TagSpec{
						{
							Name: "testtag",
							Type: databasev1.TagType_TAG_TYPE_STRING,
						},
					},
				},
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Eventually(func() bool {
			_, ok := mockedObj.Data()[measureName]
			return ok
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())

		deleted, err := registry.DeleteMeasure(context.Background(), &commonv1.Metadata{
			Name:  measureName,
			Group: groupName,
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(deleted).To(gomega.BeTrue())

		gomega.Eventually(func() int {
			return int(mockedObj.deleteCalledNum.Load())
		}, flags.EventuallyTimeout).Should(gomega.Equal(1))
		gomega.Expect(mockedObj.Data()).NotTo(gomega.HaveKey(measureName))
	})

	ginkgo.It("should recover state after compaction", func() {
		watcher := registry.NewWatcher("test", schema.KindMeasure, 0, schema.CheckInterval(1*time.Hour))
		watcher.AddHandler(mockedObj)
		watcher.Start()
		ginkgo.DeferCleanup(func() {
			watcher.Close()
		})

		groupName := "testgroup-compact"
		measureName := "compact-key"
		err := registry.CreateGroup(context.Background(), &commonv1.Group{
			Metadata: &commonv1.Metadata{
				Name: groupName,
			},
			Catalog: commonv1.Catalog_CATALOG_MEASURE,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 1,
				SegmentInterval: &commonv1.IntervalRule{
					Num:  1,
					Unit: commonv1.IntervalRule_UNIT_DAY,
				},
				Ttl: &commonv1.IntervalRule{
					Num:  3,
					Unit: commonv1.IntervalRule_UNIT_DAY,
				},
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		modRev, err := registry.CreateMeasure(context.Background(), &databasev1.Measure{
			Metadata: &commonv1.Metadata{
				Name:  measureName,
				Group: groupName,
			},
			Entity: &databasev1.Entity{
				TagNames: []string{"testtag"},
			},
			TagFamilies: []*databasev1.TagFamilySpec{
				{
					Name: "testtagfamily",
					Tags: []*databasev1.TagSpec{
						{
							Name: "testtag",
							Type: databasev1.TagType_TAG_TYPE_STRING,
						},
					},
				},
			},
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Eventually(func() bool {
			_, ok := mockedObj.Data()[measureName]
			return ok
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())

		err = registry.Compact(context.Background(), modRev)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		updatedMeasure := &databasev1.Measure{
			Metadata: &commonv1.Metadata{
				Name:  measureName,
				Group: groupName,
			},
			Entity: &databasev1.Entity{
				TagNames: []string{"testtag"},
			},
			TagFamilies: []*databasev1.TagFamilySpec{
				{
					Name: "testtagfamily",
					Tags: []*databasev1.TagSpec{
						{
							Name: "testtag",
							Type: databasev1.TagType_TAG_TYPE_STRING,
						},
						{
							Name: "testtag1",
							Type: databasev1.TagType_TAG_TYPE_STRING,
						},
					},
				},
			},
		}
		_, err = registry.UpdateMeasure(context.Background(), updatedMeasure)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Eventually(func() int {
			return int(mockedObj.addOrUpdateCalledNum.Load())
		}, flags.EventuallyTimeout).Should(gomega.BeNumerically(">=", 2))
	})

	ginkgo.It("should not load node with revision -1", func() {
		err := registry.RegisterNode(context.Background(), &databasev1.Node{
			Metadata: &commonv1.Metadata{
				Name: "testnode",
			},
			Roles: []databasev1.Role{
				databasev1.Role_ROLE_DATA,
			},
		}, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		nn, err := registry.ListNode(context.Background(), databasev1.Role_ROLE_DATA)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(nn)).To(gomega.Equal(1))
		gomega.Expect(nn[0].Metadata.Name).To(gomega.Equal("testnode"))

		err = registry.Close()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Recreate registry for this test
		registry, err = schema.NewEtcdSchemaRegistry(
			schema.Namespace("test"),
			schema.ConfigureServerEndpoints(endpoints),
		)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		watcher := registry.NewWatcher("test", schema.KindNode, -1, schema.CheckInterval(1*time.Hour))
		watcher.AddHandler(mockedObj)
		watcher.Start()
		ginkgo.DeferCleanup(func() {
			watcher.Close()
		})
		gomega.Consistently(func() int {
			return int(mockedObj.addOrUpdateCalledNum.Load())
		}, flags.ConsistentlyTimeout).Should(gomega.BeZero())
	})

	ginkgo.It("should load and delete node with revision 0", func() {
		// Register node again for this test
		err := registry.RegisterNode(context.Background(), &databasev1.Node{
			Metadata: &commonv1.Metadata{
				Name: "testnode",
			},
			Roles: []databasev1.Role{
				databasev1.Role_ROLE_DATA,
			},
		}, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = registry.Close()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Recreate registry for this test
		registry, err = schema.NewEtcdSchemaRegistry(
			schema.Namespace("test"),
			schema.ConfigureServerEndpoints(endpoints),
		)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		watcher := registry.NewWatcher("test", schema.KindNode, 0, schema.CheckInterval(1*time.Hour))
		watcher.AddHandler(mockedObj)
		watcher.Start()
		ginkgo.DeferCleanup(func() {
			watcher.Close()
		})
		gomega.Eventually(func() int {
			return int(mockedObj.addOrUpdateCalledNum.Load())
		}, flags.EventuallyTimeout).Should(gomega.Equal(1))
		gomega.Eventually(func() int {
			return int(mockedObj.deleteCalledNum.Load())
		}, flags.EventuallyTimeout).Should(gomega.Equal(1))
		gomega.Expect(mockedObj.Data()).To(gomega.BeEmpty())
	})
})
