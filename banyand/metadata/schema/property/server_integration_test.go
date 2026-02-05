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

package property_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/property"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	banyandProperty "github.com/apache/skywalking-banyandb/banyand/property"
	"github.com/apache/skywalking-banyandb/banyand/property/gossip"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

func TestPropertyServerIntegration(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Property Server Integration Suite", ginkgo.Label("integration", "property"))
}

// mockedHandler is a test handler that tracks all schema events.
// It also implements metadata.HandlerRegister for capturing registered handlers.
type mockedHandler struct {
	addOrUpdateCalledNum *atomic.Int32
	deleteCalledNum      *atomic.Int32
	data                 map[string]schema.Metadata
	handlers             map[string]schema.EventHandler
	sync.RWMutex
}

func newMockedHandler() *mockedHandler {
	return &mockedHandler{
		data:                 make(map[string]schema.Metadata),
		handlers:             make(map[string]schema.EventHandler),
		addOrUpdateCalledNum: &atomic.Int32{},
		deleteCalledNum:      &atomic.Int32{},
	}
}

// RegisterHandler implements metadata.HandlerRegister.
func (m *mockedHandler) RegisterHandler(name string, _ schema.Kind, handler schema.EventHandler) {
	m.Lock()
	defer m.Unlock()
	m.handlers[name] = handler
}

// GetHandler returns a previously registered handler by name.
func (m *mockedHandler) GetHandler(name string) schema.EventHandler {
	m.RLock()
	defer m.RUnlock()
	return m.handlers[name]
}

func (m *mockedHandler) OnInit(_ []schema.Kind) (bool, []int64) {
	return false, nil
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

func (m *mockedHandler) HasKey(key string) bool {
	m.RLock()
	defer m.RUnlock()
	_, exists := m.data[key]
	return exists
}

func (m *mockedHandler) GetAddOrUpdateCount() int32 {
	return m.addOrUpdateCalledNum.Load()
}

func (m *mockedHandler) GetDeleteCount() int32 {
	return m.deleteCalledNum.Load()
}

// mockDialOptionsProvider implements common.GRPCDialOptionsProvider for testing.
type mockDialOptionsProvider struct{}

func newMockDialOptionsProvider() *mockDialOptionsProvider {
	return &mockDialOptionsProvider{}
}

func (m *mockDialOptionsProvider) GetDialOptions(_ string) ([]grpc.DialOption, error) {
	return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
}

// testEnv encapsulates the test environment for property server integration tests.
type testEnv struct {
	listener         net.Listener
	propertySvc      banyandProperty.Service
	server           *property.Server
	grpcServer       *grpc.Server
	healthServer     *health.Server
	propertySvcClose func() error
	defFn            func()
	address          string
	port             int
}

func setupServer(propertySvc banyandProperty.Service) (*testEnv, *mockedHandler, error) {
	var defFn func()
	var propertySvcClose func() error
	if propertySvc == nil {
		tempDir, tempDefFn, spaceErr := test.NewSpace()
		if spaceErr != nil {
			return nil, nil, spaceErr
		}
		defFn = tempDefFn
		omr := observability.BypassRegistry
		lfs := fs.NewLocalFileSystem()
		var svcErr error
		propertySvc, propertySvcClose, svcErr = banyandProperty.NewTestService(tempDir, tempDir, omr, lfs)
		if svcErr != nil {
			defFn()
			return nil, nil, svcErr
		}
	}
	ports, portsErr := test.AllocateFreePorts(1)
	if portsErr != nil {
		if propertySvcClose != nil {
			propertySvcClose()
		}
		if defFn != nil {
			defFn()
		}
		return nil, nil, portsErr
	}
	port := ports[0]
	address := fmt.Sprintf("127.0.0.1:%d", port)
	omr := observability.BypassRegistry
	pm := protector.Nop{}
	handlerRegister := newMockedHandler()
	server := property.NewServer(propertySvc, omr, handlerRegister, pm)
	flagSet := server.FlagSet()
	parseArgs := []string{
		"--schema-property-server-enabled=true",
	}
	if propertySvcClose == nil && defFn == nil {
		parseArgs = append(parseArgs, "--schema-property-server-repair-trigger-cron=@every 1s")
	}
	parseErr := flagSet.Parse(parseArgs)
	if parseErr != nil {
		if propertySvcClose != nil {
			propertySvcClose()
		}
		if defFn != nil {
			defFn()
		}
		return nil, nil, parseErr
	}
	if validateErr := server.Validate(); validateErr != nil {
		if propertySvcClose != nil {
			propertySvcClose()
		}
		if defFn != nil {
			defFn()
		}
		return nil, nil, validateErr
	}
	ctx := context.WithValue(context.Background(), common.ContextNodeKey, common.Node{NodeID: "test-node"})
	if preRunErr := server.PreRun(ctx); preRunErr != nil {
		if propertySvcClose != nil {
			propertySvcClose()
		}
		if defFn != nil {
			defFn()
		}
		return nil, nil, preRunErr
	}
	listener, listenErr := net.Listen("tcp", address)
	if listenErr != nil {
		if propertySvcClose != nil {
			propertySvcClose()
		}
		if defFn != nil {
			defFn()
		}
		return nil, nil, listenErr
	}
	grpcServer := grpc.NewServer()
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	server.RegisterGRPCServices(grpcServer)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	if propertySvcClose == nil && defFn == nil {
		server.Serve()
	}
	return &testEnv{
		server:           server,
		grpcServer:       grpcServer,
		healthServer:     healthServer,
		listener:         listener,
		propertySvc:      propertySvc,
		propertySvcClose: propertySvcClose,
		defFn:            defFn,
		port:             port,
		address:          address,
	}, handlerRegister, nil
}

func (e *testEnv) cleanup() {
	if e.healthServer != nil {
		e.healthServer.Shutdown()
	}
	if e.grpcServer != nil {
		e.grpcServer.GracefulStop()
	}
	if e.listener != nil {
		e.listener.Close()
	}
	if e.server != nil {
		e.server.GracefulStop()
	}
	if e.propertySvcClose != nil {
		e.propertySvcClose()
	}
	if e.defFn != nil {
		e.defFn()
	}
}

func createRegistry(address string) *property.SchemaRegistry {
	dialOptsPrv := newMockDialOptionsProvider()
	registry, registryErr := property.NewSchemaRegistryClient(&property.ClientConfig{
		GRPCTimeout:     5 * time.Second,
		SyncInterval:    100 * time.Millisecond,
		DialOptProvider: dialOptsPrv,
	})
	gomega.Expect(registryErr).NotTo(gomega.HaveOccurred())
	registry.OnAddOrUpdate(schema.Metadata{
		TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
		Spec: &databasev1.Node{
			Metadata:    &commonv1.Metadata{Name: address},
			GrpcAddress: address,
			Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
		},
	})
	return registry
}

var _ = ginkgo.Describe("Property Server CRUD Operations", func() {
	var (
		env      *testEnv
		registry *property.SchemaRegistry
		goods    []gleak.Goroutine
	)

	ginkgo.BeforeEach(func() {
		gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())
		goods = gleak.Goroutines()
		var setupErr error
		env, _, setupErr = setupServer(nil)
		gomega.Expect(setupErr).ShouldNot(gomega.HaveOccurred())
		registry = createRegistry(env.address)
		time.Sleep(100 * time.Millisecond)
	})

	ginkgo.AfterEach(func() {
		if registry != nil {
			gomega.Expect(registry.Close()).ShouldNot(gomega.HaveOccurred())
		}
		if env != nil {
			env.cleanup()
		}
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	ginkgo.Context("Group Operations", func() {
		ginkgo.It("should create, get, update, and delete a group", func() {
			ctx := context.Background()
			group := &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: "crud-group"},
				Catalog:  commonv1.Catalog_CATALOG_STREAM,
				ResourceOpts: &commonv1.ResourceOpts{
					ShardNum: 2,
				},
			}
			gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
			retrieved, getErr := registry.GetGroup(ctx, "crud-group")
			gomega.Expect(getErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(retrieved.Metadata.Name).Should(gomega.Equal("crud-group"))
			gomega.Expect(retrieved.ResourceOpts.ShardNum).Should(gomega.Equal(uint32(2)))
			group.ResourceOpts.ShardNum = 4
			gomega.Expect(registry.UpdateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
			updated, getUpdatedErr := registry.GetGroup(ctx, "crud-group")
			gomega.Expect(getUpdatedErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(updated.ResourceOpts.ShardNum).Should(gomega.Equal(uint32(4)))
			deleted, deleteErr := registry.DeleteGroup(ctx, "crud-group")
			gomega.Expect(deleteErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(deleted).Should(gomega.BeTrue())
			_, getDeletedErr := registry.GetGroup(ctx, "crud-group")
			gomega.Expect(getDeletedErr).Should(gomega.HaveOccurred())
		})

		ginkgo.It("should list all groups", func() {
			ctx := context.Background()
			for i := 0; i < 3; i++ {
				group := &commonv1.Group{
					Metadata: &commonv1.Metadata{Name: fmt.Sprintf("list-group-%d", i)},
					Catalog:  commonv1.Catalog_CATALOG_STREAM,
				}
				gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
			}
			groups, listErr := registry.ListGroup(ctx)
			gomega.Expect(listErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(len(groups)).Should(gomega.BeNumerically(">=", 3))
		})
	})

	ginkgo.Context("Stream Operations", func() {
		ginkgo.BeforeEach(func() {
			ctx := context.Background()
			group := &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: "stream-test-group"},
				Catalog:  commonv1.Catalog_CATALOG_STREAM,
			}
			gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.It("should create, get, update, and delete a stream", func() {
			ctx := context.Background()
			stream := &databasev1.Stream{
				Metadata: &commonv1.Metadata{Name: "crud-stream", Group: "stream-test-group"},
				Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
				TagFamilies: []*databasev1.TagFamilySpec{
					{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
				},
			}
			_, createErr := registry.CreateStream(ctx, stream)
			gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())
			retrieved, getErr := registry.GetStream(ctx, &commonv1.Metadata{Name: "crud-stream", Group: "stream-test-group"})
			gomega.Expect(getErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(retrieved.Metadata.Name).Should(gomega.Equal("crud-stream"))
			stream.TagFamilies[0].Tags = append(stream.TagFamilies[0].Tags,
				&databasev1.TagSpec{Name: "tag2", Type: databasev1.TagType_TAG_TYPE_STRING})
			_, updateErr := registry.UpdateStream(ctx, stream)
			gomega.Expect(updateErr).ShouldNot(gomega.HaveOccurred())
			updated, getUpdatedErr := registry.GetStream(ctx, &commonv1.Metadata{Name: "crud-stream", Group: "stream-test-group"})
			gomega.Expect(getUpdatedErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(updated.TagFamilies[0].Tags).Should(gomega.HaveLen(2))
			deleted, deleteErr := registry.DeleteStream(ctx, &commonv1.Metadata{Name: "crud-stream", Group: "stream-test-group"})
			gomega.Expect(deleteErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(deleted).Should(gomega.BeTrue())
		})

		ginkgo.It("should list streams in a group", func() {
			ctx := context.Background()
			for i := 0; i < 3; i++ {
				stream := &databasev1.Stream{
					Metadata: &commonv1.Metadata{Name: fmt.Sprintf("list-stream-%d", i), Group: "stream-test-group"},
					Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
					TagFamilies: []*databasev1.TagFamilySpec{
						{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
					},
				}
				_, createErr := registry.CreateStream(ctx, stream)
				gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())
			}
			streams, listErr := registry.ListStream(ctx, schema.ListOpt{Group: "stream-test-group"})
			gomega.Expect(listErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(streams).Should(gomega.HaveLen(3))
		})
	})

	ginkgo.Context("Measure Operations", func() {
		ginkgo.BeforeEach(func() {
			ctx := context.Background()
			group := &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: "measure-test-group"},
				Catalog:  commonv1.Catalog_CATALOG_MEASURE,
			}
			gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.It("should create, get, update, and delete a measure", func() {
			ctx := context.Background()
			measure := &databasev1.Measure{
				Metadata: &commonv1.Metadata{Name: "crud-measure", Group: "measure-test-group"},
				Entity:   &databasev1.Entity{TagNames: []string{"service"}},
				TagFamilies: []*databasev1.TagFamilySpec{
					{Name: "default", Tags: []*databasev1.TagSpec{{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING}}},
				},
				Fields: []*databasev1.FieldSpec{{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
			}
			_, createErr := registry.CreateMeasure(ctx, measure)
			gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())
			retrieved, getErr := registry.GetMeasure(ctx, &commonv1.Metadata{Name: "crud-measure", Group: "measure-test-group"})
			gomega.Expect(getErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(retrieved.Metadata.Name).Should(gomega.Equal("crud-measure"))
			measure.Fields = append(measure.Fields, &databasev1.FieldSpec{Name: "latency", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT})
			_, updateErr := registry.UpdateMeasure(ctx, measure)
			gomega.Expect(updateErr).ShouldNot(gomega.HaveOccurred())
			updated, getUpdatedErr := registry.GetMeasure(ctx, &commonv1.Metadata{Name: "crud-measure", Group: "measure-test-group"})
			gomega.Expect(getUpdatedErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(updated.Fields).Should(gomega.HaveLen(2))
			deleted, deleteErr := registry.DeleteMeasure(ctx, &commonv1.Metadata{Name: "crud-measure", Group: "measure-test-group"})
			gomega.Expect(deleteErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(deleted).Should(gomega.BeTrue())
		})

		ginkgo.It("should list measures in a group", func() {
			ctx := context.Background()
			for i := 0; i < 3; i++ {
				measure := &databasev1.Measure{
					Metadata: &commonv1.Metadata{Name: fmt.Sprintf("list-measure-%d", i), Group: "measure-test-group"},
					Entity:   &databasev1.Entity{TagNames: []string{"service"}},
					TagFamilies: []*databasev1.TagFamilySpec{
						{Name: "default", Tags: []*databasev1.TagSpec{{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING}}},
					},
					Fields: []*databasev1.FieldSpec{{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
				}
				_, createErr := registry.CreateMeasure(ctx, measure)
				gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())
			}
			measures, listErr := registry.ListMeasure(ctx, schema.ListOpt{Group: "measure-test-group"})
			gomega.Expect(listErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(measures).Should(gomega.HaveLen(3))
		})
	})

	ginkgo.Context("Trace Operations", func() {
		ginkgo.BeforeEach(func() {
			ctx := context.Background()
			group := &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: "trace-test-group"},
				Catalog:  commonv1.Catalog_CATALOG_TRACE,
			}
			gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.It("should create, get, update, and delete a trace", func() {
			ctx := context.Background()
			trace := &databasev1.Trace{
				Metadata: &commonv1.Metadata{Name: "crud-trace", Group: "trace-test-group"},
				Tags: []*databasev1.TraceTagSpec{
					{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
				TraceIdTagName:   "trace_id",
				SpanIdTagName:    "span_id",
				TimestampTagName: "timestamp",
			}
			_, createErr := registry.CreateTrace(ctx, trace)
			gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())
			retrieved, getErr := registry.GetTrace(ctx, &commonv1.Metadata{Name: "crud-trace", Group: "trace-test-group"})
			gomega.Expect(getErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(retrieved.Metadata.Name).Should(gomega.Equal("crud-trace"))
			gomega.Expect(retrieved.Tags).Should(gomega.HaveLen(2))
			trace.Tags = append(trace.Tags, &databasev1.TraceTagSpec{Name: "parent_span_id", Type: databasev1.TagType_TAG_TYPE_STRING})
			_, updateErr := registry.UpdateTrace(ctx, trace)
			gomega.Expect(updateErr).ShouldNot(gomega.HaveOccurred())
			updated, getUpdatedErr := registry.GetTrace(ctx, &commonv1.Metadata{Name: "crud-trace", Group: "trace-test-group"})
			gomega.Expect(getUpdatedErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(updated.Tags).Should(gomega.HaveLen(3))
			deleted, deleteErr := registry.DeleteTrace(ctx, &commonv1.Metadata{Name: "crud-trace", Group: "trace-test-group"})
			gomega.Expect(deleteErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(deleted).Should(gomega.BeTrue())
		})

		ginkgo.It("should list traces in a group", func() {
			ctx := context.Background()
			for i := 0; i < 3; i++ {
				trace := &databasev1.Trace{
					Metadata: &commonv1.Metadata{Name: fmt.Sprintf("list-trace-%d", i), Group: "trace-test-group"},
					Tags: []*databasev1.TraceTagSpec{
						{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
						{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					},
					TraceIdTagName:   "trace_id",
					SpanIdTagName:    "span_id",
					TimestampTagName: "timestamp",
				}
				_, createErr := registry.CreateTrace(ctx, trace)
				gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())
			}
			traces, listErr := registry.ListTrace(ctx, schema.ListOpt{Group: "trace-test-group"})
			gomega.Expect(listErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(traces).Should(gomega.HaveLen(3))
		})
	})

	ginkgo.Context("IndexRule Operations", func() {
		ginkgo.BeforeEach(func() {
			ctx := context.Background()
			group := &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: "index-test-group"},
				Catalog:  commonv1.Catalog_CATALOG_STREAM,
			}
			gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.It("should create, get, update, and delete an index rule", func() {
			ctx := context.Background()
			indexRule := &databasev1.IndexRule{
				Metadata: &commonv1.Metadata{Name: "crud-index-rule", Group: "index-test-group"},
				Tags:     []string{"tag1"},
				Type:     databasev1.IndexRule_TYPE_INVERTED,
				Analyzer: "keyword",
			}
			gomega.Expect(registry.CreateIndexRule(ctx, indexRule)).ShouldNot(gomega.HaveOccurred())
			retrieved, getErr := registry.GetIndexRule(ctx, &commonv1.Metadata{Name: "crud-index-rule", Group: "index-test-group"})
			gomega.Expect(getErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(retrieved.Metadata.Name).Should(gomega.Equal("crud-index-rule"))
			indexRule.Tags = append(indexRule.Tags, "tag2")
			gomega.Expect(registry.UpdateIndexRule(ctx, indexRule)).ShouldNot(gomega.HaveOccurred())
			updated, getUpdatedErr := registry.GetIndexRule(ctx, &commonv1.Metadata{Name: "crud-index-rule", Group: "index-test-group"})
			gomega.Expect(getUpdatedErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(updated.Tags).Should(gomega.HaveLen(2))
			deleted, deleteErr := registry.DeleteIndexRule(ctx, &commonv1.Metadata{Name: "crud-index-rule", Group: "index-test-group"})
			gomega.Expect(deleteErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(deleted).Should(gomega.BeTrue())
		})

		ginkgo.It("should list index rules in a group", func() {
			ctx := context.Background()
			for i := 0; i < 3; i++ {
				indexRule := &databasev1.IndexRule{
					Metadata: &commonv1.Metadata{Name: fmt.Sprintf("list-index-rule-%d", i), Group: "index-test-group"},
					Tags:     []string{"tag1"},
					Type:     databasev1.IndexRule_TYPE_INVERTED,
					Analyzer: "keyword",
				}
				gomega.Expect(registry.CreateIndexRule(ctx, indexRule)).ShouldNot(gomega.HaveOccurred())
			}
			rules, listErr := registry.ListIndexRule(ctx, schema.ListOpt{Group: "index-test-group"})
			gomega.Expect(listErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(rules).Should(gomega.HaveLen(3))
		})
	})

	ginkgo.Context("IndexRuleBinding Operations", func() {
		ginkgo.BeforeEach(func() {
			ctx := context.Background()
			group := &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: "binding-test-group"},
				Catalog:  commonv1.Catalog_CATALOG_STREAM,
			}
			gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.It("should create, get, update, and delete an index rule binding", func() {
			ctx := context.Background()
			binding := &databasev1.IndexRuleBinding{
				Metadata: &commonv1.Metadata{Name: "crud-binding", Group: "binding-test-group"},
				Rules:    []string{"rule1", "rule2"},
				Subject: &databasev1.Subject{
					Catalog: commonv1.Catalog_CATALOG_STREAM,
					Name:    "test-stream",
				},
			}
			gomega.Expect(registry.CreateIndexRuleBinding(ctx, binding)).ShouldNot(gomega.HaveOccurred())
			retrieved, getErr := registry.GetIndexRuleBinding(ctx, &commonv1.Metadata{Name: "crud-binding", Group: "binding-test-group"})
			gomega.Expect(getErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(retrieved.Metadata.Name).Should(gomega.Equal("crud-binding"))
			gomega.Expect(retrieved.Rules).Should(gomega.HaveLen(2))
			binding.Rules = append(binding.Rules, "rule3")
			gomega.Expect(registry.UpdateIndexRuleBinding(ctx, binding)).ShouldNot(gomega.HaveOccurred())
			updated, getUpdatedErr := registry.GetIndexRuleBinding(ctx, &commonv1.Metadata{Name: "crud-binding", Group: "binding-test-group"})
			gomega.Expect(getUpdatedErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(updated.Rules).Should(gomega.HaveLen(3))
			deleted, deleteErr := registry.DeleteIndexRuleBinding(ctx, &commonv1.Metadata{Name: "crud-binding", Group: "binding-test-group"})
			gomega.Expect(deleteErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(deleted).Should(gomega.BeTrue())
		})
	})

	ginkgo.Context("TopNAggregation Operations", func() {
		ginkgo.BeforeEach(func() {
			ctx := context.Background()
			group := &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: "topn-test-group"},
				Catalog:  commonv1.Catalog_CATALOG_MEASURE,
			}
			gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.It("should create, get, update, and delete a topN aggregation", func() {
			ctx := context.Background()
			topN := &databasev1.TopNAggregation{
				Metadata: &commonv1.Metadata{Name: "crud-topn", Group: "topn-test-group"},
				SourceMeasure: &commonv1.Metadata{
					Name:  "source-measure",
					Group: "topn-test-group",
				},
				FieldName:      "value",
				FieldValueSort: modelv1.Sort_SORT_DESC,
				CountersNumber: 10,
			}
			gomega.Expect(registry.CreateTopNAggregation(ctx, topN)).ShouldNot(gomega.HaveOccurred())
			retrieved, getErr := registry.GetTopNAggregation(ctx, &commonv1.Metadata{Name: "crud-topn", Group: "topn-test-group"})
			gomega.Expect(getErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(retrieved.Metadata.Name).Should(gomega.Equal("crud-topn"))
			gomega.Expect(retrieved.CountersNumber).Should(gomega.Equal(int32(10)))
			topN.CountersNumber = 20
			gomega.Expect(registry.UpdateTopNAggregation(ctx, topN)).ShouldNot(gomega.HaveOccurred())
			updated, getUpdatedErr := registry.GetTopNAggregation(ctx, &commonv1.Metadata{Name: "crud-topn", Group: "topn-test-group"})
			gomega.Expect(getUpdatedErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(updated.CountersNumber).Should(gomega.Equal(int32(20)))
			deleted, deleteErr := registry.DeleteTopNAggregation(ctx, &commonv1.Metadata{Name: "crud-topn", Group: "topn-test-group"})
			gomega.Expect(deleteErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(deleted).Should(gomega.BeTrue())
		})

		ginkgo.It("should list topN aggregations in a group", func() {
			ctx := context.Background()
			for i := 0; i < 3; i++ {
				topN := &databasev1.TopNAggregation{
					Metadata: &commonv1.Metadata{Name: fmt.Sprintf("list-topn-%d", i), Group: "topn-test-group"},
					SourceMeasure: &commonv1.Metadata{
						Name:  "source-measure",
						Group: "topn-test-group",
					},
					FieldName:      "value",
					FieldValueSort: modelv1.Sort_SORT_DESC,
					CountersNumber: 10,
				}
				gomega.Expect(registry.CreateTopNAggregation(ctx, topN)).ShouldNot(gomega.HaveOccurred())
			}
			topNAggs, listErr := registry.ListTopNAggregation(ctx, schema.ListOpt{Group: "topn-test-group"})
			gomega.Expect(listErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(topNAggs).Should(gomega.HaveLen(3))
		})
	})

	ginkgo.Context("Property Operations", func() {
		ginkgo.BeforeEach(func() {
			ctx := context.Background()
			group := &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: "property-test-group"},
				Catalog:  commonv1.Catalog_CATALOG_PROPERTY,
			}
			gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.It("should create, get, update, and delete a property", func() {
			ctx := context.Background()
			prop := &databasev1.Property{
				Metadata: &commonv1.Metadata{Name: "crud-property", Group: "property-test-group"},
				Tags: []*databasev1.TagSpec{
					{Name: "key1", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			}
			gomega.Expect(registry.CreateProperty(ctx, prop)).ShouldNot(gomega.HaveOccurred())
			retrieved, getErr := registry.GetProperty(ctx, &commonv1.Metadata{Name: "crud-property", Group: "property-test-group"})
			gomega.Expect(getErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(retrieved.Metadata.Name).Should(gomega.Equal("crud-property"))
			gomega.Expect(retrieved.Tags).Should(gomega.HaveLen(1))
			prop.Tags = append(prop.Tags, &databasev1.TagSpec{Name: "key2", Type: databasev1.TagType_TAG_TYPE_INT})
			gomega.Expect(registry.UpdateProperty(ctx, prop)).ShouldNot(gomega.HaveOccurred())
			updated, getUpdatedErr := registry.GetProperty(ctx, &commonv1.Metadata{Name: "crud-property", Group: "property-test-group"})
			gomega.Expect(getUpdatedErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(updated.Tags).Should(gomega.HaveLen(2))
			deleted, deleteErr := registry.DeleteProperty(ctx, &commonv1.Metadata{Name: "crud-property", Group: "property-test-group"})
			gomega.Expect(deleteErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(deleted).Should(gomega.BeTrue())
		})

		ginkgo.It("should list properties in a group", func() {
			ctx := context.Background()
			for i := 0; i < 3; i++ {
				prop := &databasev1.Property{
					Metadata: &commonv1.Metadata{Name: fmt.Sprintf("list-property-%d", i), Group: "property-test-group"},
					Tags: []*databasev1.TagSpec{
						{Name: "key1", Type: databasev1.TagType_TAG_TYPE_STRING},
					},
				}
				gomega.Expect(registry.CreateProperty(ctx, prop)).ShouldNot(gomega.HaveOccurred())
			}
			properties, listErr := registry.ListProperty(ctx, schema.ListOpt{Group: "property-test-group"})
			gomega.Expect(listErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(properties).Should(gomega.HaveLen(3))
		})
	})
})

var _ = ginkgo.Describe("Property Handler Notifications", func() {
	var (
		env      *testEnv
		registry *property.SchemaRegistry
		goods    []gleak.Goroutine
	)

	ginkgo.BeforeEach(func() {
		gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())
		goods = gleak.Goroutines()
		var setupErr error
		env, _, setupErr = setupServer(nil)
		gomega.Expect(setupErr).ShouldNot(gomega.HaveOccurred())
		registry = createRegistry(env.address)
		time.Sleep(100 * time.Millisecond)
	})

	ginkgo.AfterEach(func() {
		if registry != nil {
			gomega.Expect(registry.Close()).ShouldNot(gomega.HaveOccurred())
		}
		if env != nil {
			env.cleanup()
		}
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	ginkgo.It("should notify handler on resource creation via polling", func() {
		ctx := context.Background()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "notify-create-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: "notify-create-stream", Group: "notify-create-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
		}
		_, createErr := registry.CreateStream(ctx, stream)
		gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())
		handler := newMockedHandler()
		registry.RegisterHandler("create-handler", schema.KindStream, handler)
		registry.StartWatcher()
		gomega.Eventually(func() bool {
			return handler.HasKey("notify-create-stream")
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
	})

	ginkgo.It("should notify multiple handlers for the same kind", func() {
		ctx := context.Background()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "multi-handler-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: "multi-handler-stream", Group: "multi-handler-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
		}
		_, createErr := registry.CreateStream(ctx, stream)
		gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())
		handler1 := newMockedHandler()
		handler2 := newMockedHandler()
		registry.RegisterHandler("handler-1", schema.KindStream, handler1)
		registry.RegisterHandler("handler-2", schema.KindStream, handler2)
		registry.StartWatcher()
		gomega.Eventually(func() bool {
			return handler1.HasKey("multi-handler-stream") && handler2.HasKey("multi-handler-stream")
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
	})

	ginkgo.It("should notify handler for multiple kinds", func() {
		ctx := context.Background()
		streamGroup := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "multi-kind-stream-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry.CreateGroup(ctx, streamGroup)).ShouldNot(gomega.HaveOccurred())
		measureGroup := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "multi-kind-measure-group"},
			Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		}
		gomega.Expect(registry.CreateGroup(ctx, measureGroup)).ShouldNot(gomega.HaveOccurred())
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: "multi-kind-stream", Group: "multi-kind-stream-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
		}
		_, streamErr := registry.CreateStream(ctx, stream)
		gomega.Expect(streamErr).ShouldNot(gomega.HaveOccurred())
		measure := &databasev1.Measure{
			Metadata: &commonv1.Metadata{Name: "multi-kind-measure", Group: "multi-kind-measure-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
			Fields: []*databasev1.FieldSpec{{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
		}
		_, measureErr := registry.CreateMeasure(ctx, measure)
		gomega.Expect(measureErr).ShouldNot(gomega.HaveOccurred())
		streamHandler := newMockedHandler()
		measureHandler := newMockedHandler()
		registry.RegisterHandler("stream-handler", schema.KindStream, streamHandler)
		registry.RegisterHandler("measure-handler", schema.KindMeasure, measureHandler)
		registry.StartWatcher()
		gomega.Eventually(func() bool {
			return streamHandler.HasKey("multi-kind-stream") && measureHandler.HasKey("multi-kind-measure")
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
	})

	ginkgo.It("should notify handler on resource update via polling", func() {
		ctx := context.Background()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "notify-update-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: "notify-update-stream", Group: "notify-update-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
		}
		_, createErr := registry.CreateStream(ctx, stream)
		gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())
		handler := newMockedHandler()
		registry.RegisterHandler("update-handler", schema.KindStream, handler)
		registry.StartWatcher()
		gomega.Eventually(func() bool {
			return handler.HasKey("notify-update-stream")
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
		initialCount := handler.GetAddOrUpdateCount()
		stream.TagFamilies[0].Tags = append(stream.TagFamilies[0].Tags,
			&databasev1.TagSpec{Name: "tag2", Type: databasev1.TagType_TAG_TYPE_STRING})
		_, updateErr := registry.UpdateStream(ctx, stream)
		gomega.Expect(updateErr).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(func() bool {
			return handler.GetAddOrUpdateCount() > initialCount
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
	})

	ginkgo.It("should notify handler on resource deletion via polling", func() {
		ctx := context.Background()
		handler := newMockedHandler()
		registry.RegisterHandler("delete-handler", schema.KindStream, handler)
		registry.StartWatcher()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "notify-delete-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: "notify-delete-stream", Group: "notify-delete-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
		}

		_, createErr := registry.CreateStream(ctx, stream)
		fmt.Println("client create", stream.UpdatedAt.AsTime().UnixNano())
		gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())

		time.Sleep(time.Second)
		gomega.Eventually(func() bool {
			return handler.HasKey("notify-delete-stream")
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
		_, deleteErr := registry.DeleteStream(ctx, &commonv1.Metadata{Name: "notify-delete-stream", Group: "notify-delete-group"})
		gomega.Expect(deleteErr).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(func() bool {
			return !handler.HasKey("notify-delete-stream")
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
		gomega.Expect(handler.GetDeleteCount()).Should(gomega.BeNumerically(">=", 1))
	})
})

var _ = ginkgo.Describe("Property Sync Deletion Detection", func() {
	var (
		env       *testEnv
		registry1 *property.SchemaRegistry
		registry2 *property.SchemaRegistry
		goods     []gleak.Goroutine
	)

	ginkgo.BeforeEach(func() {
		gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())
		goods = gleak.Goroutines()
		var setupErr error
		env, _, setupErr = setupServer(nil)
		gomega.Expect(setupErr).ShouldNot(gomega.HaveOccurred())
		registry1 = createRegistry(env.address)
		registry2 = createRegistry(env.address)
		time.Sleep(100 * time.Millisecond)
	})

	ginkgo.AfterEach(func() {
		if registry1 != nil {
			gomega.Expect(registry1.Close()).ShouldNot(gomega.HaveOccurred())
		}
		if registry2 != nil {
			gomega.Expect(registry2.Close()).ShouldNot(gomega.HaveOccurred())
		}
		if env != nil {
			env.cleanup()
		}
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	ginkgo.It("should detect deletion when a resource is removed from server", func() {
		ctx := context.Background()
		handler := newMockedHandler()
		registry2.RegisterHandler("deletion-handler", schema.KindStream, handler)
		registry2.StartWatcher()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "deletion-detect-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry1.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: "deletion-detect-stream", Group: "deletion-detect-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
		}
		_, createErr := registry1.CreateStream(ctx, stream)
		gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(func() bool {
			return handler.HasKey("deletion-detect-stream")
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
		_, deleteErr := registry1.DeleteStream(ctx, &commonv1.Metadata{Name: "deletion-detect-stream", Group: "deletion-detect-group"})
		gomega.Expect(deleteErr).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(func() bool {
			return !handler.HasKey("deletion-detect-stream")
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
		gomega.Expect(handler.GetDeleteCount()).Should(gomega.BeNumerically(">=", 1))
	})

	ginkgo.It("should detect group deletion and notify handler", func() {
		ctx := context.Background()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "group-deletion-detect"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry1.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		handler := newMockedHandler()
		registry2.RegisterHandler("group-deletion-handler", schema.KindGroup, handler)
		registry2.StartWatcher()
		gomega.Eventually(func() bool {
			return handler.HasKey("group-deletion-detect")
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
		_, deleteErr := registry1.DeleteGroup(ctx, "group-deletion-detect")
		gomega.Expect(deleteErr).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(func() bool {
			return !handler.HasKey("group-deletion-detect")
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
		gomega.Expect(handler.GetDeleteCount()).Should(gomega.BeNumerically(">=", 1))
	})

	ginkgo.It("should sync new resources from another registry", func() {
		ctx := context.Background()
		handler := newMockedHandler()
		registry2.RegisterHandler("sync-handler", schema.KindStream, handler)
		registry2.StartWatcher()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "cross-registry-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry1.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: "cross-registry-stream", Group: "cross-registry-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
		}
		_, createErr := registry1.CreateStream(ctx, stream)
		gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())
		gomega.Eventually(func() bool {
			return handler.HasKey("cross-registry-stream")
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
	})
})

var _ = ginkgo.Describe("Property Error Handling", func() {
	var (
		env      *testEnv
		registry *property.SchemaRegistry
		goods    []gleak.Goroutine
	)

	ginkgo.BeforeEach(func() {
		gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())
		goods = gleak.Goroutines()
		var setupErr error
		env, _, setupErr = setupServer(nil)
		gomega.Expect(setupErr).ShouldNot(gomega.HaveOccurred())
		registry = createRegistry(env.address)
		time.Sleep(100 * time.Millisecond)
	})

	ginkgo.AfterEach(func() {
		if registry != nil {
			gomega.Expect(registry.Close()).ShouldNot(gomega.HaveOccurred())
		}
		if env != nil {
			env.cleanup()
		}
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	ginkgo.It("should return error when getting non-existent group", func() {
		ctx := context.Background()
		_, getErr := registry.GetGroup(ctx, "non-existent-group")
		gomega.Expect(getErr).Should(gomega.HaveOccurred())
	})

	ginkgo.It("should return false when deleting non-existent resource", func() {
		ctx := context.Background()
		deleted, deleteErr := registry.DeleteGroup(ctx, "non-existent-group")
		gomega.Expect(deleteErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(deleted).Should(gomega.BeFalse())
	})

	ginkgo.It("should return error when listing streams without group", func() {
		ctx := context.Background()
		_, listErr := registry.ListStream(ctx, schema.ListOpt{Group: ""})
		gomega.Expect(listErr).Should(gomega.HaveOccurred())
	})

	ginkgo.It("should return empty list when group has no resources", func() {
		ctx := context.Background()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "empty-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		streams, listErr := registry.ListStream(ctx, schema.ListOpt{Group: "empty-group"})
		gomega.Expect(listErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(streams).Should(gomega.BeEmpty())
	})

	ginkgo.It("should return error when getting stream with empty group", func() {
		ctx := context.Background()
		_, getErr := registry.GetStream(ctx, &commonv1.Metadata{Name: "test-stream", Group: ""})
		gomega.Expect(getErr).Should(gomega.HaveOccurred())
	})

	ginkgo.It("should return error when getting stream with empty name", func() {
		ctx := context.Background()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "empty-name-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		_, getErr := registry.GetStream(ctx, &commonv1.Metadata{Name: "", Group: "empty-name-group"})
		gomega.Expect(getErr).Should(gomega.HaveOccurred())
	})
})

var _ = ginkgo.Describe("Property Schema Repair Mechanism When Query", func() {
	var (
		env1      *testEnv
		env2      *testEnv
		registry1 *property.SchemaRegistry
		registry2 *property.SchemaRegistry
		goods     []gleak.Goroutine
	)

	ginkgo.BeforeEach(func() {
		gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())
		goods = gleak.Goroutines()
		var setupErr1, setupErr2 error
		env1, _, setupErr1 = setupServer(nil)
		gomega.Expect(setupErr1).ShouldNot(gomega.HaveOccurred())
		env2, _, setupErr2 = setupServer(nil)
		gomega.Expect(setupErr2).ShouldNot(gomega.HaveOccurred())
		registry1 = createRegistry(env1.address)
		registry2 = createRegistry(env2.address)
		time.Sleep(100 * time.Millisecond)
	})

	ginkgo.AfterEach(func() {
		if registry1 != nil {
			gomega.Expect(registry1.Close()).ShouldNot(gomega.HaveOccurred())
		}
		if registry2 != nil {
			gomega.Expect(registry2.Close()).ShouldNot(gomega.HaveOccurred())
		}
		if env1 != nil {
			env1.cleanup()
		}
		if env2 != nil {
			env2.cleanup()
		}
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	ginkgo.It("should repair inconsistent schema across nodes using updated_at", func() {
		ctx := context.Background()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "repair-test-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry1.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: "repair-test-stream", Group: "repair-test-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
		}
		_, createErr := registry1.CreateStream(ctx, stream)
		gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		stream.TagFamilies[0].Tags = append(stream.TagFamilies[0].Tags,
			&databasev1.TagSpec{Name: "tag2", Type: databasev1.TagType_TAG_TYPE_STRING})
		_, updateErr := registry1.UpdateStream(ctx, stream)
		gomega.Expect(updateErr).ShouldNot(gomega.HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		multiNodeRegistry, multiNodeRegistryErr := property.NewSchemaRegistryClient(&property.ClientConfig{
			GRPCTimeout:     5 * time.Second,
			SyncInterval:    100 * time.Millisecond,
			DialOptProvider: newMockDialOptionsProvider(),
		})
		gomega.Expect(multiNodeRegistryErr).NotTo(gomega.HaveOccurred())
		multiNodeRegistry.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env1.address},
				GrpcAddress: env1.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		multiNodeRegistry.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env2.address},
				GrpcAddress: env2.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		time.Sleep(200 * time.Millisecond)
		streams, listErr := multiNodeRegistry.ListStream(ctx, schema.ListOpt{Group: "repair-test-group"})
		gomega.Expect(listErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(streams).Should(gomega.HaveLen(1))
		gomega.Expect(streams[0].TagFamilies[0].Tags).Should(gomega.HaveLen(2))
		retrieved1, get1Err := registry1.GetStream(ctx, &commonv1.Metadata{Name: "repair-test-stream", Group: "repair-test-group"})
		gomega.Expect(get1Err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(retrieved1.TagFamilies[0].Tags).Should(gomega.HaveLen(2))
		retrieved2, get2Err := registry2.GetStream(ctx, &commonv1.Metadata{Name: "repair-test-stream", Group: "repair-test-group"})
		gomega.Expect(get2Err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(retrieved2.TagFamilies[0].Tags).Should(gomega.HaveLen(2))
		gomega.Expect(multiNodeRegistry.Close()).ShouldNot(gomega.HaveOccurred())
	})

	ginkgo.It("should repair deleted schema across nodes using updated_at", func() {
		ctx := context.Background()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "repair-delete-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry1.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(registry2.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: "repair-delete-stream", Group: "repair-delete-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
		}
		_, create1Err := registry1.CreateStream(ctx, stream)
		gomega.Expect(create1Err).ShouldNot(gomega.HaveOccurred())
		_, create2Err := registry2.CreateStream(ctx, stream)
		gomega.Expect(create2Err).ShouldNot(gomega.HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		retrieved2Before, get2BeforeErr := registry2.GetStream(ctx, &commonv1.Metadata{Name: "repair-delete-stream", Group: "repair-delete-group"})
		gomega.Expect(get2BeforeErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(retrieved2Before).ShouldNot(gomega.BeNil())
		_, deleteErr := registry1.DeleteStream(ctx, &commonv1.Metadata{Name: "repair-delete-stream", Group: "repair-delete-group"})
		gomega.Expect(deleteErr).ShouldNot(gomega.HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		multiNodeRegistry, multiNodeRegistryErr := property.NewSchemaRegistryClient(&property.ClientConfig{
			GRPCTimeout:     5 * time.Second,
			SyncInterval:    100 * time.Millisecond,
			DialOptProvider: newMockDialOptionsProvider(),
		})
		gomega.Expect(multiNodeRegistryErr).NotTo(gomega.HaveOccurred())
		multiNodeRegistry.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env1.address},
				GrpcAddress: env1.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		multiNodeRegistry.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env2.address},
				GrpcAddress: env2.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		time.Sleep(300 * time.Millisecond)
		streams, listErr := multiNodeRegistry.ListStream(ctx, schema.ListOpt{Group: "repair-delete-group"})
		gomega.Expect(listErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(streams).Should(gomega.BeEmpty())
		_, get1Err := registry1.GetStream(ctx, &commonv1.Metadata{Name: "repair-delete-stream", Group: "repair-delete-group"})
		gomega.Expect(get1Err).Should(gomega.HaveOccurred())
		gomega.Expect(multiNodeRegistry.Close()).ShouldNot(gomega.HaveOccurred())
	})

	ginkgo.It("should repair when one node has newer version based on updated_at", func() {
		ctx := context.Background()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "repair-newer-group"},
			Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		}
		gomega.Expect(registry1.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(registry2.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		measure := &databasev1.Measure{
			Metadata: &commonv1.Metadata{Name: "repair-newer-measure", Group: "repair-newer-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"service"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
			Fields: []*databasev1.FieldSpec{{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
		}
		_, create1Err := registry1.CreateMeasure(ctx, measure)
		gomega.Expect(create1Err).ShouldNot(gomega.HaveOccurred())
		_, create2Err := registry2.CreateMeasure(ctx, measure)
		gomega.Expect(create2Err).ShouldNot(gomega.HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		measure.Fields = append(measure.Fields, &databasev1.FieldSpec{Name: "latency", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT})
		_, updateErr := registry1.UpdateMeasure(ctx, measure)
		gomega.Expect(updateErr).ShouldNot(gomega.HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		multiNodeRegistry, multiNodeRegistryErr := property.NewSchemaRegistryClient(&property.ClientConfig{
			GRPCTimeout:     5 * time.Second,
			SyncInterval:    100 * time.Millisecond,
			DialOptProvider: newMockDialOptionsProvider(),
		})
		gomega.Expect(multiNodeRegistryErr).NotTo(gomega.HaveOccurred())
		multiNodeRegistry.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env1.address},
				GrpcAddress: env1.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		multiNodeRegistry.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env2.address},
				GrpcAddress: env2.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		time.Sleep(300 * time.Millisecond)
		measures, listErr := multiNodeRegistry.ListMeasure(ctx, schema.ListOpt{Group: "repair-newer-group"})
		gomega.Expect(listErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(measures).Should(gomega.HaveLen(1))
		gomega.Expect(measures[0].Fields).Should(gomega.HaveLen(2))
		gomega.Expect(multiNodeRegistry.Close()).ShouldNot(gomega.HaveOccurred())
	})

	ginkgo.It("should handle repair with multiple concurrent updates using updated_at", func() {
		ctx := context.Background()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "repair-concurrent-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry1.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(registry2.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		for i := 0; i < 5; i++ {
			stream := &databasev1.Stream{
				Metadata: &commonv1.Metadata{Name: fmt.Sprintf("concurrent-stream-%d", i), Group: "repair-concurrent-group"},
				Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
				TagFamilies: []*databasev1.TagFamilySpec{
					{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
				},
			}
			if i%2 == 0 {
				_, createErr := registry1.CreateStream(ctx, stream)
				gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())
			} else {
				_, createErr := registry2.CreateStream(ctx, stream)
				gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())
			}
			time.Sleep(50 * time.Millisecond)
		}
		time.Sleep(200 * time.Millisecond)
		multiNodeRegistry, multiNodeRegistryErr := property.NewSchemaRegistryClient(&property.ClientConfig{
			GRPCTimeout:     5 * time.Second,
			SyncInterval:    100 * time.Millisecond,
			DialOptProvider: newMockDialOptionsProvider(),
		})
		gomega.Expect(multiNodeRegistryErr).NotTo(gomega.HaveOccurred())
		multiNodeRegistry.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env1.address},
				GrpcAddress: env1.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		multiNodeRegistry.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env2.address},
				GrpcAddress: env2.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		time.Sleep(300 * time.Millisecond)
		streams, listErr := multiNodeRegistry.ListStream(ctx, schema.ListOpt{Group: "repair-concurrent-group"})
		gomega.Expect(listErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(streams).Should(gomega.HaveLen(5))
		streams1, list1Err := registry1.ListStream(ctx, schema.ListOpt{Group: "repair-concurrent-group"})
		gomega.Expect(list1Err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(streams1).Should(gomega.HaveLen(5))
		streams2, list2Err := registry2.ListStream(ctx, schema.ListOpt{Group: "repair-concurrent-group"})
		gomega.Expect(list2Err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(streams2).Should(gomega.HaveLen(5))
		gomega.Expect(multiNodeRegistry.Close()).ShouldNot(gomega.HaveOccurred())
	})

	ginkgo.It("should repair group schema with updated_at timestamp", func() {
		ctx := context.Background()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "repair-group-test"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 2,
			},
		}
		gomega.Expect(registry1.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		group.ResourceOpts.ShardNum = 4
		gomega.Expect(registry1.UpdateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		multiNodeRegistry, multiNodeRegistryErr := property.NewSchemaRegistryClient(&property.ClientConfig{
			GRPCTimeout:     5 * time.Second,
			SyncInterval:    100 * time.Millisecond,
			DialOptProvider: newMockDialOptionsProvider(),
		})
		gomega.Expect(multiNodeRegistryErr).NotTo(gomega.HaveOccurred())
		multiNodeRegistry.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env1.address},
				GrpcAddress: env1.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		multiNodeRegistry.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env2.address},
				GrpcAddress: env2.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		time.Sleep(200 * time.Millisecond)
		groups, listErr := multiNodeRegistry.ListGroup(ctx)
		gomega.Expect(listErr).ShouldNot(gomega.HaveOccurred())
		var found bool
		for _, g := range groups {
			if g.Metadata.Name == "repair-group-test" {
				found = true
				gomega.Expect(g.ResourceOpts.ShardNum).Should(gomega.Equal(uint32(4)))
				break
			}
		}
		gomega.Expect(found).Should(gomega.BeTrue())
		retrieved2, get2Err := registry2.GetGroup(ctx, "repair-group-test")
		gomega.Expect(get2Err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(retrieved2.ResourceOpts.ShardNum).Should(gomega.Equal(uint32(4)))
		gomega.Expect(multiNodeRegistry.Close()).ShouldNot(gomega.HaveOccurred())
	})

	ginkgo.It("should detect existing schemas when handler is registered after node connection", func() {
		ctx := context.Background()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "existing-data-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry1.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		stream1 := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: "existing-stream-1", Group: "existing-data-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
		}
		_, create1Err := registry1.CreateStream(ctx, stream1)
		gomega.Expect(create1Err).ShouldNot(gomega.HaveOccurred())
		stream2 := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: "existing-stream-2", Group: "existing-data-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
		}
		_, create2Err := registry1.CreateStream(ctx, stream2)
		gomega.Expect(create2Err).ShouldNot(gomega.HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		retrieved1, get1Err := registry1.GetStream(ctx, &commonv1.Metadata{Name: "existing-stream-1", Group: "existing-data-group"})
		gomega.Expect(get1Err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(retrieved1).ShouldNot(gomega.BeNil())
		retrieved2, get2Err := registry1.GetStream(ctx, &commonv1.Metadata{Name: "existing-stream-2", Group: "existing-data-group"})
		gomega.Expect(get2Err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(retrieved2).ShouldNot(gomega.BeNil())
		newRegistry, newRegistryErr := property.NewSchemaRegistryClient(&property.ClientConfig{
			GRPCTimeout:     5 * time.Second,
			SyncInterval:    100 * time.Millisecond,
			DialOptProvider: newMockDialOptionsProvider(),
		})
		gomega.Expect(newRegistryErr).NotTo(gomega.HaveOccurred())
		defer func() {
			if newRegistry != nil {
				gomega.Expect(newRegistry.Close()).ShouldNot(gomega.HaveOccurred())
			}
		}()
		handler := newMockedHandler()
		newRegistry.RegisterHandler("existing-data-handler", schema.KindStream, handler)
		newRegistry.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env1.address},
				GrpcAddress: env1.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		time.Sleep(200 * time.Millisecond)
		gomega.Eventually(func() bool {
			return handler.HasKey("existing-stream-1") && handler.HasKey("existing-stream-2")
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
		gomega.Expect(handler.GetAddOrUpdateCount()).Should(gomega.BeNumerically(">=", 2))
	})

	ginkgo.It("should repair node that was offline during schema update", func() {
		ctx := context.Background()
		var env3 *testEnv
		var setupErr3 error
		env3, _, setupErr3 = setupServer(nil)
		gomega.Expect(setupErr3).ShouldNot(gomega.HaveOccurred())
		defer env3.cleanup()
		registry3 := createRegistry(env3.address)
		defer func() {
			if registry3 != nil {
				gomega.Expect(registry3.Close()).ShouldNot(gomega.HaveOccurred())
			}
		}()
		time.Sleep(100 * time.Millisecond)
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "three-node-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry1.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(registry2.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(registry3.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: "three-node-stream", Group: "three-node-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
		}
		_, create1Err := registry1.CreateStream(ctx, stream)
		gomega.Expect(create1Err).ShouldNot(gomega.HaveOccurred())
		_, create2Err := registry2.CreateStream(ctx, stream)
		gomega.Expect(create2Err).ShouldNot(gomega.HaveOccurred())
		_, create3Err := registry3.CreateStream(ctx, stream)
		gomega.Expect(create3Err).ShouldNot(gomega.HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		retrieved1, get1Err := registry1.GetStream(ctx, &commonv1.Metadata{Name: "three-node-stream", Group: "three-node-group"})
		gomega.Expect(get1Err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(retrieved1.TagFamilies[0].Tags).Should(gomega.HaveLen(1))
		retrieved2, get2Err := registry2.GetStream(ctx, &commonv1.Metadata{Name: "three-node-stream", Group: "three-node-group"})
		gomega.Expect(get2Err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(retrieved2.TagFamilies[0].Tags).Should(gomega.HaveLen(1))
		retrieved3, get3Err := registry3.GetStream(ctx, &commonv1.Metadata{Name: "three-node-stream", Group: "three-node-group"})
		gomega.Expect(get3Err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(retrieved3.TagFamilies[0].Tags).Should(gomega.HaveLen(1))
		multiNodeRegistry, multiNodeRegistryErr := property.NewSchemaRegistryClient(&property.ClientConfig{
			GRPCTimeout:     5 * time.Second,
			SyncInterval:    100 * time.Millisecond,
			DialOptProvider: newMockDialOptionsProvider(),
		})
		gomega.Expect(multiNodeRegistryErr).NotTo(gomega.HaveOccurred())
		multiNodeRegistry.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env1.address},
				GrpcAddress: env1.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		multiNodeRegistry.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env2.address},
				GrpcAddress: env2.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		multiNodeRegistry.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env3.address},
				GrpcAddress: env3.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		time.Sleep(200 * time.Millisecond)
		multiNodeRegistry.OnDelete(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env3.address},
				GrpcAddress: env3.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		time.Sleep(200 * time.Millisecond)
		stream.TagFamilies[0].Tags = append(stream.TagFamilies[0].Tags,
			&databasev1.TagSpec{Name: "tag2", Type: databasev1.TagType_TAG_TYPE_STRING})
		_, update1Err := registry1.UpdateStream(ctx, stream)
		gomega.Expect(update1Err).ShouldNot(gomega.HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		retrieved3After, get3AfterErr := registry3.GetStream(ctx, &commonv1.Metadata{Name: "three-node-stream", Group: "three-node-group"})
		gomega.Expect(get3AfterErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(retrieved3After.TagFamilies[0].Tags).Should(gomega.HaveLen(1))
		multiNodeRegistry.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env3.address},
				GrpcAddress: env3.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		time.Sleep(300 * time.Millisecond)
		streams, listErr := multiNodeRegistry.ListStream(ctx, schema.ListOpt{Group: "three-node-group"})
		gomega.Expect(listErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(streams).Should(gomega.HaveLen(1))
		gomega.Expect(streams[0].TagFamilies[0].Tags).Should(gomega.HaveLen(2))
		retrieved1Final, get1FinalErr := registry1.GetStream(ctx, &commonv1.Metadata{Name: "three-node-stream", Group: "three-node-group"})
		gomega.Expect(get1FinalErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(retrieved1Final.TagFamilies[0].Tags).Should(gomega.HaveLen(2))
		gomega.Expect(multiNodeRegistry.Close()).ShouldNot(gomega.HaveOccurred())
	})
})

var _ = ginkgo.Describe("Property Concurrent Operations", func() {
	var (
		env      *testEnv
		registry *property.SchemaRegistry
		goods    []gleak.Goroutine
	)

	ginkgo.BeforeEach(func() {
		gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())
		goods = gleak.Goroutines()
		var setupErr error
		env, _, setupErr = setupServer(nil)
		gomega.Expect(setupErr).ShouldNot(gomega.HaveOccurred())
		registry = createRegistry(env.address)
		time.Sleep(100 * time.Millisecond)
	})

	ginkgo.AfterEach(func() {
		if registry != nil {
			gomega.Expect(registry.Close()).ShouldNot(gomega.HaveOccurred())
		}
		if env != nil {
			env.cleanup()
		}
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	ginkgo.It("should handle concurrent stream creations", func() {
		ctx := context.Background()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "concurrent-create-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		var wg sync.WaitGroup
		numStreams := 10
		errors := make([]error, numStreams)
		for i := 0; i < numStreams; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				stream := &databasev1.Stream{
					Metadata: &commonv1.Metadata{Name: fmt.Sprintf("concurrent-stream-%d", idx), Group: "concurrent-create-group"},
					Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
					TagFamilies: []*databasev1.TagFamilySpec{
						{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
					},
				}
				_, createErr := registry.CreateStream(ctx, stream)
				errors[idx] = createErr
			}(i)
		}
		wg.Wait()
		successCount := 0
		for _, err := range errors {
			if err == nil {
				successCount++
			}
		}
		gomega.Expect(successCount).Should(gomega.Equal(numStreams))
		streams, listErr := registry.ListStream(ctx, schema.ListOpt{Group: "concurrent-create-group"})
		gomega.Expect(listErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(streams).Should(gomega.HaveLen(numStreams))
	})

	ginkgo.It("should handle concurrent updates to different resources", func() {
		ctx := context.Background()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "concurrent-update-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		numStreams := 5
		for i := 0; i < numStreams; i++ {
			stream := &databasev1.Stream{
				Metadata: &commonv1.Metadata{Name: fmt.Sprintf("update-stream-%d", i), Group: "concurrent-update-group"},
				Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
				TagFamilies: []*databasev1.TagFamilySpec{
					{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
				},
			}
			_, createErr := registry.CreateStream(ctx, stream)
			gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())
		}
		var wg sync.WaitGroup
		errors := make([]error, numStreams)
		for i := 0; i < numStreams; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				stream := &databasev1.Stream{
					Metadata: &commonv1.Metadata{Name: fmt.Sprintf("update-stream-%d", idx), Group: "concurrent-update-group"},
					Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
					TagFamilies: []*databasev1.TagFamilySpec{
						{Name: "default", Tags: []*databasev1.TagSpec{
							{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING},
							{Name: fmt.Sprintf("tag2-%d", idx), Type: databasev1.TagType_TAG_TYPE_STRING},
						}},
					},
				}
				_, updateErr := registry.UpdateStream(ctx, stream)
				errors[idx] = updateErr
			}(i)
		}
		wg.Wait()
		for _, err := range errors {
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		}
		for i := 0; i < numStreams; i++ {
			retrieved, getErr := registry.GetStream(ctx, &commonv1.Metadata{Name: fmt.Sprintf("update-stream-%d", i), Group: "concurrent-update-group"})
			gomega.Expect(getErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(retrieved.TagFamilies[0].Tags).Should(gomega.HaveLen(2))
		}
	})

	ginkgo.It("should handle concurrent reads and writes", func() {
		ctx := context.Background()
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "concurrent-rw-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: "rw-stream", Group: "concurrent-rw-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
		}
		_, createErr := registry.CreateStream(ctx, stream)
		gomega.Expect(createErr).ShouldNot(gomega.HaveOccurred())
		var wg sync.WaitGroup
		numReaders := 10
		numWriters := 5
		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 5; j++ {
					_, err := registry.GetStream(ctx, &commonv1.Metadata{Name: "rw-stream", Group: "concurrent-rw-group"})
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					time.Sleep(10 * time.Millisecond)
				}
			}()
		}
		for i := 0; i < numWriters; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				updateStream := &databasev1.Stream{
					Metadata: &commonv1.Metadata{Name: "rw-stream", Group: "concurrent-rw-group"},
					Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
					TagFamilies: []*databasev1.TagFamilySpec{
						{Name: "default", Tags: []*databasev1.TagSpec{
							{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING},
							{Name: fmt.Sprintf("tag-writer-%d", idx), Type: databasev1.TagType_TAG_TYPE_STRING},
						}},
					},
				}
				_, err := registry.UpdateStream(ctx, updateStream)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			}(i)
		}
		wg.Wait()
		retrieved, getErr := registry.GetStream(ctx, &commonv1.Metadata{Name: "rw-stream", Group: "concurrent-rw-group"})
		gomega.Expect(getErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(retrieved).ShouldNot(gomega.BeNil())
	})
})

var _ = ginkgo.Describe("Mixed Schema Cluster Operations", func() {
	var (
		env1      *testEnv
		env2      *testEnv
		env3      *testEnv
		registry1 *property.SchemaRegistry
		registry2 *property.SchemaRegistry
		registry3 *property.SchemaRegistry
		goods     []gleak.Goroutine
	)

	createMultiNodeRegistry := func(addresses ...string) *property.SchemaRegistry {
		multiReg, multiRegErr := property.NewSchemaRegistryClient(&property.ClientConfig{
			GRPCTimeout:     5 * time.Second,
			SyncInterval:    100 * time.Millisecond,
			DialOptProvider: newMockDialOptionsProvider(),
		})
		gomega.Expect(multiRegErr).NotTo(gomega.HaveOccurred())
		for _, addr := range addresses {
			multiReg.OnAddOrUpdate(schema.Metadata{
				TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
				Spec: &databasev1.Node{
					Metadata:    &commonv1.Metadata{Name: addr},
					GrpcAddress: addr,
					Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
				},
			})
		}
		return multiReg
	}

	ginkgo.BeforeEach(func() {
		gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())
		goods = gleak.Goroutines()
		var setupErr1, setupErr2, setupErr3 error
		env1, _, setupErr1 = setupServer(nil)
		gomega.Expect(setupErr1).ShouldNot(gomega.HaveOccurred())
		env2, _, setupErr2 = setupServer(nil)
		gomega.Expect(setupErr2).ShouldNot(gomega.HaveOccurred())
		env3, _, setupErr3 = setupServer(nil)
		gomega.Expect(setupErr3).ShouldNot(gomega.HaveOccurred())
		registry1 = createRegistry(env1.address)
		registry2 = createRegistry(env2.address)
		registry3 = createRegistry(env3.address)
		time.Sleep(100 * time.Millisecond)
	})

	ginkgo.AfterEach(func() {
		if registry1 != nil {
			gomega.Expect(registry1.Close()).ShouldNot(gomega.HaveOccurred())
		}
		if registry2 != nil {
			gomega.Expect(registry2.Close()).ShouldNot(gomega.HaveOccurred())
		}
		if registry3 != nil {
			gomega.Expect(registry3.Close()).ShouldNot(gomega.HaveOccurred())
		}
		if env1 != nil {
			env1.cleanup()
		}
		if env2 != nil {
			env2.cleanup()
		}
		if env3 != nil {
			env3.cleanup()
		}
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	ginkgo.It("should create mixed schemas across nodes and verify content via multi-node registry", func() {
		ctx := context.Background()

		// Create groups on different nodes
		streamGroup := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "mixed-stream-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 2,
			},
		}
		gomega.Expect(registry1.CreateGroup(ctx, streamGroup)).ShouldNot(gomega.HaveOccurred())

		measureGroup := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "mixed-measure-group"},
			Catalog:  commonv1.Catalog_CATALOG_MEASURE,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 4,
			},
		}
		gomega.Expect(registry2.CreateGroup(ctx, measureGroup)).ShouldNot(gomega.HaveOccurred())

		propertyGroup := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "mixed-property-group"},
			Catalog:  commonv1.Catalog_CATALOG_PROPERTY,
		}
		gomega.Expect(registry3.CreateGroup(ctx, propertyGroup)).ShouldNot(gomega.HaveOccurred())

		// Create stream on node1
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: "access-log", Group: "mixed-stream-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"service_id", "instance_id"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{
					Name: "searchable",
					Tags: []*databasev1.TagSpec{
						{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
						{Name: "instance_id", Type: databasev1.TagType_TAG_TYPE_STRING},
						{Name: "endpoint", Type: databasev1.TagType_TAG_TYPE_STRING},
					},
				},
				{
					Name: "data",
					Tags: []*databasev1.TagSpec{
						{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
						{Name: "status_code", Type: databasev1.TagType_TAG_TYPE_INT},
					},
				},
			},
		}
		_, streamCreateErr := registry1.CreateStream(ctx, stream)
		gomega.Expect(streamCreateErr).ShouldNot(gomega.HaveOccurred())

		// Create measure on node2
		measure := &databasev1.Measure{
			Metadata: &commonv1.Metadata{Name: "service-latency", Group: "mixed-measure-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"service_id"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{
					Name: "default",
					Tags: []*databasev1.TagSpec{
						{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
						{Name: "endpoint", Type: databasev1.TagType_TAG_TYPE_STRING},
					},
				},
			},
			Fields: []*databasev1.FieldSpec{
				{Name: "latency", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
				{Name: "count", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
				{Name: "error_rate", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT},
			},
		}
		_, measureCreateErr := registry2.CreateMeasure(ctx, measure)
		gomega.Expect(measureCreateErr).ShouldNot(gomega.HaveOccurred())

		// Create index rule on node1
		indexRule := &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: "service-endpoint-idx", Group: "mixed-stream-group"},
			Tags:     []string{"service_id", "endpoint"},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
			Analyzer: "keyword",
		}
		gomega.Expect(registry1.CreateIndexRule(ctx, indexRule)).ShouldNot(gomega.HaveOccurred())

		// Create index rule binding on node2
		binding := &databasev1.IndexRuleBinding{
			Metadata: &commonv1.Metadata{Name: "access-log-binding", Group: "mixed-stream-group"},
			Rules:    []string{"service-endpoint-idx"},
			Subject: &databasev1.Subject{
				Catalog: commonv1.Catalog_CATALOG_STREAM,
				Name:    "access-log",
			},
		}
		gomega.Expect(registry2.CreateIndexRuleBinding(ctx, binding)).ShouldNot(gomega.HaveOccurred())

		// Create topN aggregation on node3
		topN := &databasev1.TopNAggregation{
			Metadata: &commonv1.Metadata{Name: "top-latency", Group: "mixed-measure-group"},
			SourceMeasure: &commonv1.Metadata{
				Name:  "service-latency",
				Group: "mixed-measure-group",
			},
			FieldName:       "latency",
			FieldValueSort:  modelv1.Sort_SORT_DESC,
			CountersNumber:  50,
			GroupByTagNames: []string{"service_id"},
		}
		gomega.Expect(registry3.CreateTopNAggregation(ctx, topN)).ShouldNot(gomega.HaveOccurred())

		// Create property on node3
		prop := &databasev1.Property{
			Metadata: &commonv1.Metadata{Name: "service-metadata", Group: "mixed-property-group"},
			Tags: []*databasev1.TagSpec{
				{Name: "owner", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "version", Type: databasev1.TagType_TAG_TYPE_INT},
			},
		}
		gomega.Expect(registry3.CreateProperty(ctx, prop)).ShouldNot(gomega.HaveOccurred())

		time.Sleep(300 * time.Millisecond)

		// Create multi-node registry and verify all data
		multiReg := createMultiNodeRegistry(env1.address, env2.address, env3.address)
		defer func() {
			gomega.Expect(multiReg.Close()).ShouldNot(gomega.HaveOccurred())
		}()
		time.Sleep(300 * time.Millisecond)

		// Verify groups
		groups, listGroupErr := multiReg.ListGroup(ctx)
		gomega.Expect(listGroupErr).ShouldNot(gomega.HaveOccurred())
		groupNames := make(map[string]bool)
		for _, g := range groups {
			groupNames[g.Metadata.Name] = true
		}
		gomega.Expect(groupNames).Should(gomega.HaveKey("mixed-stream-group"))
		gomega.Expect(groupNames).Should(gomega.HaveKey("mixed-measure-group"))
		gomega.Expect(groupNames).Should(gomega.HaveKey("mixed-property-group"))

		// Verify group content details
		for _, g := range groups {
			if g.Metadata.Name == "mixed-stream-group" {
				gomega.Expect(g.Catalog).Should(gomega.Equal(commonv1.Catalog_CATALOG_STREAM))
				gomega.Expect(g.ResourceOpts.ShardNum).Should(gomega.Equal(uint32(2)))
			}
			if g.Metadata.Name == "mixed-measure-group" {
				gomega.Expect(g.Catalog).Should(gomega.Equal(commonv1.Catalog_CATALOG_MEASURE))
				gomega.Expect(g.ResourceOpts.ShardNum).Should(gomega.Equal(uint32(4)))
			}
		}

		// Verify stream content
		streams, listStreamErr := multiReg.ListStream(ctx, schema.ListOpt{Group: "mixed-stream-group"})
		gomega.Expect(listStreamErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(streams).Should(gomega.HaveLen(1))
		gomega.Expect(streams[0].Metadata.Name).Should(gomega.Equal("access-log"))
		gomega.Expect(streams[0].Entity.TagNames).Should(gomega.Equal([]string{"service_id", "instance_id"}))
		gomega.Expect(streams[0].TagFamilies).Should(gomega.HaveLen(2))
		gomega.Expect(streams[0].TagFamilies[0].Name).Should(gomega.Equal("searchable"))
		gomega.Expect(streams[0].TagFamilies[0].Tags).Should(gomega.HaveLen(3))
		gomega.Expect(streams[0].TagFamilies[1].Name).Should(gomega.Equal("data"))
		gomega.Expect(streams[0].TagFamilies[1].Tags).Should(gomega.HaveLen(2))

		// Verify measure content
		measures, listMeasureErr := multiReg.ListMeasure(ctx, schema.ListOpt{Group: "mixed-measure-group"})
		gomega.Expect(listMeasureErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(measures).Should(gomega.HaveLen(1))
		gomega.Expect(measures[0].Metadata.Name).Should(gomega.Equal("service-latency"))
		gomega.Expect(measures[0].Fields).Should(gomega.HaveLen(3))
		fieldNames := make([]string, 0, len(measures[0].Fields))
		for _, f := range measures[0].Fields {
			fieldNames = append(fieldNames, f.Name)
		}
		gomega.Expect(fieldNames).Should(gomega.ConsistOf("latency", "count", "error_rate"))

		// Verify index rule content
		rules, listRuleErr := multiReg.ListIndexRule(ctx, schema.ListOpt{Group: "mixed-stream-group"})
		gomega.Expect(listRuleErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(rules).Should(gomega.HaveLen(1))
		gomega.Expect(rules[0].Metadata.Name).Should(gomega.Equal("service-endpoint-idx"))
		gomega.Expect(rules[0].Tags).Should(gomega.Equal([]string{"service_id", "endpoint"}))
		gomega.Expect(rules[0].Type).Should(gomega.Equal(databasev1.IndexRule_TYPE_INVERTED))
		gomega.Expect(rules[0].Analyzer).Should(gomega.Equal("keyword"))

		// Verify index rule binding content
		bindings, listBindingErr := multiReg.ListIndexRuleBinding(ctx, schema.ListOpt{Group: "mixed-stream-group"})
		gomega.Expect(listBindingErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(bindings).Should(gomega.HaveLen(1))
		gomega.Expect(bindings[0].Metadata.Name).Should(gomega.Equal("access-log-binding"))
		gomega.Expect(bindings[0].Rules).Should(gomega.Equal([]string{"service-endpoint-idx"}))
		gomega.Expect(bindings[0].Subject.Name).Should(gomega.Equal("access-log"))
		gomega.Expect(bindings[0].Subject.Catalog).Should(gomega.Equal(commonv1.Catalog_CATALOG_STREAM))

		// Verify topN aggregation content
		topNAggs, listTopNErr := multiReg.ListTopNAggregation(ctx, schema.ListOpt{Group: "mixed-measure-group"})
		gomega.Expect(listTopNErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(topNAggs).Should(gomega.HaveLen(1))
		gomega.Expect(topNAggs[0].Metadata.Name).Should(gomega.Equal("top-latency"))
		gomega.Expect(topNAggs[0].FieldName).Should(gomega.Equal("latency"))
		gomega.Expect(topNAggs[0].FieldValueSort).Should(gomega.Equal(modelv1.Sort_SORT_DESC))
		gomega.Expect(topNAggs[0].CountersNumber).Should(gomega.Equal(int32(50)))
		gomega.Expect(topNAggs[0].GroupByTagNames).Should(gomega.Equal([]string{"service_id"}))

		// Verify property content
		properties, listPropErr := multiReg.ListProperty(ctx, schema.ListOpt{Group: "mixed-property-group"})
		gomega.Expect(listPropErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(properties).Should(gomega.HaveLen(1))
		gomega.Expect(properties[0].Metadata.Name).Should(gomega.Equal("service-metadata"))
		gomega.Expect(properties[0].Tags).Should(gomega.HaveLen(2))
	})

	ginkgo.It("should update mixed schemas on different nodes and verify consistency", func() {
		ctx := context.Background()

		// Setup: create groups on all nodes
		streamGroup := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "update-mixed-stream-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		measureGroup := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "update-mixed-measure-group"},
			Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		}
		gomega.Expect(registry1.CreateGroup(ctx, streamGroup)).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(registry1.CreateGroup(ctx, measureGroup)).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(registry2.CreateGroup(ctx, streamGroup)).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(registry2.CreateGroup(ctx, measureGroup)).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(registry3.CreateGroup(ctx, streamGroup)).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(registry3.CreateGroup(ctx, measureGroup)).ShouldNot(gomega.HaveOccurred())

		// Create initial schemas on all nodes
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: "update-stream", Group: "update-mixed-stream-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{
					{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING},
				}},
			},
		}
		_, streamErr1 := registry1.CreateStream(ctx, stream)
		gomega.Expect(streamErr1).ShouldNot(gomega.HaveOccurred())
		_, streamErr2 := registry2.CreateStream(ctx, stream)
		gomega.Expect(streamErr2).ShouldNot(gomega.HaveOccurred())
		_, streamErr3 := registry3.CreateStream(ctx, stream)
		gomega.Expect(streamErr3).ShouldNot(gomega.HaveOccurred())

		measure := &databasev1.Measure{
			Metadata: &commonv1.Metadata{Name: "update-measure", Group: "update-mixed-measure-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"svc"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{
					{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
				}},
			},
			Fields: []*databasev1.FieldSpec{
				{Name: "val", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
			},
		}
		_, measureErr1 := registry1.CreateMeasure(ctx, measure)
		gomega.Expect(measureErr1).ShouldNot(gomega.HaveOccurred())
		_, measureErr2 := registry2.CreateMeasure(ctx, measure)
		gomega.Expect(measureErr2).ShouldNot(gomega.HaveOccurred())
		_, measureErr3 := registry3.CreateMeasure(ctx, measure)
		gomega.Expect(measureErr3).ShouldNot(gomega.HaveOccurred())

		time.Sleep(200 * time.Millisecond)

		// Update stream on node1 (add tags)
		stream.TagFamilies[0].Tags = append(stream.TagFamilies[0].Tags,
			&databasev1.TagSpec{Name: "tag2", Type: databasev1.TagType_TAG_TYPE_STRING},
			&databasev1.TagSpec{Name: "tag3", Type: databasev1.TagType_TAG_TYPE_INT},
		)
		_, updateStreamErr := registry1.UpdateStream(ctx, stream)
		gomega.Expect(updateStreamErr).ShouldNot(gomega.HaveOccurred())

		// Update measure on node2 (add fields)
		measure.Fields = append(measure.Fields,
			&databasev1.FieldSpec{Name: "latency_p99", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT},
		)
		_, updateMeasureErr := registry2.UpdateMeasure(ctx, measure)
		gomega.Expect(updateMeasureErr).ShouldNot(gomega.HaveOccurred())

		time.Sleep(300 * time.Millisecond)

		// Verify via multi-node registry
		multiReg := createMultiNodeRegistry(env1.address, env2.address, env3.address)
		defer func() {
			gomega.Expect(multiReg.Close()).ShouldNot(gomega.HaveOccurred())
		}()
		time.Sleep(300 * time.Millisecond)

		// Verify updated stream
		retrievedStream, getStreamErr := multiReg.GetStream(ctx, &commonv1.Metadata{Name: "update-stream", Group: "update-mixed-stream-group"})
		gomega.Expect(getStreamErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(retrievedStream.TagFamilies[0].Tags).Should(gomega.HaveLen(3))
		tagNames := make([]string, 0, len(retrievedStream.TagFamilies[0].Tags))
		for _, tag := range retrievedStream.TagFamilies[0].Tags {
			tagNames = append(tagNames, tag.Name)
		}
		gomega.Expect(tagNames).Should(gomega.ConsistOf("tag1", "tag2", "tag3"))

		// Verify updated measure
		retrievedMeasure, getMeasureErr := multiReg.GetMeasure(ctx, &commonv1.Metadata{Name: "update-measure", Group: "update-mixed-measure-group"})
		gomega.Expect(getMeasureErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(retrievedMeasure.Fields).Should(gomega.HaveLen(2))
		measureFieldNames := make([]string, 0, len(retrievedMeasure.Fields))
		for _, f := range retrievedMeasure.Fields {
			measureFieldNames = append(measureFieldNames, f.Name)
		}
		gomega.Expect(measureFieldNames).Should(gomega.ConsistOf("val", "latency_p99"))

		// Verify all three nodes converge
		for _, reg := range []*property.SchemaRegistry{registry1, registry2, registry3} {
			s, sErr := reg.GetStream(ctx, &commonv1.Metadata{Name: "update-stream", Group: "update-mixed-stream-group"})
			gomega.Expect(sErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(s.TagFamilies[0].Tags).Should(gomega.HaveLen(3))
		}
	})

	ginkgo.It("should handle mixed schema deletions across cluster and verify via polling", func() {
		ctx := context.Background()

		// Setup groups on node1
		streamGroup := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "delete-mixed-stream-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		measureGroup := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "delete-mixed-measure-group"},
			Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		}
		gomega.Expect(registry1.CreateGroup(ctx, streamGroup)).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(registry1.CreateGroup(ctx, measureGroup)).ShouldNot(gomega.HaveOccurred())

		// Create schemas on node1
		for i := 0; i < 3; i++ {
			s := &databasev1.Stream{
				Metadata: &commonv1.Metadata{Name: fmt.Sprintf("del-stream-%d", i), Group: "delete-mixed-stream-group"},
				Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
				TagFamilies: []*databasev1.TagFamilySpec{
					{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
				},
			}
			_, sErr := registry1.CreateStream(ctx, s)
			gomega.Expect(sErr).ShouldNot(gomega.HaveOccurred())
		}
		for i := 0; i < 2; i++ {
			m := &databasev1.Measure{
				Metadata: &commonv1.Metadata{Name: fmt.Sprintf("del-measure-%d", i), Group: "delete-mixed-measure-group"},
				Entity:   &databasev1.Entity{TagNames: []string{"svc"}},
				TagFamilies: []*databasev1.TagFamilySpec{
					{Name: "default", Tags: []*databasev1.TagSpec{{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING}}},
				},
				Fields: []*databasev1.FieldSpec{{Name: "val", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
			}
			_, mErr := registry1.CreateMeasure(ctx, m)
			gomega.Expect(mErr).ShouldNot(gomega.HaveOccurred())
		}

		// Create an observer registry: register handlers BEFORE connecting to node
		// so initial resource sync triggers handler notifications
		observerReg, observerRegErr := property.NewSchemaRegistryClient(&property.ClientConfig{
			GRPCTimeout:     5 * time.Second,
			SyncInterval:    100 * time.Millisecond,
			DialOptProvider: newMockDialOptionsProvider(),
		})
		gomega.Expect(observerRegErr).NotTo(gomega.HaveOccurred())
		defer func() {
			gomega.Expect(observerReg.Close()).ShouldNot(gomega.HaveOccurred())
		}()

		streamHandler := newMockedHandler()
		measureHandler := newMockedHandler()
		observerReg.RegisterHandler("del-stream-handler", schema.KindStream, streamHandler)
		observerReg.RegisterHandler("del-measure-handler", schema.KindMeasure, measureHandler)

		// Connect to env1 after handler registration
		observerReg.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env1.address},
				GrpcAddress: env1.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		observerReg.StartWatcher()

		// Wait for watcher to pick up all resources
		gomega.Eventually(func() int32 {
			return streamHandler.GetAddOrUpdateCount()
		}, flags.EventuallyTimeout).Should(gomega.BeNumerically(">=", 3))
		gomega.Eventually(func() int32 {
			return measureHandler.GetAddOrUpdateCount()
		}, flags.EventuallyTimeout).Should(gomega.BeNumerically(">=", 2))

		// Delete one stream and one measure via registry1
		_, delStreamErr := registry1.DeleteStream(ctx, &commonv1.Metadata{Name: "del-stream-1", Group: "delete-mixed-stream-group"})
		gomega.Expect(delStreamErr).ShouldNot(gomega.HaveOccurred())
		_, delMeasureErr := registry1.DeleteMeasure(ctx, &commonv1.Metadata{Name: "del-measure-0", Group: "delete-mixed-measure-group"})
		gomega.Expect(delMeasureErr).ShouldNot(gomega.HaveOccurred())

		// Verify deletion events via polling
		gomega.Eventually(func() bool {
			return !streamHandler.HasKey("del-stream-1")
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())
		gomega.Eventually(func() bool {
			return !measureHandler.HasKey("del-measure-0")
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())

		// Verify remaining resources
		gomega.Expect(streamHandler.HasKey("del-stream-0")).Should(gomega.BeTrue())
		gomega.Expect(streamHandler.HasKey("del-stream-2")).Should(gomega.BeTrue())
		gomega.Expect(measureHandler.HasKey("del-measure-1")).Should(gomega.BeTrue())

		// Verify counts via direct query
		streams, listStreamErr := registry1.ListStream(ctx, schema.ListOpt{Group: "delete-mixed-stream-group"})
		gomega.Expect(listStreamErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(streams).Should(gomega.HaveLen(2))
		measures, listMeasureErr := registry1.ListMeasure(ctx, schema.ListOpt{Group: "delete-mixed-measure-group"})
		gomega.Expect(listMeasureErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(measures).Should(gomega.HaveLen(1))
	})

	ginkgo.It("should handle concurrent mixed schema writes across three nodes and verify data", func() {
		ctx := context.Background()

		// Create groups on all nodes
		streamGroup := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "conc-mixed-stream-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		measureGroup := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "conc-mixed-measure-group"},
			Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		}
		for _, reg := range []*property.SchemaRegistry{registry1, registry2, registry3} {
			gomega.Expect(reg.CreateGroup(ctx, streamGroup)).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(reg.CreateGroup(ctx, measureGroup)).ShouldNot(gomega.HaveOccurred())
		}

		// Concurrent writes: each node creates different resources
		var wg sync.WaitGroup
		registries := []*property.SchemaRegistry{registry1, registry2, registry3}
		for nodeIdx, reg := range registries {
			wg.Add(1)
			go func(idx int, r *property.SchemaRegistry) {
				defer ginkgo.GinkgoRecover()
				defer wg.Done()
				for i := 0; i < 3; i++ {
					s := &databasev1.Stream{
						Metadata: &commonv1.Metadata{
							Name:  fmt.Sprintf("conc-stream-n%d-%d", idx, i),
							Group: "conc-mixed-stream-group",
						},
						Entity: &databasev1.Entity{TagNames: []string{"tag1"}},
						TagFamilies: []*databasev1.TagFamilySpec{
							{Name: "default", Tags: []*databasev1.TagSpec{
								{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING},
								{Name: fmt.Sprintf("node%d_tag", idx), Type: databasev1.TagType_TAG_TYPE_STRING},
							}},
						},
					}
					_, sErr := r.CreateStream(ctx, s)
					gomega.Expect(sErr).ShouldNot(gomega.HaveOccurred())
				}
				for i := 0; i < 2; i++ {
					m := &databasev1.Measure{
						Metadata: &commonv1.Metadata{
							Name:  fmt.Sprintf("conc-measure-n%d-%d", idx, i),
							Group: "conc-mixed-measure-group",
						},
						Entity: &databasev1.Entity{TagNames: []string{"svc"}},
						TagFamilies: []*databasev1.TagFamilySpec{
							{Name: "default", Tags: []*databasev1.TagSpec{
								{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
							}},
						},
						Fields: []*databasev1.FieldSpec{
							{Name: "val", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
						},
					}
					_, mErr := r.CreateMeasure(ctx, m)
					gomega.Expect(mErr).ShouldNot(gomega.HaveOccurred())
				}
			}(nodeIdx, reg)
		}
		wg.Wait()

		time.Sleep(300 * time.Millisecond)

		// Verify from each node individually
		for nodeIdx, reg := range registries {
			streams, listErr := reg.ListStream(ctx, schema.ListOpt{Group: "conc-mixed-stream-group"})
			gomega.Expect(listErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(streams).Should(gomega.HaveLen(3),
				fmt.Sprintf("node%d should have 3 streams (its own)", nodeIdx))

			measures, listMErr := reg.ListMeasure(ctx, schema.ListOpt{Group: "conc-mixed-measure-group"})
			gomega.Expect(listMErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(measures).Should(gomega.HaveLen(2),
				fmt.Sprintf("node%d should have 2 measures (its own)", nodeIdx))
		}

		// Verify via multi-node registry that aggregates all nodes
		multiReg := createMultiNodeRegistry(env1.address, env2.address, env3.address)
		defer func() {
			gomega.Expect(multiReg.Close()).ShouldNot(gomega.HaveOccurred())
		}()
		time.Sleep(300 * time.Millisecond)

		allStreams, allStreamErr := multiReg.ListStream(ctx, schema.ListOpt{Group: "conc-mixed-stream-group"})
		gomega.Expect(allStreamErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(allStreams).Should(gomega.HaveLen(9)) // 3 nodes * 3 streams each

		allMeasures, allMeasureErr := multiReg.ListMeasure(ctx, schema.ListOpt{Group: "conc-mixed-measure-group"})
		gomega.Expect(allMeasureErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(allMeasures).Should(gomega.HaveLen(6)) // 3 nodes * 2 measures each

		// Verify specific stream content from each node
		for nodeIdx := 0; nodeIdx < 3; nodeIdx++ {
			for i := 0; i < 3; i++ {
				name := fmt.Sprintf("conc-stream-n%d-%d", nodeIdx, i)
				found := false
				for _, s := range allStreams {
					if s.Metadata.Name == name {
						found = true
						gomega.Expect(s.TagFamilies[0].Tags).Should(gomega.HaveLen(2))
						gomega.Expect(s.TagFamilies[0].Tags[1].Name).Should(gomega.Equal(fmt.Sprintf("node%d_tag", nodeIdx)))
						break
					}
				}
				gomega.Expect(found).Should(gomega.BeTrue(), fmt.Sprintf("stream %s should exist", name))
			}
		}
	})

	ginkgo.It("should handle mixed concurrent reads and writes with handler notifications", func() {
		ctx := context.Background()

		// Setup groups on node1
		streamGroup := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "rw-notify-stream-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		measureGroup := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "rw-notify-measure-group"},
			Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		}
		gomega.Expect(registry1.CreateGroup(ctx, streamGroup)).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(registry1.CreateGroup(ctx, measureGroup)).ShouldNot(gomega.HaveOccurred())

		// Create an observer registry connected to the SAME server (env1) for polling
		observerReg := createRegistry(env1.address)
		defer func() {
			gomega.Expect(observerReg.Close()).ShouldNot(gomega.HaveOccurred())
		}()
		time.Sleep(100 * time.Millisecond)

		streamHandler := newMockedHandler()
		measureHandler := newMockedHandler()
		observerReg.RegisterHandler("rw-stream-handler", schema.KindStream, streamHandler)
		observerReg.RegisterHandler("rw-measure-handler", schema.KindMeasure, measureHandler)
		observerReg.StartWatcher()

		// Concurrent writers (to node1)
		var wg sync.WaitGroup
		numStreams := 5
		numMeasures := 3

		for i := 0; i < numStreams; i++ {
			wg.Add(1)
			go func(idx int) {
				defer ginkgo.GinkgoRecover()
				defer wg.Done()
				s := &databasev1.Stream{
					Metadata: &commonv1.Metadata{Name: fmt.Sprintf("rw-notify-stream-%d", idx), Group: "rw-notify-stream-group"},
					Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
					TagFamilies: []*databasev1.TagFamilySpec{
						{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
					},
				}
				_, sErr := registry1.CreateStream(ctx, s)
				gomega.Expect(sErr).ShouldNot(gomega.HaveOccurred())
			}(i)
		}

		for i := 0; i < numMeasures; i++ {
			wg.Add(1)
			go func(idx int) {
				defer ginkgo.GinkgoRecover()
				defer wg.Done()
				m := &databasev1.Measure{
					Metadata: &commonv1.Metadata{Name: fmt.Sprintf("rw-notify-measure-%d", idx), Group: "rw-notify-measure-group"},
					Entity:   &databasev1.Entity{TagNames: []string{"svc"}},
					TagFamilies: []*databasev1.TagFamilySpec{
						{Name: "default", Tags: []*databasev1.TagSpec{{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING}}},
					},
					Fields: []*databasev1.FieldSpec{{Name: "val", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
				}
				_, mErr := registry1.CreateMeasure(ctx, m)
				gomega.Expect(mErr).ShouldNot(gomega.HaveOccurred())
			}(i)
		}

		// Concurrent readers (from separate registries on other nodes)
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer ginkgo.GinkgoRecover()
				defer wg.Done()
				for j := 0; j < 3; j++ {
					_, _ = registry2.ListStream(ctx, schema.ListOpt{Group: "rw-notify-stream-group"})
					_, _ = registry3.ListMeasure(ctx, schema.ListOpt{Group: "rw-notify-measure-group"})
					time.Sleep(20 * time.Millisecond)
				}
			}()
		}
		wg.Wait()

		// Wait for all notifications via polling on the observer (connected to same server as writer)
		gomega.Eventually(func() int32 {
			return streamHandler.GetAddOrUpdateCount()
		}, flags.EventuallyTimeout).Should(gomega.BeNumerically(">=", int32(numStreams)))
		gomega.Eventually(func() int32 {
			return measureHandler.GetAddOrUpdateCount()
		}, flags.EventuallyTimeout).Should(gomega.BeNumerically(">=", int32(numMeasures)))

		// Verify handler received all resources by name
		for i := 0; i < numStreams; i++ {
			gomega.Expect(streamHandler.HasKey(fmt.Sprintf("rw-notify-stream-%d", i))).Should(gomega.BeTrue())
		}
		for i := 0; i < numMeasures; i++ {
			gomega.Expect(measureHandler.HasKey(fmt.Sprintf("rw-notify-measure-%d", i))).Should(gomega.BeTrue())
		}

		// Verify final data content
		streams, listStreamErr := registry1.ListStream(ctx, schema.ListOpt{Group: "rw-notify-stream-group"})
		gomega.Expect(listStreamErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(streams).Should(gomega.HaveLen(numStreams))
		measures, listMeasureErr := registry1.ListMeasure(ctx, schema.ListOpt{Group: "rw-notify-measure-group"})
		gomega.Expect(listMeasureErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(measures).Should(gomega.HaveLen(numMeasures))
	})

	ginkgo.It("should repair mixed schemas across three-node cluster", func() {
		ctx := context.Background()

		// Create groups on node1 only (simulate partial cluster state)
		streamGroup := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "repair-mixed-stream-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 2,
			},
		}
		measureGroup := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "repair-mixed-measure-group"},
			Catalog:  commonv1.Catalog_CATALOG_MEASURE,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum: 3,
			},
		}
		gomega.Expect(registry1.CreateGroup(ctx, streamGroup)).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(registry1.CreateGroup(ctx, measureGroup)).ShouldNot(gomega.HaveOccurred())

		// Create resources only on node1
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: "repair-mixed-stream", Group: "repair-mixed-stream-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"svc_id"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{
					{Name: "svc_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "endpoint", Type: databasev1.TagType_TAG_TYPE_STRING},
				}},
			},
		}
		_, streamErr := registry1.CreateStream(ctx, stream)
		gomega.Expect(streamErr).ShouldNot(gomega.HaveOccurred())

		measure := &databasev1.Measure{
			Metadata: &commonv1.Metadata{Name: "repair-mixed-measure", Group: "repair-mixed-measure-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"svc_id"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{
					{Name: "svc_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				}},
			},
			Fields: []*databasev1.FieldSpec{
				{Name: "latency", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
				{Name: "throughput", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT},
			},
		}
		_, measureErr := registry1.CreateMeasure(ctx, measure)
		gomega.Expect(measureErr).ShouldNot(gomega.HaveOccurred())

		indexRule := &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: "repair-mixed-idx", Group: "repair-mixed-stream-group"},
			Tags:     []string{"svc_id", "endpoint"},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
			Analyzer: "keyword",
		}
		gomega.Expect(registry1.CreateIndexRule(ctx, indexRule)).ShouldNot(gomega.HaveOccurred())

		time.Sleep(200 * time.Millisecond)

		// Update stream on node1 (add a tag)
		stream.TagFamilies[0].Tags = append(stream.TagFamilies[0].Tags,
			&databasev1.TagSpec{Name: "status", Type: databasev1.TagType_TAG_TYPE_INT})
		_, updateStreamErr := registry1.UpdateStream(ctx, stream)
		gomega.Expect(updateStreamErr).ShouldNot(gomega.HaveOccurred())

		time.Sleep(200 * time.Millisecond)

		// Repair via multi-node registry spanning all 3 nodes
		multiReg := createMultiNodeRegistry(env1.address, env2.address, env3.address)
		defer func() {
			gomega.Expect(multiReg.Close()).ShouldNot(gomega.HaveOccurred())
		}()
		time.Sleep(400 * time.Millisecond)

		// Verify stream was repaired with latest version (3 tags)
		retrievedStream, getStreamErr := multiReg.GetStream(ctx,
			&commonv1.Metadata{Name: "repair-mixed-stream", Group: "repair-mixed-stream-group"})
		gomega.Expect(getStreamErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(retrievedStream.TagFamilies[0].Tags).Should(gomega.HaveLen(3))
		streamTagNames := make([]string, 0, len(retrievedStream.TagFamilies[0].Tags))
		for _, tag := range retrievedStream.TagFamilies[0].Tags {
			streamTagNames = append(streamTagNames, tag.Name)
		}
		gomega.Expect(streamTagNames).Should(gomega.ConsistOf("svc_id", "endpoint", "status"))

		// Verify measure was repaired
		retrievedMeasure, getMeasureErr := multiReg.GetMeasure(ctx,
			&commonv1.Metadata{Name: "repair-mixed-measure", Group: "repair-mixed-measure-group"})
		gomega.Expect(getMeasureErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(retrievedMeasure.Fields).Should(gomega.HaveLen(2))
		gomega.Expect(retrievedMeasure.Fields[0].Name).Should(gomega.Equal("latency"))
		gomega.Expect(retrievedMeasure.Fields[1].Name).Should(gomega.Equal("throughput"))

		// Verify index rule was repaired
		retrievedRule, getRuleErr := multiReg.GetIndexRule(ctx,
			&commonv1.Metadata{Name: "repair-mixed-idx", Group: "repair-mixed-stream-group"})
		gomega.Expect(getRuleErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(retrievedRule.Tags).Should(gomega.Equal([]string{"svc_id", "endpoint"}))
		gomega.Expect(retrievedRule.Type).Should(gomega.Equal(databasev1.IndexRule_TYPE_INVERTED))

		// Verify node2 and node3 got the repaired data
		for _, reg := range []*property.SchemaRegistry{registry2, registry3} {
			s, sErr := reg.GetStream(ctx, &commonv1.Metadata{Name: "repair-mixed-stream", Group: "repair-mixed-stream-group"})
			gomega.Expect(sErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(s.TagFamilies[0].Tags).Should(gomega.HaveLen(3))

			m, mErr := reg.GetMeasure(ctx, &commonv1.Metadata{Name: "repair-mixed-measure", Group: "repair-mixed-measure-group"})
			gomega.Expect(mErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(m.Fields).Should(gomega.HaveLen(2))

			r, rErr := reg.GetIndexRule(ctx, &commonv1.Metadata{Name: "repair-mixed-idx", Group: "repair-mixed-stream-group"})
			gomega.Expect(rErr).ShouldNot(gomega.HaveOccurred())
			gomega.Expect(r.Tags).Should(gomega.HaveLen(2))
		}
	})
})

var _ = ginkgo.Describe("Property Schema Repair Scheduler", func() {
	var goods []gleak.Goroutine

	ginkgo.BeforeEach(func() {
		gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())
		goods = gleak.Goroutines()
	})

	ginkgo.AfterEach(func() {
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	ginkgo.It("should propagate schema data to a new node via repair scheduler", func() {
		ctx := context.Background()
		omr := observability.BypassRegistry
		lfs := fs.NewLocalFileSystem()
		gossipPorts, gossipPortsErr := test.AllocateFreePorts(3)
		gomega.Expect(gossipPortsErr).ShouldNot(gomega.HaveOccurred())

		// Create 3 gossip-enabled property services
		tempDir1, defFn1, spaceErr1 := test.NewSpace()
		gomega.Expect(spaceErr1).ShouldNot(gomega.HaveOccurred())
		defer defFn1()
		svc1, svc1Close, svc1Err := banyandProperty.NewTestServiceWithGossipRepair(
			tempDir1, tempDir1, omr, lfs, "node1", gossipPorts[0])
		gomega.Expect(svc1Err).ShouldNot(gomega.HaveOccurred())
		defer svc1Close()

		tempDir2, defFn2, spaceErr2 := test.NewSpace()
		gomega.Expect(spaceErr2).ShouldNot(gomega.HaveOccurred())
		defer defFn2()
		svc2, svc2Close, svc2Err := banyandProperty.NewTestServiceWithGossipRepair(
			tempDir2, tempDir2, omr, lfs, "node2", gossipPorts[1])
		gomega.Expect(svc2Err).ShouldNot(gomega.HaveOccurred())
		defer svc2Close()

		tempDir3, defFn3, spaceErr3 := test.NewSpace()
		gomega.Expect(spaceErr3).ShouldNot(gomega.HaveOccurred())
		defer defFn3()
		svc3, svc3Close, svc3Err := banyandProperty.NewTestServiceWithGossipRepair(
			tempDir3, tempDir3, omr, lfs, "node3", gossipPorts[2])
		gomega.Expect(svc3Err).ShouldNot(gomega.HaveOccurred())
		defer svc3Close()

		// Wrap all three in metadata property servers with repair schedulers
		env1, _, env1Err := setupServer(svc1)
		gomega.Expect(env1Err).ShouldNot(gomega.HaveOccurred())
		defer env1.cleanup()

		env2, _, env2Err := setupServer(svc2)
		gomega.Expect(env2Err).ShouldNot(gomega.HaveOccurred())
		defer env2.cleanup()

		env3, handlerRegister, env3Err := setupServer(svc3)
		gomega.Expect(env3Err).ShouldNot(gomega.HaveOccurred())
		defer env3.cleanup()

		// Create a multi-node registry connected to env1 and env2
		registry, registryErr := property.NewSchemaRegistryClient(&property.ClientConfig{
			GRPCTimeout:     5 * time.Second,
			SyncInterval:    100 * time.Millisecond,
			DialOptProvider: newMockDialOptionsProvider(),
		})
		gomega.Expect(registryErr).NotTo(gomega.HaveOccurred())
		registry.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env1.address},
				GrpcAddress: env1.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		registry.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: env2.address},
				GrpcAddress: env2.address,
				Roles:       []databasev1.Role{databasev1.Role_ROLE_META},
			},
		})
		defer func() {
			gomega.Expect(registry.Close()).ShouldNot(gomega.HaveOccurred())
		}()
		time.Sleep(200 * time.Millisecond)

		// Write schema data through the registry (data goes directly to svc1 and svc2)
		group := &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "repair-scheduler-group"},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
		}
		gomega.Expect(registry.CreateGroup(ctx, group)).ShouldNot(gomega.HaveOccurred())
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: "repair-scheduler-stream", Group: "repair-scheduler-group"},
			Entity:   &databasev1.Entity{TagNames: []string{"tag1"}},
			TagFamilies: []*databasev1.TagFamilySpec{
				{Name: "default", Tags: []*databasev1.TagSpec{{Name: "tag1", Type: databasev1.TagType_TAG_TYPE_STRING}}},
			},
		}
		_, createStreamErr := registry.CreateStream(ctx, stream)
		gomega.Expect(createStreamErr).ShouldNot(gomega.HaveOccurred())
		time.Sleep(200 * time.Millisecond)

		// Build node specs with gossip addresses
		gossipAddr1 := fmt.Sprintf("127.0.0.1:%d", gossipPorts[0])
		gossipAddr2 := fmt.Sprintf("127.0.0.1:%d", gossipPorts[1])
		gossipAddr3 := fmt.Sprintf("127.0.0.1:%d", gossipPorts[2])
		nodeSpec1 := schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode, Name: "node1"},
			Spec: &databasev1.Node{
				Metadata:                        &commonv1.Metadata{Name: "node1"},
				PropertyRepairGossipGrpcAddress: gossipAddr1,
				Roles:                           []databasev1.Role{databasev1.Role_ROLE_META},
			},
		}
		nodeSpec2 := schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode, Name: "node2"},
			Spec: &databasev1.Node{
				Metadata:                        &commonv1.Metadata{Name: "node2"},
				PropertyRepairGossipGrpcAddress: gossipAddr2,
				Roles:                           []databasev1.Role{databasev1.Role_ROLE_META},
			},
		}
		nodeSpec3 := schema.Metadata{
			TypeMeta: schema.TypeMeta{Kind: schema.KindNode, Name: "node3"},
			Spec: &databasev1.Node{
				Metadata:                        &commonv1.Metadata{Name: "node3"},
				PropertyRepairGossipGrpcAddress: gossipAddr3,
				Roles:                           []databasev1.Role{databasev1.Role_ROLE_META},
			},
		}

		// Register all nodes on all gossip messengers so they can route to each other
		messenger1 := svc1.GetGossIPMessenger()
		messenger2 := svc2.GetGossIPMessenger()
		messenger3 := svc3.GetGossIPMessenger()
		for _, m := range []gossip.Messenger{messenger1, messenger2, messenger3} {
			handler := m.(schema.EventHandler)
			handler.OnAddOrUpdate(nodeSpec1)
			handler.OnAddOrUpdate(nodeSpec2)
			handler.OnAddOrUpdate(nodeSpec3)
		}

		// Verify svc3 initially has NO stream data
		streamPropID := property.BuildPropertyID(schema.KindStream,
			&commonv1.Metadata{Group: "repair-scheduler-group", Name: "repair-scheduler-stream"})
		prop, _ := svc3.DirectGet(ctx, property.SchemaGroup, "stream", streamPropID)
		gomega.Expect(prop).Should(gomega.BeNil())

		// Register nodes with the repair scheduler via captured handler on env3
		repairHandler := handlerRegister.GetHandler("metadata-node-property")
		gomega.Expect(repairHandler).ShouldNot(gomega.BeNil())
		repairHandler.OnAddOrUpdate(nodeSpec1)
		repairHandler.OnAddOrUpdate(nodeSpec2)
		repairHandler.OnAddOrUpdate(nodeSpec3)

		// Wait for repair scheduler cron to fire and propagate all schema data to svc3
		groupPropID := property.BuildPropertyID(schema.KindGroup,
			&commonv1.Metadata{Name: "repair-scheduler-group"})
		gomega.Eventually(func() bool {
			streamProp, _ := svc3.DirectGet(ctx, property.SchemaGroup, "stream", streamPropID)
			groupProp, _ := svc3.DirectGet(ctx, property.SchemaGroup, "group", groupPropID)
			return streamProp != nil && groupProp != nil
		}, flags.EventuallyTimeout).Should(gomega.BeTrue())

		// Verify stream property is valid
		streamProp, streamGetErr := svc3.DirectGet(ctx, property.SchemaGroup, "stream", streamPropID)
		gomega.Expect(streamGetErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(streamProp).ShouldNot(gomega.BeNil())
		gomega.Expect(streamProp.Id).Should(gomega.Equal(streamPropID))

		// Verify group property is valid
		groupProp, groupGetErr := svc3.DirectGet(ctx, property.SchemaGroup, "group", groupPropID)
		gomega.Expect(groupGetErr).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(groupProp).ShouldNot(gomega.BeNil())
		gomega.Expect(groupProp.Id).Should(gomega.Equal(groupPropID))
	})
})
