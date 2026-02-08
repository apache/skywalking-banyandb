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
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	cases "github.com/apache/skywalking-banyandb/test/cases/schema"
	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
)

func TestStandaloneSchemaDeletion(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Standalone Schema Deletion Suite", Label(integration_standalone.Labels...))
}

var (
	connection *grpc.ClientConn
	deferFunc  func()
	goods      []gleak.Goroutine
	clients    *cases.Clients
)

var _ = SynchronizedBeforeSuite(func() []byte {
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(Succeed())
	pool.EnableStackTracking(true)
	goods = gleak.Goroutines()
	By("Starting standalone server")
	addr, _, closeFn := setup.EmptyStandalone()
	deferFunc = closeFn
	return []byte(addr)
}, func(address []byte) {
	var err error
	connection, err = grpchelper.Conn(string(address), 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())

	clients = &cases.Clients{
		GroupClient:        databasev1.NewGroupRegistryServiceClient(connection),
		MeasureRegClient:   databasev1.NewMeasureRegistryServiceClient(connection),
		StreamRegClient:    databasev1.NewStreamRegistryServiceClient(connection),
		TraceRegClient:     databasev1.NewTraceRegistryServiceClient(connection),
		MeasureWriteClient: measurev1.NewMeasureServiceClient(connection),
		StreamWriteClient:  streamv1.NewStreamServiceClient(connection),
		TraceWriteClient:   tracev1.NewTraceServiceClient(connection),
	}
})

var _ = SynchronizedAfterSuite(func() {
	if connection != nil {
		Expect(connection.Close()).To(Succeed())
	}
}, func() {
	if deferFunc != nil {
		deferFunc()
	}
})

var _ = ReportAfterSuite("Standalone Schema Deletion Suite", func(report Report) {
	if report.SuiteSucceeded {
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
	}
})

var _ = Describe("Schema deletion in standalone mode", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("should delete measure correctly", func() {
		groupName := fmt.Sprintf("del-measure-%d", time.Now().UnixNano())
		measureName := "test_measure"

		By("Creating measure group")
		_, err := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: &commonv1.Group{
				Metadata: &commonv1.Metadata{
					Name: groupName,
				},
				Catalog: commonv1.Catalog_CATALOG_MEASURE,
				ResourceOpts: &commonv1.ResourceOpts{
					ShardNum: 2,
					SegmentInterval: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  1,
					},
					Ttl: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  7,
					},
				},
			},
		})
		Expect(err).ShouldNot(HaveOccurred())

		By("Creating measure schema")
		err = cases.VerifyMeasureDeletion(ctx, clients, groupName, measureName)
		Expect(err).ShouldNot(HaveOccurred())

		// Cleanup
		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	It("should delete stream correctly", func() {
		groupName := fmt.Sprintf("del-stream-%d", time.Now().UnixNano())
		streamName := "test_stream"

		By("Creating stream group")
		_, err := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: &commonv1.Group{
				Metadata: &commonv1.Metadata{
					Name: groupName,
				},
				Catalog: commonv1.Catalog_CATALOG_STREAM,
				ResourceOpts: &commonv1.ResourceOpts{
					ShardNum: 2,
					SegmentInterval: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  1,
					},
					Ttl: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  7,
					},
				},
			},
		})
		Expect(err).ShouldNot(HaveOccurred())

		By("Creating stream schema and verifying deletion")
		err = cases.VerifyStreamDeletion(ctx, clients, groupName, streamName)
		Expect(err).ShouldNot(HaveOccurred())

		// Cleanup
		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	It("should delete trace correctly", func() {
		groupName := fmt.Sprintf("del-trace-%d", time.Now().UnixNano())
		traceName := "test_trace"

		By("Creating trace group")
		_, err := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: &commonv1.Group{
				Metadata: &commonv1.Metadata{
					Name: groupName,
				},
				Catalog: commonv1.Catalog_CATALOG_TRACE,
				ResourceOpts: &commonv1.ResourceOpts{
					ShardNum: 2,
					SegmentInterval: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  1,
					},
					Ttl: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  7,
					},
				},
			},
		})
		Expect(err).ShouldNot(HaveOccurred())

		By("Creating trace schema and verifying deletion")
		err = cases.VerifyTraceDeletion(ctx, clients, groupName, traceName)
		Expect(err).ShouldNot(HaveOccurred())

		// Cleanup
		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})
})
