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

// Package integration_inspect_test provides integration tests for the inspect functionality in standalone mode.
package integration_inspect_test

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
)

func TestInspect(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Standalone Inspect Suite", Label(integration_standalone.Labels...))
}

var (
	deferFunc          func()
	goods              []gleak.Goroutine
	connection         *grpc.ClientConn
	groupClient        databasev1.GroupRegistryServiceClient
	measureRegClient   databasev1.MeasureRegistryServiceClient
	streamRegClient    databasev1.StreamRegistryServiceClient
	traceRegClient     databasev1.TraceRegistryServiceClient
	measureWriteClient measurev1.MeasureServiceClient
	streamWriteClient  streamv1.StreamServiceClient
	traceWriteClient   tracev1.TraceServiceClient
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
	groupClient = databasev1.NewGroupRegistryServiceClient(connection)
	measureRegClient = databasev1.NewMeasureRegistryServiceClient(connection)
	streamRegClient = databasev1.NewStreamRegistryServiceClient(connection)
	traceRegClient = databasev1.NewTraceRegistryServiceClient(connection)
	measureWriteClient = measurev1.NewMeasureServiceClient(connection)
	streamWriteClient = streamv1.NewStreamServiceClient(connection)
	traceWriteClient = tracev1.NewTraceServiceClient(connection)
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

var _ = ReportAfterSuite("Standalone Inspect Suite", func(report Report) {
	if report.SuiteSucceeded {
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
	}
})

func writeMeasureData(ctx context.Context, groupName, measureName string, dataCount int) {
	writeClient, writeErr := measureWriteClient.Write(ctx)
	Expect(writeErr).ShouldNot(HaveOccurred())

	metadata := &commonv1.Metadata{
		Name:  measureName,
		Group: groupName,
	}
	baseTime := time.Now().Truncate(time.Millisecond)
	for idx := 0; idx < dataCount; idx++ {
		req := &measurev1.WriteRequest{
			Metadata: metadata,
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(baseTime.Add(time.Duration(idx) * time.Second)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{
					Tags: []*modelv1.TagValue{{
						Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "id_" + strconv.Itoa(idx)}},
					}},
				}},
				Fields: []*modelv1.FieldValue{{
					Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(idx * 100)}},
				}},
			},
			MessageId: uint64(time.Now().UnixNano()),
		}
		sendErr := writeClient.Send(req)
		Expect(sendErr).ShouldNot(HaveOccurred())
	}
	Expect(writeClient.CloseSend()).To(Succeed())
	Eventually(func() error {
		_, recvErr := writeClient.Recv()
		return recvErr
	}, flags.EventuallyTimeout).Should(Equal(io.EOF))
}

func writeStreamData(ctx context.Context, groupName, streamName string, dataCount int) {
	writeClient, writeErr := streamWriteClient.Write(ctx)
	Expect(writeErr).ShouldNot(HaveOccurred())

	metadata := &commonv1.Metadata{
		Name:  streamName,
		Group: groupName,
	}
	baseTime := time.Now().Truncate(time.Millisecond)
	for idx := 0; idx < dataCount; idx++ {
		req := &streamv1.WriteRequest{
			Metadata: metadata,
			Element: &streamv1.ElementValue{
				ElementId: strconv.Itoa(idx),
				Timestamp: timestamppb.New(baseTime.Add(time.Duration(idx) * time.Second)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{
					Tags: []*modelv1.TagValue{{
						Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc_" + strconv.Itoa(idx)}},
					}},
				}},
			},
			MessageId: uint64(time.Now().UnixNano()),
		}
		sendErr := writeClient.Send(req)
		Expect(sendErr).ShouldNot(HaveOccurred())
	}
	Expect(writeClient.CloseSend()).To(Succeed())
	Eventually(func() error {
		_, recvErr := writeClient.Recv()
		return recvErr
	}, flags.EventuallyTimeout).Should(Equal(io.EOF))
}

func writeTraceData(ctx context.Context, groupName, traceName string, dataCount int) {
	writeClient, writeErr := traceWriteClient.Write(ctx)
	Expect(writeErr).ShouldNot(HaveOccurred())

	metadata := &commonv1.Metadata{
		Name:  traceName,
		Group: groupName,
	}
	baseTime := time.Now().Truncate(time.Millisecond)
	for idx := 0; idx < dataCount; idx++ {
		traceID := fmt.Sprintf("trace_%d", idx)
		spanID := fmt.Sprintf("span_%d", idx)
		req := &tracev1.WriteRequest{
			Metadata: metadata,
			Tags: []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: traceID}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: spanID}}},
				{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(baseTime.Add(time.Duration(idx) * time.Second))}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "test_service"}}},
				{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(idx * 10)}}},
			},
			Span:    []byte(fmt.Sprintf("span_data_%d", idx)),
			Version: uint64(idx + 1),
		}
		sendErr := writeClient.Send(req)
		Expect(sendErr).ShouldNot(HaveOccurred())
	}
	Expect(writeClient.CloseSend()).To(Succeed())
	Eventually(func() error {
		_, recvErr := writeClient.Recv()
		return recvErr
	}, flags.EventuallyTimeout).Should(Equal(io.EOF))
}

var _ = Describe("Inspect measure in standalone mode", func() {
	var groupName string
	var measureName string
	var ctx context.Context
	const dataCount = 10

	BeforeEach(func() {
		ctx = context.TODO()
		groupName = fmt.Sprintf("inspect-measure-test-%d", time.Now().UnixNano())
		measureName = "test_measure"

		By("Creating measure group")
		_, createErr := groupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
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
		Expect(createErr).ShouldNot(HaveOccurred())

		By("Creating measure schema")
		_, measureErr := measureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: &databasev1.Measure{
				Metadata: &commonv1.Metadata{
					Name:  measureName,
					Group: groupName,
				},
				Entity: &databasev1.Entity{
					TagNames: []string{"id"},
				},
				TagFamilies: []*databasev1.TagFamilySpec{{
					Name: "default",
					Tags: []*databasev1.TagSpec{{
						Name: "id",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					}},
				}},
				Fields: []*databasev1.FieldSpec{{
					Name:              "value",
					FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
					EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
					CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
				}},
			},
		})
		Expect(measureErr).ShouldNot(HaveOccurred())
		time.Sleep(2 * time.Second)

		By("Writing measure data")
		writeMeasureData(ctx, groupName, measureName, dataCount)
		time.Sleep(2 * time.Second)
	})

	AfterEach(func() {
		_, _ = groupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	It("should return group info", func() {
		By("Inspecting group")
		resp, err := groupClient.Inspect(ctx, &databasev1.GroupRegistryServiceInspectRequest{Group: groupName})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(resp).NotTo(BeNil())
		By("Verifying group info")
		Expect(resp.Group).NotTo(BeNil())
		Expect(resp.Group.Metadata.Name).To(Equal(groupName))
		Expect(resp.Group.Catalog).To(Equal(commonv1.Catalog_CATALOG_MEASURE))
	})

	It("should return schema info", func() {
		By("Inspecting group")
		resp, err := groupClient.Inspect(ctx, &databasev1.GroupRegistryServiceInspectRequest{Group: groupName})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(resp).NotTo(BeNil())
		By("Verifying schema info contains the measure")
		Expect(resp.SchemaInfo).NotTo(BeNil())
		Expect(len(resp.SchemaInfo.Measures)).Should(BeNumerically(">=", 1))
		Expect(resp.SchemaInfo.Measures).To(ContainElement(measureName))
		GinkgoWriter.Printf("Schema info: measures=%d, streams=%d, traces=%d, indexRules=%d\n",
			len(resp.SchemaInfo.Measures),
			len(resp.SchemaInfo.Streams),
			len(resp.SchemaInfo.Traces),
			len(resp.SchemaInfo.IndexRules))
	})

	It("should return data info", func() {
		By("Inspecting group")
		resp, err := groupClient.Inspect(ctx, &databasev1.GroupRegistryServiceInspectRequest{Group: groupName})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(resp).NotTo(BeNil())
		By("Verifying data info collected")
		Expect(len(resp.DataInfo)).Should(BeNumerically(">=", 1), "should collect from standalone node")
		var totalDataSize int64
		for idx, dataInfo := range resp.DataInfo {
			Expect(dataInfo.Node).NotTo(BeNil(), "data node %d should have node info", idx)
			Expect(dataInfo.DataSizeBytes).Should(BeNumerically(">", 0), "data node %d should have data size > 0", idx)
			totalDataSize += dataInfo.DataSizeBytes
			GinkgoWriter.Printf("Data node %d: %s, segments: %d, size: %d bytes\n",
				idx, dataInfo.Node.Metadata.Name, len(dataInfo.SegmentInfo), dataInfo.DataSizeBytes)
		}
		Expect(totalDataSize).Should(BeNumerically(">", 0), "total data size should be > 0")
		GinkgoWriter.Printf("Total data size: %d bytes\n", totalDataSize)
	})

	It("should return liaison info", func() {
		By("Inspecting group")
		resp, err := groupClient.Inspect(ctx, &databasev1.GroupRegistryServiceInspectRequest{Group: groupName})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(resp).NotTo(BeNil())
		By("Verifying liaison info in standalone mode")
		Expect(len(resp.LiaisonInfo)).Should(BeNumerically(">=", 1), "should have liaison info in standalone mode")
		for idx, liaisonInfo := range resp.LiaisonInfo {
			GinkgoWriter.Printf("Liaison node %d: PendingWrite=%d\n", idx, liaisonInfo.PendingWriteDataCount)
		}
	})
})

var _ = Describe("Inspect stream in standalone mode", func() {
	var groupName string
	var streamName string
	var ctx context.Context
	const dataCount = 10

	BeforeEach(func() {
		ctx = context.TODO()
		groupName = fmt.Sprintf("inspect-stream-test-%d", time.Now().UnixNano())
		streamName = "test_stream"

		By("Creating stream group")
		_, createErr := groupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
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
		Expect(createErr).ShouldNot(HaveOccurred())

		By("Creating stream schema")
		_, streamErr := streamRegClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
			Stream: &databasev1.Stream{
				Metadata: &commonv1.Metadata{
					Name:  streamName,
					Group: groupName,
				},
				Entity: &databasev1.Entity{
					TagNames: []string{"svc"},
				},
				TagFamilies: []*databasev1.TagFamilySpec{{
					Name: "default",
					Tags: []*databasev1.TagSpec{{
						Name: "svc",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					}},
				}},
			},
		})
		Expect(streamErr).ShouldNot(HaveOccurred())
		time.Sleep(2 * time.Second)

		By("Writing stream data")
		writeStreamData(ctx, groupName, streamName, dataCount)
		time.Sleep(2 * time.Second)
	})

	AfterEach(func() {
		_, _ = groupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	It("should return group info", func() {
		By("Inspecting stream group")
		resp, err := groupClient.Inspect(ctx, &databasev1.GroupRegistryServiceInspectRequest{Group: groupName})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(resp).NotTo(BeNil())
		By("Verifying group info")
		Expect(resp.Group).NotTo(BeNil())
		Expect(resp.Group.Metadata.Name).To(Equal(groupName))
		Expect(resp.Group.Catalog).To(Equal(commonv1.Catalog_CATALOG_STREAM))
	})

	It("should return data info", func() {
		By("Inspecting stream group")
		resp, err := groupClient.Inspect(ctx, &databasev1.GroupRegistryServiceInspectRequest{Group: groupName})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(resp).NotTo(BeNil())
		By("Verifying stream data info collected")
		Expect(len(resp.DataInfo)).Should(BeNumerically(">=", 1), "should collect from standalone node")
		var totalDataSize int64
		for idx, dataInfo := range resp.DataInfo {
			Expect(dataInfo.Node).NotTo(BeNil(), "stream data node %d should have node info", idx)
			Expect(dataInfo.DataSizeBytes).Should(BeNumerically(">", 0), "stream data node %d should have data size > 0", idx)
			totalDataSize += dataInfo.DataSizeBytes
			GinkgoWriter.Printf("Stream data node %d: %s, segments: %d, size: %d bytes\n",
				idx, dataInfo.Node.Metadata.Name, len(dataInfo.SegmentInfo), dataInfo.DataSizeBytes)
		}
		Expect(totalDataSize).Should(BeNumerically(">", 0), "total stream data size should be > 0")
		GinkgoWriter.Printf("Total stream data size: %d bytes\n", totalDataSize)
	})

	It("should return schema info", func() {
		By("Inspecting stream group")
		resp, err := groupClient.Inspect(ctx, &databasev1.GroupRegistryServiceInspectRequest{Group: groupName})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(resp).NotTo(BeNil())
		By("Verifying schema info contains the stream")
		Expect(resp.SchemaInfo).NotTo(BeNil())
		Expect(len(resp.SchemaInfo.Streams)).Should(BeNumerically(">=", 1))
		Expect(resp.SchemaInfo.Streams).To(ContainElement(streamName))
		GinkgoWriter.Printf("Stream schema info: streams=%d, indexRules=%d\n",
			len(resp.SchemaInfo.Streams),
			len(resp.SchemaInfo.IndexRules))
	})
})

var _ = Describe("Inspect trace in standalone mode", func() {
	var groupName string
	var traceName string
	var ctx context.Context
	const dataCount = 10

	BeforeEach(func() {
		ctx = context.TODO()
		groupName = fmt.Sprintf("inspect-trace-test-%d", time.Now().UnixNano())
		traceName = "test_trace"

		By("Creating trace group")
		_, createErr := groupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
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
		Expect(createErr).ShouldNot(HaveOccurred())

		By("Creating trace schema")
		_, traceErr := traceRegClient.Create(ctx, &databasev1.TraceRegistryServiceCreateRequest{
			Trace: &databasev1.Trace{
				Metadata: &commonv1.Metadata{
					Name:  traceName,
					Group: groupName,
				},
				Tags: []*databasev1.TraceTagSpec{
					{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
					{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
				},
				TraceIdTagName:   "trace_id",
				SpanIdTagName:    "span_id",
				TimestampTagName: "timestamp",
			},
		})
		Expect(traceErr).ShouldNot(HaveOccurred())
		time.Sleep(2 * time.Second)

		By("Writing trace data")
		writeTraceData(ctx, groupName, traceName, dataCount)
		time.Sleep(2 * time.Second)
	})

	AfterEach(func() {
		_, _ = groupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	It("should return group info", func() {
		By("Inspecting trace group")
		resp, err := groupClient.Inspect(ctx, &databasev1.GroupRegistryServiceInspectRequest{Group: groupName})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(resp).NotTo(BeNil())
		By("Verifying group info")
		Expect(resp.Group).NotTo(BeNil())
		Expect(resp.Group.Metadata.Name).To(Equal(groupName))
		Expect(resp.Group.Catalog).To(Equal(commonv1.Catalog_CATALOG_TRACE))
	})

	It("should return data info", func() {
		By("Inspecting trace group")
		resp, err := groupClient.Inspect(ctx, &databasev1.GroupRegistryServiceInspectRequest{Group: groupName})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(resp).NotTo(BeNil())
		By("Verifying trace data info collected")
		Expect(len(resp.DataInfo)).Should(BeNumerically(">=", 1), "should collect from standalone node")
		var totalDataSize int64
		for idx, dataInfo := range resp.DataInfo {
			Expect(dataInfo.Node).NotTo(BeNil(), "trace data node %d should have node info", idx)
			Expect(dataInfo.DataSizeBytes).Should(BeNumerically(">", 0), "trace data node %d should have data size > 0", idx)
			totalDataSize += dataInfo.DataSizeBytes
			GinkgoWriter.Printf("Trace data node %d: %s, segments: %d, size: %d bytes\n",
				idx, dataInfo.Node.Metadata.Name, len(dataInfo.SegmentInfo), dataInfo.DataSizeBytes)
		}
		Expect(totalDataSize).Should(BeNumerically(">", 0), "total trace data size should be > 0")
		GinkgoWriter.Printf("Total trace data size: %d bytes\n", totalDataSize)
	})

	It("should return schema info", func() {
		By("Inspecting trace group")
		resp, err := groupClient.Inspect(ctx, &databasev1.GroupRegistryServiceInspectRequest{Group: groupName})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(resp).NotTo(BeNil())
		By("Verifying schema info contains the trace")
		Expect(resp.SchemaInfo).NotTo(BeNil())
		Expect(len(resp.SchemaInfo.Traces)).Should(BeNumerically(">=", 1))
		Expect(resp.SchemaInfo.Traces).To(ContainElement(traceName))
		GinkgoWriter.Printf("Trace schema info: traces=%d, indexRules=%d\n",
			len(resp.SchemaInfo.Traces),
			len(resp.SchemaInfo.IndexRules))
	})
})
