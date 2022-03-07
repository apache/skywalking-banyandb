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

package query_test

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"math"
	"strconv"
	"time"

	"github.com/golang/protobuf/jsonpb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/query"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pb "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

var (
	svcs         *services
	deferFn      func()
	streamSchema stream.Stream
	sT, eT       time.Time
)

// BeforeSuite - Init logger
var _ = BeforeSuite(func() {
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "info",
	})).To(Succeed())

	svcs, deferFn = setUpServices()
	var err error
	streamSchema, err = svcs.stream.Stream(&commonv1.Metadata{
		Name:  "sw",
		Group: "default",
	})
	Expect(err).ShouldNot(HaveOccurred())
	baseTs := setUpStreamQueryData("multiple_shards.json", streamSchema)

	sT, eT = baseTs, baseTs.Add(1*time.Hour)
})

var _ = AfterSuite(func() {
	deferFn()
})

type services struct {
	stream   stream.Service
	measure  measure.Service
	pipeline queue.Queue
}

func setUpServices() (*services, func()) {
	// Init `Discovery` module
	repo, err := discovery.NewServiceRepo(context.Background())
	Expect(err).ShouldNot(HaveOccurred())

	// Init `Queue` module
	pipeline, err := queue.NewQueue(context.TODO(), repo)
	Expect(err).ShouldNot(HaveOccurred())

	// Init `Metadata` module
	metadataService, err := metadata.NewService(context.TODO())
	Expect(err).ShouldNot(HaveOccurred())

	// Init `Stream` module
	streamService, err := stream.NewService(context.TODO(), metadataService, repo, pipeline)
	Expect(err).ShouldNot(HaveOccurred())

	// Init `Measure` module
	measureService, err := measure.NewService(context.TODO(), metadataService, repo, pipeline)
	Expect(err).ShouldNot(HaveOccurred())
	preloadMeasureSvc := &preloadMeasureService{metaSvc: metadataService}
	preloadStreamSvc := &preloadStreamService{metaSvc: metadataService}

	var flags []string
	metaPath, metaDeferFunc, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	flags = append(flags, "--metadata-root-path="+metaPath)
	rootPath, deferFunc, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	flags = append(flags, "--root-path="+rootPath)
	executor, err := query.NewExecutor(context.TODO(), streamService, measureService, metadataService, repo, pipeline)
	Expect(err).NotTo(HaveOccurred())
	moduleDeferFunc := test.SetUpModules(
		flags,
		repo,
		pipeline,
		metadataService,
		preloadMeasureSvc,
		measureService,
		preloadStreamSvc,
		streamService,
		executor,
	)

	return &services{
			measure:  measureService,
			stream:   streamService,
			pipeline: pipeline,
		}, func() {
			moduleDeferFunc()
			metaDeferFunc()
			deferFunc()
		}
}

//go:embed testdata/*.json
var dataFS embed.FS

func setUpStreamQueryData(dataFile string, stream stream.Stream) (baseTime time.Time) {
	var templates []interface{}
	baseTime = time.Now()
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(json.Unmarshal(content, &templates)).Should(Succeed())
	bb, _ := base64.StdEncoding.DecodeString("YWJjMTIzIT8kKiYoKSctPUB+")
	for i, template := range templates {
		rawSearchTagFamily, errMarshal := json.Marshal(template)
		Expect(errMarshal).ShouldNot(HaveOccurred())
		searchTagFamily := &modelv1.TagFamilyForWrite{}
		Expect(jsonpb.UnmarshalString(string(rawSearchTagFamily), searchTagFamily)).Should(Succeed())
		e := &streamv1.ElementValue{
			ElementId: strconv.Itoa(i),
			Timestamp: timestamppb.New(baseTime.Add(500 * time.Millisecond * time.Duration(i))),
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{
					Tags: []*modelv1.TagValue{
						{
							Value: &modelv1.TagValue_BinaryData{
								BinaryData: bb,
							},
						},
					},
				},
			},
		}
		e.TagFamilies = append(e.TagFamilies, searchTagFamily)
		Expect(stream.Write(e)).Should(Succeed())
	}
	return baseTime
}

var _ = Describe("Stream Query", func() {
	It("should return nothing when querying given timeRange which is out of the time range of data", func() {
		query := pb.NewStreamQueryRequestBuilder().
			Limit(10).
			Offset(0).
			Metadata("default", "sw").
			TimeRange(time.Unix(0, 0), time.Unix(0, 1)).
			Projection("searchable", "trace_id").
			Build()
		now := time.Now()
		m := bus.NewMessage(bus.MessageID(now.UnixNano()), query)
		f, err := svcs.pipeline.Publish(data.TopicStreamQuery, m)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(f).ShouldNot(BeNil())
		msg, err := f.Get()
		Expect(err).ShouldNot(HaveOccurred())
		Expect(msg).ShouldNot(BeNil())
	})

	It("should return segments with data binary projection while querying given timeRange", func() {
		query := pb.NewStreamQueryRequestBuilder().
			Limit(10).
			Offset(0).
			Metadata("default", "sw").
			TimeRange(sT, eT).
			Projection("searchable", "trace_id").
			Projection("data", "data_binary").
			Build()
		now := time.Now()
		m := bus.NewMessage(bus.MessageID(now.UnixNano()), query)
		f, err := svcs.pipeline.Publish(data.TopicStreamQuery, m)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(f).ShouldNot(BeNil())
		msg, err := f.Get()
		Expect(err).ShouldNot(HaveOccurred())
		Expect(msg).ShouldNot(BeNil())
		Expect(msg.Data()).Should(HaveLen(5))
		Expect(msg.Data()).Should(HaveBinary())
	})

	It("should return all segments with data binary projection when querying max valid time-range", func() {
		query := pb.NewStreamQueryRequestBuilder().
			Limit(10).
			Offset(0).
			Metadata("default", "sw").
			// min: 1677-09-21T00:12:43.145224194Z
			// max: 2262-04-11T23:47:16.854775806Z
			TimeRange(time.Unix(0, math.MinInt64), time.Unix(0, math.MaxInt64)).
			Projection("searchable", "trace_id").
			Projection("data", "data_binary").
			Build()
		now := time.Now()
		m := bus.NewMessage(bus.MessageID(now.UnixNano()), query)
		f, err := svcs.pipeline.Publish(data.TopicStreamQuery, m)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(f).ShouldNot(BeNil())
		msg, err := f.Get()
		Expect(err).ShouldNot(HaveOccurred())
		Expect(msg).ShouldNot(BeNil())
		Expect(msg.Data()).Should(HaveLen(5))
		Expect(msg.Data()).Should(HaveBinary())
	})

	It("should return all segments sorted by duration DESC", func() {
		query := pb.NewStreamQueryRequestBuilder().
			Limit(10).
			Offset(0).
			Metadata("default", "sw").
			TimeRange(sT, eT).
			OrderBy("duration", modelv1.Sort_SORT_DESC).
			Projection("searchable", "trace_id", "duration").
			Build()
		now := time.Now()
		m := bus.NewMessage(bus.MessageID(now.UnixNano()), query)
		f, err := svcs.pipeline.Publish(data.TopicStreamQuery, m)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(f).ShouldNot(BeNil())
		msg, err := f.Get()
		Expect(err).ShouldNot(HaveOccurred())
		Expect(msg).ShouldNot(BeNil())
		Expect(msg.Data()).Should(HaveLen(5))
		sortedChecker := func(elements []*streamv1.Element) bool {
			return logical.SortedByIndex(elements, 0, 1, modelv1.Sort_SORT_DESC)
		}
		Expect(sortedChecker(msg.Data().([]*streamv1.Element))).To(BeTrue())
	})

	It("should return segments without binary when querying a given TraceID", func() {
		query := pb.NewStreamQueryRequestBuilder().
			Limit(10).
			Offset(0).
			Metadata("default", "sw").
			TagsInTagFamily("searchable", "trace_id", "=", "1").
			TimeRange(sT, eT).
			Projection("searchable", "trace_id").
			Build()
		now := time.Now()
		m := bus.NewMessage(bus.MessageID(now.UnixNano()), query)
		f, err := svcs.pipeline.Publish(data.TopicStreamQuery, m)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(f).ShouldNot(BeNil())
		msg, err := f.Get()
		Expect(err).ShouldNot(HaveOccurred())
		Expect(msg).ShouldNot(BeNil())
		Expect(msg.Data()).Should(HaveLen(1))
		Expect(msg.Data()).Should(NotHaveBinary())
	})

	It("should return segments with binary when querying a given TraceID", func() {
		query := pb.NewStreamQueryRequestBuilder().
			Limit(10).
			Offset(0).
			Metadata("default", "sw").
			TagsInTagFamily("searchable", "trace_id", "=", "1").
			TimeRange(sT, eT).
			Projection("data", "data_binary").
			Projection("searchable", "trace_id").
			Build()
		now := time.Now()
		m := bus.NewMessage(bus.MessageID(now.UnixNano()), query)
		f, err := svcs.pipeline.Publish(data.TopicStreamQuery, m)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(f).ShouldNot(BeNil())
		msg, err := f.Get()
		Expect(err).ShouldNot(HaveOccurred())
		Expect(msg).ShouldNot(BeNil())
		Expect(msg.Data()).Should(HaveLen(1))
		Expect(msg.Data()).Should(HaveBinary())
	})

	It("uses numerical index - query duration < 500", func() {
		query := pb.NewStreamQueryRequestBuilder().
			Limit(10).
			Offset(0).
			Metadata("default", "sw").
			TagsInTagFamily("searchable", "duration", "<", 500).
			TimeRange(sT, eT).
			Projection("searchable", "trace_id").
			Build()
		now := time.Now()
		m := bus.NewMessage(bus.MessageID(now.UnixNano()), query)
		f, err := svcs.pipeline.Publish(data.TopicStreamQuery, m)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(f).ShouldNot(BeNil())
		msg, err := f.Get()
		Expect(err).ShouldNot(HaveOccurred())
		Expect(msg).ShouldNot(BeNil())
		Expect(msg.Data()).Should(HaveLen(3))
	})

	It("uses numerical index - query duration <= 500", func() {
		query := pb.NewStreamQueryRequestBuilder().
			Limit(10).
			Offset(0).
			Metadata("default", "sw").
			TagsInTagFamily("searchable", "duration", "<=", 500).
			TimeRange(sT, eT).
			Projection("searchable", "trace_id").
			Build()
		now := time.Now()
		m := bus.NewMessage(bus.MessageID(now.UnixNano()), query)
		f, err := svcs.pipeline.Publish(data.TopicStreamQuery, m)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(f).ShouldNot(BeNil())
		msg, err := f.Get()
		Expect(err).ShouldNot(HaveOccurred())
		Expect(msg).ShouldNot(BeNil())
		Expect(msg.Data()).Should(HaveLen(4))
	})

	It("uses textual index - http.method == GET", func() {
		query := pb.NewStreamQueryRequestBuilder().
			Limit(10).
			Offset(0).
			Metadata("default", "sw").
			TagsInTagFamily("searchable", "http.method", "=", "GET").
			TimeRange(sT, eT).
			Projection("searchable", "trace_id").
			Build()
		now := time.Now()
		m := bus.NewMessage(bus.MessageID(now.UnixNano()), query)
		f, err := svcs.pipeline.Publish(data.TopicStreamQuery, m)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(f).ShouldNot(BeNil())
		msg, err := f.Get()
		Expect(err).ShouldNot(HaveOccurred())
		Expect(msg).ShouldNot(BeNil())
		Expect(msg.Data()).Should(HaveLen(3))
		Expect(msg.Data()).Should(NotHaveBinary())
	})

	It("uses textual index - http.method == GET with dataBinary projection", func() {
		query := pb.NewStreamQueryRequestBuilder().
			Limit(10).
			Offset(0).
			Metadata("default", "sw").
			TagsInTagFamily("searchable", "http.method", "=", "GET").
			TimeRange(sT, eT).
			Projection("data", "data_binary").
			Projection("searchable", "trace_id").
			Build()
		now := time.Now()
		m := bus.NewMessage(bus.MessageID(now.UnixNano()), query)
		f, err := svcs.pipeline.Publish(data.TopicStreamQuery, m)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(f).ShouldNot(BeNil())
		msg, err := f.Get()
		Expect(err).ShouldNot(HaveOccurred())
		Expect(msg).ShouldNot(BeNil())
		Expect(msg.Data()).Should(HaveLen(3))
		Expect(msg.Data()).Should(HaveBinary())
	})

	It("uses mixed index - status_code == 500 AND duration <= 100", func() {
		query := pb.NewStreamQueryRequestBuilder().
			Limit(10).
			Offset(0).
			Metadata("default", "sw").
			TagsInTagFamily("searchable", "status_code", "=", "500", "duration", "<=", 100).
			TimeRange(sT, eT).
			Projection("searchable", "trace_id").
			Build()
		now := time.Now()
		m := bus.NewMessage(bus.MessageID(now.UnixNano()), query)
		f, err := svcs.pipeline.Publish(data.TopicStreamQuery, m)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(f).ShouldNot(BeNil())
		msg, err := f.Get()
		Expect(err).ShouldNot(HaveOccurred())
		Expect(msg).ShouldNot(BeNil())
		Expect(msg.Data()).Should(HaveLen(1))
	})
})
