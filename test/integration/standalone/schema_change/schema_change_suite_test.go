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

package integration_schema_change_test

import (
	"context"
	"encoding/base64"
	"errors"
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
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
)

func TestIntegrationSchemaChange(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Schema Change Suite", Label(integration_standalone.Labels...))
}

var (
	connection *grpc.ClientConn
	now        time.Time
	deferFunc  func()
	goods      []gleak.Goroutine
)

var _ = SynchronizedBeforeSuite(func() []byte {
	goods = gleak.Goroutines()
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(Succeed())
	var addr string
	addr, _, deferFunc = setup.EmptyStandalone()
	ns := timestamp.NowMilli().UnixNano()
	now = time.Unix(0, ns-ns%int64(time.Minute))
	return []byte(addr)
}, func(address []byte) {
	var err error
	connection, err = grpchelper.Conn(string(address), 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())
})

var _ = SynchronizedAfterSuite(func() {
	if connection != nil {
		Expect(connection.Close()).To(Succeed())
	}
}, func() {})

var _ = ReportAfterSuite("Integration Schema Change Suite", func(report Report) {
	if report.SuiteSucceeded {
		if deferFunc != nil {
			deferFunc()
		}
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
	}
})

var _ = Describe("Schema Change in Same Group", func() {
	var groupName string
	var groupCounter int

	BeforeEach(func() {
		groupCounter++
		groupName = fmt.Sprintf("test-schema-change-%d", groupCounter)
		groupClient := databasev1.NewGroupRegistryServiceClient(connection)
		_, err := groupClient.Create(context.Background(), &databasev1.GroupRegistryServiceCreateRequest{
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
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(3 * time.Second)
	})

	AfterEach(func() {
		groupClient := databasev1.NewGroupRegistryServiceClient(connection)
		_, _ = groupClient.Delete(context.Background(), &databasev1.GroupRegistryServiceDeleteRequest{
			Group: groupName,
		})
	})

	Context("Stream schema with deleted tag", func() {
		It("should query data across schema change when a tag is deleted", func() {
			ctx := context.Background()
			streamName := "schema_change_deleted_tag"
			streamClient := databasev1.NewStreamRegistryServiceClient(connection)
			initialStream := &databasev1.Stream{
				Metadata: &commonv1.Metadata{
					Name:  streamName,
					Group: groupName,
				},
				TagFamilies: []*databasev1.TagFamilySpec{
					{
						Name: "data",
						Tags: []*databasev1.TagSpec{
							{Name: "data_binary", Type: databasev1.TagType_TAG_TYPE_DATA_BINARY},
						},
					},
					{
						Name: "searchable",
						Tags: []*databasev1.TagSpec{
							{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
							{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
							{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
							{Name: "status_code", Type: databasev1.TagType_TAG_TYPE_INT},
						},
					},
				},
				Entity: &databasev1.Entity{
					TagNames: []string{"service_id"},
				},
			}
			_, err := streamClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
				Stream: initialStream,
			})
			Expect(err).NotTo(HaveOccurred())

			writeStreamData(ctx, streamName, groupName, now.Add(-2*time.Hour), 5, true)

			getResp, err := streamClient.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{
				Metadata: &commonv1.Metadata{
					Name:  streamName,
					Group: groupName,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			updatedStream := getResp.GetStream()
			updatedStream.TagFamilies[1].Tags = []*databasev1.TagSpec{
				{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
			}
			_, err = streamClient.Update(ctx, &databasev1.StreamRegistryServiceUpdateRequest{
				Stream: updatedStream,
			})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(2 * time.Second)
			writeStreamData(ctx, streamName, groupName, now.Add(-1*time.Hour), 3, false)

			Eventually(func(innerGm Gomega) {
				queryClient := streamv1.NewStreamServiceClient(connection)
				queryResp, queryErr := queryClient.Query(ctx, &streamv1.QueryRequest{
					Groups: []string{groupName},
					Name:   streamName,
					TimeRange: &modelv1.TimeRange{
						Begin: timestamppb.New(now.Add(-3 * time.Hour)),
						End:   timestamppb.New(now),
					},
					Projection: &modelv1.TagProjection{
						TagFamilies: []*modelv1.TagProjection_TagFamily{
							{
								Name: "searchable",
								Tags: []string{"trace_id", "service_id", "duration"},
							},
						},
					},
				})
				innerGm.Expect(queryErr).NotTo(HaveOccurred())
				innerGm.Expect(queryResp.Elements).To(HaveLen(8))

				for _, elem := range queryResp.Elements {
					for _, tf := range elem.TagFamilies {
						if tf.Name == "searchable" {
							for _, tag := range tf.Tags {
								innerGm.Expect(tag.Key).NotTo(Equal("status_code"),
									"deleted tag should not be returned in query results")
							}
						}
					}
				}
			}, flags.EventuallyTimeout).Should(Succeed())

			_, err = streamClient.Delete(ctx, &databasev1.StreamRegistryServiceDeleteRequest{
				Metadata: &commonv1.Metadata{
					Name:  streamName,
					Group: groupName,
				},
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("Stream schema with added tag", func() {
		It("should query data across schema change when a new tag is added", func() {
			ctx := context.Background()
			streamName := "schema_change_added_tag"
			streamClient := databasev1.NewStreamRegistryServiceClient(connection)
			initialStream := &databasev1.Stream{
				Metadata: &commonv1.Metadata{
					Name:  streamName,
					Group: groupName,
				},
				TagFamilies: []*databasev1.TagFamilySpec{
					{
						Name: "data",
						Tags: []*databasev1.TagSpec{
							{Name: "data_binary", Type: databasev1.TagType_TAG_TYPE_DATA_BINARY},
						},
					},
					{
						Name: "searchable",
						Tags: []*databasev1.TagSpec{
							{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
							{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
							{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
						},
					},
				},
				Entity: &databasev1.Entity{
					TagNames: []string{"service_id"},
				},
			}
			_, err := streamClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
				Stream: initialStream,
			})
			Expect(err).NotTo(HaveOccurred())

			writeStreamData(ctx, streamName, groupName, now.Add(-2*time.Hour), 5, false)

			getResp, err := streamClient.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{
				Metadata: &commonv1.Metadata{
					Name:  streamName,
					Group: groupName,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			updatedStream := getResp.GetStream()
			updatedStream.TagFamilies[1].Tags = append(updatedStream.TagFamilies[1].Tags,
				&databasev1.TagSpec{Name: "status_code", Type: databasev1.TagType_TAG_TYPE_INT})
			_, err = streamClient.Update(ctx, &databasev1.StreamRegistryServiceUpdateRequest{
				Stream: updatedStream,
			})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(2 * time.Second)
			writeStreamData(ctx, streamName, groupName, now.Add(-1*time.Hour), 3, true)

			Eventually(func(innerGm Gomega) {
				queryClient := streamv1.NewStreamServiceClient(connection)
				queryResp, queryErr := queryClient.Query(ctx, &streamv1.QueryRequest{
					Groups: []string{groupName},
					Name:   streamName,
					TimeRange: &modelv1.TimeRange{
						Begin: timestamppb.New(now.Add(-3 * time.Hour)),
						End:   timestamppb.New(now),
					},
					Projection: &modelv1.TagProjection{
						TagFamilies: []*modelv1.TagProjection_TagFamily{
							{
								Name: "searchable",
								Tags: []string{"trace_id", "service_id", "duration", "status_code"},
							},
						},
					},
				})
				innerGm.Expect(queryErr).NotTo(HaveOccurred())
				innerGm.Expect(queryResp.Elements).To(HaveLen(8)) // 5 old + 3 new

				oldDataCount := 0
				newDataCount := 0
				for _, elem := range queryResp.Elements {
					for _, tf := range elem.TagFamilies {
						if tf.Name == "searchable" {
							for _, tag := range tf.Tags {
								if tag.Key == "status_code" {
									if tag.Value.GetInt() != nil {
										newDataCount++
									} else {
										oldDataCount++
									}
								}
							}
						}
					}
				}
				innerGm.Expect(oldDataCount).To(Equal(5), "old data should have null status_code")
				innerGm.Expect(newDataCount).To(Equal(3), "new data should have status_code values")
			}, flags.EventuallyTimeout).Should(Succeed())

			_, err = streamClient.Delete(ctx, &databasev1.StreamRegistryServiceDeleteRequest{
				Metadata: &commonv1.Metadata{
					Name:  streamName,
					Group: groupName,
				},
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("Stream schema with filter on deleted tag", func() {
		It("should filter data correctly after tag is deleted", func() {
			ctx := context.Background()
			streamName := "schema_change_filter_deleted"
			streamClient := databasev1.NewStreamRegistryServiceClient(connection)
			initialStream := &databasev1.Stream{
				Metadata: &commonv1.Metadata{
					Name:  streamName,
					Group: groupName,
				},
				TagFamilies: []*databasev1.TagFamilySpec{
					{
						Name: "data",
						Tags: []*databasev1.TagSpec{
							{Name: "data_binary", Type: databasev1.TagType_TAG_TYPE_DATA_BINARY},
						},
					},
					{
						Name: "searchable",
						Tags: []*databasev1.TagSpec{
							{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
							{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
							{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
							{Name: "status_code", Type: databasev1.TagType_TAG_TYPE_INT},
						},
					},
				},
				Entity: &databasev1.Entity{
					TagNames: []string{"service_id"},
				},
			}
			_, err := streamClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
				Stream: initialStream,
			})
			Expect(err).NotTo(HaveOccurred())

			writeStreamData(ctx, streamName, groupName, now.Add(-2*time.Hour), 5, true)

			getResp, err := streamClient.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{
				Metadata: &commonv1.Metadata{
					Name:  streamName,
					Group: groupName,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			updatedStream := getResp.GetStream()
			updatedStream.TagFamilies[1].Tags = []*databasev1.TagSpec{
				{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
			}
			_, err = streamClient.Update(ctx, &databasev1.StreamRegistryServiceUpdateRequest{
				Stream: updatedStream,
			})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(2 * time.Second)
			writeStreamData(ctx, streamName, groupName, now.Add(-1*time.Hour), 3, false)

			Eventually(func(innerGm Gomega) {
				queryClient := streamv1.NewStreamServiceClient(connection)
				queryResp, queryErr := queryClient.Query(ctx, &streamv1.QueryRequest{
					Groups: []string{groupName},
					Name:   streamName,
					TimeRange: &modelv1.TimeRange{
						Begin: timestamppb.New(now.Add(-3 * time.Hour)),
						End:   timestamppb.New(now),
					},
					Criteria: &modelv1.Criteria{
						Exp: &modelv1.Criteria_Condition{
							Condition: &modelv1.Condition{
								Name: "status_code",
								Op:   modelv1.Condition_BINARY_OP_EQ,
								Value: &modelv1.TagValue{
									Value: &modelv1.TagValue_Int{
										Int: &modelv1.Int{Value: 200},
									},
								},
							},
						},
					},
					Projection: &modelv1.TagProjection{
						TagFamilies: []*modelv1.TagProjection_TagFamily{
							{
								Name: "searchable",
								Tags: []string{"trace_id", "service_id"},
							},
						},
					},
				})
				if queryErr != nil {
					return
				}
				innerGm.Expect(queryResp.Elements).To(BeEmpty(),
					"filtering by deleted tag should return no results")
			}, flags.EventuallyTimeout).Should(Succeed())

			_, err = streamClient.Delete(ctx, &databasev1.StreamRegistryServiceDeleteRequest{
				Metadata: &commonv1.Metadata{
					Name:  streamName,
					Group: groupName,
				},
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

func writeStreamData(ctx context.Context, name, group string, baseTime time.Time, count int, includeOptionalTag bool) {
	Eventually(func() error {
		return doWriteStreamData(ctx, name, group, baseTime, count, includeOptionalTag)
	}, flags.EventuallyTimeout).Should(Succeed())
}

func doWriteStreamData(ctx context.Context, name, group string, baseTime time.Time, count int, includeOptionalTag bool) error {
	c := streamv1.NewStreamServiceClient(connection)
	writeClient, err := c.Write(ctx)
	if err != nil {
		return err
	}
	bb, _ := base64.StdEncoding.DecodeString("YmFueWFuZGI=")
	interval := 500 * time.Millisecond

	for i := 0; i < count; i++ {
		searchableTags := []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "trace_" + strconv.Itoa(i)}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "service_1"}}},
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(100 * (i + 1))}}},
		}
		if includeOptionalTag {
			searchableTags = append(searchableTags,
				&modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(200 + i)}}})
		}
		e := &streamv1.ElementValue{
			ElementId: strconv.Itoa(i),
			Timestamp: timestamppb.New(baseTime.Add(interval * time.Duration(i))),
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{
					Tags: []*modelv1.TagValue{
						{Value: &modelv1.TagValue_BinaryData{BinaryData: bb}},
					},
				},
				{
					Tags: searchableTags,
				},
			},
		}
		err = writeClient.Send(&streamv1.WriteRequest{
			Metadata: &commonv1.Metadata{
				Name:  name,
				Group: group,
			},
			Element:   e,
			MessageId: uint64(time.Now().UnixNano()),
		})
		if err != nil {
			return err
		}
	}
	if err = writeClient.CloseSend(); err != nil {
		return err
	}

	for {
		_, err = writeClient.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
