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

package stream_test

import (
	"context"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ = Describe("Metadata", func() {
	var svcs *services
	var deferFn func()
	var goods []gleak.Goroutine
	BeforeEach(func() {
		svcs, deferFn = setUp()
		goods = gleak.Goroutines()
		Eventually(func() bool {
			_, ok := svcs.stream.LoadGroup("default")
			return ok
		}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
	})

	AfterEach(func() {
		deferFn()
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	Context("Manage group", func() {
		It("should close the group", func() {
			deleted, err := svcs.metadataService.GroupRegistry().DeleteGroup(context.TODO(), "default")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(deleted).Should(BeTrue())
			Eventually(func() bool {
				_, ok := svcs.stream.LoadGroup("default")
				return ok
			}).WithTimeout(flags.EventuallyTimeout).Should(BeFalse())
		})

		It("should add shards", func() {
			groupSchema, err := svcs.metadataService.GroupRegistry().GetGroup(context.TODO(), "default")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(groupSchema).ShouldNot(BeNil())
			groupSchema.ResourceOpts.ShardNum = 4

			Expect(svcs.metadataService.GroupRegistry().UpdateGroup(context.TODO(), groupSchema)).Should(Succeed())

			Eventually(func() bool {
				group, ok := svcs.stream.LoadGroup("default")
				if !ok {
					return false
				}
				return group.GetSchema().GetResourceOpts().GetShardNum() == 4
			}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
		})
	})

	Context("Manage stream", func() {
		It("should pass smoke test", func() {
			Eventually(func() bool {
				_, err := svcs.stream.Stream(&commonv1.Metadata{
					Name:  "sw",
					Group: "default",
				})
				return err == nil
			}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
		})
		It("should close the stream", func() {
			deleted, err := svcs.metadataService.StreamRegistry().DeleteStream(context.TODO(), &commonv1.Metadata{
				Name:  "sw",
				Group: "default",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(deleted).Should(BeTrue())
			Eventually(func() error {
				_, err := svcs.stream.Stream(&commonv1.Metadata{
					Name:  "sw",
					Group: "default",
				})
				return err
			}).WithTimeout(flags.EventuallyTimeout).Should(MatchError(stream.ErrStreamNotExist))
		})

		Context("Update a stream", func() {
			var streamSchema *databasev1.Stream

			BeforeEach(func() {
				var err error
				streamSchema, err = svcs.metadataService.StreamRegistry().GetStream(context.TODO(), &commonv1.Metadata{
					Name:  "sw",
					Group: "default",
				})

				Expect(err).ShouldNot(HaveOccurred())
				Expect(streamSchema).ShouldNot(BeNil())
			})
		})

		Context("Add tags to the stream", func() {
			var streamSchema *databasev1.Stream
			size := 3
			var independentFamily bool
			JustBeforeEach(func() {
				var err error
				streamSchema, err = svcs.metadataService.StreamRegistry().GetStream(context.TODO(), &commonv1.Metadata{
					Name:  "sw",
					Group: "default",
				})
				Expect(err).ShouldNot(HaveOccurred())
				Expect(streamSchema).ShouldNot(BeNil())
				writeData(svcs, size, nil, false)
				_ = queryAllMeasurements(svcs, size, nil, "")

				l := len(streamSchema.TagFamilies)
				if independentFamily {
					streamSchema.TagFamilies = append(streamSchema.TagFamilies, &databasev1.TagFamilySpec{
						Name: "independent_family",
						Tags: []*databasev1.TagSpec{
							{
								Name: "new_tag",
								Type: databasev1.TagType_TAG_TYPE_STRING,
							},
						},
					})
				} else {
					streamSchema.TagFamilies[l-1].Tags = append(streamSchema.TagFamilies[l-1].Tags, &databasev1.TagSpec{
						Name: "new_tag",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					})
				}

				modRevision, err := svcs.metadataService.StreamRegistry().UpdateStream(context.TODO(), streamSchema)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(modRevision).ShouldNot(BeZero())

				Eventually(func() bool {
					val, err := svcs.stream.Stream(&commonv1.Metadata{
						Name:  "sw",
						Group: "default",
					})
					if err != nil {
						return false
					}
					if independentFamily {
						return len(val.GetSchema().TagFamilies) == 3 && len(val.GetSchema().TagFamilies[2].Tags) == 1
					}
					return len(val.GetSchema().TagFamilies) == 2 && len(val.GetSchema().TagFamilies[1].Tags) == len(streamSchema.TagFamilies[1].Tags)
				}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
			})

			When("a new tag added to an existed family", func() {
				BeforeEach(func() {
					independentFamily = false
				})
				It("returns nil for the new added tags", func() {
					dp := queryAllMeasurements(svcs, size, []string{"new_tag"}, "")
					for i := range dp {
						Expect(dp[i].TagFamilies[0].Tags[3].Key).Should(Equal("new_tag"))
						Expect(dp[i].TagFamilies[0].Tags[3].Value.GetValue()).Should(BeAssignableToTypeOf(&modelv1.TagValue_Null{}))
					}
				})
				It("get new values for the new added tags", func() {
					writeData(svcs, size, []string{"test1", "test2", "test3"}, independentFamily)
					dp := queryAllMeasurements(svcs, size*2, []string{"new_tag"}, "")
					for i := 0; i < size; i++ {
						Expect(dp[i].TagFamilies[0].Tags[3].Key).Should(Equal("new_tag"))
						Expect(dp[i].TagFamilies[0].Tags[3].Value.GetValue()).Should(BeAssignableToTypeOf(&modelv1.TagValue_Null{}))
					}
					for i := size; i < size*2; i++ {
						Expect(dp[i].TagFamilies[0].Tags[3].Key).Should(Equal("new_tag"))
						Expect(dp[i].TagFamilies[0].Tags[3].Value.GetStr().Value).Should(Equal("test" + strconv.Itoa(i%3+1)))
					}
				})
			})

			When("a new tag added to a new family", func() {
				BeforeEach(func() {
					independentFamily = true
				})
				It("returns nil for the new added tags", func() {
					dp := queryAllMeasurements(svcs, size, []string{"new_tag"}, "independent_family")
					for i := range dp {
						Expect(dp[i].TagFamilies[1].Tags[0].Key).Should(Equal("new_tag"))
						Expect(dp[i].TagFamilies[1].Tags[0].Value.GetValue()).Should(BeAssignableToTypeOf(&modelv1.TagValue_Null{}))
					}
				})
				It("get new values for the new added tags", func() {
					writeData(svcs, size, []string{"test1", "test2", "test3"}, independentFamily)
					dp := queryAllMeasurements(svcs, size*2, []string{"new_tag"}, "independent_family")
					for i := 0; i < size; i++ {
						Expect(dp[i].TagFamilies[1].Tags[0].Key).Should(Equal("new_tag"))
						Expect(dp[i].TagFamilies[1].Tags[0].Value.GetValue()).Should(BeAssignableToTypeOf(&modelv1.TagValue_Null{}))
					}
					for i := size; i < size*2; i++ {
						Expect(dp[i].TagFamilies[1].Tags[0].Key).Should(Equal("new_tag"))
						Expect(dp[i].TagFamilies[1].Tags[0].Value.GetStr().Value).Should(Equal("test" + strconv.Itoa(i%3+1)))
					}
				})
			})
		})
	})
})

func writeData(svcs *services, expectedSize int, newTag []string, independentFamily bool) {
	bp := svcs.pipeline.NewBatchPublisher(5 * time.Second)
	defer bp.Close()
	for i := 0; i < expectedSize; i++ {
		iStr := strconv.Itoa(i)
		req := &streamv1.WriteRequest{
			Metadata: &commonv1.Metadata{
				Name:  "sw",
				Group: "default",
			},
			Element: &streamv1.ElementValue{
				Timestamp: timestamppb.New(timestamp.NowMilli()),
				ElementId: "element" + iStr,
				TagFamilies: []*modelv1.TagFamilyForWrite{
					{
						Tags: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte("binary_data" + iStr)}},
						},
					},
					{
						Tags: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "trace_id" + iStr}}},
							{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 1}}},
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "service_id" + iStr}}},
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "service_instance_id" + iStr}}},
						},
					},
				},
			},
		}
		for j := 0; j < 13; j++ {
			req.Element.TagFamilies[1].Tags = append(req.Element.TagFamilies[1].Tags, &modelv1.TagValue{
				Value: &modelv1.TagValue_Null{},
			})
		}
		if independentFamily {
			req.Element.TagFamilies = append(req.Element.TagFamilies, &modelv1.TagFamilyForWrite{})
		}
		if i < len(newTag) {
			l := len(req.Element.TagFamilies)
			req.Element.TagFamilies[l-1].Tags = append(req.Element.TagFamilies[l-1].Tags, &modelv1.TagValue{
				Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: newTag[i]}},
			})
		}
		bp.Publish(context.TODO(), data.TopicStreamWrite, bus.NewMessage(bus.MessageID(i), &streamv1.InternalWriteRequest{
			EntityValues: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity" + iStr}}}},
			Request:      req,
		}))
	}
}

func queryAllMeasurements(svcs *services, expectedSize int, newTag []string, newTagFamily string) []*streamv1.Element {
	req := &streamv1.QueryRequest{
		Groups: []string{"default"},
		Name:   "sw",
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(timestamp.NowMilli().Add(-time.Hour)),
			End:   timestamppb.New(timestamp.NowMilli().Add(time.Hour)),
		},
		Projection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{
					Name: "searchable",
					Tags: []string{"service_id", "service_instance_id", "trace_id"},
				},
			},
		},
	}
	if newTagFamily != "" {
		req.Projection.TagFamilies = append(req.Projection.TagFamilies, &modelv1.TagProjection_TagFamily{
			Name: newTagFamily,
			Tags: newTag,
		})
	} else {
		req.Projection.TagFamilies[0].Tags = append(req.Projection.TagFamilies[0].Tags, newTag...)
	}
	var resp *streamv1.QueryResponse
	Eventually(func() bool {
		feat, err := svcs.pipeline.Publish(context.Background(), data.TopicStreamQuery, bus.NewMessage(bus.MessageID(time.Now().UnixNano()), req))
		Expect(err).ShouldNot(HaveOccurred())
		msg, err := feat.Get()
		Expect(err).ShouldNot(HaveOccurred())
		data := msg.Data()
		switch d := data.(type) {
		case *streamv1.QueryResponse:
			if len(d.Elements) != expectedSize {
				GinkgoWriter.Printf("actual: %s", d.Elements)
				return false
			}
			resp = d
			return true
		case *common.Error:
			Fail(d.Error())
		default:
			Fail("unexpected data type")
		}
		return false
	}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
	return resp.Elements
}
