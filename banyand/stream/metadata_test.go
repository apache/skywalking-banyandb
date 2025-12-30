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
					expectedValues := map[string]int{"test1": 0, "test2": 0, "test3": 0}
					for i := size; i < size*2; i++ {
						Expect(dp[i].TagFamilies[0].Tags[3].Key).Should(Equal("new_tag"))
						val := dp[i].TagFamilies[0].Tags[3].Value.GetStr().Value
						Expect(val).Should(HavePrefix("test"))
						expectedValues[val]++
					}
					Expect(expectedValues["test1"]).Should(Equal(1))
					Expect(expectedValues["test2"]).Should(Equal(1))
					Expect(expectedValues["test3"]).Should(Equal(1))
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

					expectedValues := map[string]int{"test1": 0, "test2": 0, "test3": 0}
					for i := size; i < size*2; i++ {
						Expect(dp[i].TagFamilies[1].Tags[0].Key).Should(Equal("new_tag"))
						val := dp[i].TagFamilies[1].Tags[0].Value.GetStr().Value
						Expect(val).Should(HavePrefix("test"))
						expectedValues[val]++
					}

					Expect(expectedValues["test1"]).Should(Equal(1))
					Expect(expectedValues["test2"]).Should(Equal(1))
					Expect(expectedValues["test3"]).Should(Equal(1))
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
		entityValues := make([]*modelv1.TagValue, 0)
		for j := 0; j < 3; j++ {
			entityValues = append(entityValues, &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity" + iStr + strconv.Itoa(j)}}})
		}
		bp.Publish(context.TODO(), data.TopicStreamWrite, bus.NewMessage(bus.MessageID(i), &streamv1.InternalWriteRequest{
			EntityValues: entityValues,
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
				GinkgoWriter.Printf("expected: %d actual: %d \n", expectedSize, len(d.Elements))
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

var _ = Describe("Schema Change", func() {
	var svcs *services
	var deferFn func()
	var goods []gleak.Goroutine
	var groupName string
	var groupCounter int

	BeforeEach(func() {
		svcs, deferFn = setUp()
		goods = gleak.Goroutines()
		Eventually(func() bool {
			_, ok := svcs.stream.LoadGroup("default")
			return ok
		}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())

		groupCounter++
		groupName = "test-schema-change-" + strconv.Itoa(groupCounter)
		err := svcs.metadataService.GroupRegistry().CreateGroup(context.TODO(), &commonv1.Group{
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
		})
		Expect(err).ShouldNot(HaveOccurred())
		Eventually(func() bool {
			_, ok := svcs.stream.LoadGroup(groupName)
			return ok
		}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
	})

	AfterEach(func() {
		_, _ = svcs.metadataService.GroupRegistry().DeleteGroup(context.TODO(), groupName)
		deferFn()
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	Context("Stream schema with deleted tag", func() {
		It("querying data should succeed after a tag is deleted", func() {
			streamName := "schema_change_deleted_tag"
			now := timestamp.NowMilli()

			env := setupSchemaChangeStream(svcs, streamName, groupName, streamSetupOptions{withExtraTag: true})
			writeSchemaChangeData(svcs, streamName, groupName, now.Add(-2*time.Hour), 5, writeDataOptions{extraTag: extraTagInt})
			deleteExtraTag(svcs, streamName, groupName)
			writeSchemaChangeData(svcs, streamName, groupName, now.Add(-1*time.Hour), 3, writeDataOptions{})

			Eventually(func(innerGm Gomega) {
				elements := querySchemaChangeData(svcs, streamName, groupName, now.Add(-3*time.Hour), now,
					[]string{"trace_id", "service_id", "duration"}, nil)
				innerGm.Expect(elements).To(HaveLen(8))

				for _, elem := range elements {
					for _, tf := range elem.TagFamilies {
						if tf.Name == "searchable" {
							for _, tag := range tf.Tags {
								innerGm.Expect(tag.Key).NotTo(Equal("extra_tag"),
									"deleted tag should not be returned in query results")
							}
						}
					}
				}
			}, flags.EventuallyTimeout).Should(Succeed())

			env.cleanup()
		})
	})

	Context("Stream schema with added tag", func() {
		It("querying data should succeed after a new tag is added", func() {
			streamName := "schema_change_added_tag"
			now := timestamp.NowMilli()

			env := setupSchemaChangeStream(svcs, streamName, groupName, streamSetupOptions{})
			writeSchemaChangeData(svcs, streamName, groupName, now.Add(-2*time.Hour), 5, writeDataOptions{})
			addExtraTag(svcs, streamName, groupName)
			writeSchemaChangeData(svcs, streamName, groupName, now.Add(-1*time.Hour), 3, writeDataOptions{extraTag: extraTagInt})

			Eventually(func(innerGm Gomega) {
				elements := querySchemaChangeData(svcs, streamName, groupName, now.Add(-3*time.Hour), now,
					[]string{"trace_id", "service_id", "duration", "extra_tag"}, nil)
				innerGm.Expect(elements).To(HaveLen(8))

				oldDataCount := 0
				newDataCount := 0
				for _, elem := range elements {
					for _, tf := range elem.TagFamilies {
						if tf.Name == "searchable" {
							for _, tag := range tf.Tags {
								if tag.Key == "extra_tag" {
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
				innerGm.Expect(oldDataCount).To(Equal(5), "old data should have null extra_tag")
				innerGm.Expect(newDataCount).To(Equal(3), "new data should have extra_tag values")
			}, flags.EventuallyTimeout).Should(Succeed())

			env.cleanup()
		})
	})

	Context("Stream schema with changed tag type", func() {
		It("querying data should return null for type-mismatched tags", func() {
			streamName := "schema_change_tag_type"
			now := timestamp.NowMilli()

			env := setupSchemaChangeStream(svcs, streamName, groupName, streamSetupOptions{withExtraTag: true})
			writeSchemaChangeData(svcs, streamName, groupName, now.Add(-2*time.Hour), 5, writeDataOptions{extraTag: extraTagInt})
			changeExtraTagType(svcs, streamName, groupName)
			writeSchemaChangeData(svcs, streamName, groupName, now.Add(-1*time.Hour), 3, writeDataOptions{extraTag: extraTagString, traceIDPrefix: "trace_new_"})

			Eventually(func(innerGm Gomega) {
				elements := querySchemaChangeData(svcs, streamName, groupName, now.Add(-3*time.Hour), now,
					[]string{"trace_id", "service_id", "duration", "extra_tag"}, nil)
				innerGm.Expect(elements).To(HaveLen(8))

				nullCount := 0
				stringCount := 0
				for _, elem := range elements {
					for _, tf := range elem.TagFamilies {
						if tf.Name == "searchable" {
							for _, tag := range tf.Tags {
								if tag.Key == "extra_tag" {
									switch tag.Value.GetValue().(type) {
									case *modelv1.TagValue_Null:
										nullCount++
									case *modelv1.TagValue_Str:
										stringCount++
									}
								}
							}
						}
					}
				}
				innerGm.Expect(nullCount).To(Equal(5), "old data with INT type should return null after schema changed to STRING")
				innerGm.Expect(stringCount).To(Equal(3), "new data should have STRING extra_tag values")
			}, flags.EventuallyTimeout).Should(Succeed())

			env.cleanup()
		})
	})

	Context("Stream schema with deleted tag in query", func() {
		It("querying data should fail if the condition includes a deleted tag", func() {
			streamName := "schema_change_filter_deleted"
			now := timestamp.NowMilli()

			env := setupSchemaChangeStream(svcs, streamName, groupName, streamSetupOptions{withExtraTag: true})
			writeSchemaChangeData(svcs, streamName, groupName, now.Add(-2*time.Hour), 5, writeDataOptions{extraTag: extraTagInt})
			deleteExtraTag(svcs, streamName, groupName)
			writeSchemaChangeData(svcs, streamName, groupName, now.Add(-1*time.Hour), 3, writeDataOptions{})

			Eventually(func(innerGm Gomega) {
				err := queryWithDeletedTagCondition(svcs, streamName, groupName, now)
				innerGm.Expect(err).To(HaveOccurred())
				innerGm.Expect(err.Error()).To(ContainSubstring("extra_tag"))
			}, flags.EventuallyTimeout).Should(Succeed())

			env.cleanup()
		})

		It("querying data should fail if the projection includes a deleted tag", func() {
			streamName := "schema_change_projection_deleted"
			now := timestamp.NowMilli()

			env := setupSchemaChangeStream(svcs, streamName, groupName, streamSetupOptions{withExtraTag: true})
			writeSchemaChangeData(svcs, streamName, groupName, now.Add(-2*time.Hour), 5, writeDataOptions{extraTag: extraTagInt})
			deleteExtraTag(svcs, streamName, groupName)
			writeSchemaChangeData(svcs, streamName, groupName, now.Add(-1*time.Hour), 3, writeDataOptions{})

			Eventually(func(innerGm Gomega) {
				err := queryWithDeletedTagProjection(svcs, streamName, groupName, now)
				innerGm.Expect(err).To(HaveOccurred())
				innerGm.Expect(err.Error()).To(ContainSubstring("extra_tag"))
			}, flags.EventuallyTimeout).Should(Succeed())

			env.cleanup()
		})
	})

	Context("Stream schema with deleted tag family", func() {
		It("querying data should succeed after a tag family is deleted", func() {
			streamName := "schema_change_deleted_family"
			now := timestamp.NowMilli()

			env := setupSchemaChangeStream(svcs, streamName, groupName, streamSetupOptions{withExtraFamily: true})
			writeSchemaChangeData(svcs, streamName, groupName, now.Add(-2*time.Hour), 5, writeDataOptions{withExtraFamily: true})
			deleteExtraTagFamily(svcs, streamName, groupName)
			writeSchemaChangeData(svcs, streamName, groupName, now.Add(-1*time.Hour), 3, writeDataOptions{traceIDPrefix: "trace_new_", elementIDOffset: 5})

			Eventually(func(innerGm Gomega) {
				elements := querySchemaChangeData(svcs, streamName, groupName, now.Add(-3*time.Hour), now,
					[]string{"trace_id", "service_id", "duration"}, nil)
				innerGm.Expect(elements).To(HaveLen(8))

				for _, elem := range elements {
					for _, tf := range elem.TagFamilies {
						innerGm.Expect(tf.Name).NotTo(Equal("extra"),
							"deleted tag family should not be returned in query results")
					}
				}
			}, flags.EventuallyTimeout).Should(Succeed())

			env.cleanup()
		})
	})

	Context("Stream schema change restrictions", func() {
		It("updating entity should fail", func() {
			ctx := context.TODO()
			streamName := "schema_change_entity"
			env := setupSchemaChangeStream(svcs, streamName, groupName, streamSetupOptions{})

			streamSchema, err := svcs.metadataService.StreamRegistry().GetStream(ctx, &commonv1.Metadata{
				Name:  streamName,
				Group: groupName,
			})
			Expect(err).ShouldNot(HaveOccurred())

			streamSchema.Entity = &databasev1.Entity{
				TagNames: []string{"trace_id"},
			}
			_, err = svcs.metadataService.StreamRegistry().UpdateStream(ctx, streamSchema)
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("entity is different"))

			env.cleanup()
		})

		It("deleting entity tag should fail", func() {
			ctx := context.TODO()
			streamName := "schema_delete_entity_tag"
			env := setupSchemaChangeStream(svcs, streamName, groupName, streamSetupOptions{})

			streamSchema, err := svcs.metadataService.StreamRegistry().GetStream(ctx, &commonv1.Metadata{
				Name:  streamName,
				Group: groupName,
			})
			Expect(err).ShouldNot(HaveOccurred())

			streamSchema.TagFamilies[1].Tags = []*databasev1.TagSpec{
				{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
			}
			_, err = svcs.metadataService.StreamRegistry().UpdateStream(ctx, streamSchema)
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot delete entity tag"))

			env.cleanup()
		})
	})
})

type extraTagType int

const (
	extraTagNone extraTagType = iota
	extraTagInt
	extraTagString
)

type schemaChangeEnv struct {
	cleanup func()
}

type streamSetupOptions struct {
	withExtraTag    bool
	withExtraFamily bool
}

func setupSchemaChangeStream(svcs *services, streamName, groupName string, opts streamSetupOptions) *schemaChangeEnv {
	ctx := context.TODO()
	searchableTags := []*databasev1.TagSpec{
		{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
	}
	if opts.withExtraTag {
		searchableTags = append(searchableTags, &databasev1.TagSpec{Name: "extra_tag", Type: databasev1.TagType_TAG_TYPE_INT})
	}

	tagFamilies := []*databasev1.TagFamilySpec{
		{
			Name: "data",
			Tags: []*databasev1.TagSpec{
				{Name: "data_binary", Type: databasev1.TagType_TAG_TYPE_DATA_BINARY},
			},
		},
		{
			Name: "searchable",
			Tags: searchableTags,
		},
	}
	if opts.withExtraFamily {
		tagFamilies = append(tagFamilies, &databasev1.TagFamilySpec{
			Name: "extra",
			Tags: []*databasev1.TagSpec{
				{Name: "extra_tag1", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "extra_tag2", Type: databasev1.TagType_TAG_TYPE_INT},
			},
		})
	}

	initialStream := &databasev1.Stream{
		Metadata: &commonv1.Metadata{
			Name:  streamName,
			Group: groupName,
		},
		TagFamilies: tagFamilies,
		Entity: &databasev1.Entity{
			TagNames: []string{"service_id"},
		},
	}
	_, err := svcs.metadataService.StreamRegistry().CreateStream(ctx, initialStream)
	Expect(err).ShouldNot(HaveOccurred())
	Eventually(func() bool {
		_, err := svcs.stream.Stream(&commonv1.Metadata{
			Name:  streamName,
			Group: groupName,
		})
		return err == nil
	}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())

	return &schemaChangeEnv{
		cleanup: func() {
			_, _ = svcs.metadataService.StreamRegistry().DeleteStream(ctx, &commonv1.Metadata{
				Name:  streamName,
				Group: groupName,
			})
		},
	}
}

func updateStreamSchema(svcs *services, streamName, groupName string, updateFn func(*databasev1.Stream)) {
	ctx := context.TODO()
	streamSchema, err := svcs.metadataService.StreamRegistry().GetStream(ctx, &commonv1.Metadata{
		Name:  streamName,
		Group: groupName,
	})
	Expect(err).ShouldNot(HaveOccurred())
	updateFn(streamSchema)
	_, err = svcs.metadataService.StreamRegistry().UpdateStream(ctx, streamSchema)
	Expect(err).ShouldNot(HaveOccurred())
	time.Sleep(2 * time.Second)
}

func deleteExtraTag(svcs *services, streamName, groupName string) {
	updateStreamSchema(svcs, streamName, groupName, func(s *databasev1.Stream) {
		s.TagFamilies[1].Tags = []*databasev1.TagSpec{
			{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
		}
	})
}

func addExtraTag(svcs *services, streamName, groupName string) {
	updateStreamSchema(svcs, streamName, groupName, func(s *databasev1.Stream) {
		s.TagFamilies[1].Tags = append(s.TagFamilies[1].Tags,
			&databasev1.TagSpec{Name: "extra_tag", Type: databasev1.TagType_TAG_TYPE_INT})
	})
}

func changeExtraTagType(svcs *services, streamName, groupName string) {
	updateStreamSchema(svcs, streamName, groupName, func(s *databasev1.Stream) {
		s.TagFamilies[1].Tags = []*databasev1.TagSpec{
			{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
			{Name: "extra_tag", Type: databasev1.TagType_TAG_TYPE_STRING},
		}
	})
}

func deleteExtraTagFamily(svcs *services, streamName, groupName string) {
	updateStreamSchema(svcs, streamName, groupName, func(s *databasev1.Stream) {
		s.TagFamilies = []*databasev1.TagFamilySpec{
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
		}
	})
}

func executeQuery(svcs *services, req *streamv1.QueryRequest) error {
	feat, err := svcs.pipeline.Publish(context.Background(), data.TopicStreamQuery, bus.NewMessage(bus.MessageID(time.Now().UnixNano()), req))
	if err != nil {
		return err
	}
	msg, err := feat.Get()
	if err != nil {
		return err
	}
	if e, ok := msg.Data().(*common.Error); ok {
		return e
	}
	return nil
}

func queryWithDeletedTagCondition(svcs *services, streamName, groupName string, now time.Time) error {
	return executeQuery(svcs, &streamv1.QueryRequest{
		Groups: []string{groupName},
		Name:   streamName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(now.Add(-3 * time.Hour)),
			End:   timestamppb.New(now),
		},
		Criteria: &modelv1.Criteria{
			Exp: &modelv1.Criteria_Condition{
				Condition: &modelv1.Condition{
					Name: "extra_tag",
					Op:   modelv1.Condition_BINARY_OP_EQ,
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 200}},
					},
				},
			},
		},
		Projection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: "searchable", Tags: []string{"trace_id", "service_id"}},
			},
		},
	})
}

func queryWithDeletedTagProjection(svcs *services, streamName, groupName string, now time.Time) error {
	return executeQuery(svcs, &streamv1.QueryRequest{
		Groups: []string{groupName},
		Name:   streamName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(now.Add(-3 * time.Hour)),
			End:   timestamppb.New(now),
		},
		Projection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: "searchable", Tags: []string{"trace_id", "service_id", "extra_tag"}},
			},
		},
	})
}

type writeDataOptions struct {
	traceIDPrefix   string
	extraTag        extraTagType
	withExtraFamily bool
	elementIDOffset int
}

func writeSchemaChangeData(svcs *services, name, group string, baseTime time.Time, count int, opts writeDataOptions) {
	bp := svcs.pipeline.NewBatchPublisher(5 * time.Second)
	defer bp.Close()
	interval := 500 * time.Millisecond

	tracePrefix := opts.traceIDPrefix
	if tracePrefix == "" {
		tracePrefix = "trace_"
	}

	for i := 0; i < count; i++ {
		searchableTags := []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: tracePrefix + strconv.Itoa(i)}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "service_1"}}},
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(100 * (i + 1))}}},
		}
		switch opts.extraTag {
		case extraTagNone:
			// No extra tag
		case extraTagInt:
			searchableTags = append(searchableTags,
				&modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(200 + i)}}})
		case extraTagString:
			searchableTags = append(searchableTags,
				&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "extra_" + strconv.Itoa(i)}}})
		}

		tagFamilies := []*modelv1.TagFamilyForWrite{
			{Tags: []*modelv1.TagValue{{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte("banyandb")}}}},
			{Tags: searchableTags},
		}
		if opts.withExtraFamily {
			tagFamilies = append(tagFamilies, &modelv1.TagFamilyForWrite{
				Tags: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "extra_value_" + strconv.Itoa(i)}}},
					{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(i * 10)}}},
				},
			})
		}

		req := &streamv1.WriteRequest{
			Metadata: &commonv1.Metadata{Name: name, Group: group},
			Element: &streamv1.ElementValue{
				Timestamp:   timestamppb.New(baseTime.Add(interval * time.Duration(i))),
				ElementId:   strconv.Itoa(opts.elementIDOffset + i),
				TagFamilies: tagFamilies,
			},
		}
		bp.Publish(context.TODO(), data.TopicStreamWrite, bus.NewMessage(bus.MessageID(time.Now().UnixNano()+int64(i)), &streamv1.InternalWriteRequest{
			EntityValues: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "service_1"}}}},
			Request:      req,
		}))
	}
}

func querySchemaChangeData(svcs *services, name, group string, begin, end time.Time, tags []string, _ *modelv1.Criteria) []*streamv1.Element {
	req := &streamv1.QueryRequest{
		Groups: []string{group},
		Name:   name,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(begin),
			End:   timestamppb.New(end),
		},
		Projection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{
					Name: "searchable",
					Tags: tags,
				},
			},
		},
	}
	var resp *streamv1.QueryResponse
	Eventually(func() bool {
		feat, err := svcs.pipeline.Publish(context.Background(), data.TopicStreamQuery, bus.NewMessage(bus.MessageID(time.Now().UnixNano()), req))
		Expect(err).ShouldNot(HaveOccurred())
		msg, err := feat.Get()
		Expect(err).ShouldNot(HaveOccurred())
		respData := msg.Data()
		switch d := respData.(type) {
		case *streamv1.QueryResponse:
			resp = d
			return true
		case *common.Error:
			GinkgoWriter.Printf("query error: %s\n", d.Error())
			return false
		default:
			Fail("unexpected data type")
		}
		return false
	}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
	return resp.Elements
}
