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

package measure_test

import (
	"context"
	"errors"
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
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
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
			_, ok := svcs.measure.LoadGroup("sw_metric")
			return ok
		}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
	})

	AfterEach(func() {
		deferFn()
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	Context("Manage group", func() {
		It("should close the group", func() {
			deleted, err := svcs.metadataService.GroupRegistry().DeleteGroup(context.TODO(), "sw_metric")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(deleted).Should(BeTrue())
			Eventually(func() bool {
				_, ok := svcs.measure.LoadGroup("sw_metric")
				return ok
			}).WithTimeout(flags.EventuallyTimeout).Should(BeFalse())
		})

		It("should add shards", func() {
			groupSchema, err := svcs.metadataService.GroupRegistry().GetGroup(context.TODO(), "sw_metric")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(groupSchema).ShouldNot(BeNil())
			groupSchema.ResourceOpts.ShardNum = 4

			Expect(svcs.metadataService.GroupRegistry().UpdateGroup(context.TODO(), groupSchema)).Should(Succeed())

			Eventually(func() bool {
				group, ok := svcs.measure.LoadGroup("sw_metric")
				if !ok {
					return false
				}
				return group.GetSchema().GetResourceOpts().GetShardNum() == 4
			}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
		})
	})

	Context("Manage measure", func() {
		It("should pass smoke test", func() {
			Eventually(func() bool {
				_, err := svcs.measure.Measure(&commonv1.Metadata{
					Name:  "service_cpm_minute",
					Group: "sw_metric",
				})
				return err == nil
			}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
		})
		It("should close the measure", func() {
			deleted, err := svcs.metadataService.MeasureRegistry().DeleteMeasure(context.TODO(), &commonv1.Metadata{
				Name:  "service_cpm_minute",
				Group: "sw_metric",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(deleted).Should(BeTrue())
			Eventually(func() error {
				_, err := svcs.measure.Measure(&commonv1.Metadata{
					Name:  "service_cpm_minute",
					Group: "sw_metric",
				})
				return err
			}).WithTimeout(flags.EventuallyTimeout).Should(MatchError(measure.ErrMeasureNotExist))
		})

		Context("Add tags and fields to the measure", func() {
			var measureSchema *databasev1.Measure
			size := 3

			BeforeEach(func() {
				var err error
				measureSchema, err = svcs.metadataService.MeasureRegistry().GetMeasure(context.TODO(), &commonv1.Metadata{
					Name:  "service_cpm_minute",
					Group: "sw_metric",
				})
				Expect(err).ShouldNot(HaveOccurred())
				Expect(measureSchema).ShouldNot(BeNil())
				writeData(timestamp.NowMilli(), svcs, size, nil, nil)
				_ = queryAllMeasurements(svcs, size, nil, nil)

				measureSchema.TagFamilies[0].Tags = append(measureSchema.TagFamilies[0].Tags, &databasev1.TagSpec{
					Name: "new_tag",
					Type: databasev1.TagType_TAG_TYPE_STRING,
				})
				measureSchema.Fields = append(measureSchema.Fields, &databasev1.FieldSpec{
					Name:              "new_field",
					FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
					CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
					EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
				})

				modRevision, err := svcs.metadataService.MeasureRegistry().UpdateMeasure(context.TODO(), measureSchema)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(modRevision).ShouldNot(BeZero())

				Eventually(func() bool {
					val, err := svcs.measure.Measure(&commonv1.Metadata{
						Name:  "service_cpm_minute",
						Group: "sw_metric",
					})
					if err != nil {
						return false
					}

					return len(val.GetSchema().TagFamilies[0].Tags) == 3 && len(val.GetSchema().Fields) == 3
				}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
			})

			When("a tag in the data file", func() {
				It("returns nil for the new added tags", func() {
					dp := queryAllMeasurements(svcs, size, []string{"new_tag"}, nil)
					for i := range dp {
						Expect(dp[i].TagFamilies[0].Tags[2].Key).Should(Equal("new_tag"))
						Expect(dp[i].TagFamilies[0].Tags[2].Value.GetValue()).Should(BeAssignableToTypeOf(&modelv1.TagValue_Null{}))
					}
				})
				It("get new values for the new added tags", func() {
					writeData(timestamp.NowMilli(), svcs, size, []string{"test1", "test2", "test3"}, nil)
					dp := queryAllMeasurements(svcs, size*2, []string{"new_tag"}, nil)
					for i := 0; i < size; i++ {
						Expect(dp[i].TagFamilies[0].Tags[2].Key).Should(Equal("new_tag"))
						Expect(dp[i].TagFamilies[0].Tags[2].Value.GetValue()).Should(BeAssignableToTypeOf(&modelv1.TagValue_Null{}))
					}
					for i := size; i < size*2; i++ {
						Expect(dp[i].TagFamilies[0].Tags[2].Key).Should(Equal("new_tag"))
						Expect(dp[i].TagFamilies[0].Tags[2].Value.GetStr().Value).Should(Equal("test" + strconv.Itoa(i%3+1)))
					}
				})
			})
			When("a field in the data file", func() {
				It("returns nil for the new added fields", func() {
					dp := queryAllMeasurements(svcs, size, nil, []string{"new_field"})
					for i := range dp {
						Expect(dp[i].Fields[2].Name).Should(Equal("new_field"))
						Expect(dp[i].Fields[2].Value.GetValue()).Should(BeAssignableToTypeOf(&modelv1.FieldValue_Null{}))
					}
				})
				It("get new values for the new added fields", func() {
					writeData(timestamp.NowMilli(), svcs, size, nil, []int{1, 2, 3})
					dp := queryAllMeasurements(svcs, size*2, nil, []string{"new_field"})
					for i := 0; i < size; i++ {
						Expect(dp[i].Fields[2].Name).Should(Equal("new_field"))
						Expect(dp[i].Fields[2].Value.GetValue()).Should(BeAssignableToTypeOf(&modelv1.FieldValue_Null{}))
					}
					for i := size; i < size*2; i++ {
						Expect(dp[i].Fields[2].Name).Should(Equal("new_field"))
						Expect(dp[i].Fields[2].Value.GetInt().Value).Should(Equal(int64(i%3 + 1)))
					}
				})
			})
			When("a tag in the index file", func() {
				BeforeEach(func() {
					indexRule := &databasev1.IndexRule{
						Metadata: &commonv1.Metadata{
							Name:  "new_tag",
							Group: "sw_metric",
						},
						Tags: []string{"new_tag"},
						Type: databasev1.IndexRule_TYPE_INVERTED,
					}
					err := svcs.metadataService.IndexRuleRegistry().CreateIndexRule(context.TODO(), indexRule)
					Expect(err).ShouldNot(HaveOccurred())
					indexRuleBinding := &databasev1.IndexRuleBinding{
						Metadata: &commonv1.Metadata{
							Name:  "service_cpm_minute_new_tag",
							Group: "sw_metric",
						},
						Subject: &databasev1.Subject{
							Name:    "service_cpm_minute",
							Catalog: commonv1.Catalog_CATALOG_MEASURE,
						},
						Rules:    []string{"new_tag"},
						BeginAt:  timestamppb.New(timestamp.NowMilli().Add(-time.Hour)),
						ExpireAt: timestamppb.New(timestamp.NowMilli().Add(time.Hour)),
					}
					err = svcs.metadataService.IndexRuleBindingRegistry().CreateIndexRuleBinding(context.TODO(), indexRuleBinding)
					Expect(err).ShouldNot(HaveOccurred())

					Eventually(func() bool {
						val, err := svcs.measure.Measure(&commonv1.Metadata{
							Name:  "service_cpm_minute",
							Group: "sw_metric",
						})
						if err != nil {
							return false
						}
						rr := val.GetIndexRules()
						for i := range rr {
							if rr[i].Metadata.Name == "new_tag" {
								return true
							}
						}
						return false
					}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
				})

				It("returns nil for the new added tags", func() {
					dp := queryAllMeasurements(svcs, size, []string{"new_tag"}, nil)
					for i := range dp {
						Expect(dp[i].TagFamilies[0].Tags[2].Key).Should(Equal("new_tag"))
						Expect(dp[i].TagFamilies[0].Tags[2].Value.GetValue()).Should(BeAssignableToTypeOf(&modelv1.TagValue_Null{}))
					}
				})

				It("get new values for the new added tags", func() {
					ts := timestamp.NowMilli()
					Eventually(func() bool {
						writeData(ts, svcs, size, []string{"test1", "test2", "test3"}, nil)
						dp := queryAllMeasurements(svcs, size*2, []string{"new_tag"}, nil)
						for i := 0; i < size; i++ {
							if dp[i].TagFamilies[0].Tags[2].Value.GetStr() == nil {
								GinkgoWriter.Printf("actual new: %s", dp[i].TagFamilies[0].Tags[2])
								return false
							}
							Expect(dp[i].TagFamilies[0].Tags[2].Key).Should(Equal("new_tag"))
							Expect(dp[i].TagFamilies[0].Tags[2].Value.GetStr().Value).Should(Equal("test" + strconv.Itoa(i+1)))
						}
						for i := size; i < size*2; i++ {
							Expect(dp[i].TagFamilies[0].Tags[2].Key).Should(Equal("new_tag"))
							if dp[i].TagFamilies[0].Tags[2].Value.GetStr() == nil {
								GinkgoWriter.Printf("actual old: %s", dp[i].TagFamilies[0].Tags[2])
								return false
							}
							Expect(dp[i].TagFamilies[0].Tags[2].Value.GetStr().Value).Should(Equal("test" + strconv.Itoa(i%3+1)))
						}
						return true
					}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
				})
			})
		})
	})
})

func writeData(ts time.Time, svcs *services, expectedSize int, newTag []string, newFields []int) {
	bp := svcs.pipeline.NewBatchPublisher(5 * time.Second)
	defer bp.Close()
	for i := 0; i < expectedSize; i++ {
		iStr := strconv.Itoa(i)
		req := &measurev1.WriteRequest{
			Metadata: &commonv1.Metadata{
				Name:  "service_cpm_minute",
				Group: "sw_metric",
			},
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(ts.Add(time.Millisecond)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{
					Tags: []*modelv1.TagValue{{
						Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "id" + iStr}},
					}, {
						Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity" + iStr}},
					}},
				}},
				Fields: []*modelv1.FieldValue{{
					Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(i + 100)}},
				}, {
					Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(i)}},
				}},
			},
		}
		if i < len(newTag) {
			req.DataPoint.TagFamilies[0].Tags = append(req.DataPoint.TagFamilies[0].Tags, &modelv1.TagValue{
				Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: newTag[i]}},
			})
		}
		if i < len(newFields) {
			req.DataPoint.Fields = append(req.DataPoint.Fields, &modelv1.FieldValue{
				Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(newFields[i])}},
			})
		}
		bp.Publish(context.TODO(), data.TopicMeasureWrite, bus.NewMessage(bus.MessageID(i), &measurev1.InternalWriteRequest{
			EntityValues: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity" + iStr}}}},
			Request:      req,
		}))
	}
}

func queryAllMeasurements(svcs *services, expectedSize int, newTag []string, newFields []string) []*measurev1.DataPoint {
	req := &measurev1.QueryRequest{
		Groups: []string{"sw_metric"},
		Name:   "service_cpm_minute",
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(timestamp.NowMilli().Add(-time.Hour)),
			End:   timestamppb.New(timestamp.NowMilli().Add(time.Hour)),
		},
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{
					Name: "default",
					Tags: append([]string{"id", "entity_id"}, newTag...),
				},
			},
		},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{
			Names: append([]string{"total", "value"}, newFields...),
		},
	}
	var resp *measurev1.QueryResponse
	Eventually(func() bool {
		feat, err := svcs.pipeline.Publish(context.Background(), data.TopicMeasureQuery, bus.NewMessage(bus.MessageID(time.Now().UnixNano()), req))
		Expect(err).ShouldNot(HaveOccurred())
		msg, err := feat.Get()
		Expect(err).ShouldNot(HaveOccurred())
		data := msg.Data()
		switch d := data.(type) {
		case *measurev1.QueryResponse:
			if len(d.DataPoints) != expectedSize {
				GinkgoWriter.Printf("actual: %s", d.DataPoints)
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
	return resp.DataPoints
}

var _ = Describe("Schema Change", func() {
	var svcs *services
	var deferFn func()
	var goods []gleak.Goroutine
	const groupName = "sw_metric"

	BeforeEach(func() {
		svcs, deferFn = setUp()
		goods = gleak.Goroutines()
		Eventually(func() bool {
			_, ok := svcs.measure.LoadGroup(groupName)
			return ok
		}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
	})

	AfterEach(func() {
		deferFn()
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	Context("Measure schema with deleted tag", func() {
		It("querying data should succeed after a tag is deleted", func() {
			measureName := "schema_change_deleted_tag"
			now := timestamp.NowMilli()

			env := setupSchemaChangeMeasure(svcs, measureName, groupName, measureSetupOptions{withExtraTag: true})
			writeSchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-2*time.Hour), 5, measureWriteDataOptions{withExtraTag: true})
			deleteExtraMeasureTag(svcs, measureName, groupName)
			writeSchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-1*time.Hour), 3, measureWriteDataOptions{entityIDPrefix: "entity_new_"})

			Eventually(func(innerGm Gomega) {
				dataPoints := querySchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-3*time.Hour), now,
					[]string{"id", "entity_id"}, []string{"total"})
				innerGm.Expect(dataPoints).To(HaveLen(8))

				for _, dp := range dataPoints {
					for _, tf := range dp.TagFamilies {
						if tf.Name == "default" {
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

	Context("Measure schema with added tag", func() {
		It("querying data should succeed after a new tag is added", func() {
			measureName := "schema_change_added_tag"
			now := timestamp.NowMilli()

			env := setupSchemaChangeMeasure(svcs, measureName, groupName, measureSetupOptions{})
			writeSchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-2*time.Hour), 5, measureWriteDataOptions{})
			addExtraMeasureTag(svcs, measureName, groupName)
			writeSchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-1*time.Hour), 3, measureWriteDataOptions{withExtraTag: true, entityIDPrefix: "entity_new_"})

			Eventually(func(innerGm Gomega) {
				dataPoints := querySchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-3*time.Hour), now,
					[]string{"id", "entity_id", "extra_tag"}, []string{"total"})
				innerGm.Expect(dataPoints).To(HaveLen(8))

				oldDataCount := 0
				newDataCount := 0
				for _, dp := range dataPoints {
					for _, tf := range dp.TagFamilies {
						if tf.Name == "default" {
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

	Context("Measure schema with changed tag type", func() {
		It("querying data should return null for type-mismatched tags", func() {
			measureName := "schema_change_tag_type"
			now := timestamp.NowMilli()

			env := setupSchemaChangeMeasure(svcs, measureName, groupName, measureSetupOptions{withExtraTag: true})
			writeSchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-2*time.Hour), 5, measureWriteDataOptions{withExtraTag: true})
			changeExtraMeasureTagType(svcs, measureName, groupName)
			writeSchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-1*time.Hour), 3, measureWriteDataOptions{withExtraTagString: true, entityIDPrefix: "entity_new_"})

			Eventually(func(innerGm Gomega) {
				dataPoints := querySchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-3*time.Hour), now,
					[]string{"id", "entity_id", "extra_tag"}, []string{"total"})
				innerGm.Expect(dataPoints).To(HaveLen(8))

				nullCount := 0
				stringCount := 0
				for _, dp := range dataPoints {
					for _, tf := range dp.TagFamilies {
						if tf.Name == "default" {
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

	Context("Measure schema with deleted tag family", func() {
		It("querying data should succeed after a tag family is deleted", func() {
			measureName := "schema_change_deleted_family"
			now := timestamp.NowMilli()

			env := setupSchemaChangeMeasure(svcs, measureName, groupName, measureSetupOptions{withExtraFamily: true})
			writeSchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-2*time.Hour), 5, measureWriteDataOptions{withExtraFamily: true})
			deleteExtraMeasureTagFamily(svcs, measureName, groupName)
			writeSchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-1*time.Hour), 3, measureWriteDataOptions{entityIDPrefix: "entity_new_"})

			Eventually(func(innerGm Gomega) {
				dataPoints := querySchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-3*time.Hour), now,
					[]string{"id", "entity_id"}, []string{"total"})
				innerGm.Expect(dataPoints).To(HaveLen(8))

				for _, dp := range dataPoints {
					for _, tf := range dp.TagFamilies {
						innerGm.Expect(tf.Name).NotTo(Equal("extra"),
							"deleted tag family should not be returned in query results")
					}
				}
			}, flags.EventuallyTimeout).Should(Succeed())

			env.cleanup()
		})
	})

	Context("Measure schema with deleted field", func() {
		It("querying data should return null for deleted fields", func() {
			measureName := "schema_change_delete_field"
			now := timestamp.NowMilli()

			env := setupSchemaChangeMeasure(svcs, measureName, groupName, measureSetupOptions{withExtraField: true})
			writeSchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-2*time.Hour), 5, measureWriteDataOptions{withExtraField: true})
			deleteExtraField(svcs, measureName, groupName)
			writeSchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-1*time.Hour), 3, measureWriteDataOptions{entityIDPrefix: "entity_new_"})

			Eventually(func(innerGm Gomega) {
				dataPoints := querySchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-3*time.Hour), now,
					[]string{"id", "entity_id"}, []string{"total", "extra_field"})
				innerGm.Expect(dataPoints).To(HaveLen(8))

				nullCount := 0
				for _, dp := range dataPoints {
					for _, field := range dp.Fields {
						if field.Name == "extra_field" {
							if _, isNull := field.Value.GetValue().(*modelv1.FieldValue_Null); isNull {
								nullCount++
							}
						}
					}
				}
				innerGm.Expect(nullCount).To(Equal(8), "all data should return null for deleted field")
			}, flags.EventuallyTimeout).Should(Succeed())

			env.cleanup()
		})
	})

	Context("Measure schema with deleted tag in query", func() {
		It("querying data should fail if the condition includes a deleted tag", func() {
			measureName := "schema_change_filter_deleted"
			now := timestamp.NowMilli()

			env := setupSchemaChangeMeasure(svcs, measureName, groupName, measureSetupOptions{withExtraTag: true})
			writeSchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-2*time.Hour), 5, measureWriteDataOptions{withExtraTag: true})
			deleteExtraMeasureTag(svcs, measureName, groupName)
			writeSchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-1*time.Hour), 3, measureWriteDataOptions{entityIDPrefix: "entity_new_"})

			Eventually(func(innerGm Gomega) {
				err := queryMeasureWithDeletedTagCondition(svcs, measureName, groupName, now)
				innerGm.Expect(err).To(HaveOccurred())
				innerGm.Expect(err.Error()).To(ContainSubstring("extra_tag"))
			}, flags.EventuallyTimeout).Should(Succeed())

			env.cleanup()
		})

		It("querying data should fail if the projection includes a deleted tag", func() {
			measureName := "schema_change_projection_deleted"
			now := timestamp.NowMilli()

			env := setupSchemaChangeMeasure(svcs, measureName, groupName, measureSetupOptions{withExtraTag: true})
			writeSchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-2*time.Hour), 5, measureWriteDataOptions{withExtraTag: true})
			deleteExtraMeasureTag(svcs, measureName, groupName)
			writeSchemaChangeMeasureData(svcs, measureName, groupName, now.Add(-1*time.Hour), 3, measureWriteDataOptions{entityIDPrefix: "entity_new_"})

			Eventually(func(innerGm Gomega) {
				err := queryMeasureWithDeletedTagProjection(svcs, measureName, groupName, now)
				innerGm.Expect(err).To(HaveOccurred())
				innerGm.Expect(err.Error()).To(ContainSubstring("extra_tag"))
			}, flags.EventuallyTimeout).Should(Succeed())

			env.cleanup()
		})
	})

	Context("Measure schema change restrictions", func() {
		It("updating entity should fail", func() {
			ctx := context.TODO()
			measureName := "schema_change_entity"
			env := setupSchemaChangeMeasure(svcs, measureName, groupName, measureSetupOptions{})

			measureSchema, err := svcs.metadataService.MeasureRegistry().GetMeasure(ctx, &commonv1.Metadata{
				Name:  measureName,
				Group: groupName,
			})
			Expect(err).ShouldNot(HaveOccurred())

			measureSchema.Entity = &databasev1.Entity{
				TagNames: []string{"id"},
			}
			_, err = svcs.metadataService.MeasureRegistry().UpdateMeasure(ctx, measureSchema)
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("entity is different"))

			env.cleanup()
		})

		It("deleting entity tag should fail", func() {
			ctx := context.TODO()
			measureName := "schema_delete_entity_tag"
			env := setupSchemaChangeMeasure(svcs, measureName, groupName, measureSetupOptions{})

			measureSchema, err := svcs.metadataService.MeasureRegistry().GetMeasure(ctx, &commonv1.Metadata{
				Name:  measureName,
				Group: groupName,
			})
			Expect(err).ShouldNot(HaveOccurred())

			measureSchema.TagFamilies[0].Tags = []*databasev1.TagSpec{
				{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING},
			}
			_, err = svcs.metadataService.MeasureRegistry().UpdateMeasure(ctx, measureSchema)
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot delete entity tag"))

			env.cleanup()
		})

		It("modifying field definition should fail", func() {
			ctx := context.TODO()
			measureName := "schema_modify_field"
			env := setupSchemaChangeMeasure(svcs, measureName, groupName, measureSetupOptions{})

			measureSchema, err := svcs.metadataService.MeasureRegistry().GetMeasure(ctx, &commonv1.Metadata{
				Name:  measureName,
				Group: groupName,
			})
			Expect(err).ShouldNot(HaveOccurred())

			measureSchema.Fields[0].EncodingMethod = databasev1.EncodingMethod_ENCODING_METHOD_UNSPECIFIED
			_, err = svcs.metadataService.MeasureRegistry().UpdateMeasure(ctx, measureSchema)
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("field"))
			Expect(err.Error()).To(ContainSubstring("is different"))

			env.cleanup()
		})
	})
})

type measureSchemaChangeEnv struct {
	cleanup func()
}

type measureSetupOptions struct {
	withExtraTag    bool
	withExtraField  bool
	withExtraFamily bool
	withInterval    bool
}

func setupSchemaChangeMeasure(svcs *services, measureName, groupName string, opts measureSetupOptions) *measureSchemaChangeEnv {
	ctx := context.TODO()

	tags := []*databasev1.TagSpec{
		{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "entity_id", Type: databasev1.TagType_TAG_TYPE_STRING},
	}
	if opts.withExtraTag {
		tags = append(tags, &databasev1.TagSpec{Name: "extra_tag", Type: databasev1.TagType_TAG_TYPE_INT})
	}

	fields := []*databasev1.FieldSpec{
		{
			Name:              "total",
			FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
			CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
		},
	}
	if opts.withExtraField {
		fields = append(fields, &databasev1.FieldSpec{
			Name:              "extra_field",
			FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
			CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
		})
	}

	tagFamilies := []*databasev1.TagFamilySpec{
		{
			Name: "default",
			Tags: tags,
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

	initialMeasure := &databasev1.Measure{
		Metadata: &commonv1.Metadata{
			Name:  measureName,
			Group: groupName,
		},
		TagFamilies: tagFamilies,
		Fields:      fields,
		Entity: &databasev1.Entity{
			TagNames: []string{"entity_id"},
		},
	}
	if opts.withInterval {
		initialMeasure.Interval = "1m"
	}

	_, err := svcs.metadataService.MeasureRegistry().CreateMeasure(ctx, initialMeasure)
	Expect(err).ShouldNot(HaveOccurred())
	Eventually(func() bool {
		_, err := svcs.measure.Measure(&commonv1.Metadata{
			Name:  measureName,
			Group: groupName,
		})
		return err == nil
	}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())

	return &measureSchemaChangeEnv{
		cleanup: func() {
			_, _ = svcs.metadataService.MeasureRegistry().DeleteMeasure(ctx, &commonv1.Metadata{
				Name:  measureName,
				Group: groupName,
			})
		},
	}
}

func updateMeasureSchema(svcs *services, measureName, groupName string, updateFn func(*databasev1.Measure)) {
	ctx := context.TODO()
	measureSchema, err := svcs.metadataService.MeasureRegistry().GetMeasure(ctx, &commonv1.Metadata{
		Name:  measureName,
		Group: groupName,
	})
	Expect(err).ShouldNot(HaveOccurred())
	updateFn(measureSchema)
	_, err = svcs.metadataService.MeasureRegistry().UpdateMeasure(ctx, measureSchema)
	Expect(err).ShouldNot(HaveOccurred())
	time.Sleep(2 * time.Second)
}

func deleteExtraField(svcs *services, measureName, groupName string) {
	updateMeasureSchema(svcs, measureName, groupName, func(m *databasev1.Measure) {
		m.Fields = []*databasev1.FieldSpec{
			{
				Name:              "total",
				FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
			},
		}
	})
}

func deleteExtraMeasureTag(svcs *services, measureName, groupName string) {
	updateMeasureSchema(svcs, measureName, groupName, func(m *databasev1.Measure) {
		m.TagFamilies[0].Tags = []*databasev1.TagSpec{
			{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "entity_id", Type: databasev1.TagType_TAG_TYPE_STRING},
		}
	})
}

func addExtraMeasureTag(svcs *services, measureName, groupName string) {
	updateMeasureSchema(svcs, measureName, groupName, func(m *databasev1.Measure) {
		m.TagFamilies[0].Tags = append(m.TagFamilies[0].Tags,
			&databasev1.TagSpec{Name: "extra_tag", Type: databasev1.TagType_TAG_TYPE_INT})
	})
}

func changeExtraMeasureTagType(svcs *services, measureName, groupName string) {
	updateMeasureSchema(svcs, measureName, groupName, func(m *databasev1.Measure) {
		m.TagFamilies[0].Tags = []*databasev1.TagSpec{
			{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "entity_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "extra_tag", Type: databasev1.TagType_TAG_TYPE_STRING},
		}
	})
}

func deleteExtraMeasureTagFamily(svcs *services, measureName, groupName string) {
	updateMeasureSchema(svcs, measureName, groupName, func(m *databasev1.Measure) {
		m.TagFamilies = []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "entity_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		}
	})
}

type measureWriteDataOptions struct {
	withExtraTag       bool
	withExtraTagString bool
	withExtraField     bool
	withExtraFamily    bool
	entityIDPrefix     string
}

func writeSchemaChangeMeasureData(svcs *services, name, group string, baseTime time.Time, count int, opts measureWriteDataOptions) {
	bp := svcs.pipeline.NewBatchPublisher(5 * time.Second)
	defer bp.Close()
	interval := 500 * time.Millisecond

	entityPrefix := opts.entityIDPrefix
	if entityPrefix == "" {
		entityPrefix = "entity_"
	}

	for i := 0; i < count; i++ {
		iStr := strconv.Itoa(i)
		tags := []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "id_" + iStr}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityPrefix + iStr}}},
		}
		if opts.withExtraTag {
			tags = append(tags, &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(100 + i)}}})
		}
		if opts.withExtraTagString {
			tags = append(tags, &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "extra_" + iStr}}})
		}

		fields := []*modelv1.FieldValue{
			{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(i + 100)}}},
		}
		if opts.withExtraField {
			fields = append(fields, &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(i * 10)}}})
		}

		tagFamilies := []*modelv1.TagFamilyForWrite{{
			Tags: tags,
		}}
		if opts.withExtraFamily {
			tagFamilies = append(tagFamilies, &modelv1.TagFamilyForWrite{
				Tags: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "extra_value_" + iStr}}},
					{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(i * 10)}}},
				},
			})
		}

		req := &measurev1.WriteRequest{
			Metadata: &commonv1.Metadata{
				Name:  name,
				Group: group,
			},
			DataPoint: &measurev1.DataPointValue{
				Timestamp:   timestamppb.New(baseTime.Add(time.Duration(i) * interval)),
				TagFamilies: tagFamilies,
				Fields:      fields,
			},
		}
		bp.Publish(context.TODO(), data.TopicMeasureWrite, bus.NewMessage(bus.MessageID(i), &measurev1.InternalWriteRequest{
			EntityValues: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityPrefix + iStr}}}},
			Request:      req,
		}))
	}
}

func querySchemaChangeMeasureData(svcs *services, measureName, groupName string, startTime, endTime time.Time,
	tagNames []string, fieldNames []string,
) []*measurev1.DataPoint {
	req := &measurev1.QueryRequest{
		Groups: []string{groupName},
		Name:   measureName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(startTime),
			End:   timestamppb.New(endTime),
		},
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: "default", Tags: tagNames},
			},
		},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{
			Names: fieldNames,
		},
	}

	var resp *measurev1.QueryResponse
	feat, err := svcs.pipeline.Publish(context.Background(), data.TopicMeasureQuery, bus.NewMessage(bus.MessageID(time.Now().UnixNano()), req))
	Expect(err).ShouldNot(HaveOccurred())
	msg, err := feat.Get()
	Expect(err).ShouldNot(HaveOccurred())

	switch d := msg.Data().(type) {
	case *measurev1.QueryResponse:
		resp = d
	case *common.Error:
		Fail(d.Error())
	default:
		Fail("unexpected data type")
	}
	return resp.DataPoints
}

func queryMeasureWithDeletedTagCondition(svcs *services, measureName, groupName string, now time.Time) error {
	req := &measurev1.QueryRequest{
		Groups: []string{groupName},
		Name:   measureName,
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
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: "default", Tags: []string{"id", "entity_id"}},
			},
		},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{
			Names: []string{"total"},
		},
	}

	feat, err := svcs.pipeline.Publish(context.Background(), data.TopicMeasureQuery, bus.NewMessage(bus.MessageID(time.Now().UnixNano()), req))
	if err != nil {
		return err
	}
	msg, err := feat.Get()
	if err != nil {
		return err
	}

	switch d := msg.Data().(type) {
	case *measurev1.QueryResponse:
		return nil
	case *common.Error:
		return errors.New(d.Error())
	default:
		return errors.New("unexpected data type")
	}
}

func queryMeasureWithDeletedTagProjection(svcs *services, measureName, groupName string, now time.Time) error {
	req := &measurev1.QueryRequest{
		Groups: []string{groupName},
		Name:   measureName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(now.Add(-3 * time.Hour)),
			End:   timestamppb.New(now),
		},
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: "default", Tags: []string{"id", "entity_id", "extra_tag"}},
			},
		},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{
			Names: []string{"total"},
		},
	}

	feat, err := svcs.pipeline.Publish(context.Background(), data.TopicMeasureQuery, bus.NewMessage(bus.MessageID(time.Now().UnixNano()), req))
	if err != nil {
		return err
	}
	msg, err := feat.Get()
	if err != nil {
		return err
	}

	switch d := msg.Data().(type) {
	case *measurev1.QueryResponse:
		return nil
	case *common.Error:
		return errors.New(d.Error())
	default:
		return errors.New("unexpected data type")
	}
}
