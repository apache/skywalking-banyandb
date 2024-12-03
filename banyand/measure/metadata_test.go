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
				writeData(svcs, size, nil, nil)
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
					writeData(svcs, size, []string{"test1", "test2", "test3"}, nil)
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
					writeData(svcs, size, nil, []int{1, 2, 3})
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
					Eventually(func() bool {
						writeData(svcs, size, []string{"test1", "test2", "test3"}, nil)
						dp := queryAllMeasurements(svcs, size*2, []string{"new_tag"}, nil)
						for i := 0; i < size; i++ {
							if dp[i].TagFamilies[0].Tags[2].Value.GetStr() == nil {
								GinkgoWriter.Printf("actual: %s", dp[i].TagFamilies[0].Tags[2])
								return false
							}
							Expect(dp[i].TagFamilies[0].Tags[2].Key).Should(Equal("new_tag"))
							Expect(dp[i].TagFamilies[0].Tags[2].Value.GetStr().Value).Should(Equal("test" + strconv.Itoa(i+1)))
						}
						for i := size; i < size*2; i++ {
							Expect(dp[i].TagFamilies[0].Tags[2].Key).Should(Equal("new_tag"))
							if dp[i].TagFamilies[0].Tags[2].Value.GetStr() == nil {
								GinkgoWriter.Printf("actual: %s", dp[i].TagFamilies[0].Tags[2])
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

func writeData(svcs *services, expectedSize int, newTag []string, newFields []int) {
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
				Timestamp: timestamppb.New(timestamp.NowMilli().Add(time.Millisecond)),
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
		case common.Error:
			Fail(d.Msg())
		default:
			Fail("unexpected data type")
		}
		return false
	}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
	return resp.DataPoints
}
