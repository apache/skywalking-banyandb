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

package stream

import (
	"bytes"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/golang/protobuf/jsonpb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type shardStruct struct {
	id       common.ShardID
	location []string
	elements []string
}

type shardsForTest []shardStruct

var _ = Describe("Write", func() {
	Context("Select shard", func() {
		var (
			s       *stream
			deferFn func()
		)

		BeforeEach(func() {
			var svcs *services
			svcs, deferFn = setUp()
			var ok bool
			s, ok = svcs.stream.schemaRepo.loadStream(&commonv1.Metadata{
				Name:  "sw",
				Group: "default",
			})
			Expect(ok).To(BeTrue())
		})

		AfterEach(func() {
			deferFn()
		})
		tests := []struct {
			name         string
			entity       tsdb.Entity
			wantShardNum int
			wantErr      bool
		}{
			{
				name:         "all shards",
				wantShardNum: 2,
			},
			{
				name:         "select a shard",
				entity:       tsdb.Entity{tsdb.Entry("webapp_id"), tsdb.Entry("10.0.0.1_id"), convert.Int64ToBytes(0)},
				wantShardNum: 1,
			},
			{
				name:         "select shards",
				entity:       tsdb.Entity{tsdb.Entry("webapp_id"), tsdb.AnyEntry, convert.Int64ToBytes(0)},
				wantShardNum: 2,
			},
		}
		for _, tt := range tests {
			It(tt.name, func() {
				shards, err := s.Shards(tt.entity)
				if tt.wantErr {
					Expect(err).Should(HaveOccurred())
					return
				}
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(shards)).To(Equal(tt.wantShardNum))
			})
		}
	})
	Context("Querying by local indices", Ordered, func() {
		var (
			s       *stream
			now     time.Time
			deferFn func()
		)
		BeforeAll(func() {
			var svcs *services
			svcs, deferFn = setUp()
			var ok bool
			s, ok = svcs.stream.schemaRepo.loadStream(&commonv1.Metadata{
				Name:  "sw",
				Group: "default",
			})
			Expect(ok).To(BeTrue())
			now = setupQueryData("multiple_shards.json", s)
		})
		AfterAll(func() {
			deferFn()
		})
		When("", func() {
			l1 := []string{fmt.Sprintf("series_%d", tsdb.SeriesID(tsdb.Entity{
				tsdb.Entry("sw"),
				tsdb.Entry("webapp_id"),
				tsdb.Entry("10.0.0.5_id"),
				tsdb.Entry(convert.Int64ToBytes(0)),
			})), "data_flow_0"}
			l2 := []string{fmt.Sprintf("series_%d", tsdb.SeriesID(tsdb.Entity{
				tsdb.Entry("sw"),
				tsdb.Entry("webapp_id"),
				tsdb.Entry("10.0.0.1_id"),
				tsdb.Entry(convert.Int64ToBytes(1)),
			})), "data_flow_0"}
			l3 := []string{fmt.Sprintf("series_%d", tsdb.SeriesID(tsdb.Entity{
				tsdb.Entry("sw"),
				tsdb.Entry("webapp_id"),
				tsdb.Entry("10.0.0.3_id"),
				tsdb.Entry(convert.Int64ToBytes(1)),
			})), "data_flow_0"}
			l4 := []string{fmt.Sprintf("series_%d", tsdb.SeriesID(tsdb.Entity{
				tsdb.Entry("sw"),
				tsdb.Entry("webapp_id"),
				tsdb.Entry("10.0.0.1_id"),
				tsdb.Entry(convert.Int64ToBytes(0)),
			})), "data_flow_0"}

			DescribeTable("", func(args queryOpts, want shardsForTest, wantErr bool) {
				got, err := queryData(s, now, args)
				if wantErr {
					Expect(err).Should(HaveOccurred())
					return
				}
				Expect(err).ShouldNot(HaveOccurred())
				sort.SliceStable(got, func(i, j int) bool {
					a := got[i]
					b := got[j]
					if a.id > b.id {
						return false
					}
					for i, al := range a.location {
						bl := b.location[i]
						if bytes.Compare([]byte(al), []byte(bl)) > 0 {
							return false
						}
					}
					return true
				})
				Expect(got).To(Equal(want))
			},
				Entry(
					"all",
					queryOpts{
						entity:   tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
						duration: 1 * time.Hour,
					},
					shardsForTest{
						{
							id:       0,
							location: l1,
							elements: []string{"4"},
						},
						{
							id:       1,
							location: l2,
							elements: []string{"1"},
						},
						{
							id:       1,
							location: l3,
							elements: []string{"2"},
						},
						{
							id:       1,
							location: l4,
							elements: []string{"3", "5"},
						},
					},
					false,
				),
				Entry(
					"time range",
					queryOpts{
						entity:   tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
						offset:   1500 * time.Millisecond,
						duration: 1 * time.Hour,
					},
					shardsForTest{
						{
							id:       0,
							location: l1,
							elements: []string{"4"},
						},
						{
							id:       1,
							location: l2,
						},
						{
							id:       1,
							location: l3,
						},
						{
							id:       1,
							location: l4,
							elements: []string{"5"},
						},
					},
					false,
				),
				Entry(
					"find series by service_id and instance_id",
					queryOpts{
						entity:   tsdb.Entity{tsdb.Entry("webapp_id"), tsdb.Entry("10.0.0.1_id"), tsdb.AnyEntry},
						duration: 1 * time.Hour,
					},
					shardsForTest{
						{
							id:       1,
							location: l2,
							elements: []string{"1"},
						},
						{
							id:       1,
							location: l4,
							elements: []string{"3", "5"},
						},
					},
					false,
				),
				Entry(
					"find a series",
					queryOpts{
						entity:   tsdb.Entity{tsdb.Entry("webapp_id"), tsdb.Entry("10.0.0.1_id"), convert.Int64ToBytes(0)},
						duration: 1 * time.Hour,
					},
					shardsForTest{
						{
							id:       1,
							location: l4,
							elements: []string{"3", "5"},
						},
					},
					false,
				),
				Entry(

					"filter",
					queryOpts{
						entity:   tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
						duration: 1 * time.Hour,
						buildFn: func(builder tsdb.SeekerBuilder) {
							builder.Filter(&databasev1.IndexRule{
								Metadata: &commonv1.Metadata{
									Name:  "endpoint_id",
									Group: "default",
									Id:    4,
								},
								Tags:     []string{"endpoint_id"},
								Type:     databasev1.IndexRule_TYPE_INVERTED,
								Location: databasev1.IndexRule_LOCATION_SERIES,
							}, tsdb.Condition{
								"endpoint_id": []index.ConditionValue{
									{
										Op:     modelv1.Condition_BINARY_OP_EQ,
										Values: [][]byte{[]byte("/home_id")},
									},
								},
							})
						},
					},
					shardsForTest{
						{
							id:       0,
							location: l1,
						},
						{
							id:       1,
							location: l2,
							elements: []string{"1"},
						},
						{
							id:       1,
							location: l3,
						},

						{
							id:       1,
							location: l4,
							elements: []string{"3"},
						},
					},
					false,
				),
				Entry(

					"filter by status_code",
					queryOpts{
						entity:   tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
						duration: 1 * time.Hour,
						buildFn: func(builder tsdb.SeekerBuilder) {
							rule := &databasev1.IndexRule{
								Metadata: &commonv1.Metadata{
									Name:  "status_code",
									Group: "default",
									Id:    5,
								},
								Tags:     []string{"status_code"},
								Type:     databasev1.IndexRule_TYPE_INVERTED,
								Location: databasev1.IndexRule_LOCATION_SERIES,
							}
							builder.Filter(rule, tsdb.Condition{
								"status_code": []index.ConditionValue{
									{
										Op:     modelv1.Condition_BINARY_OP_EQ,
										Values: [][]byte{convert.Int64ToBytes(500)},
									},
								},
							})
						},
					},
					shardsForTest{
						{
							id:       0,
							location: l1,
						},
						{
							id:       1,
							location: l2,
						},
						{
							id:       1,
							location: l3,
						},
						{
							id:       1,
							location: l4,
							elements: []string{"3", "5"},
						},
					},
					false,
				),
				Entry(

					"order by duration",
					queryOpts{
						entity:   tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
						duration: 1 * time.Hour,
						buildFn: func(builder tsdb.SeekerBuilder) {
							builder.OrderByIndex(&databasev1.IndexRule{
								Metadata: &commonv1.Metadata{
									Name:  "duration",
									Group: "default",
									Id:    3,
								},
								Tags:     []string{"duration"},
								Type:     databasev1.IndexRule_TYPE_TREE,
								Location: databasev1.IndexRule_LOCATION_SERIES,
							}, modelv1.Sort_SORT_ASC)
						},
					},
					shardsForTest{
						{
							id:       0,
							location: l1,
							elements: []string{"4"},
						},
						{
							id:       1,
							location: l2,
							elements: []string{"1"},
						},
						{
							id:       1,
							location: l3,
							elements: []string{"2"},
						},

						{
							id:       1,
							location: l4,
							elements: []string{"3", "5"},
						},
					},
					false,
				),
				Entry(

					"filter by duration",
					queryOpts{
						entity:   tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
						duration: 1 * time.Hour,
						buildFn: func(builder tsdb.SeekerBuilder) {
							rule := &databasev1.IndexRule{
								Metadata: &commonv1.Metadata{
									Name:  "duration",
									Group: "default",
									Id:    3,
								},
								Tags:     []string{"duration"},
								Type:     databasev1.IndexRule_TYPE_TREE,
								Location: databasev1.IndexRule_LOCATION_SERIES,
							}
							builder.Filter(rule, tsdb.Condition{
								"duration": []index.ConditionValue{
									{
										Op:     modelv1.Condition_BINARY_OP_LT,
										Values: [][]byte{convert.Int64ToBytes(500)},
									},
								},
							})
						},
					},
					shardsForTest{
						{
							id:       0,
							location: l1,
							elements: []string{"4"},
						},
						{
							id:       1,
							location: l2,
						},
						{
							id:       1,
							location: l3,
						},
						{
							id:       1,
							location: l4,
							elements: []string{"3", "5"},
						},
					},
					false,
				),
				Entry(

					"filter and sort by duration",
					queryOpts{
						entity:   tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
						duration: 1 * time.Hour,
						buildFn: func(builder tsdb.SeekerBuilder) {
							rule := &databasev1.IndexRule{
								Metadata: &commonv1.Metadata{
									Name:  "duration",
									Group: "default",
									Id:    3,
								},
								Tags:     []string{"duration"},
								Type:     databasev1.IndexRule_TYPE_TREE,
								Location: databasev1.IndexRule_LOCATION_SERIES,
							}
							builder.Filter(rule, tsdb.Condition{
								"duration": []index.ConditionValue{
									{
										Op:     modelv1.Condition_BINARY_OP_LT,
										Values: [][]byte{convert.Int64ToBytes(500)},
									},
								},
							})
							builder.OrderByIndex(rule, modelv1.Sort_SORT_ASC)
						},
					},
					shardsForTest{
						{
							id:       0,
							location: l1,
							elements: []string{"4"},
						},
						{
							id:       1,
							location: l2,
						},
						{
							id:       1,
							location: l3,
						},
						{
							id:       1,
							location: l4,
							elements: []string{"3", "5"},
						},
					},
					false,
				),
				Entry(

					"filter by several conditions",
					queryOpts{
						entity:   tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
						duration: 1 * time.Hour,
						buildFn: func(builder tsdb.SeekerBuilder) {
							rule := &databasev1.IndexRule{
								Metadata: &commonv1.Metadata{
									Name:  "duration",
									Group: "default",
									Id:    3,
								},
								Tags:     []string{"duration"},
								Type:     databasev1.IndexRule_TYPE_TREE,
								Location: databasev1.IndexRule_LOCATION_SERIES,
							}
							builder.Filter(rule, tsdb.Condition{
								"duration": []index.ConditionValue{
									{
										Op:     modelv1.Condition_BINARY_OP_LT,
										Values: [][]byte{convert.Int64ToBytes(500)},
									},
								},
							})
							builder.Filter(&databasev1.IndexRule{
								Metadata: &commonv1.Metadata{
									Name:  "endpoint_id",
									Group: "default",
									Id:    4,
								},
								Tags:     []string{"endpoint_id"},
								Type:     databasev1.IndexRule_TYPE_INVERTED,
								Location: databasev1.IndexRule_LOCATION_SERIES,
							}, tsdb.Condition{
								"endpoint_id": []index.ConditionValue{
									{
										Op:     modelv1.Condition_BINARY_OP_EQ,
										Values: [][]byte{[]byte("/home_id")},
									},
								},
							})
						},
					},
					shardsForTest{
						{
							id:       0,
							location: l1,
						},
						{
							id:       1,
							location: l2,
						},
						{
							id:       1,
							location: l3,
						},
						{
							id:       1,
							location: l4,
							elements: []string{"3"},
						},
					},
					false,
				),
				Entry(
					"filter by several conditions, sort by duration",
					queryOpts{
						entity:   tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
						duration: 1 * time.Hour,
						buildFn: func(builder tsdb.SeekerBuilder) {
							rule := &databasev1.IndexRule{
								Metadata: &commonv1.Metadata{
									Name:  "duration",
									Group: "default",
									Id:    3,
								},
								Tags:     []string{"duration"},
								Type:     databasev1.IndexRule_TYPE_TREE,
								Location: databasev1.IndexRule_LOCATION_SERIES,
							}
							builder.Filter(rule, tsdb.Condition{
								"duration": []index.ConditionValue{
									{
										Op:     modelv1.Condition_BINARY_OP_LT,
										Values: [][]byte{convert.Int64ToBytes(500)},
									},
								},
							})
							builder.OrderByIndex(rule, modelv1.Sort_SORT_ASC)
							builder.Filter(&databasev1.IndexRule{
								Metadata: &commonv1.Metadata{
									Name:  "endpoint_id",
									Group: "default",
									Id:    4,
								},
								Tags:     []string{"endpoint_id"},
								Type:     databasev1.IndexRule_TYPE_INVERTED,
								Location: databasev1.IndexRule_LOCATION_SERIES,
							}, tsdb.Condition{
								"endpoint_id": []index.ConditionValue{
									{
										Op:     modelv1.Condition_BINARY_OP_EQ,
										Values: [][]byte{[]byte("/home_id")},
									},
								},
							})
						},
					},
					shardsForTest{
						{
							id:       0,
							location: l1,
						},
						{
							id:       1,
							location: l2,
						},
						{
							id:       1,
							location: l3,
						},
						{
							id:       1,
							location: l4,
							elements: []string{"3"},
						},
					},
					false,
				),
				Entry(

					"filter by extended tags c",
					queryOpts{
						entity:   tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
						duration: 1 * time.Hour,
						buildFn: func(builder tsdb.SeekerBuilder) {
							rule := &databasev1.IndexRule{
								Metadata: &commonv1.Metadata{
									Name:  "extended_tags",
									Group: "default",
									Id:    11,
								},
								Tags:     []string{"extended_tags"},
								Type:     databasev1.IndexRule_TYPE_INVERTED,
								Location: databasev1.IndexRule_LOCATION_SERIES,
							}
							builder.Filter(rule, tsdb.Condition{
								"extended_tags": []index.ConditionValue{
									{
										Op:     modelv1.Condition_BINARY_OP_HAVING,
										Values: [][]byte{[]byte("c")},
									},
								},
							})
						},
					},
					shardsForTest{
						{
							id:       0,
							location: l1,
							elements: []string{"4"},
						},
						{
							id:       1,
							location: l2,
						},
						{
							id:       1,
							location: l3,
						},
						{
							id:       1,
							location: l4,
							elements: []string{"3", "5"},
						},
					},
					false,
				),
				Entry(

					"filter by extended tags abc",
					queryOpts{
						entity:   tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
						duration: 1 * time.Hour,
						buildFn: func(builder tsdb.SeekerBuilder) {
							rule := &databasev1.IndexRule{
								Metadata: &commonv1.Metadata{
									Name:  "extended_tags",
									Group: "default",
									Id:    11,
								},
								Tags:     []string{"extended_tags"},
								Type:     databasev1.IndexRule_TYPE_INVERTED,
								Location: databasev1.IndexRule_LOCATION_SERIES,
							}
							builder.Filter(rule, tsdb.Condition{
								"extended_tags": []index.ConditionValue{
									{
										Op:     modelv1.Condition_BINARY_OP_HAVING,
										Values: [][]byte{[]byte("c"), []byte("a"), []byte("b")},
									},
								},
							})
						},
					},
					shardsForTest{
						{
							id:       0,
							location: l1,
						},
						{
							id:       1,
							location: l2,
						},
						{
							id:       1,
							location: l3,
						},
						{
							id:       1,
							location: l4,
							elements: []string{"5"},
						},
					},
					false,
				),
				Entry(

					"filter by extended tags bc",
					queryOpts{
						entity:   tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
						duration: 1 * time.Hour,
						buildFn: func(builder tsdb.SeekerBuilder) {
							rule := &databasev1.IndexRule{
								Metadata: &commonv1.Metadata{
									Name:  "extended_tags",
									Group: "default",
									Id:    11,
								},
								Tags:     []string{"extended_tags"},
								Type:     databasev1.IndexRule_TYPE_INVERTED,
								Location: databasev1.IndexRule_LOCATION_SERIES,
							}
							builder.Filter(rule, tsdb.Condition{
								"extended_tags": []index.ConditionValue{
									{
										Op:     modelv1.Condition_BINARY_OP_HAVING,
										Values: [][]byte{[]byte("c"), []byte("b")},
									},
								},
							})
						},
					},
					shardsForTest{
						{
							id:       0,
							location: l1,
							elements: []string{"4"},
						},
						{
							id:       1,
							location: l2,
						},
						{
							id:       1,
							location: l3,
						},
						{
							id:       1,
							location: l4,
							elements: []string{"5"},
						},
					},
					false,
				),
			)
		})
	})

	Context("Querying by global indices", Ordered, func() {
		var (
			s       *stream
			deferFn func()
		)
		BeforeAll(func() {
			var svcs *services
			svcs, deferFn = setUp()
			var ok bool
			s, ok = svcs.stream.schemaRepo.loadStream(&commonv1.Metadata{
				Name:  "sw",
				Group: "default",
			})
			Expect(ok).To(BeTrue())
			_ = setupQueryData("global_index.json", s)
		})
		AfterAll(func() {
			deferFn()
		})
		DescribeTable("", func(traceID string, wantTraceSegmentNum int, wantErr bool) {
			shards, errShards := s.Shards(nil)
			Expect(errShards).ShouldNot(HaveOccurred())
			err := func() error {
				itemSize := 0
				for _, shard := range shards {
					itemIDs, err := shard.Index().Seek(index.Field{
						Key: index.FieldKey{
							SeriesID: tsdb.GlobalSeriesID(tsdb.Entry(s.name)),
							// trace_id
							IndexRuleID: 10,
						},
						Term: []byte(traceID),
					})
					if err != nil {
						return errors.WithStack(err)
					}
					if len(itemIDs) < 1 {
						continue
					}
					if err != nil {
						return errors.WithStack(err)
					}
					itemSize += len(itemIDs)
					for _, itemID := range itemIDs {
						segShard, err := s.Shard(itemID.ShardID)
						if err != nil {
							return errors.WithStack(err)
						}
						series, err := segShard.Series().GetByID(itemID.SeriesID)
						if err != nil {
							return errors.WithStack(err)
						}
						err = func() error {
							item, closer, errInner := series.Get(itemID)
							defer func(closer io.Closer) {
								_ = closer.Close()
							}(closer)
							if errInner != nil {
								return errors.WithStack(errInner)
							}
							tagFamily, errInner := s.ParseTagFamily("searchable", item)
							if errInner != nil {
								return errors.WithStack(errInner)
							}
							for _, tag := range tagFamily.GetTags() {
								if tag.GetKey() == "trace_id" {
									Expect(tag.GetValue().GetStr().GetValue()).To(Equal(traceID))
								}
							}
							return nil
						}()
						if err != nil {
							return errors.WithStack(err)
						}

					}
				}
				Expect(itemSize).To(Equal(wantTraceSegmentNum))
				return nil
			}()
			if wantErr {
				Expect(err).Should(HaveOccurred())
				return
			}
			Expect(err).ShouldNot(HaveOccurred())
		},
			Entry(
				"trace id is 1",
				"1",
				2,
				false,
			),
			Entry(
				"trace id is 2",
				"2",
				3,
				false,
			),
			Entry(
				"unknown trace id",
				"foo",
				0,
				false,
			),
		)
	})
})

type queryOpts struct {
	entity   tsdb.Entity
	offset   time.Duration
	duration time.Duration
	buildFn  func(builder tsdb.SeekerBuilder)
}

func queryData(s *stream, baseTime time.Time, opts queryOpts) (shardsForTest, error) {
	shards, err := s.Shards(opts.entity)
	Expect(err).ShouldNot(HaveOccurred())
	got := shardsForTest{}
	for _, shard := range shards {
		seriesList, err := shard.Series().List(tsdb.NewPath(opts.entity))
		if err != nil {
			return nil, err
		}
		for _, series := range seriesList {
			got, err = func(g shardsForTest) (shardsForTest, error) {
				sp, errInner := series.Span(timestamp.NewInclusiveTimeRangeDuration(baseTime.Add(opts.offset), opts.duration))
				defer func(sp tsdb.SeriesSpan) {
					if sp != nil {
						_ = sp.Close()
					}
				}(sp)
				if errInner != nil {
					return nil, errInner
				}
				builder := sp.SeekerBuilder()
				if opts.buildFn != nil {
					opts.buildFn(builder)
				}
				seeker, errInner := builder.Build()
				if errInner != nil {
					return nil, errInner
				}
				iter, errInner := seeker.Seek()
				if errInner != nil {
					return nil, errInner
				}
				for dataFlowID, iterator := range iter {
					var elements []string
					for iterator.Next() {
						tagFamily, errInner := s.ParseTagFamily("searchable", iterator.Val())
						if errInner != nil {
							return nil, errInner
						}
						for _, tag := range tagFamily.GetTags() {
							if tag.GetKey() == "trace_id" {
								elements = append(elements, tag.GetValue().GetStr().GetValue())
							}
						}
						eleID, errInner := s.ParseElementID(iterator.Val())
						if errInner != nil {
							return nil, errInner
						}
						Expect(eleID).ShouldNot(BeEmpty())
					}
					_ = iterator.Close()
					g = append(g, shardStruct{
						id: shard.ID(),
						location: []string{
							fmt.Sprintf("series_%v", series.ID()),
							"data_flow_" + strconv.Itoa(dataFlowID),
						},
						elements: elements,
					})
				}

				return g, nil
			}(got)
			if err != nil {
				return nil, err
			}
		}
	}
	return got, nil
}

//go:embed testdata/*.json
var dataFS embed.FS

func setupQueryData(dataFile string, stream *stream) (baseTime time.Time) {
	var templates []interface{}
	baseTime = timestamp.NowMilli()
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(json.Unmarshal(content, &templates)).ShouldNot(HaveOccurred())
	bb, _ := base64.StdEncoding.DecodeString("YWJjMTIzIT8kKiYoKSctPUB+")
	for i, template := range templates {
		rawSearchTagFamily, errMarshal := json.Marshal(template)
		Expect(errMarshal).ShouldNot(HaveOccurred())
		searchTagFamily := &modelv1.TagFamilyForWrite{}
		Expect(jsonpb.UnmarshalString(string(rawSearchTagFamily), searchTagFamily)).ShouldNot(HaveOccurred())
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
		errInner := stream.Write(e)
		Expect(errInner).ShouldNot(HaveOccurred())
	}
	return baseTime
}
