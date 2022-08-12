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
	"sort"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ = Describe("Query service_cpm_minute", func() {
	var svcs *services
	var deferFn func()
	var measure measure.Measure
	var baseTime time.Time

	BeforeEach(func() {
		svcs, deferFn = setUp()
		var err error
		measure, err = svcs.measure.Measure(&commonv1.Metadata{
			Name:  "service_cpm_minute",
			Group: "sw_metric",
		})
		Expect(err).ShouldNot(HaveOccurred())
		baseTime = writeData("service_cpm_minute_data.json", measure)
	})
	AfterEach(func() {
		deferFn()
	})

	runTest := func(
		metadata *commonv1.Metadata,
		entityID string,
		expectedFields [][]int64,
		queryFn func(seriesSpan tsdb.SeriesSpan, idIndexRule *databasev1.IndexRule) (tsdb.Seeker, error),
	) {
		indexRules := measure.GetIndexRules()
		var idIndexRule *databasev1.IndexRule
		for _, ir := range indexRules {
			if ir.Metadata.Name == "id" {
				idIndexRule = ir
			}
		}
		Expect(idIndexRule).NotTo(BeNil())
		shard, err := measure.Shard(0)
		Expect(err).ShouldNot(HaveOccurred())
		series, err := shard.Series().Get(tsdb.Entity{tsdb.Entry(entityID)})
		Expect(err).ShouldNot(HaveOccurred())
		seriesSpan, err := series.Span(timestamp.NewInclusiveTimeRangeDuration(baseTime, 1*time.Hour))
		defer func(seriesSpan tsdb.SeriesSpan) {
			_ = seriesSpan.Close()
		}(seriesSpan)
		Expect(err).ShouldNot(HaveOccurred())

		seeker, err := queryFn(seriesSpan, idIndexRule)
		Expect(err).ShouldNot(HaveOccurred())
		iter, err := seeker.Seek()
		Expect(err).ShouldNot(HaveOccurred())
		Expect(len(iter)).To(Equal(1))
		defer func(iterator tsdb.ItemIterator) {
			Expect(iterator.Close()).ShouldNot(HaveOccurred())
		}(iter[0])
		i := 0
		for {
			item, hasNext := iter[0].Next()
			if !hasNext {
				break
			}
			tagFamily, err := measure.ParseTagFamily("default", item)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(len(tagFamily.Tags)).To(Equal(2))
			total, err := measure.ParseField("total", item)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(total.GetValue().GetInt().Value).To(Equal(expectedFields[i][0]))
			value, err := measure.ParseField("value", item)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(value.GetValue().GetInt().Value).To(Equal(expectedFields[i][1]))
			i++
		}
		Expect(len(expectedFields)).To(Equal(i))
	}

	It("queries all service_cpm_minute_data", func() {
		runTest(
			&commonv1.Metadata{
				Name:  "service_cpm_minute",
				Group: "sw_metric",
			},
			"entity_1",
			[][]int64{{300, 5}, {50, 4}, {100, 1}},
			func(seriesSpan tsdb.SeriesSpan, _ *databasev1.IndexRule) (tsdb.Seeker, error) {
				return seriesSpan.SeekerBuilder().OrderByTime(modelv1.Sort_SORT_DESC).Build()
			},
		)
	})
	It("queries service_cpm_minute_data by id", func() {
		runTest(
			&commonv1.Metadata{
				Name:  "service_cpm_minute",
				Group: "sw_metric",
			},
			"entity_1",
			[][]int64{{100, 1}},
			func(seriesSpan tsdb.SeriesSpan, idIndexRule *databasev1.IndexRule) (tsdb.Seeker, error) {
				return seriesSpan.SeekerBuilder().Filter(idIndexRule, tsdb.Condition{
					"id": []index.ConditionValue{
						{
							Op:     modelv1.Condition_BINARY_OP_EQ,
							Values: [][]byte{[]byte("1")},
						},
					},
				}).OrderByTime(modelv1.Sort_SORT_DESC).Build()
			},
		)
	})
	It("queries service_cpm_minute_data by id after updating", func() {
		writeDataWithBaseTime(baseTime, "service_cpm_minute_data1.json", measure)
		runTest(
			&commonv1.Metadata{
				Name:  "service_cpm_minute",
				Group: "sw_metric",
			},
			"entity_1",
			[][]int64{{200, 3}},
			func(seriesSpan tsdb.SeriesSpan, idIndexRule *databasev1.IndexRule) (tsdb.Seeker, error) {
				return seriesSpan.SeekerBuilder().Filter(idIndexRule, tsdb.Condition{
					"id": []index.ConditionValue{
						{
							Op:     modelv1.Condition_BINARY_OP_EQ,
							Values: [][]byte{[]byte("1")},
						},
					},
				}).OrderByTime(modelv1.Sort_SORT_DESC).Build()
			},
		)
	})
})

var _ = Describe("Query service_traffic", func() {
	var svcs *services
	var deferFn func()
	var measure measure.Measure
	var baseTime time.Time

	BeforeEach(func() {
		svcs, deferFn = setUp()
		var err error
		measure, err = svcs.measure.Measure(&commonv1.Metadata{
			Name:  "service_traffic",
			Group: "sw_metric",
		})
		Expect(err).ShouldNot(HaveOccurred())
		baseTime = writeData("service_traffic_data.json", measure)
	})
	AfterEach(func() {
		deferFn()
	})

	runTest := func(
		metadata *commonv1.Metadata,
		id tsdb.Entry,
		serviceIDs sort.StringSlice,
		queryFn func(seriesSpan tsdb.SeriesSpan) (tsdb.Seeker, error),
	) {
		shards, err := measure.Shards([]tsdb.Entry{id})
		Expect(err).ShouldNot(HaveOccurred())
		got := make(sort.StringSlice, 0, len(serviceIDs))
		for _, shard := range shards {
			sl, err := shard.Series().List(tsdb.NewPath([]tsdb.Entry{id}))
			Expect(err).ShouldNot(HaveOccurred())
			for _, series := range sl {
				seriesSpan, err := series.Span(timestamp.NewInclusiveTimeRangeDuration(baseTime, 1*time.Hour))
				defer func(seriesSpan tsdb.SeriesSpan) {
					_ = seriesSpan.Close()
				}(seriesSpan)
				Expect(err).ShouldNot(HaveOccurred())
				seeker, err := queryFn(seriesSpan)
				Expect(err).ShouldNot(HaveOccurred())
				iter, err := seeker.Seek()
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(iter)).To(Equal(1))
				defer func(iterator tsdb.ItemIterator) {
					_ = iterator.Close()
				}(iter[0])
				item, hasNext := iter[0].Next()
				if !hasNext {
					continue
				}
				tagFamily, err := measure.ParseTagFamily("default", item)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(len(tagFamily.Tags)).To(Equal(6))
				existServiceID := false
				for _, t := range tagFamily.Tags {
					if t.Key == "service_id" {
						got = append(got, t.Value.GetStr().GetValue())
						existServiceID = true
					}
				}
				if !existServiceID {
					Fail("doesn't get service_id")
				}
				if _, hasNext = iter[0].Next(); hasNext {
					Fail("should only one data point in this series")
				}
			}
		}
		sort.Sort(got)
		Expect(got).To(Equal(serviceIDs))
	}

	It("queries all", func() {
		runTest(
			&commonv1.Metadata{
				Name:  "service_cpm_minute",
				Group: "sw_metric",
			},
			tsdb.AnyEntry,
			[]string{"service_1", "service_2", "service_3"},
			func(seriesSpan tsdb.SeriesSpan) (tsdb.Seeker, error) {
				return seriesSpan.SeekerBuilder().OrderByTime(modelv1.Sort_SORT_DESC).Build()
			},
		)
	})
	It("queries by id which is the entity id", func() {
		runTest(
			&commonv1.Metadata{
				Name:  "service_cpm_minute",
				Group: "sw_metric",
			},
			tsdb.Entry("1"),
			[]string{"service_1"},
			func(seriesSpan tsdb.SeriesSpan) (tsdb.Seeker, error) {
				return seriesSpan.SeekerBuilder().OrderByTime(modelv1.Sort_SORT_DESC).Build()
			},
		)
	})
	It("queries by service_id", func() {
		runTest(
			&commonv1.Metadata{
				Name:  "service_cpm_minute",
				Group: "sw_metric",
			},
			tsdb.AnyEntry,
			[]string{"service_1"},
			func(seriesSpan tsdb.SeriesSpan) (tsdb.Seeker, error) {
				return seriesSpan.SeekerBuilder().Filter(&databasev1.IndexRule{
					Metadata: &commonv1.Metadata{
						Id: 1,
					},
					Type: databasev1.IndexRule_TYPE_INVERTED,
				}, tsdb.Condition{
					"id": []index.ConditionValue{
						{
							Op:     modelv1.Condition_BINARY_OP_EQ,
							Values: [][]byte{[]byte("service_1")},
						},
					},
				}).OrderByTime(modelv1.Sort_SORT_DESC).Build()
			},
		)
	})
})
