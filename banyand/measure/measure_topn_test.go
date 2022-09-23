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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ = Describe("Write and Count TopN for service_cpm_minute", func() {
	var svcs *services
	var deferFn func()
	var measure measure.Measure
	var baseTime time.Time

	count := func(topNMetadata *commonv1.Metadata) (num int) {
		// Retrieve all shards
		shards, err := measure.CompanionShards(topNMetadata)
		Expect(err).ShouldNot(HaveOccurred())
		topNSchema, err := svcs.metadataService.TopNAggregationRegistry().GetTopNAggregation(context.TODO(), topNMetadata)
		Expect(err).ShouldNot(HaveOccurred())
		entity := make(tsdb.Entity, 1+len(topNSchema.GetGroupByTagNames()))
		for i := range entity {
			entity[i] = tsdb.AnyEntry
		}
		for _, shard := range shards {
			sl, err := shard.Series().List(tsdb.NewPath(entity))
			Expect(err).ShouldNot(HaveOccurred())
			for _, series := range sl {
				seriesSpan, err := series.Span(timestamp.NewInclusiveTimeRangeDuration(baseTime, 1*time.Hour))
				defer func(seriesSpan tsdb.SeriesSpan) {
					_ = seriesSpan.Close()
				}(seriesSpan)
				Expect(err).ShouldNot(HaveOccurred())
				seeker, err := seriesSpan.SeekerBuilder().OrderByTime(modelv1.Sort_SORT_ASC).Build()
				Expect(err).ShouldNot(HaveOccurred())
				iters, err := seeker.Seek()
				Expect(err).ShouldNot(HaveOccurred())
				for _, iter := range iters {
					defer func(iterator tsdb.Iterator) {
						Expect(iterator.Close()).ShouldNot(HaveOccurred())
					}(iter)
					for {
						if hasNext := iter.Next(); hasNext {
							num++
						} else {
							break
						}
					}
				}
			}
		}
		return num
	}

	BeforeEach(func() {
		svcs, deferFn = setUp()
		var err error
		measure, err = svcs.measure.Measure(&commonv1.Metadata{
			Name:  "service_cpm_minute",
			Group: "sw_metric",
		})
		Expect(err).ShouldNot(HaveOccurred())
	})
	AfterEach(func() {
		deferFn()
	})
	DescribeTable("writes", func(measureMetadata *commonv1.Metadata, topNMetadata *commonv1.Metadata,
		dataTextualFile string, expectDataPointsNum int,
	) {
		var err error
		measure, err = svcs.measure.Measure(measureMetadata)
		Expect(err).ShouldNot(HaveOccurred())
		baseTime = writeData(dataTextualFile, measure)
		Eventually(func(g Gomega) {
			g.Expect(count(topNMetadata)).To(Equal(expectDataPointsNum))
		}).WithTimeout(5 * time.Second).WithPolling(1 * time.Second).Should(Succeed())
	},
		Entry("service_cpm_minute", &commonv1.Metadata{
			Name:  "service_cpm_minute",
			Group: "sw_metric",
		}, &commonv1.Metadata{
			Name:  "service_cpm_minute_top100",
			Group: "sw_metric",
		}, "service_cpm_minute_data.json", 2),
		Entry("service_cpm_minute without groupBy", &commonv1.Metadata{
			Name:  "service_cpm_minute",
			Group: "sw_metric",
		}, &commonv1.Metadata{
			Name:  "service_cpm_minute_no_group_by_top100",
			Group: "sw_metric",
		}, "service_cpm_minute_data.json", 2),
	)
})
