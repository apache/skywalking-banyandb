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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ = Describe("Write", func() {
	var svcs *services
	var deferFn func()
	var measure measure.Measure

	BeforeEach(func() {
		svcs, deferFn = setUp()
		var err error
		measure, err = svcs.measure.Measure(&commonv1.Metadata{
			Name:  "cpm",
			Group: "default",
		})
		Expect(err).ShouldNot(HaveOccurred())
	})
	AfterEach(func() {
		deferFn()
	})
	It("queries data", func() {
		baseTime := writeData("query_data.json", measure)
		shard, err := measure.Shard(0)
		Expect(err).ShouldNot(HaveOccurred())
		series, err := shard.Series().Get(tsdb.Entity{tsdb.Entry("1")})
		Expect(err).ShouldNot(HaveOccurred())
		seriesSpan, err := series.Span(timestamp.NewInclusiveTimeRangeDuration(baseTime, 1*time.Hour))
		defer func(seriesSpan tsdb.SeriesSpan) {
			_ = seriesSpan.Close()
		}(seriesSpan)
		Expect(err).ShouldNot(HaveOccurred())
		seeker, err := seriesSpan.SeekerBuilder().OrderByTime(modelv1.Sort_SORT_DESC).Build()
		Expect(err).ShouldNot(HaveOccurred())
		iter, err := seeker.Seek()
		Expect(err).ShouldNot(HaveOccurred())
		Expect(len(iter)).To(Equal(1))
		defer func(iterator tsdb.Iterator) {
			_ = iterator.Close()
		}(iter[0])
		i := 0
		expectedFields := [][]int64{{150, 300, 5}, {200, 50, 4}, {100, 100, 1}}
		for ; iter[0].Next(); i++ {
			item := iter[0].Val()
			tagFamily, err := measure.ParseTagFamily("default", item)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(len(tagFamily.Tags)).To(Equal(2))
			summation, err := measure.ParseField("summation", item)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(summation.GetValue().GetInt().Value).To(Equal(expectedFields[i][0]))
			count, err := measure.ParseField("count", item)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(count.GetValue().GetInt().Value).To(Equal(expectedFields[i][1]))
			value, err := measure.ParseField("value", item)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(value.GetValue().GetInt().Value).To(Equal(expectedFields[i][2]))
		}
	})
})
