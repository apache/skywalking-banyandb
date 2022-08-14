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
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func setupMeasureQueryData(dataFile string, measureSchema measure.Measure,
	interval time.Duration, lastDelay time.Duration,
) (baseTime time.Time) {
	var templates []interface{}
	baseTime = timestamp.NowMilli()
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(json.Unmarshal(content, &templates)).Should(Succeed())
	for i, template := range templates {
		rawDataPointValue, errMarshal := json.Marshal(template)
		Expect(errMarshal).ShouldNot(HaveOccurred())
		dataPointValue := &measurev1.DataPointValue{}
		dataPointValue.Timestamp = timestamppb.New(baseTime.Add(time.Duration(i) * interval))
		Expect(protojson.Unmarshal(rawDataPointValue, dataPointValue)).ShouldNot(HaveOccurred())
		errInner := measureSchema.Write(dataPointValue)
		Expect(errInner).ShouldNot(HaveOccurred())
		if i == len(templates)-1 {
			// add delay
			dataPointValue.Timestamp.Seconds += int64(lastDelay.Seconds())
			lastErr := measureSchema.Write(dataPointValue)
			Expect(lastErr).ShouldNot(HaveOccurred())
		}
	}
	return baseTime
}

var _ = Describe("TopN Query", Ordered, func() {
	var measureSchema measure.Measure
	var sT, eT time.Time

	BeforeAll(func() {
		var err error
		// setup measure data
		measureSchema, err = svcs.measure.Measure(&commonv1.Metadata{
			Name:  "service_cpm_minute",
			Group: "sw_metric",
		})
		Expect(err).ShouldNot(HaveOccurred())
		baseTs := setupMeasureQueryData("service_cpm_minute_data.json", measureSchema, 15*time.Second,
			2*time.Minute)
		sT, eT = baseTs.Add(-1*time.Minute), baseTs.Add(10*time.Minute)
	})

	It("Query without condition", func() {
		query := pbv1.NewMeasureTopNRequestBuilder().
			Metadata("sw_metric", "service_cpm_minute_top100").
			TimeRange(sT, eT).
			TopN(1).
			Max().
			Build()
		Eventually(func(g Gomega) int64 {
			now := time.Now()
			msg := bus.NewMessage(bus.MessageID(now.UnixNano()), query)
			f, err := svcs.pipeline.Publish(data.TopicTopNQuery, msg)
			g.Expect(err).ShouldNot(HaveOccurred())
			g.Expect(f).ShouldNot(BeNil())
			resp, err := f.Get()
			g.Expect(err).ShouldNot(HaveOccurred())
			g.Expect(resp).ShouldNot(BeNil())
			g.Expect(resp.Data()).Should(HaveLen(1))
			return resp.Data().([]*measurev1.TopNList_Item)[0].GetValue().GetInt().GetValue()
		}).WithTimeout(time.Second * 10).Should(BeNumerically("==", 5))
	})

	It("Query with condition and min aggregation", func() {
		query := pbv1.NewMeasureTopNRequestBuilder().
			Metadata("sw_metric", "service_cpm_minute_top100").
			TimeRange(sT, eT).
			TopN(1).
			Min().
			Build()
		Eventually(func(g Gomega) int64 {
			now := time.Now()
			msg := bus.NewMessage(bus.MessageID(now.UnixNano()), query)
			f, err := svcs.pipeline.Publish(data.TopicTopNQuery, msg)
			g.Expect(err).ShouldNot(HaveOccurred())
			g.Expect(f).ShouldNot(BeNil())
			resp, err := f.Get()
			g.Expect(err).ShouldNot(HaveOccurred())
			g.Expect(resp).ShouldNot(BeNil())
			g.Expect(resp.Data()).Should(HaveLen(1))
			return resp.Data().([]*measurev1.TopNList_Item)[0].GetValue().GetInt().GetValue()
		}).WithTimeout(time.Second * 10).Should(BeNumerically("==", 1))
	})

	It("Query with condition", func() {
		query := pbv1.NewMeasureTopNRequestBuilder().
			Metadata("sw_metric", "service_cpm_minute_top100").
			TimeRange(sT, eT).
			Conditions(pbv1.Eq("entity_id", "entity_2")).
			TopN(1).
			Max().
			Build()
		Eventually(func(g Gomega) int64 {
			now := time.Now()
			msg := bus.NewMessage(bus.MessageID(now.UnixNano()), query)
			f, err := svcs.pipeline.Publish(data.TopicTopNQuery, msg)
			g.Expect(err).ShouldNot(HaveOccurred())
			g.Expect(f).ShouldNot(BeNil())
			resp, err := f.Get()
			g.Expect(err).ShouldNot(HaveOccurred())
			g.Expect(resp).ShouldNot(BeNil())
			g.Expect(resp.Data()).Should(HaveLen(1))
			return resp.Data().([]*measurev1.TopNList_Item)[0].GetValue().GetInt().GetValue()
		}).WithTimeout(time.Second * 10).Should(BeNumerically("==", 5))
	})

	It("Query with condition for non-group-by topN", func() {
		query := pbv1.NewMeasureTopNRequestBuilder().
			Metadata("sw_metric", "service_cpm_minute_no_group_by_top100").
			TimeRange(sT, eT).
			TopN(1).
			Max().
			Build()
		Eventually(func(g Gomega) int64 {
			now := time.Now()
			msg := bus.NewMessage(bus.MessageID(now.UnixNano()), query)
			f, err := svcs.pipeline.Publish(data.TopicTopNQuery, msg)
			g.Expect(err).ShouldNot(HaveOccurred())
			g.Expect(f).ShouldNot(BeNil())
			resp, err := f.Get()
			g.Expect(err).ShouldNot(HaveOccurred())
			g.Expect(resp).ShouldNot(BeNil())
			g.Expect(resp.Data()).Should(HaveLen(1))
			return resp.Data().([]*measurev1.TopNList_Item)[0].GetValue().GetInt().GetValue()
		}).WithTimeout(time.Second * 10).Should(BeNumerically("==", 5))
	})
})
