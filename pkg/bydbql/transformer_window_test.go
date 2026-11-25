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

package bydbql_test

import (
	"context"
	"math"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	. "github.com/apache/skywalking-banyandb/pkg/bydbql"
)

// A native request reads a limit of 0 as "the default limit", so a statement that asks for no row (LIMIT 0) must be marked
// empty for the caller to answer without running it; and a property request has no offset, so a statement that pages must
// carry the rows to skip. Every query type is checked, since each builds its request on its own.
var _ = Describe("the window of a query", func() {
	var transformer *Transformer

	BeforeEach(func() {
		ctrl := gomock.NewController(GinkgoT())
		streamRegistry := schema.NewMockStream(ctrl)
		streamRegistry.EXPECT().GetStream(gomock.Any(), gomock.Any()).Return(equivalenceStream(), nil).AnyTimes()
		measureRegistry := schema.NewMockMeasure(ctrl)
		measureRegistry.EXPECT().GetMeasure(gomock.Any(), gomock.Any()).Return(&databasev1.Measure{
			Metadata: &commonv1.Metadata{Name: "svc_metrics", Group: "default"},
			TagFamilies: []*databasev1.TagFamilySpec{{
				Name: "default",
				Tags: []*databasev1.TagSpec{{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING}},
			}},
			Fields: []*databasev1.FieldSpec{{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
		}, nil).AnyTimes()
		propertyRegistry := schema.NewMockProperty(ctrl)
		propertyRegistry.EXPECT().GetProperty(gomock.Any(), gomock.Any()).Return(&databasev1.Property{
			Metadata: &commonv1.Metadata{Name: "sw_prop", Group: "default"},
			Tags:     []*databasev1.TagSpec{{Name: "env", Type: databasev1.TagType_TAG_TYPE_STRING}},
		}, nil).AnyTimes()
		traceRegistry := schema.NewMockTrace(ctrl)
		traceRegistry.EXPECT().GetTrace(gomock.Any(), gomock.Any()).Return(&databasev1.Trace{
			Metadata: &commonv1.Metadata{Name: "sw_trace", Group: "default"},
			Tags: []*databasev1.TraceTagSpec{
				{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
			},
			TraceIdTagName: "trace_id",
		}, nil).AnyTimes()
		repo := metadata.NewMockRepo(ctrl)
		repo.EXPECT().StreamRegistry().AnyTimes().Return(streamRegistry)
		repo.EXPECT().MeasureRegistry().AnyTimes().Return(measureRegistry)
		repo.EXPECT().PropertyRegistry().AnyTimes().Return(propertyRegistry)
		repo.EXPECT().TraceRegistry().AnyTimes().Return(traceRegistry)
		transformer = NewTransformer(repo)
	})

	transform := func(query string, params ...*modelv1.TagValue) *TransformResult {
		grammar, err := ParseQuery(query)
		Expect(err).NotTo(HaveOccurred(), query)
		Expect(BindParams(grammar, params)).To(Succeed(), query)
		result, err := transformer.Transform(context.Background(), grammar)
		Expect(err).NotTo(HaveOccurred(), query)
		return result
	}

	DescribeTable("a statement that asks for no row is marked empty, and no other is",
		func(prefix string) {
			Expect(transform(prefix+" LIMIT 0").Empty).To(BeTrue(), "LIMIT 0")
			Expect(transform(prefix+" LIMIT ?", intParam(0)).Empty).To(BeTrue(), "LIMIT ? bound to 0")
			Expect(transform(prefix+" LIMIT 5").Empty).To(BeFalse(), "LIMIT 5")
			Expect(transform(prefix+" LIMIT ?", intParam(5)).Empty).To(BeFalse(), "LIMIT ? bound to 5")
			Expect(transform(prefix).Empty).To(BeFalse(), "no LIMIT at all takes the default, it does not ask for no row")
		},
		Entry("stream", "SELECT * FROM STREAM sw IN default TIME > '-30m'"),
		Entry("trace", "SELECT * FROM TRACE sw_trace IN default TIME > '-30m' ORDER BY duration DESC"),
		Entry("property", "SELECT env FROM PROPERTY sw_prop IN default"),
	)

	It("keeps the legacy measure contract, where LIMIT 0 is the default page", func() {
		Expect(transform("SELECT service, value FROM MEASURE svc_metrics IN default TIME > '-30m' LIMIT 0").Empty).To(BeFalse())
	})

	Describe("the offset of a property query", func() {
		propertyLimit := func(result *TransformResult) uint32 {
			return result.QueryRequest.(*propertyv1.QueryRequest).GetLimit()
		}

		It("is carried in the result, and the request asks for offset+limit rows", func() {
			result := transform("SELECT env FROM PROPERTY sw_prop IN default LIMIT 10 OFFSET 20")
			Expect(result.Offset).To(Equal(uint32(20)))
			Expect(propertyLimit(result)).To(Equal(uint32(30)))
			Expect(result.Empty).To(BeFalse())
		})

		It("pages from the default limit when the statement names none", func() {
			result := transform("SELECT env FROM PROPERTY sw_prop IN default OFFSET 20")
			Expect(result.Offset).To(Equal(uint32(20)))
			Expect(propertyLimit(result)).To(Equal(uint32(120)), "the default limit of 100 after the 20 skipped rows")
		})

		It("is read from a bound parameter", func() {
			result := transform("SELECT env FROM PROPERTY sw_prop IN default LIMIT ? OFFSET ?", intParam(10), intParam(5))
			Expect(result.Offset).To(Equal(uint32(5)))
			Expect(propertyLimit(result)).To(Equal(uint32(15)))
		})

		It("leaves the request alone when there is no offset", func() {
			result := transform("SELECT env FROM PROPERTY sw_prop IN default LIMIT 10")
			Expect(result.Offset).To(BeZero())
			Expect(propertyLimit(result)).To(Equal(uint32(10)))
		})

		It("saturates instead of wrapping when offset+limit passes the uint32 maximum", func() {
			result := transform("SELECT env FROM PROPERTY sw_prop IN default LIMIT 4294967295 OFFSET 10")
			Expect(propertyLimit(result)).To(Equal(uint32(math.MaxUint32)))
			Expect(result.Offset).To(Equal(uint32(10)))
		})

		It("asks for nothing when the limit is 0, whatever the offset", func() {
			result := transform("SELECT env FROM PROPERTY sw_prop IN default LIMIT 0 OFFSET 20")
			Expect(result.Empty).To(BeTrue())
			Expect(propertyLimit(result)).To(BeZero())
		})
	})
})
