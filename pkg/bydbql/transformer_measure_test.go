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

	"github.com/google/go-cmp/cmp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/testing/protocmp"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	. "github.com/apache/skywalking-banyandb/pkg/bydbql"
)

// measureTagAggSchema mirrors the composite_entity_metric/time_bucket_metric
// fixtures used elsewhere in this repo's vec-engine test suite: two string
// tags (one plays the entity/routing role) and one INT field, enough to
// exercise tag-vs-field ambiguity, tag-targeted aggregation, and
// GROUP BY TIME_BUCKET without pulling in a live schema registry.
func measureTagAggSchema() *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "tag_agg_metrics", Group: "default"},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{
				{Name: "entity_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		}},
		Fields:   []*databasev1.FieldSpec{{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
		Entity:   &databasev1.Entity{TagNames: []string{"entity_id"}},
		Interval: "1m",
	}
}

var _ = Describe("Transform: COUNT_DISTINCT and TIME_BUCKET for measures", func() {
	var transformer *Transformer

	BeforeEach(func() {
		ctrl := gomock.NewController(GinkgoT())
		measureRegistry := schema.NewMockMeasure(ctrl)
		measureRegistry.EXPECT().GetMeasure(gomock.Any(), gomock.Any()).Return(measureTagAggSchema(), nil).AnyTimes()
		mockRepo := metadata.NewMockRepo(ctrl)
		mockRepo.EXPECT().MeasureRegistry().AnyTimes().Return(measureRegistry)
		transformer = NewTransformer(mockRepo)
	})

	transform := func(query string) (*measurev1.QueryRequest, error) {
		grammar, parseErr := ParseQuery(query)
		Expect(parseErr).To(BeNil())
		Expect(BindParams(grammar, nil)).To(Succeed())
		result, transformErr := transformer.Transform(context.Background(), grammar)
		if transformErr != nil {
			return nil, transformErr
		}
		req, ok := result.QueryRequest.(*measurev1.QueryRequest)
		Expect(ok).To(BeTrue(), "expected a measure QueryRequest, got %T", result.QueryRequest)
		return req, nil
	}

	diffOpts := []cmp.Option{
		protocmp.Transform(),
		protocmp.IgnoreFields(&measurev1.QueryRequest{}, "time_range"),
		protocmp.SortRepeatedFields(&modelv1.TagProjection_TagFamily{}, "tags"),
	}

	It("resolves COUNT(DISTINCT tag) to a tag-targeted Aggregation", func() {
		req, err := transform(
			"SELECT entity_id, COUNT(DISTINCT entity_id) FROM MEASURE tag_agg_metrics IN default TIME > '-15m' GROUP BY entity_id",
		)
		Expect(err).To(BeNil())
		entityIDProjection := &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: "default", Tags: []string{"entity_id"}}},
		}
		want := &measurev1.QueryRequest{
			Name:          "tag_agg_metrics",
			Groups:        []string{"default"},
			TagProjection: entityIDProjection,
			Agg: &measurev1.QueryRequest_Aggregation{
				Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT, TagName: "entity_id", TagFamily: "default",
			},
			GroupBy: &measurev1.QueryRequest_GroupBy{TagProjection: entityIDProjection},
		}
		Expect(cmp.Diff(want, req, diffOpts...)).To(BeEmpty())
	})

	It("resolves a non-distinct COUNT over a tag (general tag-agg gap, not just COUNT_DISTINCT)", func() {
		req, err := transform("SELECT id, COUNT(entity_id) FROM MEASURE tag_agg_metrics IN default TIME > '-15m' GROUP BY id")
		Expect(err).To(BeNil())
		Expect(req.GetAgg().GetFunction()).To(Equal(modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT))
		Expect(req.GetAgg().GetTagName()).To(Equal("entity_id"))
		Expect(req.GetAgg().GetTagFamily()).To(Equal("default"))
		Expect(req.GetAgg().GetFieldName()).To(BeEmpty())
	})

	It("still resolves SUM over a field targeted by name", func() {
		req, err := transform("SELECT SUM(value) FROM MEASURE tag_agg_metrics IN default TIME > '-15m'")
		Expect(err).To(BeNil())
		Expect(req.GetAgg().GetFunction()).To(Equal(modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM))
		Expect(req.GetAgg().GetFieldName()).To(Equal("value"))
		Expect(req.GetAgg().GetTagName()).To(BeEmpty())
	})

	It("accepts a tag-only GROUP BY combined with a field-targeted Agg (the deleted row-path constraint no longer applies)", func() {
		req, err := transform("SELECT entity_id, SUM(value) FROM MEASURE tag_agg_metrics IN default TIME > '-15m' GROUP BY entity_id")
		Expect(err).To(BeNil())
		Expect(req.GetGroupBy().GetFieldName()).To(BeEmpty())
		Expect(req.GetGroupBy().GetTagProjection().GetTagFamilies()).NotTo(BeEmpty())
	})

	It("rejects an aggregation column that resolves to neither a tag nor a field", func() {
		_, err := transform("SELECT COUNT(DISTINCT nonexistent) FROM MEASURE tag_agg_metrics IN default TIME > '-15m'")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("nonexistent"))
	})

	It("maps GROUP BY TIME_BUCKET() with no width to an empty Width", func() {
		req, err := transform("SELECT SUM(value) FROM MEASURE tag_agg_metrics IN default TIME > '-15m' GROUP BY TIME_BUCKET()")
		Expect(err).To(BeNil())
		want := &measurev1.QueryRequest{
			Name:    "tag_agg_metrics",
			Groups:  []string{"default"},
			Agg:     &measurev1.QueryRequest_Aggregation{Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, FieldName: "value"},
			GroupBy: &measurev1.QueryRequest_GroupBy{TimeBucket: &measurev1.QueryRequest_GroupBy_TimeBucket{Width: ""}},
		}
		Expect(cmp.Diff(want, req, diffOpts...)).To(BeEmpty())
		// TagProjection must be nil, not a present-but-empty message — a
		// TIME_BUCKET-only GROUP BY has no tag key at all.
		Expect(req.GetGroupBy().TagProjection).To(BeNil())
	})

	It("maps GROUP BY TIME_BUCKET('5m') with an explicit width", func() {
		req, err := transform("SELECT SUM(value) FROM MEASURE tag_agg_metrics IN default TIME > '-15m' GROUP BY TIME_BUCKET('5m')")
		Expect(err).To(BeNil())
		Expect(req.GetGroupBy().GetTimeBucket().GetWidth()).To(Equal("5m"))
	})

	It("maps GROUP BY TIME_BUCKET('5m'), tag combined", func() {
		req, err := transform("SELECT id, SUM(value) FROM MEASURE tag_agg_metrics IN default TIME > '-15m' GROUP BY TIME_BUCKET('5m'), id")
		Expect(err).To(BeNil())
		Expect(req.GetGroupBy().GetTimeBucket().GetWidth()).To(Equal("5m"))
		Expect(req.GetGroupBy().GetTagProjection().GetTagFamilies()).To(HaveLen(1))
		Expect(req.GetGroupBy().GetTagProjection().GetTagFamilies()[0].GetTags()).To(ConsistOf("id"))
	})

	It("ranks TOP N by a tag-targeted aggregation's own output column", func() {
		req, err := transform("SELECT TOP 5 entity_id DESC, id, COUNT(DISTINCT entity_id) FROM MEASURE tag_agg_metrics IN default TIME > '-15m' GROUP BY id")
		Expect(err).To(BeNil())
		Expect(req.GetTop()).NotTo(BeNil())
		Expect(req.GetTop().GetFieldName()).To(Equal("entity_id"))
		Expect(req.GetTop().GetFieldValueSort()).To(Equal(modelv1.Sort_SORT_DESC))
	})
})
