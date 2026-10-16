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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	. "github.com/apache/skywalking-banyandb/pkg/bydbql"
)

// fakeSchemaSource is a map-backed SchemaSource that counts lookups.
type fakeSchemaSource struct {
	measures map[string]*databasev1.Measure
	streams  map[string]*databasev1.Stream
	traces   map[string]*databasev1.Trace
	lookups  int
}

func (f *fakeSchemaSource) Measure(group, name string) (*databasev1.Measure, bool) {
	f.lookups++
	m, ok := f.measures[group+"/"+name]
	return m, ok
}

func (f *fakeSchemaSource) Stream(group, name string) (*databasev1.Stream, bool) {
	f.lookups++
	s, ok := f.streams[group+"/"+name]
	return s, ok
}

func (f *fakeSchemaSource) Trace(group, name string) (*databasev1.Trace, bool) {
	f.lookups++
	t, ok := f.traces[group+"/"+name]
	return t, ok
}

func streamSourceSchema() *databasev1.Stream {
	return &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: "sw", Group: "default"},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "searchable",
			Tags: []*databasev1.TagSpec{{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING}},
		}},
		Entity: &databasev1.Entity{TagNames: []string{"trace_id"}},
	}
}

func traceSourceSchema() *databasev1.Trace {
	return &databasev1.Trace{
		Metadata: &commonv1.Metadata{Name: "sw_trace", Group: "default"},
		Tags: []*databasev1.TraceTagSpec{
			{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
		},
		TraceIdTagName:   "trace_id",
		TimestampTagName: "timestamp",
	}
}

var _ = Describe("Transform with a local SchemaSource", func() {
	var (
		ctrl   *gomock.Controller
		repo   *metadata.MockRepo
		source *fakeSchemaSource
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		repo = metadata.NewMockRepo(ctrl)
		source = &fakeSchemaSource{
			measures: map[string]*databasev1.Measure{"default/tag_agg_metrics": measureTagAggSchema()},
			streams:  map[string]*databasev1.Stream{"default/sw": streamSourceSchema()},
			traces:   map[string]*databasev1.Trace{"default/sw_trace": traceSourceSchema()},
		}
	})

	transform := func(transformer *Transformer, query string) (proto.Message, error) {
		grammar, parseErr := ParseQuery(query)
		Expect(parseErr).To(BeNil())
		Expect(BindParams(grammar, nil)).To(Succeed())
		result, err := transformer.Transform(context.Background(), grammar)
		if err != nil {
			return nil, err
		}
		return result.QueryRequest, nil
	}

	// The mock repo has no expectations, so any registry call fails the test:
	// a local hit must not reach the registry.
	DescribeTable("serves a hit without calling the registry",
		func(query string, want proto.Message) {
			got, err := transform(NewTransformerWithSource(repo, source), query)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(BeAssignableToTypeOf(want))
			Expect(source.lookups).To(BeNumerically(">", 0))
		},
		Entry("measure", "SELECT entity_id, value FROM MEASURE tag_agg_metrics IN default TIME > '-15m'", &measurev1.QueryRequest{}),
		Entry("stream", "SELECT trace_id FROM STREAM sw IN default TIME > '-15m'", &streamv1.QueryRequest{}),
		Entry("trace", "SELECT trace_id FROM TRACE sw_trace IN default TIME > '-15m'", &tracev1.QueryRequest{}),
	)

	It("produces the same request as the registry path", func() {
		measureRegistry := schema.NewMockMeasure(ctrl)
		measureRegistry.EXPECT().GetMeasure(gomock.Any(), gomock.Any()).Return(measureTagAggSchema(), nil).AnyTimes()
		repo.EXPECT().MeasureRegistry().AnyTimes().Return(measureRegistry)
		query := "SELECT entity_id, value FROM MEASURE tag_agg_metrics IN default TIME > '2024-01-01T00:00:00Z' ORDER BY TIME DESC LIMIT 10"

		fromRegistry, err := transform(NewTransformer(repo), query)
		Expect(err).NotTo(HaveOccurred())
		fromSource, err := transform(NewTransformerWithSource(repo, source), query)
		Expect(err).NotTo(HaveOccurred())
		// TIME > sets end = now, so compare everything but the time range.
		fromRegistry.(*measurev1.QueryRequest).TimeRange = nil
		fromSource.(*measurev1.QueryRequest).TimeRange = nil
		Expect(proto.Equal(fromRegistry, fromSource)).To(BeTrue(), "registry: %v\nsource: %v", fromRegistry, fromSource)
	})

	It("falls back to the registry on a miss", func() {
		source.measures = nil
		measureRegistry := schema.NewMockMeasure(ctrl)
		measureRegistry.EXPECT().GetMeasure(gomock.Any(), gomock.Any()).Return(measureTagAggSchema(), nil).Times(1)
		repo.EXPECT().MeasureRegistry().Return(measureRegistry).Times(1)

		_, err := transform(NewTransformerWithSource(repo, source), "SELECT entity_id FROM MEASURE tag_agg_metrics IN default TIME > '-15m'")
		Expect(err).NotTo(HaveOccurred())
	})

	It("reports the registry error when both miss", func() {
		source.measures = nil
		measureRegistry := schema.NewMockMeasure(ctrl)
		measureRegistry.EXPECT().GetMeasure(gomock.Any(), gomock.Any()).Return(nil, schema.ErrGRPCResourceNotFound)
		repo.EXPECT().MeasureRegistry().Return(measureRegistry)

		_, err := transform(NewTransformerWithSource(repo, source), "SELECT entity_id FROM MEASURE tag_agg_metrics IN default TIME > '-15m'")
		Expect(err).To(MatchError(ContainSubstring("tag_agg_metrics")))
	})
})
