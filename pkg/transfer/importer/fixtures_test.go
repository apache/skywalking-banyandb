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

package importer

import (
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/schema/topnschema"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

var (
	artifactTime = timestamppb.New(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
	targetTime   = timestamppb.New(time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC))
)

func md(group, name string) *commonv1.Metadata {
	return &commonv1.Metadata{Group: group, Name: name, Id: 1, CreateRevision: 10, ModRevision: 20}
}

func testGroup(name string, catalog commonv1.Catalog) *commonv1.Group {
	g := &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: name, CreateRevision: 1, ModRevision: 2},
		Catalog:  catalog, UpdatedAt: artifactTime, CreatedAt: artifactTime,
	}
	if catalog == commonv1.Catalog_CATALOG_PROPERTY {
		g.ResourceOpts = &commonv1.ResourceOpts{ShardNum: 2}
		return g
	}
	g.ResourceOpts = &commonv1.ResourceOpts{
		ShardNum: 2, SegmentInterval: dayRule(1), Ttl: dayRule(3),
		Stages: []*commonv1.LifecycleStage{{Name: "warm", ShardNum: 1, SegmentInterval: dayRule(3), Ttl: dayRule(7), NodeSelector: "type=warm"}},
	}
	return g
}

func testStream(group, name string) *databasev1.Stream {
	return &databasev1.Stream{
		Metadata: md(group, name),
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "latency", Type: databasev1.TagType_TAG_TYPE_INT},
		}}},
		Entity:    &databasev1.Entity{TagNames: []string{"svc"}},
		UpdatedAt: artifactTime, CreatedAt: artifactTime,
	}
}

func testMeasure(group, name string) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: md(group, name),
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "entity_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "tier", Type: databasev1.TagType_TAG_TYPE_STRING},
		}}},
		Fields: []*databasev1.FieldSpec{{
			Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT,
			EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		}},
		Entity:    &databasev1.Entity{TagNames: []string{"entity_id"}},
		Interval:  "1m",
		UpdatedAt: artifactTime, CreatedAt: artifactTime,
	}
}

func testTopNResult(group string) *databasev1.Measure {
	return topnschema.Measure(md(group, csvfmt.TopNResultMeasure))
}

func testTrace(group, name string) *databasev1.Trace {
	return &databasev1.Trace{
		Metadata: md(group, name),
		Tags: []*databasev1.TraceTagSpec{
			{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "ts", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
		},
		TraceIdTagName: "trace_id", SpanIdTagName: "span_id", TimestampTagName: "ts",
		UpdatedAt: artifactTime, CreatedAt: artifactTime,
	}
}

func testProperty(group, name string) *databasev1.Property {
	return &databasev1.Property{
		Metadata:  md(group, name),
		Tags:      []*databasev1.TagSpec{{Name: "content", Type: databasev1.TagType_TAG_TYPE_STRING}},
		UpdatedAt: artifactTime, CreatedAt: artifactTime,
	}
}

func testIndexRule(group, name string) *databasev1.IndexRule {
	return &databasev1.IndexRule{
		Metadata: md(group, name), Tags: []string{"tier"}, Type: databasev1.IndexRule_TYPE_INVERTED,
		UpdatedAt: artifactTime, CreatedAt: artifactTime,
	}
}

func testBinding(group, name string) *databasev1.IndexRuleBinding {
	return &databasev1.IndexRuleBinding{
		Metadata: md(group, name), Rules: []string{"tier"},
		Subject: &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: "cpm"},
		BeginAt: artifactTime, ExpireAt: timestamppb.New(time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)),
		UpdatedAt: artifactTime, CreatedAt: artifactTime,
	}
}

func testTopN(group, name string) *databasev1.TopNAggregation {
	return &databasev1.TopNAggregation{
		Metadata: md(group, name), SourceMeasure: &commonv1.Metadata{Group: group, Name: "cpm"},
		FieldName: "value", CountersNumber: 100, LruSize: 10,
		UpdatedAt: artifactTime, CreatedAt: artifactTime,
	}
}

// artifactSchemas is a full artifact schema: a measure group with every measure-side kind,
// a stream, a trace and a property group.
func artifactSchemas() *Schemas {
	s := NewSchemas()
	for _, g := range []*commonv1.Group{
		testGroup("sw_metric", commonv1.Catalog_CATALOG_MEASURE), testGroup("sw_stream", commonv1.Catalog_CATALOG_STREAM),
		testGroup("sw_trace", commonv1.Catalog_CATALOG_TRACE), testGroup("sw_prop", commonv1.Catalog_CATALOG_PROPERTY),
	} {
		s.Groups[g.GetMetadata().GetName()] = g
	}
	s.Measures[ResourceKey{"sw_metric", "cpm"}] = testMeasure("sw_metric", "cpm")
	s.IndexRules[ResourceKey{"sw_metric", "tier"}] = testIndexRule("sw_metric", "tier")
	s.IndexRuleBindings[ResourceKey{"sw_metric", "cpm"}] = testBinding("sw_metric", "cpm")
	s.TopNs[ResourceKey{"sw_metric", "top"}] = testTopN("sw_metric", "top")
	s.Streams[ResourceKey{"sw_stream", "log"}] = testStream("sw_stream", "log")
	s.Traces[ResourceKey{"sw_trace", "segment"}] = testTrace("sw_trace", "segment")
	s.Properties[ResourceKey{"sw_prop", "ui"}] = testProperty("sw_prop", "ui")
	return &s
}

// mirrorTarget is a target holding a copy of every artifact resource, stamped with other
// revisions and times as another cluster would, and known to be empty.
func mirrorTarget(art *Schemas) *TargetState {
	t := NewTargetState()
	for name, g := range art.Groups {
		t.Groups[name] = restamp(g).(*commonv1.Group)
		t.NonEmptyGroups[name] = NonEmptyState{Known: true}
	}
	mirror(t.Streams, art.Streams)
	mirror(t.Measures, art.Measures)
	mirror(t.Traces, art.Traces)
	mirror(t.Properties, art.Properties)
	mirror(t.IndexRules, art.IndexRules)
	mirror(t.IndexRuleBindings, art.IndexRuleBindings)
	mirror(t.TopNs, art.TopNs)
	return t
}

func mirror[T proto.Message](dst, src map[ResourceKey]T) {
	for k, v := range src {
		dst[k] = restamp(v).(T)
	}
}

// restamp clones a resource with the target cluster's times, revisions and id.
func restamp(m proto.Message) proto.Message {
	out := proto.Clone(m)
	r := out.ProtoReflect()
	for _, name := range []protoreflect.Name{"updated_at", "created_at"} {
		if fd := r.Descriptor().Fields().ByName(name); fd != nil {
			r.Set(fd, protoreflect.ValueOfMessage(proto.Clone(targetTime).ProtoReflect()))
		}
	}
	mdFD := r.Descriptor().Fields().ByName("metadata")
	meta := r.Mutable(mdFD).Message().Interface().(*commonv1.Metadata)
	meta.Id, meta.CreateRevision, meta.ModRevision = meta.Id+7, 500, 600
	return out
}
