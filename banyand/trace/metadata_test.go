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

package trace_test

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
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
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
			_, ok := svcs.trace.LoadGroup("test-trace-group")
			return ok
		}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
	})

	AfterEach(func() {
		deferFn()
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	Context("Schema Change", func() {
		var groupName string
		var groupCounter int

		BeforeEach(func() {
			groupCounter++
			groupName = "test-schema-change-" + strconv.Itoa(groupCounter)
			err := svcs.metadataService.GroupRegistry().CreateGroup(context.TODO(), &commonv1.Group{
				Metadata: &commonv1.Metadata{
					Name: groupName,
				},
				Catalog: commonv1.Catalog_CATALOG_TRACE,
				ResourceOpts: &commonv1.ResourceOpts{
					ShardNum: 2,
					SegmentInterval: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  1,
					},
					Ttl: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  7,
					},
				},
			})
			Expect(err).ShouldNot(HaveOccurred())
			Eventually(func() bool {
				_, ok := svcs.trace.LoadGroup(groupName)
				return ok
			}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
		})

		AfterEach(func() {
			_, _ = svcs.metadataService.GroupRegistry().DeleteGroup(context.TODO(), groupName)
		})

		It("deleting reserved tag should fail", func() {
			ctx := context.TODO()
			traceName := "schema_change_reserved_tag"

			initialTrace := &databasev1.Trace{
				Metadata: &commonv1.Metadata{
					Name:  traceName,
					Group: groupName,
				},
				Tags: []*databasev1.TraceTagSpec{
					{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
					{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
				},
				TraceIdTagName:   "trace_id",
				SpanIdTagName:    "span_id",
				TimestampTagName: "timestamp",
			}
			_, err := svcs.metadataService.TraceRegistry().CreateTrace(ctx, initialTrace)
			Expect(err).ShouldNot(HaveOccurred())

			Eventually(func() bool {
				_, traceErr := svcs.trace.Trace(&commonv1.Metadata{
					Name:  traceName,
					Group: groupName,
				})
				return traceErr == nil
			}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())

			updatedTrace := &databasev1.Trace{
				Metadata: &commonv1.Metadata{
					Name:  traceName,
					Group: groupName,
				},
				Tags: []*databasev1.TraceTagSpec{
					{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
					{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
				},
				TraceIdTagName:   "trace_id",
				SpanIdTagName:    "span_id",
				TimestampTagName: "timestamp",
			}
			_, err = svcs.metadataService.TraceRegistry().UpdateTrace(ctx, updatedTrace)
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot delete reserved tag"))
			Expect(err.Error()).To(ContainSubstring("trace_id"))
		})

		It("deleting non-reserved tag should succeed", func() {
			ctx := context.TODO()
			traceName := "schema_change_delete_non_reserved"

			initialTrace := &databasev1.Trace{
				Metadata: &commonv1.Metadata{
					Name:  traceName,
					Group: groupName,
				},
				Tags: []*databasev1.TraceTagSpec{
					{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
					{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
				},
				TraceIdTagName:   "trace_id",
				SpanIdTagName:    "span_id",
				TimestampTagName: "timestamp",
			}
			_, err := svcs.metadataService.TraceRegistry().CreateTrace(ctx, initialTrace)
			Expect(err).ShouldNot(HaveOccurred())

			Eventually(func() bool {
				_, traceErr := svcs.trace.Trace(&commonv1.Metadata{
					Name:  traceName,
					Group: groupName,
				})
				return traceErr == nil
			}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())

			updatedTrace := &databasev1.Trace{
				Metadata: &commonv1.Metadata{
					Name:  traceName,
					Group: groupName,
				},
				Tags: []*databasev1.TraceTagSpec{
					{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
					{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
				TraceIdTagName:   "trace_id",
				SpanIdTagName:    "span_id",
				TimestampTagName: "timestamp",
			}
			_, err = svcs.metadataService.TraceRegistry().UpdateTrace(ctx, updatedTrace)
			Expect(err).ShouldNot(HaveOccurred())

			fetchedTrace, err := svcs.metadataService.TraceRegistry().GetTrace(ctx, &commonv1.Metadata{
				Name:  traceName,
				Group: groupName,
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(fetchedTrace.Tags).To(HaveLen(4))
		})

		It("adding new tag should succeed", func() {
			ctx := context.TODO()
			traceName := "schema_change_add_tag"

			initialTrace := &databasev1.Trace{
				Metadata: &commonv1.Metadata{
					Name:  traceName,
					Group: groupName,
				},
				Tags: []*databasev1.TraceTagSpec{
					{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
					{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
				TraceIdTagName:   "trace_id",
				SpanIdTagName:    "span_id",
				TimestampTagName: "timestamp",
			}
			_, err := svcs.metadataService.TraceRegistry().CreateTrace(ctx, initialTrace)
			Expect(err).ShouldNot(HaveOccurred())

			Eventually(func() bool {
				_, traceErr := svcs.trace.Trace(&commonv1.Metadata{
					Name:  traceName,
					Group: groupName,
				})
				return traceErr == nil
			}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())

			updatedTrace := &databasev1.Trace{
				Metadata: &commonv1.Metadata{
					Name:  traceName,
					Group: groupName,
				},
				Tags: []*databasev1.TraceTagSpec{
					{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
					{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "new_tag", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
				TraceIdTagName:   "trace_id",
				SpanIdTagName:    "span_id",
				TimestampTagName: "timestamp",
			}
			_, err = svcs.metadataService.TraceRegistry().UpdateTrace(ctx, updatedTrace)
			Expect(err).ShouldNot(HaveOccurred())

			fetchedTrace, err := svcs.metadataService.TraceRegistry().GetTrace(ctx, &commonv1.Metadata{
				Name:  traceName,
				Group: groupName,
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(fetchedTrace.Tags).To(HaveLen(5))
		})

		Context("Trace schema with deleted tag", func() {
			It("querying data should succeed after a tag is deleted", func() {
				traceName := "schema_change_deleted_tag"
				now := timestamp.NowMilli()

				env := setupSchemaChangeTrace(svcs, traceName, groupName, traceSetupOptions{withExtraTag: true})
				writeSchemaChangeTraceData(svcs, traceName, groupName, now.Add(-2*time.Hour), 5, writeTraceDataOptions{extraTag: extraTagInt})
				deleteTraceExtraTag(svcs, traceName, groupName)
				writeSchemaChangeTraceData(svcs, traceName, groupName, now.Add(-1*time.Hour), 3, writeTraceDataOptions{})

				Eventually(func(innerGm Gomega) {
					spans := querySchemaChangeTraceData(svcs, traceName, groupName, now.Add(-3*time.Hour), now,
						[]string{"trace_id", "service_id", "duration"}, nil)
					innerGm.Expect(spans).To(HaveLen(8))

					for _, span := range spans {
						for _, tag := range span.Tags {
							innerGm.Expect(tag.Key).NotTo(Equal("extra_tag"),
								"deleted tag should not be returned in query results")
						}
					}
				}, flags.EventuallyTimeout).Should(Succeed())

				env.cleanup()
			})
		})

		Context("Trace schema with added tag", func() {
			It("querying data should succeed after a new tag is added", func() {
				traceName := "schema_change_added_tag"
				now := timestamp.NowMilli()

				env := setupSchemaChangeTrace(svcs, traceName, groupName, traceSetupOptions{})
				writeSchemaChangeTraceData(svcs, traceName, groupName, now.Add(-2*time.Hour), 5, writeTraceDataOptions{})
				addTraceExtraTag(svcs, traceName, groupName)
				writeSchemaChangeTraceData(svcs, traceName, groupName, now.Add(-1*time.Hour), 3, writeTraceDataOptions{extraTag: extraTagInt})

				Eventually(func(innerGm Gomega) {
					spans := querySchemaChangeTraceData(svcs, traceName, groupName, now.Add(-3*time.Hour), now,
						[]string{"trace_id", "service_id", "duration", "extra_tag"}, nil)
					innerGm.Expect(spans).To(HaveLen(8))

					oldDataCount := 0
					newDataCount := 0
					for _, span := range spans {
						for _, tag := range span.Tags {
							if tag.Key == "extra_tag" {
								if tag.Value.GetInt() != nil {
									newDataCount++
								} else {
									oldDataCount++
								}
							}
						}
					}
					innerGm.Expect(oldDataCount).To(Equal(5), "old data should have null extra_tag")
					innerGm.Expect(newDataCount).To(Equal(3), "new data should have extra_tag values")
				}, flags.EventuallyTimeout).Should(Succeed())

				env.cleanup()
			})
		})

		Context("Trace schema with changed tag type", func() {
			It("querying data should return null for type-mismatched tags", func() {
				traceName := "schema_change_tag_type"
				now := timestamp.NowMilli()

				env := setupSchemaChangeTrace(svcs, traceName, groupName, traceSetupOptions{withExtraTag: true})
				writeSchemaChangeTraceData(svcs, traceName, groupName, now.Add(-2*time.Hour), 5, writeTraceDataOptions{extraTag: extraTagInt})
				changeTraceExtraTagType(svcs, traceName, groupName)
				writeSchemaChangeTraceData(svcs, traceName, groupName, now.Add(-1*time.Hour), 3, writeTraceDataOptions{extraTag: extraTagString, traceIDPrefix: "trace_new_"})

				Eventually(func(innerGm Gomega) {
					spans := querySchemaChangeTraceData(svcs, traceName, groupName, now.Add(-3*time.Hour), now,
						[]string{"trace_id", "service_id", "duration", "extra_tag"}, nil)
					innerGm.Expect(spans).To(HaveLen(8))

					nullCount := 0
					stringCount := 0
					for _, span := range spans {
						for _, tag := range span.Tags {
							if tag.Key == "extra_tag" {
								switch tag.Value.GetValue().(type) {
								case *modelv1.TagValue_Null:
									nullCount++
								case *modelv1.TagValue_Str:
									stringCount++
								}
							}
						}
					}
					innerGm.Expect(nullCount).To(Equal(5), "old data with INT type should return null after schema changed to STRING")
					innerGm.Expect(stringCount).To(Equal(3), "new data should have STRING extra_tag values")
				}, flags.EventuallyTimeout).Should(Succeed())

				env.cleanup()
			})
		})

		Context("Trace schema with deleted tag in query", func() {
			It("querying data should fail if the condition includes a deleted tag", func() {
				traceName := "schema_change_filter_deleted"
				now := timestamp.NowMilli()

				env := setupSchemaChangeTrace(svcs, traceName, groupName, traceSetupOptions{withExtraTag: true})
				writeSchemaChangeTraceData(svcs, traceName, groupName, now.Add(-2*time.Hour), 5, writeTraceDataOptions{extraTag: extraTagInt})
				deleteTraceExtraTag(svcs, traceName, groupName)
				writeSchemaChangeTraceData(svcs, traceName, groupName, now.Add(-1*time.Hour), 3, writeTraceDataOptions{})

				Eventually(func(innerGm Gomega) {
					err := queryWithDeletedTagCondition(svcs, traceName, groupName, now)
					innerGm.Expect(err).To(HaveOccurred())
					innerGm.Expect(err.Error()).To(ContainSubstring("extra_tag"))
				}, flags.EventuallyTimeout).Should(Succeed())

				env.cleanup()
			})

			It("querying data should fail if the projection includes a deleted tag", func() {
				traceName := "schema_change_projection_deleted"
				now := timestamp.NowMilli()

				env := setupSchemaChangeTrace(svcs, traceName, groupName, traceSetupOptions{withExtraTag: true})
				writeSchemaChangeTraceData(svcs, traceName, groupName, now.Add(-2*time.Hour), 5, writeTraceDataOptions{extraTag: extraTagInt})
				deleteTraceExtraTag(svcs, traceName, groupName)
				writeSchemaChangeTraceData(svcs, traceName, groupName, now.Add(-1*time.Hour), 3, writeTraceDataOptions{})

				Eventually(func(innerGm Gomega) {
					err := queryWithDeletedTagProjection(svcs, traceName, groupName, now)
					innerGm.Expect(err).To(HaveOccurred())
					innerGm.Expect(err.Error()).To(ContainSubstring("extra_tag"))
				}, flags.EventuallyTimeout).Should(Succeed())

				env.cleanup()
			})
		})
	})
})

type extraTagType int

const (
	extraTagNone extraTagType = iota
	extraTagInt
	extraTagString
)

type schemaChangeEnv struct {
	cleanup func()
}

type traceSetupOptions struct {
	withExtraTag bool
}

func setupSchemaChangeTrace(svcs *services, traceName, groupName string, opts traceSetupOptions) *schemaChangeEnv {
	ctx := context.TODO()
	tags := []*databasev1.TraceTagSpec{
		{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
		{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
	}
	if opts.withExtraTag {
		tags = append(tags, &databasev1.TraceTagSpec{Name: "extra_tag", Type: databasev1.TagType_TAG_TYPE_INT})
	}

	initialTrace := &databasev1.Trace{
		Metadata: &commonv1.Metadata{
			Name:  traceName,
			Group: groupName,
		},
		Tags:             tags,
		TraceIdTagName:   "trace_id",
		SpanIdTagName:    "span_id",
		TimestampTagName: "timestamp",
	}
	_, err := svcs.metadataService.TraceRegistry().CreateTrace(ctx, initialTrace)
	Expect(err).ShouldNot(HaveOccurred())

	// Create index rule for duration
	indexRuleName := traceName + "-duration"
	indexRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{
			Name:  indexRuleName,
			Group: groupName,
		},
		Tags: []string{"duration"},
		Type: databasev1.IndexRule_TYPE_TREE,
	}
	err = svcs.metadataService.IndexRuleRegistry().CreateIndexRule(ctx, indexRule)
	Expect(err).ShouldNot(HaveOccurred())

	// Create index rule binding
	indexRuleBindingName := traceName + "-index-rule-binding"
	indexRuleBinding := &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{
			Name:  indexRuleBindingName,
			Group: groupName,
		},
		Rules: []string{indexRuleName},
		Subject: &databasev1.Subject{
			Catalog: commonv1.Catalog_CATALOG_TRACE,
			Name:    traceName,
		},
		BeginAt:  timestamppb.Now(),
		ExpireAt: timestamppb.New(time.Now().Add(24 * time.Hour)),
	}
	err = svcs.metadataService.IndexRuleBindingRegistry().CreateIndexRuleBinding(ctx, indexRuleBinding)
	Expect(err).ShouldNot(HaveOccurred())

	Eventually(func() bool {
		_, err := svcs.trace.Trace(&commonv1.Metadata{
			Name:  traceName,
			Group: groupName,
		})
		return err == nil
	}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())

	return &schemaChangeEnv{
		cleanup: func() {
			_, _ = svcs.metadataService.IndexRuleBindingRegistry().DeleteIndexRuleBinding(ctx, &commonv1.Metadata{
				Name:  indexRuleBindingName,
				Group: groupName,
			})
			_, _ = svcs.metadataService.IndexRuleRegistry().DeleteIndexRule(ctx, &commonv1.Metadata{
				Name:  indexRuleName,
				Group: groupName,
			})
			_, _ = svcs.metadataService.TraceRegistry().DeleteTrace(ctx, &commonv1.Metadata{
				Name:  traceName,
				Group: groupName,
			})
		},
	}
}

func updateTraceSchema(svcs *services, traceName, groupName string, updateFn func(*databasev1.Trace)) {
	ctx := context.TODO()
	traceSchema, err := svcs.metadataService.TraceRegistry().GetTrace(ctx, &commonv1.Metadata{
		Name:  traceName,
		Group: groupName,
	})
	Expect(err).ShouldNot(HaveOccurred())
	updateFn(traceSchema)
	_, err = svcs.metadataService.TraceRegistry().UpdateTrace(ctx, traceSchema)
	Expect(err).ShouldNot(HaveOccurred())
	time.Sleep(2 * time.Second)
}

func deleteTraceExtraTag(svcs *services, traceName, groupName string) {
	updateTraceSchema(svcs, traceName, groupName, func(t *databasev1.Trace) {
		t.Tags = []*databasev1.TraceTagSpec{
			{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
			{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
		}
	})
}

func addTraceExtraTag(svcs *services, traceName, groupName string) {
	updateTraceSchema(svcs, traceName, groupName, func(t *databasev1.Trace) {
		t.Tags = append(t.Tags,
			&databasev1.TraceTagSpec{Name: "extra_tag", Type: databasev1.TagType_TAG_TYPE_INT})
	})
}

func changeTraceExtraTagType(svcs *services, traceName, groupName string) {
	updateTraceSchema(svcs, traceName, groupName, func(t *databasev1.Trace) {
		t.Tags = []*databasev1.TraceTagSpec{
			{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
			{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
			{Name: "extra_tag", Type: databasev1.TagType_TAG_TYPE_STRING},
		}
	})
}

func executeTraceQuery(svcs *services, req *tracev1.QueryRequest) error {
	feat, err := svcs.pipeline.Publish(context.Background(), data.TopicTraceQuery, bus.NewMessage(bus.MessageID(time.Now().UnixNano()), req))
	if err != nil {
		return err
	}
	msg, err := feat.Get()
	if err != nil {
		return err
	}
	if e, ok := msg.Data().(*common.Error); ok {
		return e
	}
	return nil
}

func queryWithDeletedTagCondition(svcs *services, traceName, groupName string, now time.Time) error {
	return executeTraceQuery(svcs, &tracev1.QueryRequest{
		Groups: []string{groupName},
		Name:   traceName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(now.Add(-3 * time.Hour)),
			End:   timestamppb.New(now),
		},
		Criteria: &modelv1.Criteria{
			Exp: &modelv1.Criteria_Condition{
				Condition: &modelv1.Condition{
					Name: "extra_tag",
					Op:   modelv1.Condition_BINARY_OP_EQ,
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 200}},
					},
				},
			},
		},
		TagProjection: []string{"trace_id", "service_id"},
		OrderBy: &modelv1.QueryOrder{
			IndexRuleName: traceName + "-duration",
			Sort:          modelv1.Sort_SORT_DESC,
		},
	})
}

func queryWithDeletedTagProjection(svcs *services, traceName, groupName string, now time.Time) error {
	return executeTraceQuery(svcs, &tracev1.QueryRequest{
		Groups: []string{groupName},
		Name:   traceName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(now.Add(-3 * time.Hour)),
			End:   timestamppb.New(now),
		},
		TagProjection: []string{"trace_id", "service_id", "extra_tag"},
		OrderBy: &modelv1.QueryOrder{
			IndexRuleName: traceName + "-duration",
			Sort:          modelv1.Sort_SORT_DESC,
		},
	})
}

type writeTraceDataOptions struct {
	traceIDPrefix string
	extraTag      extraTagType
	spanIDOffset  int
}

func writeSchemaChangeTraceData(svcs *services, name, group string, baseTime time.Time, count int, opts writeTraceDataOptions) {
	bp := svcs.pipeline.NewBatchPublisher(5 * time.Second)
	defer bp.Close()
	interval := 500 * time.Millisecond

	tracePrefix := opts.traceIDPrefix
	if tracePrefix == "" {
		tracePrefix = "trace_"
	}

	for i := 0; i < count; i++ {
		tags := []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: tracePrefix + strconv.Itoa(i)}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "span_" + strconv.Itoa(opts.spanIDOffset+i)}}},
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(baseTime.Add(interval * time.Duration(i)))}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "service_1"}}},
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(100 * (i + 1))}}},
		}
		switch opts.extraTag {
		case extraTagNone:
			// No extra tag
		case extraTagInt:
			tags = append(tags,
				&modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(200 + i)}}})
		case extraTagString:
			tags = append(tags,
				&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "extra_" + strconv.Itoa(i)}}})
		}

		tagNames := []string{"trace_id", "span_id", "timestamp", "service_id", "duration"}
		if opts.extraTag != extraTagNone {
			tagNames = append(tagNames, "extra_tag")
		}

		req := &tracev1.WriteRequest{
			Metadata: &commonv1.Metadata{Name: name, Group: group},
			Tags:     tags,
			Span:     []byte("span_data_" + strconv.Itoa(i)),
			TagSpec:  &tracev1.TagSpec{TagNames: tagNames},
		}
		bp.Publish(context.TODO(), data.TopicTraceWrite, bus.NewMessage(bus.MessageID(time.Now().UnixNano()+int64(i)), &tracev1.InternalWriteRequest{
			Request: req,
		}))
	}
}

func querySchemaChangeTraceData(svcs *services, name, group string, begin, end time.Time, tags []string, criteria *modelv1.Criteria) []*tracev1.Span {
	req := &tracev1.QueryRequest{
		Groups: []string{group},
		Name:   name,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(begin),
			End:   timestamppb.New(end),
		},
		TagProjection: tags,
		OrderBy: &modelv1.QueryOrder{
			IndexRuleName: name + "-duration",
			Sort:          modelv1.Sort_SORT_DESC,
		},
	}
	if criteria != nil {
		req.Criteria = criteria
	}
	var spans []*tracev1.Span
	Eventually(func() bool {
		feat, err := svcs.pipeline.Publish(context.Background(), data.TopicTraceQuery, bus.NewMessage(bus.MessageID(time.Now().UnixNano()), req))
		Expect(err).ShouldNot(HaveOccurred())
		msg, err := feat.Get()
		Expect(err).ShouldNot(HaveOccurred())
		respData := msg.Data()
		switch d := respData.(type) {
		case *tracev1.InternalQueryResponse:
			spans = nil
			for _, trace := range d.InternalTraces {
				spans = append(spans, trace.Spans...)
			}
			return true
		case *common.Error:
			GinkgoWriter.Printf("query error: %s\n", d.Error())
			return false
		default:
			GinkgoWriter.Printf("unexpected data type: %T\n", respData)
			Fail("unexpected data type")
		}
		return false
	}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
	return spans
}
