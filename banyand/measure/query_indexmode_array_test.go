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
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

// An index-mode measure stores an array tag as one document field per element,
// so the query has to fold every element back into the array.
var _ = Describe("index-mode measure query of array tags", func() {
	const (
		group       = "indexmode_array_query"
		measureName = "indexmode_array_metric"
		ruleName    = "indexmode_array_labels_rule"
	)

	It("returns every element of indexed and stored array tags", func() {
		ctx := context.TODO()
		svcs, deferFn := setUpMigrationTarget(GinkgoT().TempDir())
		defer deferFn()
		day := &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1}
		_, err := svcs.metadataService.GroupRegistry().CreateGroup(ctx, &commonv1.Group{
			Metadata:     &commonv1.Metadata{Name: group},
			Catalog:      commonv1.Catalog_CATALOG_MEASURE,
			ResourceOpts: &commonv1.ResourceOpts{ShardNum: 1, SegmentInterval: day, Ttl: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7}},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = svcs.metadataService.MeasureRegistry().CreateMeasure(ctx, &databasev1.Measure{
			Metadata:  &commonv1.Metadata{Name: measureName, Group: group},
			IndexMode: true,
			Entity:    &databasev1.Entity{TagNames: []string{"id"}},
			TagFamilies: []*databasev1.TagFamilySpec{{Name: defaultTagFamily, Tags: []*databasev1.TagSpec{
				{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "labels", Type: databasev1.TagType_TAG_TYPE_STRING_ARRAY},
				{Name: "tags", Type: databasev1.TagType_TAG_TYPE_STRING_ARRAY},
				{Name: "codes", Type: databasev1.TagType_TAG_TYPE_INT_ARRAY},
			}}},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = svcs.metadataService.IndexRuleRegistry().CreateIndexRule(ctx, &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: ruleName, Group: group},
			Tags:     []string{"labels"},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
			Analyzer: index.AnalyzerKeyword,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = svcs.metadataService.IndexRuleBindingRegistry().CreateIndexRuleBinding(ctx, &databasev1.IndexRuleBinding{
			Metadata: &commonv1.Metadata{Name: "indexmode_array_binding", Group: group},
			Rules:    []string{ruleName},
			Subject:  &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: measureName},
			BeginAt:  timestamppb.New(time.Now().Add(-time.Hour)),
			ExpireAt: timestamppb.New(time.Now().Add(24 * time.Hour)),
		})
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() []string {
			return measureIndexRuleNames(svcs, group, measureName)
		}).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(ContainElement(ruleName))

		now := time.Now().Truncate(time.Millisecond)
		strArr := func(v ...string) *modelv1.TagValue {
			return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: v}}}
		}
		intArr := func(v ...int64) *modelv1.TagValue {
			return &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: v}}}
		}
		id := &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "id-0"}}}
		bp := svcs.pipeline.NewBatchPublisher(5 * time.Second)
		bp.Publish(ctx, data.TopicMeasureWrite, bus.NewMessage(bus.MessageID(1), &measurev1.InternalWriteRequest{
			EntityValues: []*modelv1.TagValue{id},
			Request: &measurev1.WriteRequest{
				Metadata: &commonv1.Metadata{Name: measureName, Group: group},
				DataPoint: &measurev1.DataPointValue{
					Timestamp: timestamppb.New(now),
					Version:   1,
					TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
						id, strArr("a", "b|c", `d\e`), strArr("solo"), intArr(1, -2, 3),
					}}},
				},
			},
		}))
		cees, err := bp.Close()
		Expect(err).NotTo(HaveOccurred())
		Expect(cees).To(BeEmpty())

		want := map[string]*modelv1.TagValue{
			"id": id, "labels": strArr("a", "b|c", `d\e`), "tags": strArr("solo"), "codes": intArr(1, -2, 3),
		}
		Eventually(func(g Gomega) {
			req := &measurev1.QueryRequest{
				Groups:    []string{group},
				Name:      measureName,
				TimeRange: &modelv1.TimeRange{Begin: timestamppb.New(now.Add(-time.Minute)), End: timestamppb.New(now.Add(time.Minute))},
				TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
					{Name: defaultTagFamily, Tags: []string{"id", "labels", "tags", "codes"}},
				}},
			}
			feat, pubErr := svcs.pipeline.Publish(ctx, data.TopicMeasureQuery, bus.NewMessage(bus.MessageID(time.Now().UnixNano()), req))
			g.Expect(pubErr).NotTo(HaveOccurred())
			msg, getErr := feat.Get()
			g.Expect(getErr).NotTo(HaveOccurred())
			if e, isErr := msg.Data().(*common.Error); isErr {
				g.Expect(e.Error()).To(BeEmpty())
			}
			resp, ok := msg.Data().(*measurev1.QueryResponse)
			g.Expect(ok).To(BeTrue())
			g.Expect(resp.DataPoints).To(HaveLen(1))
			got := map[string]*modelv1.TagValue{}
			for _, tag := range resp.DataPoints[0].GetTagFamilies()[0].GetTags() {
				got[tag.GetKey()] = tag.GetValue()
			}
			for key, value := range want {
				g.Expect(got[key].String()).To(Equal(value.String()), key)
			}
		}).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
	})
})
