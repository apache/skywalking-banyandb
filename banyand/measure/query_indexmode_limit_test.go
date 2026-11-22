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
	"fmt"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// chargeRecorder is a query lease that only counts what it is charged.
type chargeRecorder struct{ used atomic.Uint64 }

func (c *chargeRecorder) Charge(bytes uint64) error { c.used.Add(bytes); return nil }
func (c *chargeRecorder) Refund(uint64)             {}
func (c *chargeRecorder) Limit() uint64             { return 0 }
func (c *chargeRecorder) Used() uint64              { return c.used.Load() }
func (c *chargeRecorder) Owner() any                { return c }

// An index-mode query with a limit and no order accepts any rows, so it must read and charge about the limit and not
// every matching document.
var _ = Describe("index-mode measure query with a limit and no order", func() {
	const (
		group       = "indexmode_limit_query"
		measureName = "indexmode_limit_metric"
		total       = 300
		limit       = 5
	)

	It("charges the rows it keeps, not every document that matches", func() {
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
			}}},
		})
		Expect(err).NotTo(HaveOccurred())

		metric := &commonv1.Metadata{Name: measureName, Group: group}
		Eventually(func() error {
			_, getErr := svcs.measure.Measure(metric)
			return getErr
		}).WithTimeout(30 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		now := time.Now().Truncate(time.Millisecond)
		bp := svcs.pipeline.NewBatchPublisher(5 * time.Second)
		for i := 0; i < total; i++ {
			id := &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("endpoint-%04d", i)}}}
			bp.Publish(ctx, data.TopicMeasureWrite, bus.NewMessage(bus.MessageID(i+1), &measurev1.InternalWriteRequest{
				EntityValues: []*modelv1.TagValue{id},
				Request: &measurev1.WriteRequest{
					Metadata: &commonv1.Metadata{Name: measureName, Group: group},
					DataPoint: &measurev1.DataPointValue{
						Timestamp:   timestamppb.New(now),
						Version:     1,
						TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{id}}},
					},
				},
			}))
		}
		cees, err := bp.Close()
		Expect(err).NotTo(HaveOccurred())
		Expect(cees).To(BeEmpty())

		run := func(queryLimit int) (rows int, charged uint64) {
			m, getErr := svcs.measure.Measure(metric)
			Expect(getErr).NotTo(HaveOccurred())
			lease := &chargeRecorder{}
			result, queryErr := m.Query(query.WithBudgetLease(ctx, lease), model.MeasureQueryOptions{
				Name:      measureName,
				TimeRange: &timestamp.TimeRange{Start: now.Add(-time.Minute), End: now.Add(time.Minute), IncludeStart: true, IncludeEnd: true},
				TagProjection: []model.TagProjection{
					{Family: defaultTagFamily, Names: []string{"id"}},
				},
				Limit: queryLimit,
			})
			Expect(queryErr).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			defer result.Release()
			for r := result.Pull(); r != nil; r = result.Pull() {
				rows += len(r.Timestamps)
			}
			return rows, lease.Used()
		}
		var allRows int
		var allCharged uint64
		Eventually(func() int {
			allRows, allCharged = run(0)
			return allRows
		}).WithTimeout(30 * time.Second).WithPolling(500 * time.Millisecond).Should(Equal(total))

		limitedRows, limitedCharged := run(limit)
		Expect(limitedRows).To(BeNumerically(">=", limit))
		Expect(limitedRows).To(BeNumerically("<", allRows))
		Expect(limitedCharged).To(BeNumerically("<", allCharged/10),
			"a limit of %d charged %d bytes, against %d for all %d rows", limit, limitedCharged, allCharged, allRows)
	})
})
