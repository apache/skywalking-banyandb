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

// indexModeLimitFixture is an index-mode measure in a group with a one-day segment interval, with helpers to write rows at a
// time and to run a query that reports how many rows it returned and how much memory it was charged.
type indexModeLimitFixture struct {
	svcs   *services
	metric *commonv1.Metadata
}

func newIndexModeLimitFixture(ctx context.Context, svcs *services, group, measureName string) *indexModeLimitFixture {
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
	return &indexModeLimitFixture{svcs: svcs, metric: metric}
}

// write writes count rows at ts, with the ids endpoint-<firstID> upward.
func (f *indexModeLimitFixture) write(ctx context.Context, ts time.Time, firstID, count int) {
	bp := f.svcs.pipeline.NewBatchPublisher(5 * time.Second)
	for i := 0; i < count; i++ {
		id := &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("endpoint-%05d", firstID+i)}}}
		bp.Publish(ctx, data.TopicMeasureWrite, bus.NewMessage(bus.MessageID(firstID+i+1), &measurev1.InternalWriteRequest{
			EntityValues: []*modelv1.TagValue{id},
			Request: &measurev1.WriteRequest{
				Metadata: f.metric,
				DataPoint: &measurev1.DataPointValue{
					Timestamp:   timestamppb.New(ts),
					Version:     1,
					TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{id}}},
				},
			},
		}))
	}
	cees, err := bp.Close()
	Expect(err).NotTo(HaveOccurred())
	Expect(cees).To(BeEmpty())
}

// run queries [from, to] with the limit and returns the rows it returned and the bytes it was charged.
func (f *indexModeLimitFixture) run(ctx context.Context, from, to time.Time, limit int) (rows int, charged uint64) {
	m, err := f.svcs.measure.Measure(f.metric)
	Expect(err).NotTo(HaveOccurred())
	lease := &chargeRecorder{}
	result, err := m.Query(query.WithBudgetLease(ctx, lease), model.MeasureQueryOptions{
		Name:          f.metric.Name,
		TimeRange:     &timestamp.TimeRange{Start: from, End: to, IncludeStart: true, IncludeEnd: true},
		TagProjection: []model.TagProjection{{Family: defaultTagFamily, Names: []string{"id"}}},
		Limit:         limit,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(result).NotTo(BeNil())
	defer result.Release()
	for r := result.Pull(); r != nil; r = result.Pull() {
		rows += len(r.Timestamps)
	}
	return rows, lease.Used()
}

// An index-mode query with a limit and no order accepts any rows, so it must read and charge about the limit and not
// every matching document, and it must stop at the newest segment that fills the limit.
var _ = Describe("index-mode measure query with a limit and no order", func() {
	const (
		total = 300
		limit = 5
	)

	It("charges the rows it keeps, not every document that matches", func() {
		ctx := context.TODO()
		svcs, deferFn := setUpMigrationTarget(GinkgoT().TempDir())
		defer deferFn()
		fixture := newIndexModeLimitFixture(ctx, svcs, "indexmode_limit_query", "indexmode_limit_metric")
		now := time.Now().Truncate(time.Millisecond)
		fixture.write(ctx, now, 0, total)

		from, to := now.Add(-time.Minute), now.Add(time.Minute)
		var allRows int
		var allCharged uint64
		Eventually(func() int {
			allRows, allCharged = fixture.run(ctx, from, to, 0)
			return allRows
		}).WithTimeout(30 * time.Second).WithPolling(500 * time.Millisecond).Should(Equal(total))

		limitedRows, limitedCharged := fixture.run(ctx, from, to, limit)
		Expect(limitedRows).To(Equal(limit))
		Expect(limitedCharged).To(BeNumerically("<", allCharged/10),
			"a limit of %d charged %d bytes, against %d for all %d rows", limit, limitedCharged, allCharged, allRows)
	})

	It("does not search an older segment once a newer one fills the limit", func() {
		ctx := context.TODO()
		svcs, deferFn := setUpMigrationTarget(GinkgoT().TempDir())
		defer deferFn()
		fixture := newIndexModeLimitFixture(ctx, svcs, "indexmode_limit_segments", "indexmode_limit_segments_metric")
		now := time.Now().Truncate(time.Millisecond)
		older := now.Add(-30 * time.Hour) // a segment interval is a day, so this is a segment of its own
		fixture.write(ctx, now, 0, total)
		fixture.write(ctx, older, total, total)

		both := func(queryLimit int) (int, uint64) {
			return fixture.run(ctx, older.Add(-time.Minute), now.Add(time.Minute), queryLimit)
		}
		Eventually(func() int {
			rows, _ := both(0)
			return rows
		}).WithTimeout(30 * time.Second).WithPolling(500 * time.Millisecond).Should(Equal(2 * total))

		// The newest segment fills the limit on its own, so the older segment is never searched: the query is charged what a
		// query that only spans the newest segment is charged.
		rows, charged := both(limit)
		Expect(rows).To(Equal(limit))
		newestRows, newestCharged := fixture.run(ctx, now.Add(-time.Minute), now.Add(time.Minute), limit)
		Expect(newestRows).To(Equal(limit))
		Expect(charged).To(Equal(newestCharged), "the older segment was searched: %d bytes against %d for the newest segment alone", charged, newestCharged)
	})

	It("reaches into an older segment when the newest cannot fill the limit", func() {
		ctx := context.TODO()
		svcs, deferFn := setUpMigrationTarget(GinkgoT().TempDir())
		defer deferFn()
		fixture := newIndexModeLimitFixture(ctx, svcs, "indexmode_limit_short", "indexmode_limit_short_metric")
		now := time.Now().Truncate(time.Millisecond)
		older := now.Add(-30 * time.Hour)
		fixture.write(ctx, now, 0, 3)
		fixture.write(ctx, older, 3, total)

		both := func(queryLimit int) (int, uint64) {
			return fixture.run(ctx, older.Add(-time.Minute), now.Add(time.Minute), queryLimit)
		}
		Eventually(func() int {
			rows, _ := both(0)
			return rows
		}).WithTimeout(30 * time.Second).WithPolling(500 * time.Millisecond).Should(Equal(3 + total))

		// Three rows in the newest segment are not enough: the two that are missing come from the older one, and the older
		// segment is asked for only those two.
		rows, _ := both(limit)
		Expect(rows).To(Equal(limit))
	})
})
