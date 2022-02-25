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

	"github.com/apache/skywalking-banyandb/api/event"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

var _ = Describe("Metadata", func() {
	var svcs *services
	var deferFn func()

	BeforeEach(func() {
		svcs, deferFn = setUp()
	})

	AfterEach(func() {
		deferFn()
	})

	Context("Manage group", func() {
		It("should pass smoke test", func() {
			Eventually(func() bool {
				_, ok := svcs.measure.LoadGroup("default")
				return ok
			}).WithTimeout(10 * time.Second).Should(BeTrue())
		})
		It("should close the group", func() {
			svcs.repo.EXPECT().Publish(event.MeasureTopicShardEvent, test.NewShardEventMatcher(databasev1.Action_ACTION_DELETE)).Times(2)
			deleted, err := svcs.metadataService.GroupRegistry().DeleteGroup(context.TODO(), "default")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(deleted).Should(BeTrue())
			Eventually(func() bool {
				_, ok := svcs.measure.LoadGroup("default")
				return ok
			}).WithTimeout(10 * time.Second).Should(BeFalse())
		})

		It("should add shards", func() {
			svcs.repo.EXPECT().Publish(event.MeasureTopicShardEvent, test.NewShardEventMatcher(databasev1.Action_ACTION_DELETE)).Times(2)
			svcs.repo.EXPECT().Publish(event.MeasureTopicShardEvent, test.NewShardEventMatcher(databasev1.Action_ACTION_PUT)).Times(4)
			groupSchema, err := svcs.metadataService.GroupRegistry().GetGroup(context.TODO(), "default")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(groupSchema).ShouldNot(BeNil())
			groupSchema.ResourceOpts.ShardNum = 4

			Expect(svcs.metadataService.GroupRegistry().UpdateGroup(context.TODO(), groupSchema)).Should(Succeed())

			Eventually(func() bool {
				group, ok := svcs.measure.LoadGroup("default")
				if !ok {
					return false
				}
				return group.GetSchema().GetResourceOpts().GetShardNum() == 4
			}).WithTimeout(10 * time.Second).Should(BeTrue())
		})
	})

	Context("Manage measure", func() {
		It("should pass smoke test", func() {
			Eventually(func() bool {
				_, err := svcs.measure.Measure(&commonv1.Metadata{
					Name:  "cpm",
					Group: "default",
				})
				return err == nil
			}).WithTimeout(10 * time.Second).Should(BeTrue())
		})
		It("should close the measure", func() {
			svcs.repo.EXPECT().Publish(event.MeasureTopicEntityEvent, test.NewEntityEventMatcher(databasev1.Action_ACTION_DELETE)).Times(1)
			deleted, err := svcs.metadataService.MeasureRegistry().DeleteMeasure(context.TODO(), &commonv1.Metadata{
				Name:  "cpm",
				Group: "default",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(deleted).Should(BeTrue())
			Eventually(func() bool {
				_, err := svcs.measure.Measure(&commonv1.Metadata{
					Name:  "cpm",
					Group: "default",
				})
				return err != nil
			}).WithTimeout(10 * time.Second).Should(BeFalse())
		})

		Context("Update a measure", func() {
			var measureSchema *databasev1.Measure

			BeforeEach(func() {
				var err error
				measureSchema, err = svcs.metadataService.MeasureRegistry().GetMeasure(context.TODO(), &commonv1.Metadata{
					Name:  "cpm",
					Group: "default",
				})

				Expect(err).ShouldNot(HaveOccurred())
				Expect(measureSchema).ShouldNot(BeNil())
			})

			It("should update a new measure", func() {
				svcs.repo.EXPECT().Publish(event.MeasureTopicEntityEvent, test.NewEntityEventMatcher(databasev1.Action_ACTION_PUT)).Times(1)
				// Remove the first tag from the entity
				measureSchema.Entity.TagNames = measureSchema.Entity.TagNames[1:]
				entitySize := len(measureSchema.Entity.TagNames)

				Expect(svcs.metadataService.MeasureRegistry().UpdateMeasure(context.TODO(), measureSchema)).Should(Succeed())

				Eventually(func() bool {
					val, err := svcs.measure.Measure(&commonv1.Metadata{
						Name:  "cpm",
						Group: "default",
					})
					if err != nil {
						return false
					}

					return len(val.GetSchema().GetEntity().TagNames) == entitySize
				}).WithTimeout(10 * time.Second).Should(BeTrue())
			})
		})
	})

})
