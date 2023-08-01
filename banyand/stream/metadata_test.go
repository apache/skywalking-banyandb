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

package stream

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

var _ = Describe("Metadata", func() {
	var svcs *services
	var deferFn func()
	var goods []gleak.Goroutine

	BeforeEach(func() {
		goods = gleak.Goroutines()
		svcs, deferFn = setUp()
	})

	AfterEach(func() {
		deferFn()
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	Context("Manage group", func() {
		It("should pass smoke test", func() {
			Eventually(func() bool {
				_, ok := svcs.stream.schemaRepo.LoadGroup("default")
				return ok
			}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
		})
		It("should close the group", func() {
			deleted, err := svcs.metadataService.GroupRegistry().DeleteGroup(context.TODO(), "default")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(deleted).Should(BeTrue())
			Eventually(func() bool {
				_, ok := svcs.stream.schemaRepo.LoadGroup("default")
				return ok
			}).WithTimeout(flags.EventuallyTimeout).Should(BeFalse())
		})

		It("should add shards", func() {
			groupSchema, err := svcs.metadataService.GroupRegistry().GetGroup(context.TODO(), "default")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(groupSchema).ShouldNot(BeNil())
			groupSchema.ResourceOpts.ShardNum = 4

			Expect(svcs.metadataService.GroupRegistry().UpdateGroup(context.TODO(), groupSchema)).Should(Succeed())

			Eventually(func() bool {
				group, ok := svcs.stream.schemaRepo.LoadGroup("default")
				if !ok {
					return false
				}
				return group.GetSchema().GetResourceOpts().GetShardNum() == 4
			}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
		})
	})

	Context("Manage stream", func() {
		It("should pass smoke test", func() {
			Eventually(func() bool {
				_, ok := svcs.stream.schemaRepo.loadStream(&commonv1.Metadata{
					Name:  "sw",
					Group: "default",
				})
				return ok
			}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
		})
		It("should close the stream", func() {
			deleted, err := svcs.metadataService.StreamRegistry().DeleteStream(context.TODO(), &commonv1.Metadata{
				Name:  "sw",
				Group: "default",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(deleted).Should(BeTrue())
			Eventually(func() bool {
				_, ok := svcs.stream.schemaRepo.loadStream(&commonv1.Metadata{
					Name:  "sw",
					Group: "default",
				})
				return ok
			}).WithTimeout(flags.EventuallyTimeout).Should(BeFalse())
		})

		Context("Update a stream", func() {
			var streamSchema *databasev1.Stream

			BeforeEach(func() {
				var err error
				streamSchema, err = svcs.metadataService.StreamRegistry().GetStream(context.TODO(), &commonv1.Metadata{
					Name:  "sw",
					Group: "default",
				})

				Expect(err).ShouldNot(HaveOccurred())
				Expect(streamSchema).ShouldNot(BeNil())
			})

			It("should update a new stream", func() {
				// Remove the first tag from the entity
				streamSchema.Entity.TagNames = streamSchema.Entity.TagNames[1:]
				entitySize := len(streamSchema.Entity.TagNames)

				Expect(svcs.metadataService.StreamRegistry().UpdateStream(context.TODO(), streamSchema)).Should(Succeed())

				Eventually(func() bool {
					val, ok := svcs.stream.schemaRepo.loadStream(&commonv1.Metadata{
						Name:  "sw",
						Group: "default",
					})
					if !ok {
						return false
					}

					return len(val.schema.GetEntity().TagNames) == entitySize
				}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
			})
		})
	})
})
