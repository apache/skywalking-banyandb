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

package schema

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

func loadStream() *databasev1.Stream {
	s := &databasev1.Stream{}
	// preload stream
	gomega.Expect(protojson.Unmarshal([]byte(streamJSON), s)).To(gomega.Succeed())
	return s
}

func loadIndexRuleBinding() *databasev1.IndexRuleBinding {
	irb := &databasev1.IndexRuleBinding{}
	// preload index rule binding
	gomega.Expect(protojson.Unmarshal([]byte(indexRuleBindingJSON), irb)).To(gomega.Succeed())
	return irb
}

func loadIndexRule() *databasev1.IndexRule {
	ir := &databasev1.IndexRule{}
	data, err := indexRuleStore.ReadFile(indexRuleDir + "/db.instance.json")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(protojson.Unmarshal(data, ir)).To(gomega.Succeed())
	return ir
}

var _ = ginkgo.Describe("Utils", func() {
	ginkgo.Context("Check equality for Stream", func() {
		var s *databasev1.Stream
		var checker equalityChecker
		var goods []gleak.Goroutine

		ginkgo.BeforeEach(func() {
			s = loadStream()
			checker = checkerMap[KindStream]
			goods = gleak.Goroutines()
		})

		ginkgo.AfterEach(func() {
			gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		})

		ginkgo.It("should be equal if nothing changed", func() {
			gomega.Expect(checker(s, s)).Should(gomega.BeTrue())
		})

		ginkgo.It("should not be equal if metadata.name changed", func() {
			newS := loadStream()
			newS.Metadata.Name = "new-name"
			gomega.Expect(checker(s, newS)).Should(gomega.BeFalse())
		})

		ginkgo.It("should not be equal if metadata.group changed", func() {
			newS := loadStream()
			newS.GetMetadata().Group = "new-group"
			gomega.Expect(checker(s, newS)).Should(gomega.BeFalse())
		})

		ginkgo.It("should not be equal if entity changed", func() {
			newS := loadStream()
			newS.GetEntity().TagNames = []string{"new-entity-tag"}
			gomega.Expect(checker(s, newS)).Should(gomega.BeFalse())
		})

		ginkgo.It("should not be equal if tag name changed", func() {
			newS := loadStream()
			newS.GetTagFamilies()[0].Tags[0].Name = "binary-tag"
			gomega.Expect(checker(s, newS)).Should(gomega.BeFalse())
		})

		ginkgo.It("should not be equal if tag type changed", func() {
			newS := loadStream()
			newS.GetTagFamilies()[0].Tags[0].Type = databasev1.TagType_TAG_TYPE_STRING
			gomega.Expect(checker(s, newS)).Should(gomega.BeFalse())
		})

		ginkgo.It("should be equal if UpdatedAt changed", func() {
			newS := loadStream()
			newS.UpdatedAt = timestamppb.Now()
			gomega.Expect(checker(s, newS)).Should(gomega.BeTrue())
		})

		ginkgo.It("should be equal if metadata.mod_revision changed", func() {
			newS := loadStream()
			newS.Metadata.ModRevision = 10000
			gomega.Expect(checker(s, newS)).Should(gomega.BeTrue())
		})

		ginkgo.It("should be equal if metadata.create_revision changed", func() {
			newS := loadStream()
			newS.Metadata.CreateRevision = 10000
			gomega.Expect(checker(s, newS)).Should(gomega.BeTrue())
		})
	})

	ginkgo.Context("Check equality for IndexRuleBinding", func() {
		var irb *databasev1.IndexRuleBinding
		var checker equalityChecker
		var goods []gleak.Goroutine

		ginkgo.BeforeEach(func() {
			irb = loadIndexRuleBinding()
			checker = checkerMap[KindIndexRuleBinding]
			goods = gleak.Goroutines()
		})

		ginkgo.AfterEach(func() {
			gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		})

		ginkgo.It("should be equal if nothing changed", func() {
			gomega.Expect(checker(irb, irb)).Should(gomega.BeTrue())
		})

		ginkgo.It("should not be equal if metadata.name changed", func() {
			newIrb := loadIndexRuleBinding()
			newIrb.Metadata.Name = "new-name"
			gomega.Expect(checker(irb, newIrb)).Should(gomega.BeFalse())
		})

		ginkgo.It("should not be equal if metadata.group changed", func() {
			newIrb := loadIndexRuleBinding()
			newIrb.GetMetadata().Group = "new-group"
			gomega.Expect(checker(irb, newIrb)).Should(gomega.BeFalse())
		})

		ginkgo.It("should not be equal if rules changed", func() {
			newIrb := loadIndexRuleBinding()
			newIrb.Rules = []string{}
			gomega.Expect(checker(irb, newIrb)).Should(gomega.BeFalse())
		})

		ginkgo.It("should not be equal if beginAt changed", func() {
			newIrb := loadIndexRuleBinding()
			newIrb.BeginAt = timestamppb.New(time.Now())
			gomega.Expect(checker(irb, newIrb)).Should(gomega.BeFalse())
		})

		ginkgo.It("should not be equal if expireAt changed", func() {
			newIrb := loadIndexRuleBinding()
			newIrb.ExpireAt = timestamppb.New(time.Now())
			gomega.Expect(checker(irb, newIrb)).Should(gomega.BeFalse())
		})

		ginkgo.It("should be equal if UpdatedAtNanoseconds changed", func() {
			newIrb := loadIndexRuleBinding()
			newIrb.UpdatedAt = timestamppb.Now()
			gomega.Expect(checker(irb, newIrb)).Should(gomega.BeTrue())
		})
	})

	ginkgo.Context("Check equality for IndexRule", func() {
		var ir *databasev1.IndexRule
		var checker equalityChecker
		var goods []gleak.Goroutine
		ginkgo.BeforeEach(func() {
			ir = loadIndexRule()
			checker = checkerMap[KindIndexRule]
			goods = gleak.Goroutines()
		})

		ginkgo.AfterEach(func() {
			gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		})

		ginkgo.It("should be equal if nothing changed", func() {
			gomega.Expect(checker(ir, ir)).Should(gomega.BeTrue())
		})

		ginkgo.It("should not be equal if metadata.name changed", func() {
			newIr := loadIndexRule()
			newIr.Metadata.Name = "new-name"
			gomega.Expect(checker(ir, newIr)).Should(gomega.BeFalse())
		})

		ginkgo.It("should not be equal if metadata.id changed", func() {
			newIr := loadIndexRule()
			newIr.Metadata.Id = 1000
			gomega.Expect(checker(ir, newIr)).Should(gomega.BeTrue())
		})

		ginkgo.It("should not be equal if metadata.group changed", func() {
			newIr := loadIndexRule()
			newIr.GetMetadata().Group = "new-group"
			gomega.Expect(checker(ir, newIr)).Should(gomega.BeFalse())
		})

		ginkgo.It("should not be equal if rules changed", func() {
			newIr := loadIndexRule()
			newIr.Tags = []string{"new-tag"}
			gomega.Expect(checker(ir, newIr)).Should(gomega.BeFalse())
		})

		ginkgo.It("should be equal if UpdatedAtNanoseconds changed", func() {
			newIr := loadIndexRule()
			newIr.UpdatedAt = timestamppb.Now()
			gomega.Expect(checker(ir, newIr)).Should(gomega.BeTrue())
		})
	})
})
