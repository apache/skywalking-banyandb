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
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

func loadStream() *databasev1.Stream {
	s := &databasev1.Stream{}
	// preload stream
	Expect(protojson.Unmarshal([]byte(streamJSON), s)).To(Succeed())
	return s
}

func loadIndexRuleBinding() *databasev1.IndexRuleBinding {
	irb := &databasev1.IndexRuleBinding{}
	// preload index rule binding
	Expect(protojson.Unmarshal([]byte(indexRuleBindingJSON), irb)).To(Succeed())
	return irb
}

func loadIndexRule() *databasev1.IndexRule {
	ir := &databasev1.IndexRule{}
	data, err := indexRuleStore.ReadFile(indexRuleDir + "/db.instance.json")
	Expect(err).NotTo(HaveOccurred())
	Expect(protojson.Unmarshal(data, ir)).To(Succeed())
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
			Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		})

		ginkgo.It("should be equal if nothing changed", func() {
			Expect(checker(s, s)).Should(BeTrue())
		})

		ginkgo.It("should not be equal if metadata.name changed", func() {
			newS := loadStream()
			newS.Metadata.Name = "new-name"
			Expect(checker(s, newS)).Should(BeFalse())
		})

		ginkgo.It("should not be equal if metadata.group changed", func() {
			newS := loadStream()
			newS.GetMetadata().Group = "new-group"
			Expect(checker(s, newS)).Should(BeFalse())
		})

		ginkgo.It("should not be equal if entity changed", func() {
			newS := loadStream()
			newS.GetEntity().TagNames = []string{"new-entity-tag"}
			Expect(checker(s, newS)).Should(BeFalse())
		})

		ginkgo.It("should not be equal if tag name changed", func() {
			newS := loadStream()
			newS.GetTagFamilies()[0].Tags[0].Name = "binary-tag"
			Expect(checker(s, newS)).Should(BeFalse())
		})

		ginkgo.It("should not be equal if tag type changed", func() {
			newS := loadStream()
			newS.GetTagFamilies()[0].Tags[0].Type = databasev1.TagType_TAG_TYPE_STRING
			Expect(checker(s, newS)).Should(BeFalse())
		})

		ginkgo.It("should be equal if UpdatedAt changed", func() {
			newS := loadStream()
			newS.UpdatedAt = timestamppb.Now()
			Expect(checker(s, newS)).Should(BeTrue())
		})

		ginkgo.It("should be equal if metadata.mod_revision changed", func() {
			newS := loadStream()
			newS.Metadata.ModRevision = 10000
			Expect(checker(s, newS)).Should(BeTrue())
		})

		ginkgo.It("should be equal if metadata.create_revision changed", func() {
			newS := loadStream()
			newS.Metadata.CreateRevision = 10000
			Expect(checker(s, newS)).Should(BeTrue())
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
			Eventually(gleak.Goroutines()).ShouldNot(gleak.HaveLeaked(goods))
		})

		ginkgo.It("should be equal if nothing changed", func() {
			Expect(checker(irb, irb)).Should(BeTrue())
		})

		ginkgo.It("should not be equal if metadata.name changed", func() {
			newIrb := loadIndexRuleBinding()
			newIrb.Metadata.Name = "new-name"
			Expect(checker(irb, newIrb)).Should(BeFalse())
		})

		ginkgo.It("should not be equal if metadata.group changed", func() {
			newIrb := loadIndexRuleBinding()
			newIrb.GetMetadata().Group = "new-group"
			Expect(checker(irb, newIrb)).Should(BeFalse())
		})

		ginkgo.It("should not be equal if rules changed", func() {
			newIrb := loadIndexRuleBinding()
			newIrb.Rules = []string{}
			Expect(checker(irb, newIrb)).Should(BeFalse())
		})

		ginkgo.It("should not be equal if beginAt changed", func() {
			newIrb := loadIndexRuleBinding()
			newIrb.BeginAt = timestamppb.New(time.Now())
			Expect(checker(irb, newIrb)).Should(BeFalse())
		})

		ginkgo.It("should not be equal if expireAt changed", func() {
			newIrb := loadIndexRuleBinding()
			newIrb.ExpireAt = timestamppb.New(time.Now())
			Expect(checker(irb, newIrb)).Should(BeFalse())
		})

		ginkgo.It("should be equal if UpdatedAtNanoseconds changed", func() {
			newIrb := loadIndexRuleBinding()
			newIrb.UpdatedAt = timestamppb.Now()
			Expect(checker(irb, newIrb)).Should(BeTrue())
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
			Eventually(gleak.Goroutines()).ShouldNot(gleak.HaveLeaked(goods))
		})

		ginkgo.It("should be equal if nothing changed", func() {
			Expect(checker(ir, ir)).Should(BeTrue())
		})

		ginkgo.It("should not be equal if metadata.name changed", func() {
			newIr := loadIndexRule()
			newIr.Metadata.Name = "new-name"
			Expect(checker(ir, newIr)).Should(BeFalse())
		})

		ginkgo.It("should not be equal if metadata.id changed", func() {
			newIr := loadIndexRule()
			newIr.Metadata.Id = 1000
			Expect(checker(ir, newIr)).Should(BeTrue())
		})

		ginkgo.It("should not be equal if metadata.group changed", func() {
			newIr := loadIndexRule()
			newIr.GetMetadata().Group = "new-group"
			Expect(checker(ir, newIr)).Should(BeFalse())
		})

		ginkgo.It("should not be equal if rules changed", func() {
			newIr := loadIndexRule()
			newIr.Tags = []string{"new-tag"}
			Expect(checker(ir, newIr)).Should(BeFalse())
		})

		ginkgo.It("should be equal if UpdatedAtNanoseconds changed", func() {
			newIr := loadIndexRule()
			newIr.UpdatedAt = timestamppb.Now()
			Expect(checker(ir, newIr)).Should(BeTrue())
		})
	})
})
