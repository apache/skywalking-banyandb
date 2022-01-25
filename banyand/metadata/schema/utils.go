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
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

type equalityChecker func(a, b proto.Message) bool

var (
	streamEqualityChecker = func(a, b proto.Message) bool {
		if bStream, ok := b.(*databasev1.Stream); ok {
			if aStream, ok := a.(*databasev1.Stream); ok {
				return metadataEqual(aStream.GetMetadata(), bStream.GetMetadata()) && // metadata
					resourceOptEqual(aStream.GetOpts(), bStream.GetOpts()) && // resourceOpts
					cmp.Equal(aStream.GetEntity().GetTagNames(), bStream.GetEntity().GetTagNames()) && // entities
					tagFamilySpecsEqual(aStream.GetTagFamilies(), bStream.GetTagFamilies()) // tagFamilies
			}
		}

		return false
	}

	measureEqualityChecker = func(a, b proto.Message) bool {
		if bMeasure, ok := b.(*databasev1.Measure); ok {
			if aMeasure, ok := a.(*databasev1.Measure); ok {
				return metadataEqual(aMeasure.GetMetadata(), bMeasure.GetMetadata()) &&
					resourceOptEqual(aMeasure.GetOpts(), bMeasure.GetOpts()) && // resourceOpts
					cmp.Equal(aMeasure.GetEntity().GetTagNames(), bMeasure.GetEntity().GetTagNames()) && // entities
					tagFamilySpecsEqual(aMeasure.GetTagFamilies(), bMeasure.GetTagFamilies()) && // tagFamilies
					intervalSpecsEqual(aMeasure.GetIntervalRules(), bMeasure.GetIntervalRules()) && // intervalRules
					fieldSpecsEqual(aMeasure.GetFields(), bMeasure.GetFields()) // fields
			}
		}

		return false
	}

	indexRuleChecker = func(a, b proto.Message) bool {
		if bIndexRule, ok := b.(*databasev1.IndexRule); ok {
			if aIndexRule, ok := a.(*databasev1.IndexRule); ok {
				return metadataEqual(aIndexRule.GetMetadata(), bIndexRule.GetMetadata()) &&
					cmp.Equal(aIndexRule.GetTags(), bIndexRule.GetTags()) && // tags
					aIndexRule.GetType() == bIndexRule.GetType() &&
					aIndexRule.GetLocation() == bIndexRule.GetLocation()
			}
		}

		return false
	}

	indexRuleBindingChecker = func(a, b proto.Message) bool {
		if bIndexRuleBinding, ok := b.(*databasev1.IndexRuleBinding); ok {
			if aIndexRuleBinding, ok := a.(*databasev1.IndexRuleBinding); ok {
				return metadataEqual(aIndexRuleBinding.GetMetadata(), bIndexRuleBinding.GetMetadata()) &&
					cmp.Equal(aIndexRuleBinding.GetRules(), bIndexRuleBinding.GetRules()) && // rules
					timestamppbEqual(aIndexRuleBinding.GetBeginAt(), bIndexRuleBinding.GetBeginAt()) &&
					timestamppbEqual(aIndexRuleBinding.GetExpireAt(), bIndexRuleBinding.GetExpireAt()) &&
					subjectEqual(aIndexRuleBinding.GetSubject(), bIndexRuleBinding.GetSubject())
			}
		}

		return false
	}
)

var (
	checkerMap = map[Kind]equalityChecker{
		KindIndexRuleBinding: indexRuleBindingChecker,
		KindIndexRule:        indexRuleChecker,
		KindMeasure:          measureEqualityChecker,
		KindStream:           streamEqualityChecker,
	}
)

func metadataEqual(a, b *commonv1.Metadata) bool {
	return a.GetName() == b.GetName() && a.GetGroup() == b.GetGroup()
}

func timestamppbEqual(a, b *timestamppb.Timestamp) bool {
	return a.GetSeconds() == b.GetSeconds() && a.GetNanos() == b.GetNanos()
}

func subjectEqual(a, b *databasev1.Subject) bool {
	return a.GetCatalog() == b.GetCatalog() && a.GetName() == b.GetName()
}

func resourceOptEqual(a, b *databasev1.ResourceOpts) bool {
	return a.GetShardNum() == b.GetShardNum() && // opts
		a.GetTtl().GetVal() == b.GetTtl().GetVal() &&
		a.GetTtl().GetUnit() == b.GetTtl().GetUnit()
}

func tagFamilySpecsEqual(a, b []*databasev1.TagFamilySpec) bool {
	return cmp.Equal(a, b, cmp.Comparer(tagFamilySpecEqual))
}

func tagFamilySpecEqual(a, b *databasev1.TagFamilySpec) bool {
	return a.GetName() == b.GetName() && cmp.Equal(a.GetTags(), b.GetTags(), cmp.Comparer(func(a, b *databasev1.TagSpec) bool {
		return a.GetName() == b.GetName() && a.GetType() == b.GetType()
	}))
}

func intervalSpecsEqual(a, b []*databasev1.IntervalRule) bool {
	return cmp.Equal(a, b, cmp.Comparer(intervalSpecEqual))
}

func intervalSpecEqual(a, b *databasev1.IntervalRule) bool {
	return a.GetTagName() == b.GetTagName() && a.GetInterval() == b.GetInterval() && (a.GetInt() == b.GetInt() || a.GetStr() == b.GetStr())
}

func fieldSpecsEqual(a, b []*databasev1.FieldSpec) bool {
	return cmp.Equal(a, b, cmp.Comparer(fieldSpecEqual))
}

func fieldSpecEqual(a, b *databasev1.FieldSpec) bool {
	return a.GetName() == b.GetName() && a.GetFieldType() == b.GetFieldType() && a.GetCompressionMethod() == b.GetCompressionMethod() && a.GetEncodingMethod() == b.GetEncodingMethod()
}
