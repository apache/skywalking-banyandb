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

package etcd

import (
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

func kindKeyPrefix(k schema.Kind) string {
	switch k {
	case schema.KindGroup:
		return groupsKeyPrefix
	case schema.KindStream:
		return streamKeyPrefix
	case schema.KindMeasure:
		return measureKeyPrefix
	case schema.KindTrace:
		return traceKeyPrefix
	case schema.KindIndexRuleBinding:
		return indexRuleBindingKeyPrefix
	case schema.KindIndexRule:
		return indexRuleKeyPrefix
	case schema.KindTopNAggregation:
		return topNAggregationKeyPrefix
	case schema.KindNode:
		return nodeKeyPrefix
	case schema.KindProperty:
		return propertyKeyPrefix
	default:
		return "unknown"
	}
}

func allKeys() []string {
	var keys []string
	for i := 0; i < schema.KindSize; i++ {
		ki := schema.Kind(1 << i)
		if schema.KindMask&ki > 0 {
			keys = append(keys, kindKeyPrefix(ki))
		}
	}
	return keys
}

// metadataKey returns the unique key string for the metadata based on its Kind.
func metadataKey(m schema.Metadata) (string, error) {
	switch m.Kind {
	case schema.KindGroup:
		return formatGroupKey(m.Name), nil
	case schema.KindMeasure:
		return formatMeasureKey(&commonv1.Metadata{
			Group: m.Group,
			Name:  m.Name,
		}), nil
	case schema.KindStream:
		return formatStreamKey(&commonv1.Metadata{
			Group: m.Group,
			Name:  m.Name,
		}), nil
	case schema.KindTrace:
		return formatTraceKey(&commonv1.Metadata{
			Group: m.Group,
			Name:  m.Name,
		}), nil
	case schema.KindIndexRule:
		return formatIndexRuleKey(&commonv1.Metadata{
			Group: m.Group,
			Name:  m.Name,
		}), nil
	case schema.KindIndexRuleBinding:
		return formatIndexRuleBindingKey(&commonv1.Metadata{
			Group: m.Group,
			Name:  m.Name,
		}), nil

	case schema.KindTopNAggregation:
		return formatTopNAggregationKey(&commonv1.Metadata{
			Group: m.Group,
			Name:  m.Name,
		}), nil
	case schema.KindNode:
		return formatNodeKey(m.Name), nil
	case schema.KindProperty:
		return formatPropertyKey(&commonv1.Metadata{
			Group: m.Group,
			Name:  m.Name,
		}), nil
	default:
		return "", schema.ErrUnsupportedEntityType
	}
}
