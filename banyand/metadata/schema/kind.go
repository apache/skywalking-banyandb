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
	"io"

	"github.com/pkg/errors"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
)

// Kind is the type of a resource.
type Kind int

// KindMask tends to check whether an event is valid.
const (
	KindGroup Kind = 1 << iota
	KindStream
	KindMeasure
	KindIndexRuleBinding
	KindIndexRule
	KindTopNAggregation
	KindProperty
	KindNode
	KindMask = KindGroup | KindStream | KindMeasure |
		KindIndexRuleBinding | KindIndexRule |
		KindTopNAggregation | KindProperty | KindNode
	KindSize = 9
)

func (k Kind) key() string {
	switch k {
	case KindGroup:
		return groupsKeyPrefix
	case KindStream:
		return streamKeyPrefix
	case KindMeasure:
		return measureKeyPrefix
	case KindIndexRuleBinding:
		return indexRuleBindingKeyPrefix
	case KindIndexRule:
		return indexRuleKeyPrefix
	case KindTopNAggregation:
		return topNAggregationKeyPrefix
	case KindProperty:
		return propertyKeyPrefix
	case KindNode:
		return nodeKeyPrefix
	default:
		return "unknown"
	}
}

// Unmarshal encode bytes to proto.Message.
func (k Kind) Unmarshal(kv *mvccpb.KeyValue) (Metadata, error) {
	if len(kv.Value) == 0 {
		return Metadata{}, io.EOF
	}
	var m proto.Message
	switch k {
	case KindGroup:
		m = &commonv1.Group{}
	case KindStream:
		m = &databasev1.Stream{}
	case KindMeasure:
		m = &databasev1.Measure{}
	case KindIndexRuleBinding:
		m = &databasev1.IndexRuleBinding{}
	case KindIndexRule:
		m = &databasev1.IndexRule{}
	case KindProperty:
		m = &propertyv1.Property{}
	case KindTopNAggregation:
		m = &databasev1.TopNAggregation{}
	case KindNode:
		m = &databasev1.Node{}
	default:
		return Metadata{}, errUnsupportedEntityType
	}
	err := proto.Unmarshal(kv.Value, m)
	if err != nil {
		return Metadata{}, err
	}
	if messageWithMetadata, ok := m.(HasMetadata); ok {
		if messageWithMetadata.GetMetadata() == nil {
			return Metadata{}, errors.Errorf("message %s does not have metadata", k.key())
		}
		// Assign readonly fields
		messageWithMetadata.GetMetadata().CreateRevision = kv.CreateRevision
		messageWithMetadata.GetMetadata().ModRevision = kv.ModRevision
		return Metadata{
			TypeMeta: TypeMeta{
				Kind:  k,
				Name:  messageWithMetadata.GetMetadata().GetName(),
				Group: messageWithMetadata.GetMetadata().GetGroup(),
			},
			Spec: messageWithMetadata,
		}, nil
	}
	return Metadata{Spec: m}, nil
}

func (k Kind) String() string {
	switch k {
	case KindGroup:
		return "group"
	case KindStream:
		return "stream"
	case KindMeasure:
		return "measure"
	case KindIndexRuleBinding:
		return "indexRuleBinding"
	case KindIndexRule:
		return "indexRule"
	case KindTopNAggregation:
		return "topNAggregation"
	case KindProperty:
		return "property"
	case KindNode:
		return "node"
	default:
		return "unknown"
	}
}

func allKeys() []string {
	var keys []string
	for i := 0; i < KindSize; i++ {
		ki := Kind(1 << i)
		if KindMask&ki > 0 {
			keys = append(keys, ki.key())
		}
	}
	return keys
}
