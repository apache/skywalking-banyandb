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
	"context"
	"io"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
)

var (
	ErrUnsupportedEntityType = errors.New("unsupported entity type")
)

type Kind int

type EventHandler interface {
	OnAddOrUpdate(Metadata)
	OnDelete(Metadata)
}

const (
	KindGroup Kind = 1 << iota
	KindStream
	KindMeasure
	KindIndexRuleBinding
	KindIndexRule
	KindTopNAggregation
	KindProperty
)

const KindMask = KindGroup | KindStream | KindMeasure | KindIndexRuleBinding | KindIndexRule | KindTopNAggregation

type ListOpt struct {
	Group string
}

type Registry interface {
	io.Closer
	ReadyNotify() <-chan struct{}
	StopNotify() <-chan struct{}
	StoppingNotify() <-chan struct{}
	Stream
	IndexRule
	IndexRuleBinding
	Measure
	Group
	TopNAggregation
	Property
}

type TypeMeta struct {
	Kind  Kind
	Name  string
	Group string
}

type Metadata struct {
	TypeMeta

	// Spec holds the configuration object as a protobuf message
	// Or a metadataHolder as a container
	Spec Spec
}

type Spec interface {
}

func (tm TypeMeta) Unmarshal(data []byte) (m proto.Message, err error) {
	switch tm.Kind {
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
	default:
		return nil, ErrUnsupportedEntityType
	}
	err = proto.Unmarshal(data, m)
	return
}

func (m Metadata) Key() (string, error) {
	switch m.Kind {
	case KindGroup:
		return formatGroupKey(m.Name), nil
	case KindMeasure:
		return formatMeasureKey(&commonv1.Metadata{
			Group: m.Group,
			Name:  m.Name,
		}), nil
	case KindStream:
		return formatStreamKey(&commonv1.Metadata{
			Group: m.Group,
			Name:  m.Name,
		}), nil
	case KindIndexRule:
		return formatIndexRuleKey(&commonv1.Metadata{
			Group: m.Group,
			Name:  m.Name,
		}), nil
	case KindIndexRuleBinding:
		return formatIndexRuleBindingKey(&commonv1.Metadata{
			Group: m.Group,
			Name:  m.Name,
		}), nil
	case KindProperty:
		return formatPropertyKey(&commonv1.Metadata{
			Group: m.Group,
			Name:  m.Name,
		}), nil
	default:
		return "", ErrUnsupportedEntityType
	}
}

func (m Metadata) Equal(other proto.Message) bool {
	if other == nil {
		return false
	}

	if checker, ok := checkerMap[m.Kind]; ok {
		return checker(m.Spec.(proto.Message), other)
	}

	return false
}

type Stream interface {
	GetStream(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Stream, error)
	ListStream(ctx context.Context, opt ListOpt) ([]*databasev1.Stream, error)
	CreateStream(ctx context.Context, stream *databasev1.Stream) error
	UpdateStream(ctx context.Context, stream *databasev1.Stream) error
	DeleteStream(ctx context.Context, metadata *commonv1.Metadata) (bool, error)
	RegisterHandler(Kind, EventHandler)
}

type IndexRule interface {
	GetIndexRule(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRule, error)
	ListIndexRule(ctx context.Context, opt ListOpt) ([]*databasev1.IndexRule, error)
	CreateIndexRule(ctx context.Context, indexRule *databasev1.IndexRule) error
	UpdateIndexRule(ctx context.Context, indexRule *databasev1.IndexRule) error
	DeleteIndexRule(ctx context.Context, metadata *commonv1.Metadata) (bool, error)
}

type IndexRuleBinding interface {
	GetIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRuleBinding, error)
	ListIndexRuleBinding(ctx context.Context, opt ListOpt) ([]*databasev1.IndexRuleBinding, error)
	CreateIndexRuleBinding(ctx context.Context, indexRuleBinding *databasev1.IndexRuleBinding) error
	UpdateIndexRuleBinding(ctx context.Context, indexRuleBinding *databasev1.IndexRuleBinding) error
	DeleteIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (bool, error)
}

type Measure interface {
	GetMeasure(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Measure, error)
	ListMeasure(ctx context.Context, opt ListOpt) ([]*databasev1.Measure, error)
	CreateMeasure(ctx context.Context, measure *databasev1.Measure) error
	UpdateMeasure(ctx context.Context, measure *databasev1.Measure) error
	DeleteMeasure(ctx context.Context, metadata *commonv1.Metadata) (bool, error)
	RegisterHandler(Kind, EventHandler)
}

type Group interface {
	GetGroup(ctx context.Context, group string) (*commonv1.Group, error)
	ListGroup(ctx context.Context) ([]*commonv1.Group, error)
	// DeleteGroup delete all items belonging to the group
	DeleteGroup(ctx context.Context, group string) (bool, error)
	CreateGroup(ctx context.Context, group *commonv1.Group) error
	UpdateGroup(ctx context.Context, group *commonv1.Group) error
}

type TopNAggregation interface {
	GetTopNAggregation(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.TopNAggregation, error)
	ListTopNAggregation(ctx context.Context, opt ListOpt) ([]*databasev1.TopNAggregation, error)
	CreateTopNAggregation(ctx context.Context, measure *databasev1.TopNAggregation) error
	UpdateTopNAggregation(ctx context.Context, measure *databasev1.TopNAggregation) error
	DeleteTopNAggregation(ctx context.Context, metadata *commonv1.Metadata) (bool, error)
}

type Property interface {
	GetProperty(ctx context.Context, metadata *propertyv1.Metadata) (*propertyv1.Property, error)
	ListProperty(ctx context.Context, container *commonv1.Metadata) ([]*propertyv1.Property, error)
	CreateProperty(ctx context.Context, property *propertyv1.Property) error
	UpdateProperty(ctx context.Context, property *propertyv1.Property) error
	DeleteProperty(ctx context.Context, metadata *propertyv1.Metadata) (bool, error)
}
