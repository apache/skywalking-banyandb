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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

type Kind int

type EventHandler func(Metadata) error

const (
	KindStream Kind = 1 << iota
	KindMeasure
	KindIndexRuleBinding
	KindIndexRule
)

const KindMask = KindStream | KindMeasure | KindIndexRuleBinding | KindIndexRule

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
	RegisterHandler(Kind, EventHandler)
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

func (m Metadata) Key() string {
	switch m.Kind {
	case KindMeasure:
		return formatMeasureKey(&commonv1.Metadata{
			Group: m.Group,
			Name:  m.Name,
		})
	case KindStream:
		return formatStreamKey(&commonv1.Metadata{
			Group: m.Group,
			Name:  m.Name,
		})
	case KindIndexRule:
		return formatIndexRuleKey(&commonv1.Metadata{
			Group: m.Group,
			Name:  m.Name,
		})
	case KindIndexRuleBinding:
		return formatIndexRuleBindingKey(&commonv1.Metadata{
			Group: m.Group,
			Name:  m.Name,
		})
	default:
		panic("unsupported Kind")
	}
}

type Stream interface {
	GetStream(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Stream, error)
	ListStream(ctx context.Context, opt ListOpt) ([]*databasev1.Stream, error)
	UpdateStream(ctx context.Context, stream *databasev1.Stream) error
	DeleteStream(ctx context.Context, metadata *commonv1.Metadata) (bool, error)
}

type IndexRule interface {
	GetIndexRule(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRule, error)
	ListIndexRule(ctx context.Context, opt ListOpt) ([]*databasev1.IndexRule, error)
	UpdateIndexRule(ctx context.Context, indexRule *databasev1.IndexRule) error
	DeleteIndexRule(ctx context.Context, metadata *commonv1.Metadata) (bool, error)
}

type IndexRuleBinding interface {
	GetIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRuleBinding, error)
	ListIndexRuleBinding(ctx context.Context, opt ListOpt) ([]*databasev1.IndexRuleBinding, error)
	UpdateIndexRuleBinding(ctx context.Context, indexRuleBinding *databasev1.IndexRuleBinding) error
	DeleteIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (bool, error)
}

type Measure interface {
	GetMeasure(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Measure, error)
	ListMeasure(ctx context.Context, opt ListOpt) ([]*databasev1.Measure, error)
	UpdateMeasure(ctx context.Context, measure *databasev1.Measure) error
	DeleteMeasure(ctx context.Context, metadata *commonv1.Metadata) (bool, error)
}

type Group interface {
	GetGroup(ctx context.Context, group string) (*commonv1.Group, error)
	ListGroup(ctx context.Context) ([]string, error)
	// DeleteGroup delete all items belonging to the group
	DeleteGroup(ctx context.Context, group string) (bool, error)
	// CreateGroup works like `touch` in unix systems.
	// 1. It will create the group if it does not exist.
	// 2. It will update the updated_at timestamp to the current timestamp.
	CreateGroup(ctx context.Context, group string) error
}
