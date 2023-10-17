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

// Package schema implements CRUD schema.
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

var errUnsupportedEntityType = errors.New("unsupported entity type")

// EventHandler allows receiving and handling the resource change events.
type EventHandler interface {
	OnAddOrUpdate(Metadata)
	OnDelete(Metadata)
}

// ListOpt contains options to list resources.
type ListOpt struct {
	Group string
}

// Registry allowing depositing resources.
type Registry interface {
	io.Closer
	Stream
	IndexRule
	IndexRuleBinding
	Measure
	Group
	TopNAggregation
	Property
	Node
	RegisterHandler(string, Kind, EventHandler)
}

// TypeMeta defines the identity and type of an Event.
type TypeMeta struct {
	Name        string
	Group       string
	ModRevision int64
	Kind        Kind
}

// Metadata wrap dedicated serialized resource and its TypeMeta.
type Metadata struct {
	Spec Spec
	TypeMeta
}

// Spec is a placeholder of a serialized resource.
type Spec interface{}

func (m Metadata) key() (string, error) {
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
	case KindTopNAggregation:
		return formatTopNAggregationKey(&commonv1.Metadata{
			Group: m.Group,
			Name:  m.Name,
		}), nil
	case KindNode:
		return formatNodeKey(m.Name), nil
	default:
		return "", errUnsupportedEntityType
	}
}

func (m Metadata) equal(other Metadata) bool {
	if other.Spec == nil {
		return false
	}

	if checker, ok := checkerMap[m.Kind]; ok {
		return checker(m.Spec.(proto.Message), other.Spec.(proto.Message))
	}

	return false
}

// Stream allows CRUD stream schemas in a group.
type Stream interface {
	GetStream(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Stream, error)
	ListStream(ctx context.Context, opt ListOpt) ([]*databasev1.Stream, error)
	CreateStream(ctx context.Context, stream *databasev1.Stream) (int64, error)
	UpdateStream(ctx context.Context, stream *databasev1.Stream) (int64, error)
	DeleteStream(ctx context.Context, metadata *commonv1.Metadata) (bool, error)
}

// IndexRule allows CRUD index rule schemas in a group.
type IndexRule interface {
	GetIndexRule(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRule, error)
	ListIndexRule(ctx context.Context, opt ListOpt) ([]*databasev1.IndexRule, error)
	CreateIndexRule(ctx context.Context, indexRule *databasev1.IndexRule) error
	UpdateIndexRule(ctx context.Context, indexRule *databasev1.IndexRule) error
	DeleteIndexRule(ctx context.Context, metadata *commonv1.Metadata) (bool, error)
}

// IndexRuleBinding allows CRUD index rule binding schemas in a group.
type IndexRuleBinding interface {
	GetIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRuleBinding, error)
	ListIndexRuleBinding(ctx context.Context, opt ListOpt) ([]*databasev1.IndexRuleBinding, error)
	CreateIndexRuleBinding(ctx context.Context, indexRuleBinding *databasev1.IndexRuleBinding) error
	UpdateIndexRuleBinding(ctx context.Context, indexRuleBinding *databasev1.IndexRuleBinding) error
	DeleteIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (bool, error)
}

// Measure allows CRUD measure schemas in a group.
type Measure interface {
	GetMeasure(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Measure, error)
	ListMeasure(ctx context.Context, opt ListOpt) ([]*databasev1.Measure, error)
	CreateMeasure(ctx context.Context, measure *databasev1.Measure) (int64, error)
	UpdateMeasure(ctx context.Context, measure *databasev1.Measure) (int64, error)
	DeleteMeasure(ctx context.Context, metadata *commonv1.Metadata) (bool, error)
	TopNAggregations(ctx context.Context, metadata *commonv1.Metadata) ([]*databasev1.TopNAggregation, error)
}

// Group allows CRUD groups which is namespaces of resources.
type Group interface {
	GetGroup(ctx context.Context, group string) (*commonv1.Group, error)
	ListGroup(ctx context.Context) ([]*commonv1.Group, error)
	// DeleteGroup delete all items belonging to the group
	DeleteGroup(ctx context.Context, group string) (bool, error)
	CreateGroup(ctx context.Context, group *commonv1.Group) error
	UpdateGroup(ctx context.Context, group *commonv1.Group) error
}

// TopNAggregation allows CRUD top-n aggregation schemas in a group.
type TopNAggregation interface {
	GetTopNAggregation(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.TopNAggregation, error)
	ListTopNAggregation(ctx context.Context, opt ListOpt) ([]*databasev1.TopNAggregation, error)
	CreateTopNAggregation(ctx context.Context, measure *databasev1.TopNAggregation) error
	UpdateTopNAggregation(ctx context.Context, measure *databasev1.TopNAggregation) error
	DeleteTopNAggregation(ctx context.Context, metadata *commonv1.Metadata) (bool, error)
}

// Property allows CRUD properties or tags in group.
type Property interface {
	GetProperty(ctx context.Context, metadata *propertyv1.Metadata, tags []string) (*propertyv1.Property, error)
	ListProperty(ctx context.Context, container *commonv1.Metadata, ids []string, tags []string) ([]*propertyv1.Property, error)
	ApplyProperty(ctx context.Context, property *propertyv1.Property, strategy propertyv1.ApplyRequest_Strategy) (bool, uint32, int64, error)
	DeleteProperty(ctx context.Context, metadata *propertyv1.Metadata, tags []string) (bool, uint32, error)
	KeepAlive(ctx context.Context, leaseID int64) error
}

// Node allows CRUD node schemas in a group.
type Node interface {
	ListNode(ctx context.Context, role databasev1.Role) ([]*databasev1.Node, error)
	RegisterNode(ctx context.Context, node *databasev1.Node, forced bool) error
}
