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

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

type ResourceType uint8

const (
	ResourceStream ResourceType = iota
	ResourceMeasure
	ResourceIndexRuleBinding
	ResourceIndexRule
)

type resource struct {
	typ ResourceType
	proto.Message
}

func (r *resource) GetMetadata() *commonv1.Metadata {
	switch r.typ {
	case ResourceStream:
		return r.Message.(*databasev1.Stream).GetMetadata()
	case ResourceMeasure:
		return r.Message.(*databasev1.Measure).GetMetadata()
	case ResourceIndexRuleBinding:
		return r.Message.(*databasev1.IndexRuleBinding).GetMetadata()
	case ResourceIndexRule:
		return r.Message.(*databasev1.IndexRule).GetMetadata()
	default:
		return nil
	}
}

func (r *resource) Key() string {
	switch r.typ {
	case ResourceStream:
		return formatSteamKey(r.Message.(*databasev1.Stream).GetMetadata())
	case ResourceMeasure:
		return formatMeasureKey(r.Message.(*databasev1.Measure).GetMetadata())
	case ResourceIndexRuleBinding:
		return formatIndexRuleBindingKey(r.Message.(*databasev1.IndexRuleBinding).GetMetadata())
	case ResourceIndexRule:
		return formatIndexRuleKey(r.Message.(*databasev1.IndexRule).GetMetadata())
	default:
		return ""
	}
}

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
	RegisterEventHandler(commonv1.Catalog, EventHandler)
}

// EventHandler is called when resources (e.g. stream, measure) change or resources belonging to them change.
// Currently, it may not be necessary to distinguish Add or Update since the involved object will be reloaded completely.
type EventHandler interface {
	OnAddOrUpdate(*commonv1.Metadata)
	OnDelete(*commonv1.Metadata)
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
