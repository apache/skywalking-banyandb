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
	"fmt"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/api/validate"
)

var streamKeyPrefix = "/streams/"

func (e *etcdSchemaRegistry) GetStream(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Stream, error) {
	var entity databasev1.Stream
	if err := e.get(ctx, formatStreamKey(metadata), &entity); err != nil {
		return nil, err
	}
	return &entity, nil
}

func (e *etcdSchemaRegistry) ListStream(ctx context.Context, opt ListOpt) ([]*databasev1.Stream, error) {
	if opt.Group == "" {
		return nil, BadRequest("group", "group should not be empty")
	}
	messages, err := e.listWithPrefix(ctx, listPrefixesForEntity(opt.Group, streamKeyPrefix), KindStream)
	if err != nil {
		return nil, err
	}
	entities := make([]*databasev1.Stream, 0, len(messages))
	for _, message := range messages {
		entities = append(entities, message.(*databasev1.Stream))
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) UpdateStream(ctx context.Context, stream *databasev1.Stream) (int64, error) {
	if stream.UpdatedAt != nil {
		stream.UpdatedAt = timestamppb.Now()
	}
	if err := validate.Stream(stream); err != nil {
		return 0, err
	}
	group := stream.Metadata.GetGroup()
	g, err := e.GetGroup(ctx, group)
	if err != nil {
		return 0, err
	}
	if err = validate.GroupForStreamOrMeasure(g); err != nil {
		return 0, err
	}
	prev, err := e.GetStream(ctx, stream.GetMetadata())
	if err != nil {
		return 0, err
	}
	if prev == nil {
		return 0, errors.WithMessagef(ErrGRPCResourceNotFound, "stream %s not found", stream.GetMetadata().GetName())
	}
	if err := validateEqualExceptAppendTags(prev, stream); err != nil {
		return 0, errors.WithMessagef(ErrInputInvalid, "validation failed: %s", err)
	}
	return e.update(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:        KindStream,
			Group:       stream.GetMetadata().GetGroup(),
			Name:        stream.GetMetadata().GetName(),
			ModRevision: stream.GetMetadata().GetModRevision(),
		},
		Spec: stream,
	})
}

func validateEqualExceptAppendTags(prevStream, newStream *databasev1.Stream) error {
	if prevStream.GetEntity().String() != newStream.GetEntity().String() {
		return fmt.Errorf("entity is different: %s != %s", prevStream.GetEntity().String(), newStream.GetEntity().String())
	}
	if len(prevStream.GetTagFamilies()) > len(newStream.GetTagFamilies()) {
		return fmt.Errorf("number of tag families is less in the new stream")
	}
	for i, tagFamily := range prevStream.GetTagFamilies() {
		if tagFamily.Name != newStream.GetTagFamilies()[i].Name {
			return fmt.Errorf("tag family name is different: %s != %s", tagFamily.Name, newStream.GetTagFamilies()[i].Name)
		}
		if len(tagFamily.Tags) > len(newStream.GetTagFamilies()[i].Tags) {
			return fmt.Errorf("number of tags in tag family %s is less in the new stream", tagFamily.Name)
		}
		for j, tag := range tagFamily.Tags {
			if tag.String() != newStream.GetTagFamilies()[i].Tags[j].String() {
				return fmt.Errorf("tag %s in tag family %s is different: %s != %s", tag.Name, tagFamily.Name, tag.String(), newStream.GetTagFamilies()[i].Tags[j].String())
			}
		}
	}
	return nil
}

func (e *etcdSchemaRegistry) CreateStream(ctx context.Context, stream *databasev1.Stream) (int64, error) {
	if stream.UpdatedAt != nil {
		stream.UpdatedAt = timestamppb.Now()
	}
	if err := validate.Stream(stream); err != nil {
		return 0, err
	}
	group := stream.Metadata.GetGroup()
	g, err := e.GetGroup(ctx, group)
	if err != nil {
		return 0, err
	}
	if err := validate.GroupForStreamOrMeasure(g); err != nil {
		return 0, err
	}
	return e.create(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindStream,
			Group: stream.GetMetadata().GetGroup(),
			Name:  stream.GetMetadata().GetName(),
		},
		Spec: stream,
	})
}

func (e *etcdSchemaRegistry) DeleteStream(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return e.delete(ctx, Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindStream,
			Group: metadata.GetGroup(),
			Name:  metadata.GetName(),
		},
	})
}

func formatStreamKey(metadata *commonv1.Metadata) string {
	return formatKey(streamKeyPrefix, metadata)
}
