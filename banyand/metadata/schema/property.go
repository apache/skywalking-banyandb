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
	"path"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const all = "*"

var propertyKeyPrefix = "/properties/"

func (e *etcdSchemaRegistry) GetProperty(ctx context.Context, metadata *propertyv1.Metadata, tags []string) (*propertyv1.Property, error) {
	if !e.closer.AddRunning() {
		return nil, ErrClosed
	}
	defer e.closer.Done()
	key := e.prependNamespace(formatPropertyKey(transformKey(metadata)))
	resp, err := e.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if resp.Count == 0 {
		return nil, ErrGRPCResourceNotFound
	}
	if resp.Count > 1 {
		return nil, errUnexpectedNumberOfEntities
	}
	var property propertyv1.Property
	if err = proto.Unmarshal(resp.Kvs[0].Value, &property); err != nil {
		return nil, err
	}
	property.Metadata.Container.CreateRevision = resp.Kvs[0].CreateRevision
	property.Metadata.Container.ModRevision = resp.Kvs[0].ModRevision
	return filterTags(&property, tags), nil
}

func filterTags(property *propertyv1.Property, tags []string) *propertyv1.Property {
	if len(tags) == 0 || tags[0] == all {
		return property
	}
	filtered := &propertyv1.Property{
		Metadata:  property.Metadata,
		UpdatedAt: property.UpdatedAt,
	}

	for _, expectedTag := range tags {
		for _, t := range property.Tags {
			if t.Key == expectedTag {
				filtered.Tags = append(filtered.Tags, t)
			}
		}
	}
	return filtered
}

func (e *etcdSchemaRegistry) ListProperty(ctx context.Context, container *commonv1.Metadata, ids []string, tags []string) ([]*propertyv1.Property, error) {
	if container.Group == "" {
		return nil, BadRequest("container.group", "group should not be empty")
	}
	messages, err := e.listWithPrefix(ctx, listPrefixesForEntity(path.Join(container.Group, container.Name), propertyKeyPrefix), KindProperty)
	if err != nil {
		return nil, err
	}
	entities := make([]*propertyv1.Property, 0, len(messages))
	for _, message := range messages {
		p := message.(*propertyv1.Property)
		if len(ids) < 1 || ids[0] == all {
			entities = append(entities, filterTags(p, tags))
			continue
		}
		for _, id := range ids {
			if p.Metadata.Id == id {
				entities = append(entities, filterTags(p, tags))
			}
		}
	}
	return entities, nil
}

func (e *etcdSchemaRegistry) ApplyProperty(ctx context.Context, property *propertyv1.Property, strategy propertyv1.ApplyRequest_Strategy) (bool, uint32, error) {
	if !e.closer.AddRunning() {
		return false, 0, ErrClosed
	}
	defer e.closer.Done()
	m := transformKey(property.GetMetadata())
	group := m.GetGroup()
	if _, getGroupErr := e.GetGroup(ctx, group); getGroupErr != nil {
		return false, 0, errors.Wrap(getGroupErr, "group is not exist")
	}
	md := Metadata{
		TypeMeta: TypeMeta{
			Kind:  KindProperty,
			Group: group,
			Name:  m.GetName(),
		},
		Spec: property,
	}
	key, err := md.key()
	if err != nil {
		return false, 0, err
	}
	key = e.prependNamespace(key)
	var ttl int64
	if property.Ttl != "" {
		t, err := timestamp.ParseDuration(property.Ttl)
		if err != nil {
			return false, 0, err
		}
		if t < time.Second {
			return false, 0, errors.New("ttl should be greater than 1s")
		}
		ttl = int64(t / time.Second)
	}
	if strategy == propertyv1.ApplyRequest_STRATEGY_REPLACE {
		return e.replaceProperty(ctx, key, property, ttl)
	}
	return e.mergeProperty(ctx, key, property, ttl)
}

func (e *etcdSchemaRegistry) replaceProperty(ctx context.Context, key string, property *propertyv1.Property, ttl int64) (bool, uint32, error) {
	val, opts, err := e.marshalProperty(ctx, property, ttl)
	if err != nil {
		return false, 0, err
	}
	_, err = e.client.Put(ctx, key, string(val), opts...)
	if err != nil {
		return false, 0, err
	}
	return true, uint32(len(property.Tags)), nil
}

func (e *etcdSchemaRegistry) mergeProperty(ctx context.Context, key string, property *propertyv1.Property, ttl int64) (bool, uint32, error) {
	tagsNum := uint32(len(property.Tags))
	existed, errGet := e.GetProperty(ctx, property.Metadata, nil)
	if errors.Is(errGet, ErrGRPCResourceNotFound) {
		return e.replaceProperty(ctx, key, property, ttl)
	}
	if errGet != nil {
		return false, 0, errGet
	}
	var prevLeaseID int64
	merge := func(existed *propertyv1.Property) (*propertyv1.Property, error) {
		tags := make([]*modelv1.Tag, len(property.Tags))
		copy(tags, property.Tags)
		for i := 0; i < int(tagsNum); i++ {
			t := tags[0]
			tags = tags[1:]
			for _, et := range existed.Tags {
				if et.Key == t.Key {
					et.Value = t.Value
				}
			}
		}
		existed.Tags = append(existed.Tags, property.Tags...)
		md.Spec = existed
	}
	if err = e.update(ctx, md); err != nil {
		return false, 0, err
	}
	return false, tagsNum, nil
}

func (e *etcdSchemaRegistry) marshalProperty(ctx context.Context, property *propertyv1.Property, ttl int64) ([]byte, []clientv3.OpOption, error) {
	var opts []clientv3.OpOption
	if ttl > 0 {
		lease, err := e.client.Grant(ctx, ttl)
		if err != nil {
			return nil, nil, err
		}
		property.LeaseId = int64(lease.ID)
		opts = append(opts, clientv3.WithLease(lease.ID))
	}
	property.UpdatedAt = timestamppb.Now()
	val, err := proto.Marshal(property)
	if err != nil {
		return nil, nil, err
	}
	return val, opts, nil
}

func (e *etcdSchemaRegistry) DeleteProperty(ctx context.Context, metadata *propertyv1.Metadata, tags []string) (bool, uint32, error) {
	if len(tags) == 0 || tags[0] == all {
		m := transformKey(metadata)
		deleted, err := e.delete(ctx, Metadata{
			TypeMeta: TypeMeta{
				Kind:  KindProperty,
				Group: m.GetGroup(),
				Name:  m.GetName(),
			},
		})
		return deleted, 0, err
	}
	property, err := e.GetProperty(ctx, metadata, nil)
	if err != nil {
		return false, 0, err
	}
	filtered := &propertyv1.Property{
		Metadata:  property.Metadata,
		UpdatedAt: property.UpdatedAt,
	}

	for _, expectedTag := range tags {
		for _, t := range property.Tags {
			if t.Key != expectedTag {
				filtered.Tags = append(filtered.Tags, t)
			}
		}
	}
	_, num, err := e.ApplyProperty(ctx, filtered, propertyv1.ApplyRequest_STRATEGY_REPLACE)
	return true, num, err
}

func transformKey(metadata *propertyv1.Metadata) *commonv1.Metadata {
	return &commonv1.Metadata{
		Group: metadata.Container.GetGroup(),
		Name:  path.Join(metadata.Container.Name, metadata.Id),
	}
}

func formatPropertyKey(metadata *commonv1.Metadata) string {
	return formatKey(propertyKeyPrefix, metadata)
}
