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
// Unless required by applicable law or agreed to in writing, "property",
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package grpc

import (
	"context"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

const defaultQueryTimeout = 30 * time.Second

type propertyServer struct {
	propertyv1.UnimplementedPropertyServiceServer
	schemaRegistry metadata.Repo
	broadcaster    queue.Client
	nodeRegistry   NodeRegistry
	metrics        *metrics
}

func (ps *propertyServer) Apply(ctx context.Context, req *propertyv1.ApplyRequest) (resp *propertyv1.ApplyResponse, err error) {
	g := req.Property.Metadata.Container.Group
	ps.metrics.totalStarted.Inc(1, g, "property", "apply")
	start := time.Now()
	defer func() {
		ps.metrics.totalFinished.Inc(1, g, "property", "apply")
		ps.metrics.totalLatency.Inc(time.Since(start).Seconds(), g, "property", "apply")
		if err != nil {
			ps.metrics.totalErr.Inc(1, g, "property", "apply")
		}
	}()
	property := req.Property
	if property.Metadata == nil {
		return nil, schema.BadRequest("metadata", "metadata should not be nil")
	}
	if property.Metadata == nil {
		return nil, schema.BadRequest("metadata", "metadata should not be nil")
	}
	if property.Metadata.Container == nil {
		return nil, schema.BadRequest("metadata.container", "container should not be nil")
	}
	if property.Metadata.Container.Name == "" {
		return nil, schema.BadRequest("metadata.container.name", "container.name should not be empty")
	}
	if property.Metadata.Id == "" {
		return nil, schema.BadRequest("metadata.id", "id should not be empty")
	}
	if len(property.Tags) == 0 {
		return nil, schema.BadRequest("tags", "tags should not be empty")
	}

	var group *commonv1.Group
	if group, err := ps.schemaRegistry.GroupRegistry().GetGroup(ctx, g); err != nil {
		return nil, errors.Errorf("group %s not found", g)
	} else if group.Catalog != commonv1.Catalog_CATALOG_PROPERTY {
		return nil, errors.Errorf("group %s is not allowed to have properties", g)
	}
	qResp, err := ps.Query(ctx, &propertyv1.QueryRequest{
		Groups:    []string{g},
		Container: property.Metadata.Container.Name,
		Ids:       []string{property.Metadata.Id},
	})
	if err != nil {
		return nil, err
	}
	var prev *propertyv1.Property
	if len(qResp.Properties) > 0 {
		prev = qResp.Properties[0]
		defer func() {
			if err == nil {
				var ids [][]byte
				for _, p := range qResp.Properties {
					ids = append(ids, getPropertyID(p))
				}
				if err := ps.remove(ctx, ids); err != nil {
					err = multierr.Append(err, errors.New("fail to remove old properties"))
				}
			}
		}()
	}
	entity := getEntity(property)
	id, err := partition.ShardID(convert.StringToBytes(entity), group.ResourceOpts.ShardNum)
	if err != nil {
		return nil, err
	}
	node, err := ps.nodeRegistry.Locate(g, entity, uint32(id))
	if err != nil {
		return nil, err
	}
	if req.Strategy == propertyv1.ApplyRequest_STRATEGY_REPLACE {
		return ps.replaceProperty(ctx, start, uint64(id), node, prev, property)
	}
	return ps.mergeProperty(ctx, start, uint64(id), node, prev, property)
}

func (ps *propertyServer) mergeProperty(ctx context.Context, now time.Time, shardID uint64, node string, prev, cur *propertyv1.Property) (*propertyv1.ApplyResponse, error) {
	if prev == nil {
		return ps.replaceProperty(ctx, now, shardID, node, prev, cur)
	}
	tagCount, err := tagLen(prev)
	if err != nil {
		return nil, err
	}
	tags := make([]*modelv1.Tag, 0)
	for i := 0; i < int(tagCount); i++ {
		t := prev.Tags[i]
		tagExisted := false
		for _, et := range cur.Tags {
			if et.Key == t.Key {
				et.Value = t.Value
				tagExisted = true
				break
			}
		}
		if !tagExisted {
			tags = append(tags, t)
		}
	}
	cur.Tags = append(cur.Tags, tags...)
	return ps.replaceProperty(ctx, now, shardID, node, prev, cur)
}

func tagLen(property *propertyv1.Property) (uint32, error) {
	tagsCount := len(property.Tags)
	if tagsCount < 0 || uint64(tagsCount) > math.MaxUint32 {
		return 0, errors.New("integer overflow: tags count exceeds uint32 range")
	}
	tagsNum := uint32(tagsCount)
	return tagsNum, nil
}

func (ps *propertyServer) replaceProperty(ctx context.Context, now time.Time, shardID uint64, node string, prev, cur *propertyv1.Property) (*propertyv1.ApplyResponse, error) {
	ns := now.UnixNano()
	if prev != nil {
		cur.Metadata.Container.CreateRevision = prev.Metadata.Container.CreateRevision
	} else {
		cur.Metadata.Container.CreateRevision = ns
	}
	cur.Metadata.Container.ModRevision = ns
	cur.UpdatedAt = timestamppb.New(now)
	f, err := ps.broadcaster.Publish(ctx, data.TopicPropertyUpdate, bus.NewMessageWithNode(bus.MessageID(time.Now().Unix()), node, &propertyv1.InternalUpdateRequest{
		ShardId:  shardID,
		Id:       getPropertyID(cur),
		Property: cur,
	}))
	if err != nil {
		return nil, err
	}
	if _, err := f.Get(); err != nil {
		return nil, err
	}
	return &propertyv1.ApplyResponse{
		Created: true,
		TagsNum: uint32(len(cur.Tags)),
	}, nil
}

func (ps *propertyServer) Delete(ctx context.Context, req *propertyv1.DeleteRequest) (resp *propertyv1.DeleteResponse, err error) {
	g := req.Metadata.Container.Group
	ps.metrics.totalStarted.Inc(1, g, "property", "delete")
	start := time.Now()
	defer func() {
		ps.metrics.totalFinished.Inc(1, g, "property", "delete")
		ps.metrics.totalLatency.Inc(time.Since(start).Seconds(), g, "property", "delete")
		if err != nil {
			ps.metrics.totalErr.Inc(1, g, "property", "delete")
		}
	}()
	if req.Metadata == nil {
		return nil, schema.BadRequest("metadata", "metadata should not be nil")
	}
	if req.Metadata.Container == nil {
		return nil, schema.BadRequest("metadata.container", "container should not be nil")
	}
	md := req.Metadata
	qReq := &propertyv1.QueryRequest{
		Groups:    []string{g},
		Container: md.Container.Name,
	}
	if len(md.Id) > 0 {
		qReq.Ids = []string{md.Id}
	}
	qResp, err := ps.Query(ctx, qReq)
	if err != nil {
		return nil, err
	}
	if len(qResp.Properties) == 0 {
		return &propertyv1.DeleteResponse{Deleted: false}, nil
	}
	property := qResp.Properties[0]
	if len(req.Tags) > 0 {
		property.Metadata.Container.ModRevision = start.UnixNano()
		filtered := &propertyv1.Property{
			Metadata:  property.Metadata,
			UpdatedAt: timestamppb.New(start),
		}

		for _, expectedTag := range req.Tags {
			for _, t := range property.Tags {
				if t.Key != expectedTag {
					filtered.Tags = append(filtered.Tags, t)
				}
			}
		}
		if _, err = ps.Apply(ctx, &propertyv1.ApplyRequest{
			Property: filtered,
			Strategy: propertyv1.ApplyRequest_STRATEGY_REPLACE,
		}); err != nil {
			return nil, err
		}
		return &propertyv1.DeleteResponse{Deleted: true}, nil
	}
	var ids [][]byte
	ids = append(ids, getPropertyID(property))
	if err := ps.remove(ctx, ids); err != nil {
		return nil, err
	}
	return &propertyv1.DeleteResponse{Deleted: true}, nil
}

func (ps *propertyServer) Query(ctx context.Context, req *propertyv1.QueryRequest) (resp *propertyv1.QueryResponse, err error) {
	ps.metrics.totalStarted.Inc(1, "", "property", "query")
	start := time.Now()
	defer func() {
		ps.metrics.totalFinished.Inc(1, "", "property", "query")
		ps.metrics.totalLatency.Inc(time.Since(start).Seconds(), "", "property", "query")
		if err != nil {
			ps.metrics.totalErr.Inc(1, "", "property", "query")
		}
	}()
	if len(req.Groups) == 0 {
		return nil, schema.BadRequest("groups", "groups should not be empty")
	}
	if req.Container == "" {
		return nil, schema.BadRequest("container", "container should not be empty")
	}
	if len(req.TagProjection) == 0 {
		return nil, schema.BadRequest("tag_projection", "tag_projection should not be empty")
	}
	if req.Limit == 0 {
		req.Limit = 100
	}
	for _, gn := range req.Groups {
		if g, getGroupErr := ps.schemaRegistry.GroupRegistry().GetGroup(ctx, gn); getGroupErr != nil {
			return nil, errors.Errorf("group %s not found", gn)
		} else if g.Catalog != commonv1.Catalog_CATALOG_PROPERTY {
			return nil, errors.Errorf("group %s is not allowed to have properties", gn)
		}
	}
	ff, err := ps.broadcaster.Broadcast(defaultQueryTimeout, data.TopicPropertyQuery, bus.NewMessage(bus.MessageID(start.Unix()), req))
	if err != nil {
		return nil, err
	}
	res := make(map[string]*propertyv1.Property)
	for _, f := range ff {
		if m, getErr := f.Get(); getErr != nil {
			err = multierr.Append(err, getErr)
		} else {
			d := m.Data()
			if d == nil {
				continue
			}
			switch v := d.(type) {
			case *propertyv1.InternalQueryResponse:
				for _, s := range v.Sources {
					var p propertyv1.Property
					err = protojson.Unmarshal(s, &p)
					if err != nil {
						return nil, err
					}
					entity := getEntity(&p)
					cur, ok := res[entity]
					if !ok {
						res[entity] = &p
						continue
					}
					if cur.Metadata.Container.ModRevision < p.Metadata.Container.ModRevision {
						res[entity] = &p
						err = ps.remove(ctx, [][]byte{getPropertyID(cur)})
						if err != nil {
							return nil, err
						}
					}
				}
			case common.Error:
				err = multierr.Append(err, errors.New(v.Msg()))
			}
		}

	}
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return &propertyv1.QueryResponse{Properties: nil}, nil
	}
	properties := make([]*propertyv1.Property, 0, len(res))
	for _, p := range res {
		properties = append(properties, p)
	}
	return &propertyv1.QueryResponse{Properties: properties}, nil
}

func (ps *propertyServer) remove(ctx context.Context, ids [][]byte) error {
	ff, err := ps.broadcaster.Broadcast(defaultQueryTimeout, data.TopicPropertyDelete, bus.NewMessage(bus.MessageID(time.Now().Unix()), &propertyv1.InternalDeleteRequest{
		Ids: ids,
	}))
	if err != nil {
		return err
	}
	for _, f := range ff {
		if _, err := f.Get(); err != nil {
			return err
		}
	}
	return nil
}

func getPropertyID(prop *propertyv1.Property) []byte {
	return convert.StringToBytes(getEntity(prop) + "/" + strconv.FormatInt(prop.Metadata.Container.ModRevision, 10))
}

func getEntity(prop *propertyv1.Property) string {
	return strings.Join([]string{prop.Metadata.Container.Group, prop.Metadata.Container.Name, prop.Metadata.Id}, "/")
}
