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
	"time"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
)

type propertyServer struct {
	propertyv1.UnimplementedPropertyServiceServer
	schemaRegistry metadata.Repo
	metrics        *metrics
}

func (ps *propertyServer) Apply(ctx context.Context, req *propertyv1.ApplyRequest) (*propertyv1.ApplyResponse, error) {
	g := req.Property.Metadata.Container.Group
	ps.metrics.totalStarted.Inc(1, g, "property", "apply")
	start := time.Now()
	defer func() {
		ps.metrics.totalFinished.Inc(1, g, "property", "apply")
		ps.metrics.totalLatency.Inc(time.Since(start).Seconds(), g, "property", "apply")
	}()
	created, tagsNum, leaseID, err := ps.schemaRegistry.PropertyRegistry().ApplyProperty(ctx, req.Property, req.Strategy)
	if err != nil {
		ps.metrics.totalErr.Inc(1, g, "property", "apply")
		return nil, err
	}
	return &propertyv1.ApplyResponse{Created: created, TagsNum: tagsNum, LeaseId: leaseID}, nil
}

func (ps *propertyServer) Delete(ctx context.Context, req *propertyv1.DeleteRequest) (*propertyv1.DeleteResponse, error) {
	g := req.Metadata.Container.Group
	ps.metrics.totalStarted.Inc(1, g, "property", "delete")
	start := time.Now()
	defer func() {
		ps.metrics.totalFinished.Inc(1, g, "property", "delete")
		ps.metrics.totalLatency.Inc(time.Since(start).Seconds(), g, "property", "delete")
	}()
	ok, tagsNum, err := ps.schemaRegistry.PropertyRegistry().DeleteProperty(ctx, req.GetMetadata(), req.Tags)
	if err != nil {
		ps.metrics.totalErr.Inc(1, g, "property", "delete")
		return nil, err
	}
	return &propertyv1.DeleteResponse{
		Deleted: ok,
		TagsNum: tagsNum,
	}, nil
}

func (ps *propertyServer) Get(ctx context.Context, req *propertyv1.GetRequest) (*propertyv1.GetResponse, error) {
	g := req.Metadata.Container.Group
	ps.metrics.totalStarted.Inc(1, g, "property", "get")
	start := time.Now()
	defer func() {
		ps.metrics.totalFinished.Inc(1, g, "property", "get")
		ps.metrics.totalLatency.Inc(time.Since(start).Seconds(), g, "property", "get")
	}()
	entity, err := ps.schemaRegistry.PropertyRegistry().GetProperty(ctx, req.GetMetadata(), req.GetTags())
	if err != nil {
		ps.metrics.totalErr.Inc(1, g, "property", "get")
		return nil, err
	}
	return &propertyv1.GetResponse{
		Property: entity,
	}, nil
}

func (ps *propertyServer) List(ctx context.Context, req *propertyv1.ListRequest) (*propertyv1.ListResponse, error) {
	g := req.Container.Group
	ps.metrics.totalStarted.Inc(1, g, "property", "list")
	start := time.Now()
	defer func() {
		ps.metrics.totalFinished.Inc(1, g, "property", "list")
		ps.metrics.totalLatency.Inc(time.Since(start).Seconds(), g, "property", "list")
	}()
	entities, err := ps.schemaRegistry.PropertyRegistry().ListProperty(ctx, req.GetContainer(), req.Ids, req.Tags)
	if err != nil {
		ps.metrics.totalErr.Inc(1, g, "property", "list")
		return nil, err
	}
	return &propertyv1.ListResponse{
		Property: entities,
	}, nil
}

func (ps *propertyServer) KeepAlive(ctx context.Context, req *propertyv1.KeepAliveRequest) (*propertyv1.KeepAliveResponse, error) {
	ps.metrics.totalStarted.Inc(1, "", "property", "keep_alive")
	start := time.Now()
	defer func() {
		ps.metrics.totalFinished.Inc(1, "", "property", "keep_alive")
		ps.metrics.totalLatency.Inc(time.Since(start).Seconds(), "", "property", "keep_alive")
	}()
	err := ps.schemaRegistry.PropertyRegistry().KeepAlive(ctx, req.GetLeaseId())
	if err != nil {
		ps.metrics.totalErr.Inc(1, "", "property", "keep_alive")
		return nil, err
	}
	return &propertyv1.KeepAliveResponse{}, nil
}
