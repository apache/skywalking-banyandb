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

package grpc

import (
	"context"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
)

type propertyServer struct {
	schemaRegistry metadata.Service
	propertyv1.UnimplementedPropertyServiceServer
}

func (ps *propertyServer) Create(ctx context.Context, req *propertyv1.CreateRequest) (*propertyv1.CreateResponse, error) {
	if err := ps.schemaRegistry.PropertyRegistry().CreateProperty(ctx, req.GetProperty()); err != nil {
		return nil, err
	}
	return &propertyv1.CreateResponse{}, nil
}
func (ps *propertyServer) Update(ctx context.Context, req *propertyv1.UpdateRequest) (*propertyv1.UpdateResponse, error) {
	if err := ps.schemaRegistry.PropertyRegistry().UpdateProperty(ctx, req.GetProperty()); err != nil {
		return nil, err
	}
	return &propertyv1.UpdateResponse{}, nil
}
func (ps *propertyServer) Delete(ctx context.Context, req *propertyv1.DeleteRequest) (*propertyv1.DeleteResponse, error) {
	ok, err := ps.schemaRegistry.PropertyRegistry().DeleteProperty(ctx, req.GetMetadata())
	if err != nil {
		return nil, err
	}
	return &propertyv1.DeleteResponse{
		Deleted: ok,
	}, nil
}
func (ps *propertyServer) Get(ctx context.Context, req *propertyv1.GetRequest) (*propertyv1.GetResponse, error) {
	entity, err := ps.schemaRegistry.PropertyRegistry().GetProperty(ctx, req.GetMetadata())
	if err != nil {
		return nil, err
	}
	return &propertyv1.GetResponse{
		Property: entity,
	}, nil
}
func (ps *propertyServer) List(ctx context.Context, req *propertyv1.ListRequest) (*propertyv1.ListResponse, error) {
	entities, err := ps.schemaRegistry.PropertyRegistry().ListProperty(ctx, req.GetContaner())
	if err != nil {
		return nil, err
	}
	return &propertyv1.ListResponse{
		Property: entities,
	}, nil
}
