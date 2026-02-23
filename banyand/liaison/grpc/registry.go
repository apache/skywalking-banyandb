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
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

type streamRegistryServer struct {
	databasev1.UnimplementedStreamRegistryServiceServer
	schemaRegistry metadata.Repo
	metrics        *metrics
}

func (rs *streamRegistryServer) Create(ctx context.Context,
	req *databasev1.StreamRegistryServiceCreateRequest,
) (*databasev1.StreamRegistryServiceCreateResponse, error) {
	g := req.Stream.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "stream", "create")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "stream", "create")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "stream", "create")
	}()
	modRevision, err := rs.schemaRegistry.StreamRegistry().CreateStream(ctx, req.GetStream())
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "stream", "create")
		return nil, err
	}
	return &databasev1.StreamRegistryServiceCreateResponse{
		ModRevision: modRevision,
	}, nil
}

func (rs *streamRegistryServer) Update(ctx context.Context,
	req *databasev1.StreamRegistryServiceUpdateRequest,
) (*databasev1.StreamRegistryServiceUpdateResponse, error) {
	g := req.Stream.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "stream", "update")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "stream", "update")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "stream", "update")
	}()
	modRevision, err := rs.schemaRegistry.StreamRegistry().UpdateStream(ctx, req.GetStream())
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "stream", "update")
		return nil, err
	}
	return &databasev1.StreamRegistryServiceUpdateResponse{
		ModRevision: modRevision,
	}, nil
}

func (rs *streamRegistryServer) Delete(ctx context.Context,
	req *databasev1.StreamRegistryServiceDeleteRequest,
) (*databasev1.StreamRegistryServiceDeleteResponse, error) {
	g := req.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "stream", "delete")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "stream", "delete")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "stream", "delete")
	}()
	ok, err := rs.schemaRegistry.StreamRegistry().DeleteStream(ctx, req.GetMetadata())
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "stream", "delete")
		return nil, err
	}
	return &databasev1.StreamRegistryServiceDeleteResponse{
		Deleted: ok,
	}, nil
}

func (rs *streamRegistryServer) Get(ctx context.Context,
	req *databasev1.StreamRegistryServiceGetRequest,
) (*databasev1.StreamRegistryServiceGetResponse, error) {
	g := req.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "stream", "get")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "stream", "get")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "stream", "get")
	}()
	entity, err := rs.schemaRegistry.StreamRegistry().GetStream(ctx, req.GetMetadata())
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "stream", "get")
		return nil, err
	}
	return &databasev1.StreamRegistryServiceGetResponse{
		Stream: entity,
	}, nil
}

func (rs *streamRegistryServer) List(ctx context.Context,
	req *databasev1.StreamRegistryServiceListRequest,
) (*databasev1.StreamRegistryServiceListResponse, error) {
	g := req.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "stream", "list")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "stream", "list")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "stream", "list")
	}()
	entities, err := rs.schemaRegistry.StreamRegistry().ListStream(ctx, schema.ListOpt{Group: req.GetGroup()})
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "stream", "list")
		return nil, err
	}
	return &databasev1.StreamRegistryServiceListResponse{
		Stream: entities,
	}, nil
}

func (rs *streamRegistryServer) Exist(ctx context.Context, req *databasev1.StreamRegistryServiceExistRequest) (*databasev1.StreamRegistryServiceExistResponse, error) {
	g := req.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "stream", "exist")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "stream", "exist")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "stream", "exist")
	}()
	_, err := rs.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{Metadata: req.Metadata})
	if err == nil {
		return &databasev1.StreamRegistryServiceExistResponse{
			HasGroup:  true,
			HasStream: true,
		}, nil
	}
	exist, errGroup := groupExist(ctx, err, req.Metadata, rs.schemaRegistry.GroupRegistry())
	if errGroup != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "stream", "exist")
		return nil, errGroup
	}
	return &databasev1.StreamRegistryServiceExistResponse{HasGroup: exist, HasStream: false}, nil
}

func groupExist(ctx context.Context, errResource error, metadata *commonv1.Metadata, groupRegistry schema.Group) (bool, error) {
	if !errors.Is(errResource, schema.ErrGRPCResourceNotFound) {
		return false, errResource
	}
	if metadata == nil {
		return false, status.Error(codes.InvalidArgument, "metadata is absent")
	}
	_, errGroup := groupRegistry.GetGroup(ctx, metadata.Group)
	if errGroup == nil {
		return true, nil
	}
	if errors.Is(errGroup, schema.ErrGRPCResourceNotFound) {
		return false, nil
	}
	return false, errGroup
}

type indexRuleBindingRegistryServer struct {
	databasev1.UnimplementedIndexRuleBindingRegistryServiceServer
	schemaRegistry metadata.Repo
	metrics        *metrics
}

func (rs *indexRuleBindingRegistryServer) Create(ctx context.Context,
	req *databasev1.IndexRuleBindingRegistryServiceCreateRequest) (
	*databasev1.IndexRuleBindingRegistryServiceCreateResponse, error,
) {
	g := req.IndexRuleBinding.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "indexRuleBinding", "create")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "indexRuleBinding", "create")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "indexRuleBinding", "create")
	}()
	if err := rs.schemaRegistry.IndexRuleBindingRegistry().CreateIndexRuleBinding(ctx, req.GetIndexRuleBinding()); err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "indexRuleBinding", "create")
		return nil, err
	}
	return &databasev1.IndexRuleBindingRegistryServiceCreateResponse{}, nil
}

func (rs *indexRuleBindingRegistryServer) Update(ctx context.Context,
	req *databasev1.IndexRuleBindingRegistryServiceUpdateRequest) (
	*databasev1.IndexRuleBindingRegistryServiceUpdateResponse, error,
) {
	g := req.IndexRuleBinding.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "indexRuleBinding", "update")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "indexRuleBinding", "update")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "indexRuleBinding", "update")
	}()
	if err := rs.schemaRegistry.IndexRuleBindingRegistry().UpdateIndexRuleBinding(ctx, req.GetIndexRuleBinding()); err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "indexRuleBinding", "update")
		return nil, err
	}
	return &databasev1.IndexRuleBindingRegistryServiceUpdateResponse{}, nil
}

func (rs *indexRuleBindingRegistryServer) Delete(ctx context.Context,
	req *databasev1.IndexRuleBindingRegistryServiceDeleteRequest) (
	*databasev1.IndexRuleBindingRegistryServiceDeleteResponse, error,
) {
	g := req.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "indexRuleBinding", "delete")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "indexRuleBinding", "delete")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "indexRuleBinding", "delete")
	}()
	ok, err := rs.schemaRegistry.IndexRuleBindingRegistry().DeleteIndexRuleBinding(ctx, req.GetMetadata())
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "indexRuleBinding", "delete")
		return nil, err
	}
	return &databasev1.IndexRuleBindingRegistryServiceDeleteResponse{
		Deleted: ok,
	}, nil
}

func (rs *indexRuleBindingRegistryServer) Get(ctx context.Context,
	req *databasev1.IndexRuleBindingRegistryServiceGetRequest) (
	*databasev1.IndexRuleBindingRegistryServiceGetResponse, error,
) {
	g := req.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "indexRuleBinding", "get")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "indexRuleBinding", "get")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "indexRuleBinding", "get")
	}()
	entity, err := rs.schemaRegistry.IndexRuleBindingRegistry().GetIndexRuleBinding(ctx, req.GetMetadata())
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "indexRuleBinding", "get")
		return nil, err
	}
	return &databasev1.IndexRuleBindingRegistryServiceGetResponse{
		IndexRuleBinding: entity,
	}, nil
}

func (rs *indexRuleBindingRegistryServer) List(ctx context.Context,
	req *databasev1.IndexRuleBindingRegistryServiceListRequest) (
	*databasev1.IndexRuleBindingRegistryServiceListResponse, error,
) {
	g := req.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "indexRuleBinding", "list")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "indexRuleBinding", "list")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "indexRuleBinding", "list")
	}()
	entities, err := rs.schemaRegistry.IndexRuleBindingRegistry().
		ListIndexRuleBinding(ctx, schema.ListOpt{Group: req.GetGroup()})
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "indexRuleBinding", "list")
		return nil, err
	}
	return &databasev1.IndexRuleBindingRegistryServiceListResponse{
		IndexRuleBinding: entities,
	}, nil
}

func (rs *indexRuleBindingRegistryServer) Exist(ctx context.Context, req *databasev1.IndexRuleBindingRegistryServiceExistRequest) (
	*databasev1.IndexRuleBindingRegistryServiceExistResponse, error,
) {
	g := req.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "indexRuleBinding", "exist")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "indexRuleBinding", "exist")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "indexRuleBinding", "exist")
	}()
	_, err := rs.Get(ctx, &databasev1.IndexRuleBindingRegistryServiceGetRequest{Metadata: req.Metadata})
	if err == nil {
		return &databasev1.IndexRuleBindingRegistryServiceExistResponse{
			HasGroup:            true,
			HasIndexRuleBinding: true,
		}, nil
	}
	exist, errGroup := groupExist(ctx, err, req.Metadata, rs.schemaRegistry.GroupRegistry())
	if errGroup != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "indexRuleBinding", "exist")
		return nil, errGroup
	}
	return &databasev1.IndexRuleBindingRegistryServiceExistResponse{HasGroup: exist, HasIndexRuleBinding: false}, nil
}

type indexRuleRegistryServer struct {
	databasev1.UnimplementedIndexRuleRegistryServiceServer
	schemaRegistry metadata.Repo
	metrics        *metrics
}

func (rs *indexRuleRegistryServer) Create(ctx context.Context,
	req *databasev1.IndexRuleRegistryServiceCreateRequest) (
	*databasev1.IndexRuleRegistryServiceCreateResponse, error,
) {
	g := req.IndexRule.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "indexRule", "create")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "indexRule", "create")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "indexRule", "create")
	}()
	if err := rs.schemaRegistry.IndexRuleRegistry().CreateIndexRule(ctx, req.GetIndexRule()); err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "indexRule", "create")
		return nil, err
	}
	return &databasev1.IndexRuleRegistryServiceCreateResponse{}, nil
}

func (rs *indexRuleRegistryServer) Update(ctx context.Context,
	req *databasev1.IndexRuleRegistryServiceUpdateRequest) (
	*databasev1.IndexRuleRegistryServiceUpdateResponse, error,
) {
	g := req.IndexRule.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "indexRule", "update")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "indexRule", "update")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "indexRule", "update")
	}()
	if err := rs.schemaRegistry.IndexRuleRegistry().UpdateIndexRule(ctx, req.GetIndexRule()); err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "indexRule", "update")
		return nil, err
	}
	return &databasev1.IndexRuleRegistryServiceUpdateResponse{}, nil
}

func (rs *indexRuleRegistryServer) Delete(ctx context.Context,
	req *databasev1.IndexRuleRegistryServiceDeleteRequest) (
	*databasev1.IndexRuleRegistryServiceDeleteResponse, error,
) {
	g := req.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "indexRule", "delete")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "indexRule", "delete")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "indexRule", "delete")
	}()
	ok, err := rs.schemaRegistry.IndexRuleRegistry().DeleteIndexRule(ctx, req.GetMetadata())
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "indexRule", "delete")
		return nil, err
	}
	return &databasev1.IndexRuleRegistryServiceDeleteResponse{
		Deleted: ok,
	}, nil
}

func (rs *indexRuleRegistryServer) Get(ctx context.Context,
	req *databasev1.IndexRuleRegistryServiceGetRequest) (
	*databasev1.IndexRuleRegistryServiceGetResponse, error,
) {
	g := req.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "indexRule", "get")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "indexRule", "get")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "indexRule", "get")
	}()
	entity, err := rs.schemaRegistry.IndexRuleRegistry().GetIndexRule(ctx, req.GetMetadata())
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "indexRule", "get")
		return nil, err
	}
	return &databasev1.IndexRuleRegistryServiceGetResponse{
		IndexRule: entity,
	}, nil
}

func (rs *indexRuleRegistryServer) List(ctx context.Context,
	req *databasev1.IndexRuleRegistryServiceListRequest) (
	*databasev1.IndexRuleRegistryServiceListResponse, error,
) {
	g := req.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "indexRule", "list")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "indexRule", "list")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "indexRule", "list")
	}()
	entities, err := rs.schemaRegistry.IndexRuleRegistry().ListIndexRule(ctx, schema.ListOpt{Group: req.GetGroup()})
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "indexRule", "list")
		return nil, err
	}
	return &databasev1.IndexRuleRegistryServiceListResponse{
		IndexRule: entities,
	}, nil
}

func (rs *indexRuleRegistryServer) Exist(ctx context.Context, req *databasev1.IndexRuleRegistryServiceExistRequest) (
	*databasev1.IndexRuleRegistryServiceExistResponse, error,
) {
	g := req.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "indexRule", "exist")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "indexRule", "exist")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "indexRule", "exist")
	}()
	_, err := rs.Get(ctx, &databasev1.IndexRuleRegistryServiceGetRequest{Metadata: req.Metadata})
	if err == nil {
		return &databasev1.IndexRuleRegistryServiceExistResponse{
			HasGroup:     true,
			HasIndexRule: true,
		}, nil
	}
	exist, errGroup := groupExist(ctx, err, req.Metadata, rs.schemaRegistry.GroupRegistry())
	if errGroup != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "indexRule", "exist")
		return nil, errGroup
	}
	return &databasev1.IndexRuleRegistryServiceExistResponse{HasGroup: exist, HasIndexRule: false}, nil
}

type measureRegistryServer struct {
	databasev1.UnimplementedMeasureRegistryServiceServer
	schemaRegistry metadata.Repo
	metrics        *metrics
}

func (rs *measureRegistryServer) Create(ctx context.Context,
	req *databasev1.MeasureRegistryServiceCreateRequest) (
	*databasev1.MeasureRegistryServiceCreateResponse, error,
) {
	g := req.Measure.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "measure", "create")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "measure", "create")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "measure", "create")
	}()
	modRevision, err := rs.schemaRegistry.MeasureRegistry().CreateMeasure(ctx, req.GetMeasure())
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "measure", "create")
		return nil, err
	}
	return &databasev1.MeasureRegistryServiceCreateResponse{
		ModRevision: modRevision,
	}, nil
}

func (rs *measureRegistryServer) Update(ctx context.Context,
	req *databasev1.MeasureRegistryServiceUpdateRequest) (
	*databasev1.MeasureRegistryServiceUpdateResponse, error,
) {
	g := req.Measure.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "measure", "update")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "measure", "update")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "measure", "update")
	}()
	modRevision, err := rs.schemaRegistry.MeasureRegistry().UpdateMeasure(ctx, req.GetMeasure())
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "measure", "update")
		return nil, err
	}
	return &databasev1.MeasureRegistryServiceUpdateResponse{
		ModRevision: modRevision,
	}, nil
}

func (rs *measureRegistryServer) Delete(ctx context.Context,
	req *databasev1.MeasureRegistryServiceDeleteRequest) (
	*databasev1.MeasureRegistryServiceDeleteResponse, error,
) {
	g := req.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "measure", "delete")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "measure", "delete")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "measure", "delete")
	}()
	ok, err := rs.schemaRegistry.MeasureRegistry().DeleteMeasure(ctx, req.GetMetadata())
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "measure", "delete")
		return nil, err
	}
	return &databasev1.MeasureRegistryServiceDeleteResponse{
		Deleted: ok,
	}, nil
}

func (rs *measureRegistryServer) Get(ctx context.Context,
	req *databasev1.MeasureRegistryServiceGetRequest) (
	*databasev1.MeasureRegistryServiceGetResponse, error,
) {
	g := req.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "measure", "get")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "measure", "get")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "measure", "get")
	}()
	entity, err := rs.schemaRegistry.MeasureRegistry().GetMeasure(ctx, req.GetMetadata())
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "measure", "get")
		return nil, err
	}
	return &databasev1.MeasureRegistryServiceGetResponse{
		Measure: entity,
	}, nil
}

func (rs *measureRegistryServer) List(ctx context.Context,
	req *databasev1.MeasureRegistryServiceListRequest) (
	*databasev1.MeasureRegistryServiceListResponse, error,
) {
	g := req.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "measure", "list")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "measure", "list")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "measure", "list")
	}()
	entities, err := rs.schemaRegistry.MeasureRegistry().ListMeasure(ctx, schema.ListOpt{Group: req.GetGroup()})
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "measure", "list")
		return nil, err
	}
	return &databasev1.MeasureRegistryServiceListResponse{
		Measure: entities,
	}, nil
}

func (rs *measureRegistryServer) Exist(ctx context.Context,
	req *databasev1.MeasureRegistryServiceExistRequest) (
	*databasev1.MeasureRegistryServiceExistResponse, error,
) {
	g := req.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "measure", "exist")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "measure", "exist")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "measure", "exist")
	}()
	_, err := rs.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: req.Metadata})
	if err == nil {
		return &databasev1.MeasureRegistryServiceExistResponse{
			HasGroup:   true,
			HasMeasure: true,
		}, nil
	}
	exist, errGroup := groupExist(ctx, err, req.Metadata, rs.schemaRegistry.GroupRegistry())
	if errGroup != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "measure", "exist")
		return nil, errGroup
	}
	return &databasev1.MeasureRegistryServiceExistResponse{HasGroup: exist, HasMeasure: false}, nil
}

type groupRegistryServer struct {
	databasev1.UnimplementedGroupRegistryServiceServer
	schemaRegistry      metadata.Repo
	deletionTaskManager *groupDeletionTaskManager
	metrics             *metrics
}

func (rs *groupRegistryServer) Create(ctx context.Context, req *databasev1.GroupRegistryServiceCreateRequest) (
	*databasev1.GroupRegistryServiceCreateResponse, error,
) {
	g := ""
	rs.metrics.totalRegistryStarted.Inc(1, g, "group", "create")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "group", "create")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "group", "create")
	}()
	if err := rs.schemaRegistry.GroupRegistry().CreateGroup(ctx, req.GetGroup()); err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "group", "create")
		return nil, err
	}
	return &databasev1.GroupRegistryServiceCreateResponse{}, nil
}

func (rs *groupRegistryServer) Update(ctx context.Context, req *databasev1.GroupRegistryServiceUpdateRequest) (
	*databasev1.GroupRegistryServiceUpdateResponse, error,
) {
	g := ""
	rs.metrics.totalRegistryStarted.Inc(1, g, "group", "update")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "group", "update")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "group", "update")
	}()
	if err := rs.schemaRegistry.GroupRegistry().UpdateGroup(ctx, req.GetGroup()); err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "group", "update")
		return nil, err
	}
	return &databasev1.GroupRegistryServiceUpdateResponse{}, nil
}

func (rs *groupRegistryServer) Delete(ctx context.Context, req *databasev1.GroupRegistryServiceDeleteRequest) (
	*databasev1.GroupRegistryServiceDeleteResponse, error,
) {
	g := req.GetGroup()
	rs.metrics.totalRegistryStarted.Inc(1, g, "group", "delete")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "group", "delete")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "group", "delete")
	}()
	if _, getErr := rs.schemaRegistry.GroupRegistry().GetGroup(ctx, g); getErr != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "group", "delete")
		return nil, getErr
	}
	if !req.GetForce() {
		hasResources, checkErr := rs.deletionTaskManager.hasNonEmptyResources(ctx, g)
		if hasResources {
			rs.metrics.totalRegistryErr.Inc(1, g, "group", "delete")
			return nil, status.Errorf(codes.FailedPrecondition,
				"group %s is not empty, use force=true to delete non-empty groups", g)
		}
		if checkErr != nil {
			rs.metrics.totalRegistryErr.Inc(1, g, "group", "delete")
			return nil, checkErr
		}
	}
	if startErr := rs.deletionTaskManager.startDeletion(ctx, g); startErr != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "group", "delete")
		return nil, startErr
	}
	return &databasev1.GroupRegistryServiceDeleteResponse{}, nil
}

func (rs *groupRegistryServer) Get(ctx context.Context, req *databasev1.GroupRegistryServiceGetRequest) (
	*databasev1.GroupRegistryServiceGetResponse, error,
) {
	g := ""
	rs.metrics.totalRegistryStarted.Inc(1, g, "group", "get")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "group", "get")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "group", "get")
	}()
	group, err := rs.schemaRegistry.GroupRegistry().GetGroup(ctx, req.GetGroup())
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "group", "get")
		return nil, err
	}
	return &databasev1.GroupRegistryServiceGetResponse{
		Group: group,
	}, nil
}

func (rs *groupRegistryServer) List(ctx context.Context, _ *databasev1.GroupRegistryServiceListRequest) (
	*databasev1.GroupRegistryServiceListResponse, error,
) {
	g := ""
	rs.metrics.totalRegistryStarted.Inc(1, g, "group", "list")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "group", "list")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "group", "list")
	}()
	groups, err := rs.schemaRegistry.GroupRegistry().ListGroup(ctx)
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "group", "list")
		return nil, err
	}
	return &databasev1.GroupRegistryServiceListResponse{
		Group: groups,
	}, nil
}

func (rs *groupRegistryServer) Exist(ctx context.Context, req *databasev1.GroupRegistryServiceExistRequest) (
	*databasev1.GroupRegistryServiceExistResponse, error,
) {
	g := ""
	rs.metrics.totalRegistryStarted.Inc(1, g, "group", "exist")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "group", "exist")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "group", "exist")
	}()
	_, err := rs.Get(ctx, &databasev1.GroupRegistryServiceGetRequest{Group: req.Group})
	if err == nil {
		return &databasev1.GroupRegistryServiceExistResponse{
			HasGroup: true,
		}, nil
	}
	if errors.Is(err, schema.ErrGRPCResourceNotFound) {
		return &databasev1.GroupRegistryServiceExistResponse{
			HasGroup: false,
		}, nil
	}
	rs.metrics.totalRegistryErr.Inc(1, g, "group", "exist")
	return nil, err
}

func (rs *groupRegistryServer) Inspect(ctx context.Context, req *databasev1.GroupRegistryServiceInspectRequest) (
	*databasev1.GroupRegistryServiceInspectResponse, error,
) {
	g := req.GetGroup()
	rs.metrics.totalRegistryStarted.Inc(1, g, "group", "inspect")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "group", "inspect")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "group", "inspect")
	}()
	group, err := rs.schemaRegistry.GroupRegistry().GetGroup(ctx, g)
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "group", "inspect")
		return nil, err
	}
	schemaInfo, schemaErr := rs.collectSchemaInfo(ctx, g)
	if schemaErr != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "group", "inspect")
		return nil, schemaErr
	}
	dataInfo, dataErr := rs.schemaRegistry.CollectDataInfo(ctx, g)
	if dataErr != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "group", "inspect")
		return nil, dataErr
	}
	liaisonInfo, liaisonErr := rs.schemaRegistry.CollectLiaisonInfo(ctx, g)
	if liaisonErr != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "group", "inspect")
		return nil, liaisonErr
	}
	return &databasev1.GroupRegistryServiceInspectResponse{
		Group:       group,
		SchemaInfo:  schemaInfo,
		DataInfo:    dataInfo,
		LiaisonInfo: liaisonInfo,
	}, nil
}

func (rs *groupRegistryServer) Query(ctx context.Context, req *databasev1.GroupRegistryServiceQueryRequest) (
	*databasev1.GroupRegistryServiceQueryResponse, error,
) {
	g := req.GetGroup()
	rs.metrics.totalRegistryStarted.Inc(1, g, "group", "query")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "group", "query")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "group", "query")
	}()
	task, queryErr := rs.deletionTaskManager.getDeletionTask(ctx, g)
	if queryErr != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "group", "query")
		return nil, queryErr
	}
	return &databasev1.GroupRegistryServiceQueryResponse{
		Task: task,
	}, nil
}

func (rs *groupRegistryServer) collectSchemaInfo(ctx context.Context, group string) (*databasev1.SchemaInfo, error) {
	opt := schema.ListOpt{Group: group}
	streams, streamsErr := rs.schemaRegistry.StreamRegistry().ListStream(ctx, opt)
	if streamsErr != nil {
		return nil, streamsErr
	}
	streamNames := make([]string, 0, len(streams))
	for _, s := range streams {
		streamNames = append(streamNames, s.GetMetadata().GetName())
	}
	measures, measuresErr := rs.schemaRegistry.MeasureRegistry().ListMeasure(ctx, opt)
	if measuresErr != nil {
		return nil, measuresErr
	}
	measureNames := make([]string, 0, len(measures))
	for _, m := range measures {
		measureNames = append(measureNames, m.GetMetadata().GetName())
	}
	traces, tracesErr := rs.schemaRegistry.TraceRegistry().ListTrace(ctx, opt)
	if tracesErr != nil {
		return nil, tracesErr
	}
	traceNames := make([]string, 0, len(traces))
	for _, t := range traces {
		traceNames = append(traceNames, t.GetMetadata().GetName())
	}
	properties, propertiesErr := rs.schemaRegistry.PropertyRegistry().ListProperty(ctx, opt)
	if propertiesErr != nil {
		return nil, propertiesErr
	}
	propertyNames := make([]string, 0, len(properties))
	for _, p := range properties {
		propertyNames = append(propertyNames, p.GetMetadata().GetName())
	}
	indexRules, indexRulesErr := rs.schemaRegistry.IndexRuleRegistry().ListIndexRule(ctx, opt)
	if indexRulesErr != nil {
		return nil, indexRulesErr
	}
	indexRuleNames := make([]string, 0, len(indexRules))
	for _, ir := range indexRules {
		indexRuleNames = append(indexRuleNames, ir.GetMetadata().GetName())
	}
	indexRuleBindings, indexRuleBindingsErr := rs.schemaRegistry.IndexRuleBindingRegistry().ListIndexRuleBinding(ctx, opt)
	if indexRuleBindingsErr != nil {
		return nil, indexRuleBindingsErr
	}
	bindingNames := make([]string, 0, len(indexRuleBindings))
	for _, irb := range indexRuleBindings {
		bindingNames = append(bindingNames, irb.GetMetadata().GetName())
	}
	topNAggs, topNAggsErr := rs.schemaRegistry.TopNAggregationRegistry().ListTopNAggregation(ctx, opt)
	if topNAggsErr != nil {
		return nil, topNAggsErr
	}
	topNNames := make([]string, 0, len(topNAggs))
	for _, tn := range topNAggs {
		topNNames = append(topNNames, tn.GetMetadata().GetName())
	}
	return &databasev1.SchemaInfo{
		Streams:           streamNames,
		Measures:          measureNames,
		Traces:            traceNames,
		Properties:        propertyNames,
		IndexRules:        indexRuleNames,
		IndexRuleBindings: bindingNames,
		TopnAggregations:  topNNames,
	}, nil
}

type topNAggregationRegistryServer struct {
	databasev1.UnimplementedTopNAggregationRegistryServiceServer
	schemaRegistry metadata.Repo
	metrics        *metrics
}

func (ts *topNAggregationRegistryServer) Create(ctx context.Context,
	req *databasev1.TopNAggregationRegistryServiceCreateRequest,
) (*databasev1.TopNAggregationRegistryServiceCreateResponse, error) {
	g := req.TopNAggregation.Metadata.Group
	ts.metrics.totalRegistryStarted.Inc(1, g, "topn_aggregation", "create")
	start := time.Now()
	defer func() {
		ts.metrics.totalRegistryFinished.Inc(1, g, "topn_aggregation", "create")
		ts.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "topn_aggregation", "create")
	}()
	if err := ts.schemaRegistry.TopNAggregationRegistry().CreateTopNAggregation(ctx, req.GetTopNAggregation()); err != nil {
		ts.metrics.totalRegistryErr.Inc(1, g, "topn_aggregation", "create")
		return nil, err
	}
	return &databasev1.TopNAggregationRegistryServiceCreateResponse{}, nil
}

func (ts *topNAggregationRegistryServer) Update(ctx context.Context,
	req *databasev1.TopNAggregationRegistryServiceUpdateRequest,
) (*databasev1.TopNAggregationRegistryServiceUpdateResponse, error) {
	g := req.TopNAggregation.Metadata.Group
	ts.metrics.totalRegistryStarted.Inc(1, g, "topn_aggregation", "update")
	start := time.Now()
	defer func() {
		ts.metrics.totalRegistryFinished.Inc(1, g, "topn_aggregation", "update")
		ts.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "topn_aggregation", "update")
	}()
	if err := ts.schemaRegistry.TopNAggregationRegistry().UpdateTopNAggregation(ctx, req.GetTopNAggregation()); err != nil {
		ts.metrics.totalRegistryErr.Inc(1, g, "topn_aggregation", "update")
		return nil, err
	}
	return &databasev1.TopNAggregationRegistryServiceUpdateResponse{}, nil
}

func (ts *topNAggregationRegistryServer) Delete(ctx context.Context,
	req *databasev1.TopNAggregationRegistryServiceDeleteRequest,
) (*databasev1.TopNAggregationRegistryServiceDeleteResponse, error) {
	g := req.Metadata.Group
	ts.metrics.totalRegistryStarted.Inc(1, g, "topn_aggregation", "delete")
	start := time.Now()
	defer func() {
		ts.metrics.totalRegistryFinished.Inc(1, g, "topn_aggregation", "delete")
		ts.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "topn_aggregation", "delete")
	}()
	ok, err := ts.schemaRegistry.TopNAggregationRegistry().DeleteTopNAggregation(ctx, req.GetMetadata())
	if err != nil {
		ts.metrics.totalRegistryErr.Inc(1, g, "topn_aggregation", "delete")
		return nil, err
	}
	return &databasev1.TopNAggregationRegistryServiceDeleteResponse{
		Deleted: ok,
	}, nil
}

func (ts *topNAggregationRegistryServer) Get(ctx context.Context,
	req *databasev1.TopNAggregationRegistryServiceGetRequest,
) (*databasev1.TopNAggregationRegistryServiceGetResponse, error) {
	g := req.Metadata.Group
	ts.metrics.totalRegistryStarted.Inc(1, g, "topn_aggregation", "get")
	start := time.Now()
	defer func() {
		ts.metrics.totalRegistryFinished.Inc(1, g, "topn_aggregation", "get")
		ts.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "topn_aggregation", "get")
	}()
	entity, err := ts.schemaRegistry.TopNAggregationRegistry().GetTopNAggregation(ctx, req.GetMetadata())
	if err != nil {
		ts.metrics.totalRegistryErr.Inc(1, g, "topn_aggregation", "get")
		return nil, err
	}
	return &databasev1.TopNAggregationRegistryServiceGetResponse{
		TopNAggregation: entity,
	}, nil
}

func (ts *topNAggregationRegistryServer) List(ctx context.Context,
	req *databasev1.TopNAggregationRegistryServiceListRequest,
) (*databasev1.TopNAggregationRegistryServiceListResponse, error) {
	g := req.Group
	ts.metrics.totalRegistryStarted.Inc(1, g, "topn_aggregation", "list")
	start := time.Now()
	defer func() {
		ts.metrics.totalRegistryFinished.Inc(1, g, "topn_aggregation", "list")
		ts.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "topn_aggregation", "list")
	}()
	entities, err := ts.schemaRegistry.TopNAggregationRegistry().ListTopNAggregation(ctx, schema.ListOpt{Group: req.GetGroup()})
	if err != nil {
		ts.metrics.totalRegistryErr.Inc(1, g, "topn_aggregation", "list")
		return nil, err
	}
	return &databasev1.TopNAggregationRegistryServiceListResponse{
		TopNAggregation: entities,
	}, nil
}

func (ts *topNAggregationRegistryServer) Exist(ctx context.Context, req *databasev1.TopNAggregationRegistryServiceExistRequest) (
	*databasev1.TopNAggregationRegistryServiceExistResponse, error,
) {
	g := req.Metadata.Group
	ts.metrics.totalRegistryStarted.Inc(1, g, "topn_aggregation", "exist")
	start := time.Now()
	defer func() {
		ts.metrics.totalRegistryFinished.Inc(1, g, "topn_aggregation", "exist")
		ts.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "topn_aggregation", "exist")
	}()
	_, err := ts.Get(ctx, &databasev1.TopNAggregationRegistryServiceGetRequest{Metadata: req.Metadata})
	if err == nil {
		return &databasev1.TopNAggregationRegistryServiceExistResponse{
			HasGroup:           true,
			HasTopNAggregation: true,
		}, nil
	}
	exist, errGroup := groupExist(ctx, err, req.Metadata, ts.schemaRegistry.GroupRegistry())
	if errGroup != nil {
		ts.metrics.totalRegistryErr.Inc(1, g, "topn_aggregation", "exist")
		return nil, errGroup
	}
	return &databasev1.TopNAggregationRegistryServiceExistResponse{HasGroup: exist, HasTopNAggregation: false}, nil
}

type propertyRegistryServer struct {
	databasev1.UnimplementedPropertyRegistryServiceServer
	schemaRegistry metadata.Repo
	metrics        *metrics
}

func (ps *propertyRegistryServer) Create(ctx context.Context, req *databasev1.PropertyRegistryServiceCreateRequest) (
	*databasev1.PropertyRegistryServiceCreateResponse, error,
) {
	g := req.Property.Metadata.Group
	ps.metrics.totalRegistryStarted.Inc(1, g, "property", "create")
	start := time.Now()
	defer func() {
		ps.metrics.totalRegistryFinished.Inc(1, g, "property", "create")
		ps.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "property", "create")
	}()
	if err := ps.schemaRegistry.PropertyRegistry().CreateProperty(ctx, req.GetProperty()); err != nil {
		ps.metrics.totalRegistryErr.Inc(1, g, "property", "create")
		return nil, err
	}
	return &databasev1.PropertyRegistryServiceCreateResponse{}, nil
}

func (ps *propertyRegistryServer) Update(ctx context.Context, req *databasev1.PropertyRegistryServiceUpdateRequest) (
	*databasev1.PropertyRegistryServiceUpdateResponse, error,
) {
	g := req.Property.Metadata.Group
	ps.metrics.totalRegistryStarted.Inc(1, g, "property", "update")
	start := time.Now()
	defer func() {
		ps.metrics.totalRegistryFinished.Inc(1, g, "property", "update")
		ps.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "property", "update")
	}()
	if err := ps.schemaRegistry.PropertyRegistry().UpdateProperty(ctx, req.GetProperty()); err != nil {
		ps.metrics.totalRegistryErr.Inc(1, g, "property", "update")
		return nil, err
	}
	return &databasev1.PropertyRegistryServiceUpdateResponse{}, nil
}

func (ps *propertyRegistryServer) Delete(ctx context.Context, req *databasev1.PropertyRegistryServiceDeleteRequest) (
	*databasev1.PropertyRegistryServiceDeleteResponse, error,
) {
	g := req.Metadata.Group
	ps.metrics.totalRegistryStarted.Inc(1, g, "property", "delete")
	start := time.Now()
	defer func() {
		ps.metrics.totalRegistryFinished.Inc(1, g, "property", "delete")
		ps.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "property", "delete")
	}()
	ok, err := ps.schemaRegistry.PropertyRegistry().DeleteProperty(ctx, req.GetMetadata())
	if err != nil {
		ps.metrics.totalRegistryErr.Inc(1, g, "property", "delete")
		return nil, err
	}
	return &databasev1.PropertyRegistryServiceDeleteResponse{
		Deleted: ok,
	}, nil
}

func (ps *propertyRegistryServer) Get(ctx context.Context, req *databasev1.PropertyRegistryServiceGetRequest) (
	*databasev1.PropertyRegistryServiceGetResponse, error,
) {
	g := req.Metadata.Group
	ps.metrics.totalRegistryStarted.Inc(1, g, "property", "get")
	start := time.Now()
	defer func() {
		ps.metrics.totalRegistryFinished.Inc(1, g, "property", "get")
		ps.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "property", "get")
	}()
	entity, err := ps.schemaRegistry.PropertyRegistry().GetProperty(ctx, req.GetMetadata())
	if err != nil {
		ps.metrics.totalRegistryErr.Inc(1, g, "property", "get")
		return nil, err
	}
	return &databasev1.PropertyRegistryServiceGetResponse{
		Property: entity,
	}, nil
}

func (ps *propertyRegistryServer) List(ctx context.Context, req *databasev1.PropertyRegistryServiceListRequest) (
	*databasev1.PropertyRegistryServiceListResponse, error,
) {
	g := req.Group
	ps.metrics.totalRegistryStarted.Inc(1, g, "property", "list")
	start := time.Now()
	defer func() {
		ps.metrics.totalRegistryFinished.Inc(1, g, "property", "list")
		ps.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "property", "list")
	}()
	entities, err := ps.schemaRegistry.PropertyRegistry().ListProperty(ctx, schema.ListOpt{Group: req.GetGroup()})
	if err != nil {
		ps.metrics.totalRegistryErr.Inc(1, g, "property", "list")
		return nil, err
	}
	return &databasev1.PropertyRegistryServiceListResponse{
		Properties: entities,
	}, nil
}

func (ps *propertyRegistryServer) Exist(ctx context.Context, req *databasev1.PropertyRegistryServiceExistRequest) (
	*databasev1.PropertyRegistryServiceExistResponse, error,
) {
	g := req.Metadata.Group
	ps.metrics.totalRegistryStarted.Inc(1, g, "property", "exist")
	start := time.Now()
	defer func() {
		ps.metrics.totalRegistryFinished.Inc(1, g, "property", "exist")
		ps.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "property", "exist")
	}()
	_, err := ps.Get(ctx, &databasev1.PropertyRegistryServiceGetRequest{Metadata: req.Metadata})
	if err == nil {
		return &databasev1.PropertyRegistryServiceExistResponse{
			HasGroup:    true,
			HasProperty: true,
		}, nil
	}
	exist, errGroup := groupExist(ctx, err, req.Metadata, ps.schemaRegistry.GroupRegistry())
	if errGroup != nil {
		ps.metrics.totalRegistryErr.Inc(1, g, "property", "exist")
		return nil, errGroup
	}
	return &databasev1.PropertyRegistryServiceExistResponse{HasGroup: exist, HasProperty: false}, nil
}

type traceRegistryServer struct {
	databasev1.UnimplementedTraceRegistryServiceServer
	schemaRegistry metadata.Repo
	metrics        *metrics
}

func (rs *traceRegistryServer) Create(ctx context.Context,
	req *databasev1.TraceRegistryServiceCreateRequest,
) (*databasev1.TraceRegistryServiceCreateResponse, error) {
	g := req.Trace.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "trace", "create")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "trace", "create")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "trace", "create")
	}()
	modRevision, err := rs.schemaRegistry.TraceRegistry().CreateTrace(ctx, req.GetTrace())
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "trace", "create")
		return nil, err
	}
	return &databasev1.TraceRegistryServiceCreateResponse{
		ModRevision: modRevision,
	}, nil
}

func (rs *traceRegistryServer) Update(ctx context.Context,
	req *databasev1.TraceRegistryServiceUpdateRequest,
) (*databasev1.TraceRegistryServiceUpdateResponse, error) {
	g := req.Trace.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "trace", "update")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "trace", "update")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "trace", "update")
	}()
	modRevision, err := rs.schemaRegistry.TraceRegistry().UpdateTrace(ctx, req.GetTrace())
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "trace", "update")
		return nil, err
	}
	return &databasev1.TraceRegistryServiceUpdateResponse{
		ModRevision: modRevision,
	}, nil
}

func (rs *traceRegistryServer) Delete(ctx context.Context,
	req *databasev1.TraceRegistryServiceDeleteRequest,
) (*databasev1.TraceRegistryServiceDeleteResponse, error) {
	g := req.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "trace", "delete")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "trace", "delete")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "trace", "delete")
	}()
	ok, err := rs.schemaRegistry.TraceRegistry().DeleteTrace(ctx, req.GetMetadata())
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "trace", "delete")
		return nil, err
	}
	return &databasev1.TraceRegistryServiceDeleteResponse{
		Deleted: ok,
	}, nil
}

func (rs *traceRegistryServer) Get(ctx context.Context,
	req *databasev1.TraceRegistryServiceGetRequest,
) (*databasev1.TraceRegistryServiceGetResponse, error) {
	g := req.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "trace", "get")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "trace", "get")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "trace", "get")
	}()
	entity, err := rs.schemaRegistry.TraceRegistry().GetTrace(ctx, req.GetMetadata())
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "trace", "get")
		return nil, err
	}
	return &databasev1.TraceRegistryServiceGetResponse{
		Trace: entity,
	}, nil
}

func (rs *traceRegistryServer) List(ctx context.Context,
	req *databasev1.TraceRegistryServiceListRequest,
) (*databasev1.TraceRegistryServiceListResponse, error) {
	g := req.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "trace", "list")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "trace", "list")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "trace", "list")
	}()
	entities, err := rs.schemaRegistry.TraceRegistry().ListTrace(ctx, schema.ListOpt{Group: req.GetGroup()})
	if err != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "trace", "list")
		return nil, err
	}
	return &databasev1.TraceRegistryServiceListResponse{
		Trace: entities,
	}, nil
}

func (rs *traceRegistryServer) Exist(ctx context.Context, req *databasev1.TraceRegistryServiceExistRequest) (*databasev1.TraceRegistryServiceExistResponse, error) {
	g := req.Metadata.Group
	rs.metrics.totalRegistryStarted.Inc(1, g, "trace", "exist")
	start := time.Now()
	defer func() {
		rs.metrics.totalRegistryFinished.Inc(1, g, "trace", "exist")
		rs.metrics.totalRegistryLatency.Inc(time.Since(start).Seconds(), g, "trace", "exist")
	}()
	_, err := rs.Get(ctx, &databasev1.TraceRegistryServiceGetRequest{Metadata: req.Metadata})
	if err == nil {
		return &databasev1.TraceRegistryServiceExistResponse{
			HasGroup: true,
			HasTrace: true,
		}, nil
	}
	exist, errGroup := groupExist(ctx, err, req.Metadata, rs.schemaRegistry.GroupRegistry())
	if errGroup != nil {
		rs.metrics.totalRegistryErr.Inc(1, g, "trace", "exist")
		return nil, errGroup
	}
	return &databasev1.TraceRegistryServiceExistResponse{HasGroup: exist, HasTrace: false}, nil
}
