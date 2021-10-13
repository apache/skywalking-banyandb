package grpc

import (
	"context"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

type streamRegistryServer struct {
	schemaRegistry metadata.Service
	databasev1.UnimplementedStreamRegistryServer
}

func (rs *streamRegistryServer) Create(ctx context.Context, req *databasev1.StreamCreateRequest) (*databasev1.StreamCreateResponse, error) {
	if err := rs.schemaRegistry.StreamRegistry().UpdateStream(ctx, req.GetStream()); err != nil {
		return nil, err
	}
	return &databasev1.StreamCreateResponse{}, nil
}

func (rs *streamRegistryServer) Update(ctx context.Context, req *databasev1.StreamUpdateRequest) (*databasev1.StreamUpdateResponse, error) {
	if err := rs.schemaRegistry.StreamRegistry().UpdateStream(ctx, req.GetStream()); err != nil {
		return nil, err
	}
	return &databasev1.StreamUpdateResponse{}, nil
}

func (rs *streamRegistryServer) Delete(ctx context.Context, req *databasev1.StreamDeleteRequest) (*databasev1.StreamDeleteResponse, error) {
	ok, err := rs.schemaRegistry.StreamRegistry().DeleteStream(ctx, req.GetMetadata())
	if err != nil {
		return nil, err
	}
	return &databasev1.StreamDeleteResponse{
		Deleted: ok,
	}, nil
}

func (rs *streamRegistryServer) Get(ctx context.Context, req *databasev1.StreamGetRequest) (*databasev1.StreamGetResponse, error) {
	entity, err := rs.schemaRegistry.StreamRegistry().GetStream(ctx, req.GetMetadata())
	if err != nil {
		return nil, err
	}
	return &databasev1.StreamGetResponse{
		Stream: entity,
	}, nil
}

func (rs *streamRegistryServer) List(ctx context.Context, req *databasev1.StreamListRequest) (*databasev1.StreamListResponse, error) {
	entities, err := rs.schemaRegistry.StreamRegistry().ListStream(ctx, schema.ListOpt{Group: req.GetGroup()})
	if err != nil {
		return nil, err
	}
	return &databasev1.StreamListResponse{
		Stream: entities,
	}, nil
}

type indexRuleBindingRegistryServer struct {
	schemaRegistry metadata.Service
	databasev1.UnimplementedIndexRuleBindingRegistryServer
}

func (rs *indexRuleBindingRegistryServer) Create(ctx context.Context, req *databasev1.IndexRuleBindingCreateRequest) (*databasev1.IndexRuleBindingCreateResponse, error) {
	if err := rs.schemaRegistry.IndexRuleBindingRegistry().UpdateIndexRuleBinding(ctx, req.GetIndexRuleBinding()); err != nil {
		return nil, err
	}
	return &databasev1.IndexRuleBindingCreateResponse{}, nil
}

func (rs *indexRuleBindingRegistryServer) Update(ctx context.Context, req *databasev1.IndexRuleBindingUpdateRequest) (*databasev1.IndexRuleBindingUpdateResponse, error) {
	if err := rs.schemaRegistry.IndexRuleBindingRegistry().UpdateIndexRuleBinding(ctx, req.GetIndexRuleBinding()); err != nil {
		return nil, err
	}
	return &databasev1.IndexRuleBindingUpdateResponse{}, nil
}

func (rs *indexRuleBindingRegistryServer) Delete(ctx context.Context, req *databasev1.IndexRuleBindingDeleteRequest) (*databasev1.IndexRuleBindingDeleteResponse, error) {
	ok, err := rs.schemaRegistry.IndexRuleBindingRegistry().DeleteIndexRuleBinding(ctx, req.GetMetadata())
	if err != nil {
		return nil, err
	}
	return &databasev1.IndexRuleBindingDeleteResponse{
		Deleted: ok,
	}, nil
}

func (rs *indexRuleBindingRegistryServer) Get(ctx context.Context, req *databasev1.IndexRuleBindingGetRequest) (*databasev1.IndexRuleBindingGetResponse, error) {
	entity, err := rs.schemaRegistry.IndexRuleBindingRegistry().GetIndexRuleBinding(ctx, req.GetMetadata())
	if err != nil {
		return nil, err
	}
	return &databasev1.IndexRuleBindingGetResponse{
		IndexRuleBinding: entity,
	}, nil
}

func (rs *indexRuleBindingRegistryServer) List(ctx context.Context, req *databasev1.IndexRuleBindingListRequest) (*databasev1.IndexRuleBindingListResponse, error) {
	entities, err := rs.schemaRegistry.IndexRuleBindingRegistry().ListIndexRuleBinding(ctx, schema.ListOpt{Group: req.GetGroup()})
	if err != nil {
		return nil, err
	}
	return &databasev1.IndexRuleBindingListResponse{
		IndexRuleBinding: entities,
	}, nil
}

type indexRuleRegistryServer struct {
	schemaRegistry metadata.Service
	databasev1.UnimplementedIndexRuleRegistryServer
}

func (rs *indexRuleRegistryServer) Create(ctx context.Context, req *databasev1.IndexRuleCreateRequest) (*databasev1.IndexRuleCreateResponse, error) {
	if err := rs.schemaRegistry.IndexRuleRegistry().UpdateIndexRule(ctx, req.GetIndexRule()); err != nil {
		return nil, err
	}
	return &databasev1.IndexRuleCreateResponse{}, nil
}

func (rs *indexRuleRegistryServer) Update(ctx context.Context, req *databasev1.IndexRuleUpdateRequest) (*databasev1.IndexRuleUpdateResponse, error) {
	if err := rs.schemaRegistry.IndexRuleRegistry().UpdateIndexRule(ctx, req.GetIndexRule()); err != nil {
		return nil, err
	}
	return &databasev1.IndexRuleUpdateResponse{}, nil
}

func (rs *indexRuleRegistryServer) Delete(ctx context.Context, req *databasev1.IndexRuleDeleteRequest) (*databasev1.IndexRuleDeleteResponse, error) {
	ok, err := rs.schemaRegistry.IndexRuleRegistry().DeleteIndexRule(ctx, req.GetMetadata())
	if err != nil {
		return nil, err
	}
	return &databasev1.IndexRuleDeleteResponse{
		Deleted: ok,
	}, nil
}

func (rs *indexRuleRegistryServer) Get(ctx context.Context, req *databasev1.IndexRuleGetRequest) (*databasev1.IndexRuleGetResponse, error) {
	entity, err := rs.schemaRegistry.IndexRuleRegistry().GetIndexRule(ctx, req.GetMetadata())
	if err != nil {
		return nil, err
	}
	return &databasev1.IndexRuleGetResponse{
		IndexRule: entity,
	}, nil
}

func (rs *indexRuleRegistryServer) List(ctx context.Context, req *databasev1.IndexRuleListRequest) (*databasev1.IndexRuleListResponse, error) {
	entities, err := rs.schemaRegistry.IndexRuleRegistry().ListIndexRule(ctx, schema.ListOpt{Group: req.GetGroup()})
	if err != nil {
		return nil, err
	}
	return &databasev1.IndexRuleListResponse{
		IndexRule: entities,
	}, nil
}

type measureRegistryServer struct {
	schemaRegistry metadata.Service
	databasev1.UnimplementedMeasureRegistryServer
}

func (rs *measureRegistryServer) Create(ctx context.Context, req *databasev1.MeasureCreateRequest) (*databasev1.MeasureCreateResponse, error) {
	if err := rs.schemaRegistry.MeasureRegistry().UpdateMeasure(ctx, req.GetMeasure()); err != nil {
		return nil, err
	}
	return &databasev1.MeasureCreateResponse{}, nil
}

func (rs *measureRegistryServer) Update(ctx context.Context, req *databasev1.MeasureUpdateRequest) (*databasev1.MeasureUpdateResponse, error) {
	if err := rs.schemaRegistry.MeasureRegistry().UpdateMeasure(ctx, req.GetMeasure()); err != nil {
		return nil, err
	}
	return &databasev1.MeasureUpdateResponse{}, nil
}

func (rs *measureRegistryServer) Delete(ctx context.Context, req *databasev1.MeasureDeleteRequest) (*databasev1.MeasureDeleteResponse, error) {
	ok, err := rs.schemaRegistry.MeasureRegistry().DeleteMeasure(ctx, req.GetMetadata())
	if err != nil {
		return nil, err
	}
	return &databasev1.MeasureDeleteResponse{
		Deleted: ok,
	}, nil
}

func (rs *measureRegistryServer) Get(ctx context.Context, req *databasev1.MeasureGetRequest) (*databasev1.MeasureGetResponse, error) {
	entity, err := rs.schemaRegistry.MeasureRegistry().GetMeasure(ctx, req.GetMetadata())
	if err != nil {
		return nil, err
	}
	return &databasev1.MeasureGetResponse{
		Measure: entity,
	}, nil
}

func (rs *measureRegistryServer) List(ctx context.Context, req *databasev1.MeasureListRequest) (*databasev1.MeasureListResponse, error) {
	entities, err := rs.schemaRegistry.MeasureRegistry().ListMeasure(ctx, schema.ListOpt{Group: req.GetGroup()})
	if err != nil {
		return nil, err
	}
	return &databasev1.MeasureListResponse{
		Measure: entities,
	}, nil
}

type groupRegistryServer struct {
	schemaRegistry metadata.Service
	databasev1.UnimplementedGroupRegistryServer
}

func (rs *groupRegistryServer) Create(ctx context.Context, req *databasev1.GroupCreateRequest) (*databasev1.GroupCreateResponse, error) {
	if err := rs.schemaRegistry.GroupRegistry().CreateGroup(ctx, req.GetGroup()); err != nil {
		return nil, err
	}
	return &databasev1.GroupCreateResponse{}, nil
}

func (rs *groupRegistryServer) Delete(ctx context.Context, req *databasev1.GroupDeleteRequest) (*databasev1.GroupDeleteResponse, error) {
	deleted, err := rs.schemaRegistry.GroupRegistry().DeleteGroup(ctx, req.GetGroup())
	if err != nil {
		return nil, err
	}
	return &databasev1.GroupDeleteResponse{
		Deleted: deleted,
	}, nil
}

func (rs *groupRegistryServer) Exist(ctx context.Context, req *databasev1.GroupExistRequest) (*databasev1.GroupExistResponse, error) {
	exist, err := rs.schemaRegistry.GroupRegistry().ExistGroup(ctx, req.GetGroup())
	if err != nil {
		return nil, err
	}
	return &databasev1.GroupExistResponse{
		Existence: exist,
	}, nil
}

func (rs *groupRegistryServer) List(ctx context.Context, req *databasev1.GroupListRequest) (*databasev1.GroupListResponse, error) {
	groups, err := rs.schemaRegistry.GroupRegistry().ListGroup(ctx)
	if err != nil {
		return nil, err
	}
	return &databasev1.GroupListResponse{
		Group: groups,
	}, nil
}
