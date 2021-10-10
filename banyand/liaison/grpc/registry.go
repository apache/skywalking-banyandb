package grpc

import (
	"context"

	"github.com/pkg/errors"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

var (
	nullGetResponse = &databasev1.EntityGetResponse{
		Entity: &databasev1.EntityGetResponse_Null{},
	}

	ErrUnspecifiedEntityType = errors.New("invalid entity type")
)

func (s *Server) CreateOrUpdate(ctx context.Context, req *databasev1.EntityCreateOrUpdateRequest) (*databasev1.EntityCreateOrUpdateResponse, error) {
	switch v := req.GetEntity().(type) {
	case *databasev1.EntityCreateOrUpdateRequest_IndexRule:
		if err := s.schemaRegistry.IndexRuleRegistry().UpdateIndexRule(ctx, v.IndexRule); err != nil {
			return nil, err
		}
	case *databasev1.EntityCreateOrUpdateRequest_IndexRuleBinding:
		if err := s.schemaRegistry.IndexRuleBindingRegistry().UpdateIndexRuleBinding(ctx, v.IndexRuleBinding); err != nil {
			return nil, err
		}
	case *databasev1.EntityCreateOrUpdateRequest_Stream:
		if err := s.schemaRegistry.StreamRegistry().UpdateStream(ctx, v.Stream); err != nil {
			return nil, err
		}
	}
	return &databasev1.EntityCreateOrUpdateResponse{}, nil
}
func (s *Server) Delete(ctx context.Context, req *databasev1.GeneralEntityRequest) (*databasev1.EntityDeleteResponse, error) {
	switch req.GetEntityType() {
	case databasev1.EntityType_ENTITY_TYPE_INDEX_RULE:
		if _, err := s.schemaRegistry.IndexRuleRegistry().DeleteIndexRule(ctx, req.GetMetadata()); err != nil {
			return nil, err
		}
		return &databasev1.EntityDeleteResponse{}, nil
	case databasev1.EntityType_ENTITY_TYPE_INDEX_RULE_BINDING:
		if _, err := s.schemaRegistry.IndexRuleBindingRegistry().DeleteIndexRuleBinding(ctx, req.GetMetadata()); err != nil {
			return nil, err
		}
		return &databasev1.EntityDeleteResponse{}, nil
	case databasev1.EntityType_ENTITY_TYPE_STREAM:
		if _, err := s.schemaRegistry.StreamRegistry().DeleteStream(ctx, req.GetMetadata()); err != nil {
			return nil, err
		}
		return &databasev1.EntityDeleteResponse{}, nil
	}
	return nil, ErrUnspecifiedEntityType
}

func (s *Server) Get(ctx context.Context, req *databasev1.GeneralEntityRequest) (*databasev1.EntityGetResponse, error) {
	switch req.GetEntityType() {
	case databasev1.EntityType_ENTITY_TYPE_INDEX_RULE:
		idxRule, err := s.schemaRegistry.IndexRuleRegistry().GetIndexRule(ctx, req.GetMetadata())
		if err != nil {
			return nil, err
		}
		if idxRule == nil {
			return nullGetResponse, nil
		}
		return &databasev1.EntityGetResponse{
			Entity: &databasev1.EntityGetResponse_IndexRule{IndexRule: idxRule},
		}, nil
	case databasev1.EntityType_ENTITY_TYPE_INDEX_RULE_BINDING:
		idxRuleBinding, err := s.schemaRegistry.IndexRuleBindingRegistry().GetIndexRuleBinding(ctx, req.GetMetadata())
		if err != nil {
			return nil, err
		}
		if idxRuleBinding == nil {
			return nullGetResponse, nil
		}
		return &databasev1.EntityGetResponse{
			Entity: &databasev1.EntityGetResponse_IndexRuleBinding{IndexRuleBinding: idxRuleBinding},
		}, nil
	case databasev1.EntityType_ENTITY_TYPE_STREAM:
		stream, err := s.schemaRegistry.StreamRegistry().GetStream(ctx, req.GetMetadata())
		if err != nil {
			return nil, err
		}
		if stream == nil {
			return nullGetResponse, nil
		}
		return &databasev1.EntityGetResponse{
			Entity: &databasev1.EntityGetResponse_Stream{Stream: stream},
		}, nil
	}
	return nil, ErrUnspecifiedEntityType
}
