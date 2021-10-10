package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

func TestRegistry(t *testing.T) {
	req := require.New(t)
	gracefulStop := setup(req, testData{
		TLS:  false,
		addr: "localhost:17912",
	})
	defer gracefulStop()

	conn, err := grpc.Dial("localhost:17912", grpc.WithInsecure())
	req.NoError(err)
	req.NotNil(conn)

	client := databasev1.NewEntityRegistryClient(conn)
	req.NotNil(client)

	testCases := []struct {
		name               string
		meta               *commonv1.Metadata
		entityType         databasev1.EntityType
		respExtractor      func(*databasev1.EntityGetResponse) proto.Message
		createReqGenerator func(*databasev1.EntityGetResponse) *databasev1.EntityCreateOrUpdateRequest
	}{
		{
			name:       "Stream Registry",
			meta:       &commonv1.Metadata{Name: "sw", Group: "default"},
			entityType: databasev1.EntityType_ENTITY_TYPE_STREAM,
			respExtractor: func(response *databasev1.EntityGetResponse) proto.Message {
				return response.GetStream()
			},
			createReqGenerator: func(resp *databasev1.EntityGetResponse) *databasev1.EntityCreateOrUpdateRequest {
				return &databasev1.EntityCreateOrUpdateRequest{
					Entity: &databasev1.EntityCreateOrUpdateRequest_Stream{
						Stream: resp.GetStream(),
					},
				}
			},
		},
		{
			name:       "IndexRuleBinding Registry",
			meta:       &commonv1.Metadata{Name: "sw-index-rule-binding", Group: "default"},
			entityType: databasev1.EntityType_ENTITY_TYPE_INDEX_RULE_BINDING,
			respExtractor: func(response *databasev1.EntityGetResponse) proto.Message {
				return response.GetIndexRuleBinding()
			},
			createReqGenerator: func(resp *databasev1.EntityGetResponse) *databasev1.EntityCreateOrUpdateRequest {
				return &databasev1.EntityCreateOrUpdateRequest{
					Entity: &databasev1.EntityCreateOrUpdateRequest_IndexRuleBinding{
						IndexRuleBinding: resp.GetIndexRuleBinding(),
					},
				}
			},
		},
		{
			name:       "IndexRule Registry",
			meta:       &commonv1.Metadata{Name: "db.instance", Group: "default"},
			entityType: databasev1.EntityType_ENTITY_TYPE_INDEX_RULE,
			respExtractor: func(response *databasev1.EntityGetResponse) proto.Message {
				return response.GetIndexRule()
			},
			createReqGenerator: func(resp *databasev1.EntityGetResponse) *databasev1.EntityCreateOrUpdateRequest {
				return &databasev1.EntityCreateOrUpdateRequest{
					Entity: &databasev1.EntityCreateOrUpdateRequest_IndexRule{
						IndexRule: resp.GetIndexRule(),
					},
				}
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			tester := assert.New(t)
			// 1 - GET -> Not Nil
			getResp, err := client.Get(context.TODO(), &databasev1.GeneralEntityRequest{
				Metadata:   tt.meta,
				EntityType: tt.entityType,
			})
			tester.NoError(err)
			tester.NotNil(getResp)
			tester.NotNil(tt.respExtractor(getResp))

			// 2 - DELETE
			deleteResp, err := client.Delete(context.TODO(), &databasev1.GeneralEntityRequest{
				Metadata:   tt.meta,
				EntityType: tt.entityType,
			})
			tester.NoError(err)
			tester.NotNil(deleteResp)

			// 3 - GET -> Nil
			_, err = client.Get(context.TODO(), &databasev1.GeneralEntityRequest{
				Metadata:   tt.meta,
				EntityType: tt.entityType,
			})
			errStatus, _ := status.FromError(err)
			tester.Equal(errStatus.Message(), schema.ErrEntityNotFound.Error())

			// 4 - CREATE
			_, err = client.CreateOrUpdate(context.TODO(), tt.createReqGenerator(getResp))
			tester.NoError(err)

			// 5 - GET - > Not Nil
			getResp, err = client.Get(context.TODO(), &databasev1.GeneralEntityRequest{
				Metadata:   tt.meta,
				EntityType: tt.entityType,
			})
			tester.NoError(err)
			tester.NotNil(getResp)
			tester.NotNil(tt.respExtractor(getResp))
		})
	}
}
