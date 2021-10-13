package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

func TestStreamRegistry(t *testing.T) {
	req := require.New(t)
	gracefulStop := setup(req, testData{
		TLS:  false,
		addr: "localhost:17912",
	})
	defer gracefulStop()

	conn, err := grpc.Dial("localhost:17912", grpc.WithInsecure())
	req.NoError(err)
	req.NotNil(conn)

	client := databasev1.NewStreamRegistryClient(conn)
	req.NotNil(client)

	meta := &commonv1.Metadata{
		Group: "default",
		Name:  "sw",
	}

	getResp, err := client.Get(context.TODO(), &databasev1.StreamGetRequest{Metadata: meta})

	req.NoError(err)
	req.NotNil(getResp)

	// 2 - DELETE
	deleteResp, err := client.Delete(context.TODO(), &databasev1.StreamDeleteRequest{
		Metadata: meta,
	})
	req.NoError(err)
	req.NotNil(deleteResp)
	req.True(deleteResp.GetDeleted())

	// 3 - GET -> Nil
	_, err = client.Get(context.TODO(), &databasev1.StreamGetRequest{
		Metadata: meta,
	})
	errStatus, _ := status.FromError(err)
	req.Equal(errStatus.Message(), schema.ErrEntityNotFound.Error())

	// 4 - CREATE
	_, err = client.Create(context.TODO(), &databasev1.StreamCreateRequest{Stream: getResp.GetStream()})
	req.NoError(err)

	// 5 - GET - > Not Nil
	getResp, err = client.Get(context.TODO(), &databasev1.StreamGetRequest{
		Metadata: meta,
	})
	req.NoError(err)
	req.NotNil(getResp)
}
