package interceptors

import (
	"context"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func AuthInterceptor(cfg *config.Config) func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "metadata is not provided")
		}
		usernameList, usernameOk := md["username"]
		passwordList, passwordOk := md["password"]
		if !usernameOk || !passwordOk {
			return nil, status.Errorf(codes.Unauthenticated, "username or password is not provided")
		}
		username := usernameList[0]
		password := passwordList[0]

		var valid bool
		for _, user := range cfg.Users {
			if username == user.Username && auth.CheckPassword(password, user.Password) {
				valid = true
				break
			}
		}
		if !valid {
			return nil, status.Errorf(codes.Unauthenticated, "invalid username or password")
		}

		return handler(ctx, req)
	}
}
