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

package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/pkg/version"
)

type healthCheckClient struct {
	conn *grpc.ClientConn
}

func newHealthCheckClient(ctx context.Context, addr string, opts []grpc.DialOption) (client *healthCheckClient, err error) {
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			conn.Close()
		}
		go func() {
			<-ctx.Done()
			conn.Close()
		}()
	}()
	return &healthCheckClient{conn: conn}, nil
}

func (g *healthCheckClient) Check(ctx context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	var resp *grpc_health_v1.HealthCheckResponse
	if err := rpcRequest(ctx, 10*time.Second, func(rpcCtx context.Context) (err error) {
		resp, err = grpc_health_v1.NewHealthClient(g.conn).Check(rpcCtx,
			&grpc_health_v1.HealthCheckRequest{
				Service: "",
			})
		return err
	}); err != nil {
		return nil, err
	}
	return resp, nil
}

func rpcRequest(ctx context.Context, rpcTimeout time.Duration, fn func(rpcCtx context.Context) error) error {
	rpcCtx, rpcCancel := context.WithTimeout(ctx, rpcTimeout)
	defer rpcCancel()
	rpcCtx = metadata.NewOutgoingContext(rpcCtx, make(metadata.MD))

	err := fn(rpcCtx)
	if err != nil {
		return err
	}
	return nil
}

func newHealthCheckCmd() *cobra.Command {
	checkHealth := func() (err error) {
		var ctx context.Context
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		opts := make([]grpc.DialOption, 0, 1)
		grpcCert := viper.GetString("grpcCert")
		if grpcCert != "" {
			creds, errTLS := credentials.NewClientTLSFromFile(grpcCert, "")
			if err != nil {
				return errTLS
			}
			opts = append(opts, grpc.WithTransportCredentials(creds))
		} else {
			opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
		grpcAddr := viper.GetString("grpcAddr")
		client, err := newHealthCheckClient(ctx, grpcAddr, opts)
		if err != nil {
			return err
		}

		resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: ""})
		if err != nil {
			return err
		}
		if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
			switch resp.GetStatus() {
			case grpc_health_v1.HealthCheckResponse_NOT_SERVING, grpc_health_v1.HealthCheckResponse_UNKNOWN:
				err = status.Error(codes.Unavailable, resp.String())
			case grpc_health_v1.HealthCheckResponse_SERVICE_UNKNOWN:
				err = status.Error(codes.NotFound, resp.String())
			default:
				err = status.Error(codes.Internal, resp.String())
			}
		}
		fmt.Println(resp.String())
		return err
	}

	healthCheckCmd := &cobra.Command{
		Use:           "health",
		Version:       version.Build(),
		Short:         "Health check",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return checkHealth()
		},
	}
	healthCheckCmd.Flags().Bool("silenceError", false, "Silence error messages")
	healthCheckCmd.Flags().Bool("silenceUsage", false, "Silence usage message")

	return healthCheckCmd
}
