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
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

var grpcAddr string

func newHealthCheckCmd() *cobra.Command {
	healthCheckCmd := &cobra.Command{
		Use:           "health",
		Version:       version.Build(),
		Short:         "Health check",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			opts := make([]grpc.DialOption, 0, 1)
			if grpcAddr == "" {
				return rest(nil, func(request request) (*resty.Response, error) {
					return request.req.Get(getPath("/api/healthz"))
				}, yamlPrinter, enableTLS, insecure, cert)
			}
			opts, err = grpchelper.SecureOptions(opts, enableTLS, insecure, cert)
			if err != nil {
				return err
			}
			err = healthCheck(grpcAddr, 10*time.Second, 10*time.Second, username, password, opts...)
			if err == nil {
				fmt.Println("connected")
			}
			return err
		},
	}
	healthCheckCmd.Flags().StringVarP(&grpcAddr, "grpc-addr", "", "", "Grpc server's address, the format is Domain:Port")
	bindTLSRelatedFlag(healthCheckCmd)
	return healthCheckCmd
}

func healthCheck(grpcAddr string, connTimeout, rpcTimeout time.Duration, username, password string, opts ...grpc.DialOption) error {
	if username != "" {
		return helpers.HealthCheckWithAuth(grpcAddr, connTimeout, rpcTimeout, username, password, opts...)()
	}
	username = viper.GetString("username")
	password = viper.GetString("password")
	return helpers.HealthCheckWithAuth(grpcAddr, connTimeout, rpcTimeout, username, password, opts...)()
}
