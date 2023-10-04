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

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

func newHealthCheckCmd() *cobra.Command {
	healthCheckCmd := &cobra.Command{
		Use:           "health",
		Version:       version.Build(),
		Short:         "Health check",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(_ *cobra.Command, _ []string) (err error) {
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
			err = helpers.HealthCheck(true, grpcAddr, 10*time.Second, 10*time.Second, opts...)()
			if err == nil {
				fmt.Println("connected")
			}
			return err
		},
	}

	return healthCheckCmd
}
