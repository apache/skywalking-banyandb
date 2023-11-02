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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	ins "google.golang.org/grpc/credentials/insecure"

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
			if enableTLS {
				// #nosec G402
				config := &tls.Config{
					InsecureSkipVerify: insecure,
				}
				if grpcCert != "" {
					cert, errRead := os.ReadFile(grpcCert)
					if errRead != nil {
						return errRead
					}
					certPool := x509.NewCertPool()
					if !certPool.AppendCertsFromPEM(cert) {
						return errors.New("failed to add server's certificate")
					}
					config.RootCAs = certPool
				}
				creds := credentials.NewTLS(config)
				opts = append(opts, grpc.WithTransportCredentials(creds))
			} else {
				opts = append(opts, grpc.WithTransportCredentials(ins.NewCredentials()))
			}
			err = helpers.HealthCheck(grpcAddr, 10*time.Second, 10*time.Second, opts...)()
			if err == nil {
				fmt.Println("connected")
			}
			return err
		},
	}
	healthCheckCmd.Flags().StringVarP(&grpcAddr, "grpc-addr", "", "localhost:17912", "Grpc server's address, the format is Domain:Port")
	bindTLSRelatedFlag(healthCheckCmd)
	return healthCheckCmd
}
