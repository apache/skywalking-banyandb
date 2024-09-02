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

	"github.com/go-resty/resty/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/encoding/protojson"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

const streamSchemaPath = "/api/v1/stream/schema"

var streamSchemaPathWithParams = streamSchemaPath + pathTemp

func getPath(path string) string {
	return viper.GetString("addr") + path
}

func newStreamCmd() *cobra.Command {
	streamCmd := &cobra.Command{
		Use:     "stream",
		Version: version.Build(),
		Short:   "Stream operation",
	}

	createCmd := &cobra.Command{
		Use:     "create -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Create streams from files",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return rest(func() ([]reqBody, error) { return parseNameAndGroupFromYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					s := new(databasev1.Stream)
					err := protojson.Unmarshal(request.data, s)
					if err != nil {
						return nil, err
					}
					cr := &databasev1.StreamRegistryServiceCreateRequest{
						Stream: s,
					}
					b, err := protojson.Marshal(cr)
					if err != nil {
						return nil, err
					}
					return request.req.SetBody(b).Post(getPath(streamSchemaPath))
				},
				func(_ int, reqBody reqBody, _ []byte) error {
					fmt.Printf("stream %s.%s is created", reqBody.group, reqBody.name)
					fmt.Println()
					return nil
				}, enableTLS, insecure, cert)
		},
	}

	updateCmd := &cobra.Command{
		Use:     "update -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Update streams from files",
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			return rest(func() ([]reqBody, error) { return parseNameAndGroupFromYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					s := new(databasev1.Stream)
					err := protojson.Unmarshal(request.data, s)
					if err != nil {
						return nil, err
					}
					cr := &databasev1.StreamRegistryServiceUpdateRequest{
						Stream: s,
					}
					b, err := protojson.Marshal(cr)
					if err != nil {
						return nil, err
					}
					return request.req.SetBody(b).
						SetPathParam("name", request.name).SetPathParam("group", request.group).
						Put(getPath(streamSchemaPathWithParams))
				},
				func(_ int, reqBody reqBody, _ []byte) error {
					fmt.Printf("stream %s.%s is updated", reqBody.group, reqBody.name)
					fmt.Println()
					return nil
				}, enableTLS, insecure, cert)
		},
	}

	getCmd := &cobra.Command{
		Use:     "get [-g group] -n name",
		Version: version.Build(),
		Short:   "Get a stream",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("name", request.name).SetPathParam("group", request.group).Get(getPath(streamSchemaPathWithParams))
			}, yamlPrinter, enableTLS, insecure, cert)
		},
	}

	deleteCmd := &cobra.Command{
		Use:     "delete [-g group] -n name",
		Version: version.Build(),
		Short:   "Delete a stream",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("name", request.name).SetPathParam("group", request.group).Delete(getPath(streamSchemaPathWithParams))
			}, func(_ int, reqBody reqBody, _ []byte) error {
				fmt.Printf("stream %s.%s is deleted", reqBody.group, reqBody.name)
				fmt.Println()
				return nil
			}, enableTLS, insecure, cert)
		},
	}
	bindNameFlag(getCmd, deleteCmd)

	listCmd := &cobra.Command{
		Use:     "list [-g group]",
		Version: version.Build(),
		Short:   "List streams",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("group", request.group).Get(getPath("/api/v1/stream/schema/lists/{group}"))
			}, yamlPrinter, enableTLS, insecure, cert)
		},
	}

	queryCmd := &cobra.Command{
		Use:     "query [-s start_time] [-e end_time] -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Query data in a stream",
		Long:    timeRangeUsage,
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			return rest(func() ([]reqBody, error) { return parseTimeRangeFromFlagAndYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					return request.req.SetBody(request.data).Post(getPath("/api/v1/stream/data"))
				}, yamlPrinter, enableTLS, insecure, cert)
		},
	}
	bindFileFlag(createCmd, updateCmd, queryCmd)
	bindTimeRangeFlag(queryCmd)

	bindTLSRelatedFlag(getCmd, createCmd, deleteCmd, updateCmd, listCmd, queryCmd)
	streamCmd.AddCommand(getCmd, createCmd, deleteCmd, updateCmd, listCmd, queryCmd)
	return streamCmd
}
