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
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

func newGroupCmd() *cobra.Command {
	groupCmd := &cobra.Command{
		Use:     "group",
		Version: version.Build(),
		Short:   "Group operation",
	}

	createCmd := &cobra.Command{
		Use:     "create -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Create groups from files",
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			return rest(func() ([]reqBody, error) { return parseNameFromYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					g := new(commonv1.Group)
					err := protojson.Unmarshal(request.data, g)
					if err != nil {
						return nil, err
					}
					cr := &databasev1.GroupRegistryServiceCreateRequest{
						Group: g,
					}
					b, err := protojson.Marshal(cr)
					if err != nil {
						return nil, err
					}
					return request.req.SetBody(b).Post(getPath("/api/v1/group/schema"))
				},
				func(_ int, reqBody reqBody, _ []byte) error {
					fmt.Printf("group %s is created", reqBody.name)
					fmt.Println()
					return nil
				}, enableTLS, insecure, cert)
		},
	}

	updateCmd := &cobra.Command{
		Use:     "update -f [file|dir|-]",
		Version: version.Build(),
		Short:   "Update groups from files",
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			return rest(func() ([]reqBody, error) { return parseNameFromYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					g := new(commonv1.Group)
					err := protojson.Unmarshal(request.data, g)
					if err != nil {
						return nil, err
					}
					cr := &databasev1.GroupRegistryServiceUpdateRequest{
						Group: g,
					}
					b, err := protojson.Marshal(cr)
					if err != nil {
						return nil, err
					}
					return request.req.SetBody(b).SetPathParam("group", request.name).Put(getPath("/api/v1/group/schema/{group}"))
				},
				func(_ int, reqBody reqBody, _ []byte) error {
					fmt.Printf("group %s is updated", reqBody.name)
					fmt.Println()
					return nil
				}, enableTLS, insecure, cert)
		},
	}
	bindFileFlag(createCmd, updateCmd)

	getCmd := &cobra.Command{
		Use:     "get [-g group]",
		Version: version.Build(),
		Short:   "Get a group",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("group", request.group).Get(getPath("/api/v1/group/schema/{group}"))
			}, yamlPrinter, enableTLS, insecure, cert)
		},
	}

	deleteCmd := &cobra.Command{
		Use:     "delete [-g group]",
		Version: version.Build(),
		Short:   "Delete a group",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(parseFromFlags, func(request request) (*resty.Response, error) {
				return request.req.SetPathParam("group", request.group).Delete(getPath("/api/v1/group/schema/{group}"))
			},
				func(_ int, reqBody reqBody, _ []byte) error {
					fmt.Printf("group %s is deleted", reqBody.group)
					fmt.Println()
					return nil
				}, enableTLS, insecure, cert)
		},
	}

	listCmd := &cobra.Command{
		Use:     "list",
		Version: version.Build(),
		Short:   "List all groups",
		RunE: func(_ *cobra.Command, _ []string) (err error) {
			return rest(nil, func(request request) (*resty.Response, error) {
				return request.req.Get(getPath("/api/v1/group/schema/lists"))
			}, yamlPrinter, enableTLS, insecure, cert)
		},
	}

	bindTLSRelatedFlag(createCmd, updateCmd, listCmd, getCmd, deleteCmd)
	groupCmd.AddCommand(createCmd, updateCmd, listCmd, getCmd, deleteCmd)
	return groupCmd
}
