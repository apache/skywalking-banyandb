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
	"encoding/json"
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/version"
	"github.com/go-resty/resty/v2"
	"github.com/spf13/cobra"

	common_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	database_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
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
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			return rest(func() ([]reqBody, error) { return parseNameFromYAML(cmd.InOrStdin()) },
				func(request request) (*resty.Response, error) {
					g := new(common_v1.Group)
					err := json.Unmarshal(request.data, g)
					if err != nil {
						return nil, err
					}
					cr := &database_v1.GroupRegistryServiceCreateRequest{
						Group: g,
					}
					b, err := json.Marshal(cr)
					if err != nil {
						return nil, err
					}
					return request.req.SetBody(b).Post(getPath("/api/v1/group/schema"))
				},
				func(_ int, reqBody reqBody, _ []byte) error {
					fmt.Printf("group %s is created", reqBody.name)
					fmt.Println()
					return nil
				})
		},
	}
	bindFileFlag(createCmd)

	listCmd := &cobra.Command{
		Use:     "list",
		Version: version.Build(),
		Short:   "list all groups",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			return rest(nil, func(request request) (*resty.Response, error) {
				return request.req.Get(getPath("/api/v1/group/schema/lists"))
			}, yamlPrinter)
		},
	}

	// todo:GroupGetCmd, GroupUpdateCmd, GroupDeleteCmd
	groupCmd.AddCommand(createCmd, listCmd)
	return groupCmd
}
