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
	"github.com/ghodss/yaml"
	"github.com/go-resty/resty/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newGroupCmd() *cobra.Command {
	GroupCmd := &cobra.Command{
		Use:     "group",
		Version: version.Build(),
		Short:   "banyandb group related Operation",
	}

	GroupCreateCmd := &cobra.Command{
		Use:     "create",
		Version: version.Build(),
		Short:   "banyandb group schema Create Operation",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			client := resty.New()
			var data map[string]interface{}
			err = json.Unmarshal([]byte(raw), &data)
			if err != nil {
				return err
			}
			resp, err := client.R().SetBody(data).Post(viper.GetString("addr") + "/api/v1/group/schema")
			if err != nil {
				return err
			}
			yamlResult, err := yaml.JSONToYAML(resp.Body())
			if err != nil {
				return err
			}
			fmt.Print(string(yamlResult))
			return nil
		},
	}

	GroupListCmd := &cobra.Command{
		Use:     "list",
		Version: version.Build(),
		Short:   "banyandb group schema List Operation",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			client := resty.New()
			addr, err := cmd.Flags().GetString("addr")
			if err != nil {
				return err
			}
			resp, err := client.R().Get("http://" + addr + "/api/v1/group/schema/lists")
			if err != nil {
				return err
			}
			yamlResult, err := yaml.JSONToYAML(resp.Body())
			if err != nil {
				return err
			}
			fmt.Println(string(yamlResult))
			return nil
		},
	}

	// todo:GroupGetCmd, GroupUpdateCmd, GroupDeleteCmd
	GroupCmd.AddCommand(GroupCreateCmd, GroupListCmd)
	return GroupCmd
}