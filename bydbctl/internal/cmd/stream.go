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
	"errors"
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/version"
	"github.com/ghodss/yaml"
	"github.com/go-resty/resty/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newStreamCmd() *cobra.Command {
	StreamCmd := &cobra.Command{
		Use:     "stream",
		Version: version.Build(),
		Short:   "banyandb stream schema related Operation",
	}

	StreamCreateCmd := &cobra.Command{
		Use:     "create",
		Version: version.Build(),
		Short:   "banyandb stream schema create Operation",
		RunE: func(_ *cobra.Command, _ []string) error {
			return rest("stream", func(request *resty.Request) (*resty.Response, error) {
				return request.Post(viper.GetString("addr") + "/api/v1/stream/schema")
			})
		},
	}

	StreamUpdateCmd := &cobra.Command{
		Use:     "update",
		Version: version.Build(),
		Short:   "banyandb stream schema Update Operation",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			client := resty.New()
			var data map[string]interface{}
			err = json.Unmarshal([]byte(raw), &data)
			if err != nil {
				return err
			}
			stream, ok := data["stream"].(map[string]interface{})
			if !ok {
				return errors.New("input json format error")
			}
			metadata, ok := stream["metadata"].(map[string]interface{})
			if !ok {
				return errors.New("input json format error")
			}
			group, ok := metadata["group"].(string)
			if !ok {
				metadata["group"] = fmt.Sprintf("%v", viper.Get("group"))
				if metadata["group"] == "" {
					return errors.New("please specify a group through the input json or the config file")
				}
				group = fmt.Sprintf("%v", viper.Get("group"))
			}
			name, ok := metadata["name"].(string)
			if !ok {
				return errors.New("input json format error")
			}
			resp, err := client.R().SetBody(data).Put(viper.GetString("addr") + "/api/v1/stream/schema/" + group + "/" + name)
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

	StreamGetCmd := &cobra.Command{
		Use:     "get",
		Version: version.Build(),
		Short:   "banyandb stream schema Get Operation",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			client := resty.New()
			var data map[string]interface{}
			err = json.Unmarshal([]byte(raw), &data)
			if err != nil {
				return err
			}
			metadata, ok := data["metadata"].(map[string]interface{})
			if !ok {
				return errors.New("input json format error")
			}
			name, ok := metadata["name"].(string)
			if !ok {
				return errors.New("input json format error")
			}
			group, ok := metadata["group"].(string)
			if !ok {
				group = fmt.Sprintf("%v", viper.Get("group"))
				if metadata["group"] == "" {
					return errors.New("please specify a group through the input json or the config file")
				}
			}
			resp, err := client.R().Get(viper.GetString("addr") + "/api/v1/stream/schema/" + group + "/" + name)
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

	StreamDeleteCmd := &cobra.Command{
		Use:     "delete",
		Version: version.Build(),
		Short:   "banyandb stream schema Delete Operation",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			client := resty.New()
			var data map[string]interface{}
			err = json.Unmarshal([]byte(raw), &data)
			if err != nil {
				return err
			}
			metadata, ok := data["metadata"].(map[string]interface{})
			if !ok {
				return errors.New("input json format error")
			}
			name, ok := metadata["name"].(string)
			if !ok {
				return errors.New("input json format error")
			}
			group, ok := metadata["group"].(string)
			if !ok {
				group = fmt.Sprintf("%v", viper.Get("group"))
				if metadata["group"] == "" {
					return errors.New("please specify a group through the input json or the config file")
				}
			}
			resp, err := client.R().Delete(viper.GetString("addr") + "/api/v1/stream/schema/" + group + "/" + name)
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

	StreamListCmd := &cobra.Command{
		Use:     "list",
		Version: version.Build(),
		Short:   "banyandb stream schema List Operation",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			client := resty.New()
			var data map[string]interface{}
			err = json.Unmarshal([]byte(raw), &data)
			if err != nil {
				return err
			}
			group, ok := data["group"].(string)
			if !ok {
				group = fmt.Sprintf("%v", viper.Get("group"))
				if group == "" {
					return errors.New("please specify a group through the input json or the config file")
				}
			}
			resp, err := client.R().Get(viper.GetString("addr") + "/api/v1/stream/schema/lists/" + group)
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

	StreamQueryCmd := &cobra.Command{
		Use:     "query",
		Version: version.Build(),
		Short:   "banyandb stream schema Query Operation",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			client := resty.New()
			var data map[string]interface{}
			err = json.Unmarshal([]byte(raw), &data)
			if err != nil {
				return err
			}
			metadata, ok := data["metadata"].(map[string]interface{})
			if !ok {
				return errors.New("input json format error")
			}
			_, ok = metadata["group"].(string)
			if !ok {
				metadata["group"] = fmt.Sprintf("%v", viper.Get("group"))
				if metadata["group"] == "" {
					return errors.New("please specify a group through the input json or the config file")
				}
			}
			resp, err := client.R().SetBody(data).Post(viper.GetString("addr") + "/api/v1/stream/data")
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
	StreamCmd.AddCommand(StreamGetCmd, StreamCreateCmd, StreamDeleteCmd, StreamUpdateCmd, StreamListCmd, StreamQueryCmd)
	return StreamCmd
}
