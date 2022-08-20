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

	"github.com/apache/skywalking-banyandb/pkg/logger"
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
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
	}

	StreamCreateCmd := &cobra.Command{
		Use:     "create", // "{\"stream\":{\"metadata\":{\"group\":\"\",\"name\":\"naonao\"}}}"
		Version: version.Build(),
		Short:   "banyandb stream schema create Operation",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger.GetLogger().Info().Msg("banyandb stream schema Create Operation")
			client := resty.New()
			addr, err := cmd.Flags().GetString("addr")
			if err != nil {
				return err
			}
			body, err := cmd.Flags().GetString("json")
			var data map[string]interface{}
			err = json.Unmarshal([]byte(body), &data)
			if err != nil {
				return err
			}
			stream, ok := data["stream"].(map[string]interface{})
			if !ok {
				return errors.New("input json format error")
			}
			metadata, ok := stream["metadata"].(map[string]interface{})
			//switch .type
			//case
			if !ok {
				return errors.New("input json format error")
			}
			_, ok = metadata["group"].(string)
			if !ok {
				metadata["group"] = fmt.Sprintf("%v", viper.Get("group"))
			}
			fmt.Println(data)
			resp, err := client.R().SetBody(data).Post("http://" + addr + "/api/v1/stream/schema")
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("http request: post " + addr + "/api/v1/stream/schema")
			logger.GetLogger().Info().Msg("http response: " + resp.Status())
			yamlResult, err := yaml.JSONToYAML(resp.Body())
			if err != nil {
				return err
			}
			fmt.Println(string(yamlResult))
			return nil
		},
	}

	StreamUpdateCmd := &cobra.Command{
		Use:     "update",
		Version: version.Build(),
		Short:   "banyandb stream schema Update Operation",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger.GetLogger().Info().Msg("banyandb stream schema Update Operation")
			client := resty.New()
			addr, err := cmd.Flags().GetString("addr")
			if err != nil {
				return err
			}
			body, err := cmd.Flags().GetString("json")
			if err != nil {
				return err
			}
			var data map[string]interface{}
			err = json.Unmarshal([]byte(body), &data)
			if err != nil {
				return err
			}
			fmt.Print("data: ")
			fmt.Println(data)
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
				group = fmt.Sprintf("%v", viper.Get("group"))
			}
			name, ok := metadata["name"].(string)
			if !ok {
				return errors.New("input json format error")
			}
			fmt.Println(data)
			resp, err := client.R().SetBody(data).Put("http://" + addr + "/api/v1/stream/schema/" + group + "/" + name)
			logger.GetLogger().Info().Msg("http request: put " + "http://" + addr + "/api/v1/stream/schema/" + group + "/" + name)
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("http response: " + resp.Status())
			yamlResult, err := yaml.JSONToYAML(resp.Body())
			if err != nil {
				return err
			}
			fmt.Println(string(yamlResult))
			return nil
		},
	}

	StreamGetCmd := &cobra.Command{
		Use:     "get", // "{\"metadata\":{\"group\":\"mxm\",\"name\":\"naonao\"}}"
		Version: version.Build(),
		Short:   "banyandb stream schema Get Operation",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger.GetLogger().Info().Msg("banyandb stream schema Get Operation")
			client := resty.New()
			addr, err := cmd.Flags().GetString("addr")
			if err != nil {
				return err
			}
			body, err := cmd.Flags().GetString("json")
			var data map[string]interface{}
			err = json.Unmarshal([]byte(body), &data)
			if err != nil {
				return err
			}
			fmt.Println(data)
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
			}
			resp, err := client.R().Get("http://" + addr + "/api/v1/stream/schema/" + group + "/" + name)
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("http request: get " + addr + "/api/v1/stream/schema/" + group + "/" + name)
			logger.GetLogger().Info().Msg("http response: " + resp.Status())
			yamlResult, err := yaml.JSONToYAML(resp.Body())
			if err != nil {
				return err
			}
			fmt.Println(string(yamlResult))
			return nil
		},
	}

	StreamDeleteCmd := &cobra.Command{
		Use:     "delete",
		Version: version.Build(),
		Short:   "banyandb stream schema Delete Operation",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger.GetLogger().Info().Msg("banyandb stream schema Delete Operation")
			client := resty.New()
			addr, err := cmd.Flags().GetString("addr")
			if err != nil {
				return err
			}
			body, err := cmd.Flags().GetString("json")
			var data map[string]interface{}
			err = json.Unmarshal([]byte(body), &data)
			if err != nil {
				return err
			}
			fmt.Println(data)
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
			}
			resp, err := client.R().Delete("http://" + addr + "/api/v1/stream/schema/" + group + "/" + name)
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("http request: delete " + addr + "/api/v1/stream/schema/" + group + "/" + name)
			logger.GetLogger().Info().Msg("http response: " + resp.Status())
			yamlResult, err := yaml.JSONToYAML(resp.Body())
			if err != nil {
				return err
			}
			fmt.Println(string(yamlResult))
			return nil
		},
	}

	StreamListCmd := &cobra.Command{
		Use:     "list",
		Version: version.Build(),
		Short:   "banyandb stream schema List Operation",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			return cmd.Parent().PersistentPreRunE(cmd.Parent(), args)
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger.GetLogger().Info().Msg("banyandb stream schema List Operation")
			client := resty.New()
			addr, err := cmd.Flags().GetString("addr")
			if err != nil {
				return err
			}
			body, err := cmd.Flags().GetString("json")
			if err != nil {
				return err
			}
			var data map[string]interface{}
			err = json.Unmarshal([]byte(body), &data)
			if err != nil {
				return err
			}
			group, ok := data["group"].(string)
			if !ok {
				group = fmt.Sprintf("%v", viper.Get("group"))
			}
			resp, err := client.R().Get("http://" + addr + "/api/v1/stream/schema/lists/" + group)
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("http request: list " + "http://" + addr + "/api/v1/stream/schema/" + group)
			logger.GetLogger().Info().Msg("http response: " + resp.Status())
			yamlResult, err := yaml.JSONToYAML(resp.Body())
			if err != nil {
				return err
			}
			fmt.Println(string(yamlResult))
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
			logger.GetLogger().Info().Msg("banyandb stream schema Query Operation")
			client := resty.New()
			addr, err := cmd.Flags().GetString("addr")
			if err != nil {
				return err
			}
			body, err := cmd.Flags().GetString("json")
			var data map[string]interface{}
			err = json.Unmarshal([]byte(body), &data)
			if err != nil {
				return err
			}
			err = json.Unmarshal([]byte(body), &data)
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
			}
			resp, err := client.R().SetBody(data).Post("http://" + addr + "/api/v1/stream/data")
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("http request: post " + addr + "/api/v1/stream/data")
			logger.GetLogger().Info().Msg("http response: " + resp.Status())
			yamlResult, err := yaml.JSONToYAML(resp.Body())
			if err != nil {
				return err
			}
			fmt.Println(string(yamlResult))
			return nil
		},
	}
	StreamCmd.AddCommand(StreamGetCmd, StreamCreateCmd, StreamDeleteCmd, StreamUpdateCmd, StreamListCmd, StreamQueryCmd)
	return StreamCmd
}
