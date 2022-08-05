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

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/version"
	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newUserGroupCmd() *cobra.Command {
	GroupCmd := &cobra.Command{
		Use:     "use",
		Version: version.Build(),
		Short:   "choose banyandb group",
		Args:    cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			client := resty.New()
			addr, err := cmd.Flags().GetString("addr")
			if err != nil {
				return err
			}
			resp, err := client.R().Get("http://" + addr + "/api/v1/group/schema/" + args[0])
			if err != nil {
				return err
			}
			logger.GetLogger().Info().Msg("http request: get " + addr + "/api/v1/group/schema/" + args[0])
			logger.GetLogger().Info().Msg("http response: " + resp.Status())
			if resp.StatusCode() == 404 { // check if the group exists
				return errors.New("no such group")
			} else {
				fmt.Println(resp.StatusCode())
				viper.Set("group", args[0])
				err = viper.WriteConfig()
				fmt.Println("switched to ", viper.Get("group"), "group")
			}
			return nil
		},
	}

	return GroupCmd
}
