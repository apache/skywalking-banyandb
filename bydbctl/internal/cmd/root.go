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
	"github.com/apache/skywalking-banyandb/pkg/version"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// NewRoot returns the root command
func NewRoot() *cobra.Command {
	cmd := &cobra.Command{
		DisableAutoGenTag: true,
		Version:           version.Build(),
		Short:             "bydbctl is the command line tool of BanyanDB",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			viper.SetConfigType("yaml")
			viper.SetConfigName("config")
			viper.AddConfigPath("./bydbctl/internal/config")
			return viper.ReadInConfig()
		},
	}
	cmd.AddCommand(newBanyanDBCmd()...)
	Addr := ""
	cmd.PersistentFlags().StringVarP(&Addr, "addr", "a", "127.0.0.1:17913", "default ip/port") // 这都能读取到？
	Json := ""
	cmd.PersistentFlags().StringVarP(&Json, "json", "j", `{}`, "accept json args to call banyandb's http interface")
	return cmd
}
