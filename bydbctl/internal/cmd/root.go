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

// Package cmd is an internal package defining cli commands for bydbctl
package cmd

import (
	"os"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/version"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var Addr = ""
var JSON = ""

// NewRoot returns the root command
func NewRoot() *cobra.Command {
	cmd := &cobra.Command{
		DisableAutoGenTag: true,
		Version:           version.Build(),
		Short:             "bydbctl is the command line tool of BanyanDB",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			viper.SetConfigType("yaml")
			viper.SetConfigName("config")
			configPath := os.Getenv("HOME") + "/.bydbctl"
			viper.AddConfigPath(configPath)
			_, err = os.Stat(configPath)
			if !os.IsExist(err) {
				err = os.MkdirAll(configPath, 0o777)
				if err != nil {
					return err
				}
			}
			if err = viper.SafeWriteConfig(); err != nil {
				if os.IsNotExist(err) {
					err = viper.WriteConfig()
					if err != nil {
						return err
					}
				}
			}
			if err = viper.ReadInConfig(); err != nil {
				if _, ok := err.(viper.ConfigFileNotFoundError); ok {
					logger.GetLogger().Fatal().Err(err).Msg("Config file not found")
				} else {
					logger.GetLogger().Fatal().Err(err).Msg("Config file was found but another error was produced")
				}
				return err
			}
			return nil
		},
	}
	cmd.AddCommand(newBanyanDBCmd()...)
	cmd.PersistentFlags().StringVarP(&Addr, "addr", "a", "localhost:17913", "default ip/port")
	cmd.PersistentFlags().StringVarP(&JSON, "json", "j", `{}`, "accept json args to call banyandb's http interface")
	return cmd
}
