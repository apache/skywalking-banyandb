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
	"fmt"
	"os"

	"github.com/apache/skywalking-banyandb/pkg/version"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	filePath string
	name     string
	cfgFile  string
	rootCmd  = &cobra.Command{
		DisableAutoGenTag: true,
		Version:           version.Build(),
		Short:             "bydbctl is the command line tool of BanyanDB",
	}
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

// RootCmdFlags bind flags to a command.
func RootCmdFlags(command *cobra.Command) {
	command.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.bydbctl.yaml)")
	command.PersistentFlags().StringP("group", "g", "", "If present, list objects in this group.")
	command.PersistentFlags().StringP("addr", "a", "", "Server's address, the format is Schema://Domain:Port")
	_ = viper.BindPFlag("group", command.PersistentFlags().Lookup("group"))
	_ = viper.BindPFlag("addr", command.PersistentFlags().Lookup("addr"))
	viper.SetDefault("addr", "http://localhost:17913")

	command.AddCommand(newGroupCmd(), newUserCmd(), newStreamCmd())
}

func init() {
	cobra.OnInitialize(initConfig)
	RootCmdFlags(rootCmd)
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".bydbctl" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".bydbctl")
	}

	viper.AutomaticEnv()

	readCfg := func() error {
		if err := viper.ReadInConfig(); err != nil {
			return err
		}
		// Dump this to stderr in case of mixing up response yaml
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
		return nil
	}

	if err := readCfg(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			cobra.CheckErr(err)
		}
		cobra.CheckErr(viper.SafeWriteConfig())
		cobra.CheckErr(readCfg())
	}
}

func bindFileFlag(commands ...*cobra.Command) {
	for _, c := range commands {
		c.Flags().StringVarP(&filePath, "file", "f", "", "That contains the request to send")
	}
}

func bindNameFlag(commands ...*cobra.Command) {
	for _, c := range commands {
		c.Flags().StringVarP(&name, "name", "n", "", "the name of the resource")
		_ = c.MarkFlagRequired("name")
	}
}
