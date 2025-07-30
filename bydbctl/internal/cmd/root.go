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

// Package cmd is an internal package defining cli commands for bydbctl.
package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/apache/skywalking-banyandb/pkg/config"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

const (
	pathTemp  = "/{group}/{name}"
	envPrefix = "BYDBCTL"
)

var (
	filePath  string
	name      string
	start     string
	end       string
	cfgFile   string
	enableTLS bool
	insecure  bool
	cert      string
	username  string
	password  string
	rootCmd   = &cobra.Command{
		DisableAutoGenTag: true,
		Version:           version.Build(),
		Short:             "bydbctl is the command line tool of BanyanDB",
	}
)

// ResetFlags resets the flags.
func ResetFlags() {
	filePath = ""
	name = ""
	start = ""
	end = ""
}

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

// RootCmdFlags bind flags to a command.
func RootCmdFlags(command *cobra.Command) {
	command.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.bydbctl.yaml)")
	command.PersistentFlags().StringP("group", "g", "", "If present, list objects in this group.")
	command.PersistentFlags().StringP("addr", "a", "", "Server's address, the format is Schema://Domain:Port")
	command.PersistentFlags().StringVarP(&username, "username", "u", "", "Username for authentication")
	command.PersistentFlags().StringVarP(&password, "password", "p", "", "Password for authentication")
	_ = viper.BindPFlag("group", command.PersistentFlags().Lookup("group"))
	_ = viper.BindPFlag("addr", command.PersistentFlags().Lookup("addr"))
	viper.SetDefault("addr", "http://localhost:17913")
	_ = viper.BindPFlag("username", command.PersistentFlags().Lookup("username"))
	_ = viper.BindPFlag("password", command.PersistentFlags().Lookup("password"))

	command.AddCommand(newGroupCmd(), newUseCmd(), newStreamCmd(), newMeasureCmd(), newTopnCmd(),
		newIndexRuleCmd(), newIndexRuleBindingCmd(), newPropertyCmd(), newTraceCmd(), newHealthCheckCmd(), newAnalyzeCmd())
}

func init() {
	cobra.OnInitialize(initConfig)
	RootCmdFlags(rootCmd)
}

func initConfig() {
	if cfgFile != "" {
		if cfgFile == "-" {
			return
		}
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

	viper.SetEnvPrefix(envPrefix)
	viper.AutomaticEnv()
	err := config.BindFlags(rootCmd.PersistentFlags(), viper.GetViper(), envPrefix)
	if err != nil {
		cobra.CheckErr(err)
	}

	readCfg := func() error {
		if err := viper.ReadInConfig(); err != nil {
			return err
		}
		configFile := viper.ConfigFileUsed()
		if err := os.Chmod(configFile, 0o600); err != nil {
			cobra.CheckErr(fmt.Errorf("failed to set permissions on config file %s: %w", configFile, err))
		}
		info, err := os.Stat(configFile)
		if err != nil {
			return fmt.Errorf("unable to stat config file: %w", err)
		}
		if info.Mode().Perm() != 0o600 {
			fmt.Printf("config file %s has unsafe permissions: %o (expected 0600)", configFile, info.Mode().Perm())
		}
		// Dump this to stderr in case of mixing up response yaml
		fmt.Fprintln(os.Stderr, "Using config file:", configFile)
		return nil
	}

	if err := readCfg(); err != nil {
		if !errors.As(err, &viper.ConfigFileNotFoundError{}) {
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

func bindTimeRangeFlag(commands ...*cobra.Command) {
	for _, c := range commands {
		c.Flags().StringVarP(&start, "start", "s", "", "Start time of the time range during which the query is preformed")
		c.Flags().StringVarP(&end, "end", "e", "", "End time of the time range during which the query is preformed")
	}
}

func bindNameAndIDFlag(commands ...*cobra.Command) {
	bindNameFlag(commands...)
	for _, c := range commands {
		c.Flags().StringVarP(&id, "id", "i", "", "the property's id")
		_ = c.MarkFlagRequired("name")
		_ = c.MarkFlagRequired("id")
	}
}

func bindTLSRelatedFlag(commands ...*cobra.Command) {
	for _, c := range commands {
		c.Flags().BoolVarP(&enableTLS, "enable-tls", "", false, "Used to enable tls")
		c.Flags().BoolVarP(&insecure, "insecure", "", false, "Used to skip server's cert")
		c.Flags().StringVarP(&cert, "cert", "", "", "Certificate for tls")
	}
}
