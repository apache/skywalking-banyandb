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
	"github.com/spf13/pflag"
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
	// activeRoot is the root command whose flags initConfig binds; tests build their own.
	activeRoot = rootCmd
	// explicitRootFlags records which persistent root flags were given on the command line
	// of the current invocation. It is captured before config.BindFlags copies viper values
	// into the flag set, because after that point Changed() is true for every flag that has
	// a viper default (e.g. addr). The data commands use it to reject -a/--addr.
	explicitRootFlags = map[string]bool{}
)

// ResetFlags resets the flags, including the config file path and the test argument
// vector, so one test's invocation cannot leak into the next.
func ResetFlags() {
	filePath = ""
	name = ""
	start = ""
	end = ""
	cfgFile = ""
	enableTLS = false
	insecure = false
	cert = ""
	username = ""
	password = ""
	testArgs = nil
	explicitRootFlags = map[string]bool{}
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
		newIndexRuleCmd(), newIndexRuleBindingCmd(), newPropertyCmd(), newTraceCmd(), newHealthCheckCmd(), newAnalyzeCmd(), newAgentCmd(), newAgentToolBridgeCmd(),
		newDataCmd())
	activeRoot = command
}

// testArgs lets tests that drive a private root command through SetArgs tell initConfig
// which command line to inspect; production always reads os.Args.
var testArgs []string

// SetTestArgs records the argument vector a test is about to pass to SetArgs.
func SetTestArgs(args []string) { testArgs = args }

func activeRootArgs() []string {
	if testArgs != nil {
		return testArgs
	}
	return os.Args[1:]
}

// snapshotExplicitRootFlags records the persistent root flags the executing command saw
// on the command line. Cobra parses flags before OnInitialize runs and merges the root's
// persistent flags into the executed command's flag set, so that set is where Changed()
// still reflects the command line only.
func snapshotExplicitRootFlags(root *cobra.Command, args []string) {
	explicitRootFlags = map[string]bool{}
	command, _, err := root.Find(args)
	if err != nil {
		return
	}
	root.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		if merged := command.Flags().Lookup(f.Name); merged != nil && merged.Changed {
			explicitRootFlags[f.Name] = true
		}
	})
}

func init() {
	cobra.OnInitialize(initConfig)
	RootCmdFlags(rootCmd)
}

// bindEnvAndFlags makes viper fall back from the root's persistent flags to BYDBCTL_* env.
func bindEnvAndFlags() {
	viper.SetEnvPrefix(envPrefix)
	viper.AutomaticEnv()
	// Bind the package-level root's flags, as before: the explicit-flag snapshot in initConfig
	// is the only place the executing (possibly test-built) root is consulted, so the viper
	// fallbacks keep flowing through viper.GetString rather than being copied into the flag
	// variables of a command built for a single test.
	cobra.CheckErr(config.BindFlags(rootCmd.PersistentFlags(), viper.GetViper(), envPrefix))
}

func initConfig() {
	snapshotExplicitRootFlags(activeRoot, activeRootArgs())
	command, _, findErr := activeRoot.Find(activeRootArgs())
	if findErr == nil && command.Name() == "agent-tool-bridge" {
		return
	}
	// The data commands take their settings from flags, plan.yaml and BYDBCTL_* only: they
	// neither read nor create the config file, so they never look up the home directory.
	if findErr == nil && isDataCommand(command) {
		bindEnvAndFlags()
		return
	}
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
	bindEnvAndFlags()

	readCfg := func() error {
		if err := viper.ReadInConfig(); err != nil {
			return err
		}
		configFile := viper.ConfigFileUsed()
		errWriter := rootCmd.ErrOrStderr()
		info, err := os.Stat(configFile)
		if err != nil {
			return fmt.Errorf("unable to stat config file: %w", err)
		}
		if info.Mode().Perm() != 0o600 {
			fmt.Fprintf(errWriter, "config file %s has unsafe permissions: %o (expected 0600)\n", configFile, info.Mode().Perm())
		}
		// Dump this to stderr in case of mixing up response yaml
		fmt.Fprintln(errWriter, "Using config file:", configFile)
		return nil
	}

	if err := readCfg(); err != nil {
		if !errors.As(err, &viper.ConfigFileNotFoundError{}) {
			cobra.CheckErr(err)
		}
		cobra.CheckErr(viper.SafeWriteConfig())
		// Reload config to ensure Viper updates ConfigFileUsed(), avoiding empty path for chmod
		cobra.CheckErr(viper.ReadInConfig())
		configFile := viper.ConfigFileUsed()
		if err := os.Chmod(configFile, 0o600); err != nil {
			cobra.CheckErr(fmt.Errorf("failed to set permissions on config file %s: %w", configFile, err))
		}
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
