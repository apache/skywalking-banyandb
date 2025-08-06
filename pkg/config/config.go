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

// Package config implements a configuration system which could load configuration from flags and env vars.
package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/multierr"
)

const (
	// The environment variable prefix of all environment variables bound to our command line flags.
	envPrefix = "BYDB"
)

type config struct {
	viper *viper.Viper
	name  string
}

// Load configurations from flags.
func Load(name string, fs *pflag.FlagSet) error {
	c := new(config)
	v := viper.New()
	c.name = name
	c.viper = v
	return c.initializeConfig(fs)
}

func (c *config) initializeConfig(fs *pflag.FlagSet) error {
	v := c.viper

	// Set the base name of the config file, without the file extension.
	v.SetConfigName(c.name)

	// Set as many paths as you like where viper should look for the
	// config file. We are only looking in the current working directory.
	v.AddConfigPath(".")

	// Attempt to read the config file, gracefully ignoring errors
	// caused by a config file not being found. Return an error
	// if we cannot parse the config file.
	if err := v.ReadInConfig(); err != nil {
		// It's okay if there isn't a config file
		if !errors.As(err, &viper.ConfigFileNotFoundError{}) {
			return err
		}
	}

	// When we bind flags to environment variables expect that the
	// environment variables are prefixed, e.g. a flag like --number
	// binds to an environment variable STING_NUMBER. This helps
	// avoid conflicts.
	v.SetEnvPrefix(envPrefix)

	// Bind to environment variables
	// Works great for simple config names, but needs help for names
	// like --favorite-color which we fix in the bindFlags function
	v.AutomaticEnv()

	// Bind the current command's flags to viper
	return BindFlags(fs, v, envPrefix)
}

// BindFlags bind each cobra flag to its associated viper configuration (config file and environment variable).
func BindFlags(fs *pflag.FlagSet, v *viper.Viper, envPrefix string) error {
	var err error
	fs.VisitAll(func(f *pflag.Flag) {
		// Environment variables can't have dashes in them, so bind them to their equivalent
		// keys with underscores.
		if strings.Contains(f.Name, "-") {
			envVarSuffix := strings.ToUpper(strings.ReplaceAll(f.Name, "-", "_"))
			err = multierr.Append(err, v.BindEnv(f.Name, fmt.Sprintf("%s_%s", envPrefix, envVarSuffix)))
		}

		// Apply the viper config value to the flag when the flag is not set and viper has a value
		if !f.Changed && v.IsSet(f.Name) {
			val := v.Get(f.Name)
			err = multierr.Append(err, fs.Set(f.Name, fmt.Sprintf("%v", val)))
		}
	})
	return err
}
