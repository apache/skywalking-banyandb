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

// Package config implements the reading of the authentication configuration.
package config

import (
	"gopkg.in/yaml.v3"
	"os"
)

// Config AuthConfig.
type Config struct {
	Users   []User `yaml:"users"`
	Enabled bool   `yaml:"enabled"`
}

type User struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// NewConfig returns a new config object.
func NewConfig() *Config {
	return &Config{}
}

// LoadConfig implements the reading of the authentication configuration.
func LoadConfig(cfg *Config) error {
	data, err := os.ReadFile("../pkg/config/config.yaml")
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(data, cfg)
	if err != nil {
		return err
	}
	return nil
}
