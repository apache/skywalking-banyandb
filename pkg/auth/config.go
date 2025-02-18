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

// Package auth implements the reading of the authentication configuration.
package auth

import (
	"os"

	"gopkg.in/yaml.v3"

	autht "github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
)

// Cfg auth config.
var Cfg *Config

// Config AuthConfig.
type Config struct {
	Users   []User `yaml:"users"`
	Enabled bool   `yaml:"-"`
}

// User details from config file.
type User struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// LoadConfig implements the reading of the authentication configuration.
func LoadConfig(filePath string, hashPassword bool) error {
	Cfg = new(Config)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(data, Cfg)
	if err != nil {
		return err
	}

	if hashPassword {
		for i := range Cfg.Users {
			hashed, err := autht.HashPassword(Cfg.Users[i].Password)
			if err != nil {
				return err
			}
			Cfg.Users[i].Password = hashed
		}
		yamlData, err := yaml.Marshal(Cfg)
		if err != nil {
			return err
		}
		file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {

			}
		}(file)
		_, err = file.Write(yamlData)
		if err != nil {
			return err
		}
	}
	return nil
}

// DefaultConfig disable auth.
func DefaultConfig() {
	Cfg = new(Config)
	Cfg.Enabled = false
	Cfg.Users = []User{}
}
