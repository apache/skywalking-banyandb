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

// Package auth provides configuration management and validation logic for authentication.
package auth

import (
	"os"
	"strings"

	"sigs.k8s.io/yaml"
)

// Cfg auth config.
var Cfg *Config

// Config AuthConfig.
type Config struct {
	Users             []User `yaml:"users"`
	Enabled           bool   `yaml:"-"`
	HealthAuthEnabled bool   `yaml:"-"`
}

// User details from config file.
type User struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

func init() {
	Cfg = new(Config)
	Cfg.Enabled = false
	Cfg.HealthAuthEnabled = false
	Cfg.Users = []User{}
}

// LoadConfig implements the reading of the authentication configuration.
func LoadConfig(filePath string) error {
	Cfg.Enabled = true
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(data, Cfg)
	if err != nil {
		return err
	}
	return nil
}

// CheckUsernameAndPassword returns true if the provided username and password match any configured user.
func CheckUsernameAndPassword(username, password string) bool {
	for _, user := range Cfg.Users {
		if strings.TrimSpace(username) == strings.TrimSpace(user.Username) &&
			strings.TrimSpace(password) == strings.TrimSpace(user.Password) {
			return true
		}
	}
	return false
}
