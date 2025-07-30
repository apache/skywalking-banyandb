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
	"crypto/subtle"
	"fmt"
	"os"
	"strings"

	"sigs.k8s.io/yaml"
)

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

// InitCfg returns Config with default values.
func InitCfg() *Config {
	return &Config{
		Enabled:           false,
		HealthAuthEnabled: false,
		Users:             []User{},
	}
}

// LoadConfig implements the reading of the authentication configuration.
func LoadConfig(cfg *Config, filePath string) error {
	cfg.Enabled = true
	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	perm := info.Mode().Perm()
	if perm != 0o600 {
		return fmt.Errorf("config file %s has unsafe permissions: %o (expected 0600)", filePath, perm)
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(data, cfg)
	if err != nil {
		return err
	}
	return nil
}

// CheckUsernameAndPassword returns true if the provided username and password match any configured user.
func CheckUsernameAndPassword(cfg *Config, username, password string) bool {
	username = strings.TrimSpace(username)
	password = strings.TrimSpace(password)

	for _, user := range cfg.Users {
		storedUsername := strings.TrimSpace(user.Username)
		storedPassword := strings.TrimSpace(user.Password)

		// Convert to []byte
		usernameBytes := []byte(username)
		storedUsernameBytes := []byte(storedUsername)
		passwordBytes := []byte(password)
		storedPasswordBytes := []byte(storedPassword)

		// Length must match
		if len(usernameBytes) != len(storedUsernameBytes) || len(passwordBytes) != len(storedPasswordBytes) {
			continue
		}

		// Use constant-time comparison
		usernameMatch := subtle.ConstantTimeCompare(usernameBytes, storedUsernameBytes) == 1
		passwordMatch := subtle.ConstantTimeCompare(passwordBytes, storedPasswordBytes) == 1

		if usernameMatch && passwordMatch {
			return true
		}
	}
	return false
}
