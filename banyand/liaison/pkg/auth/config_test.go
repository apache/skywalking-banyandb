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

package auth

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	testConfigYAML := `
users:
  - username: test
    password: password
`

	tmpFile, err := os.CreateTemp("", "test_auth_config.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err = tmpFile.Write([]byte(testConfigYAML)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	if err = tmpFile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}

	err = LoadConfig(tmpFile.Name())
	assert.NoError(t, err, "LoadConfig should not return an error")
	assert.NotNil(t, Cfg, "Config should not be nil")
	assert.Len(t, Cfg.Users, 1, "There should be one user in the config")
	assert.Equal(t, "test", Cfg.Users[0].Username, "Username should be test")
	assert.Equal(t, "password", Cfg.Users[0].Password, "Password should be password")
}
