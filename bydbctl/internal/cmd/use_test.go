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

package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUseCommandWritesToConfiguredWriter(t *testing.T) {
	t.Cleanup(func() {
		cfgFile = ""
		viper.Reset()
	})

	configPath := filepath.Join(t.TempDir(), ".bydbctl.yaml")
	err := os.WriteFile(configPath, []byte("addr: http://localhost:17913\n"), 0o600)
	require.NoError(t, err)

	command := &cobra.Command{Use: "root"}
	RootCmdFlags(command)
	command.SetArgs([]string{"--config", configPath, "use", "sw"})
	var outBuf bytes.Buffer
	var errBuf bytes.Buffer
	command.SetOut(&outBuf)
	command.SetErr(&errBuf)

	err = command.Execute()
	require.NoError(t, err)
	assert.Contains(t, outBuf.String(), "Switched to [sw]")

	cfgBytes, err := os.ReadFile(configPath)
	require.NoError(t, err)
	assert.Contains(t, string(cfgBytes), "group: sw")
}
