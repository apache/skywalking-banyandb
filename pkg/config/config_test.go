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

package config

import (
	"os"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	tester := assert.New(t)
	fs := pflag.NewFlagSet("test", pflag.ExitOnError)
	tests := []struct {
		flagName        string
		flagDescription string
		envName         string
		envValue        string
	}{
		{
			flagName:        "cert-file",
			flagDescription: "path to cert file",
			envName:         "BYDB_CERT_FILE",
			envValue:        "foo",
		},
		{
			flagName:        "key-file",
			flagDescription: "path to key file",
			envName:         "BYDB_KEY_FILE",
			envValue:        "bar",
		},
	}
	for _, tt := range tests {
		name := "bind flag: " + tt.flagName
		t.Run(name, func(t *testing.T) {
			var flagValue string
			fs.StringVar(&flagValue, tt.flagName, "", tt.flagDescription)
			os.Setenv(tt.envName, tt.envValue)
			err := Load("cfg", fs)
			tester.NoError(err, name)
			tester.NotNil(flagValue, name)
			tester.Equal(flagValue, tt.envValue, name)
			tester.NotNil(fs.Lookup(tt.flagName).Value.String(), name)
			tester.Equal(fs.Lookup(tt.flagName).Value.String(), tt.envValue, name)
		})
	}
}
