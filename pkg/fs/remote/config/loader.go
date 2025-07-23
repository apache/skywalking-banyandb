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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// LoadFSConfig only decodes the azure file.
func LoadFSConfig(path string) (*FsConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := &FsConfig{}
	switch ext := filepath.Ext(path); ext {
	case ".json":
		if err := json.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse JSON: %w", err)
		}
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse YAML: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported config format: %s", ext)
	}

	if cfg.Provider != "azure" {
		return nil, fmt.Errorf("unsupported provider %q (expect azure)", cfg.Provider)
	}
	if cfg.Azure == nil ||
		cfg.Azure.AzureAccountName == "" ||
		cfg.Azure.AzureEndpoint == "" ||
		cfg.Azure.Container == "" {
		return nil, fmt.Errorf("missing required azure fields")
	}

	if cfg.Azure.AzureAccountKey == "" && cfg.Azure.AzureSASToken == "" {
		return nil, fmt.Errorf("missing azure credentials: either account_key or sas_token must be provided")
	}

	return cfg, nil
}
