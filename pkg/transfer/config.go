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

package transfer

import (
	"encoding/json"
	"fmt"
	"os"

	"sigs.k8s.io/yaml"
)

// Plan is the on-disk plan.yaml of design §2.2, restricted to what the dry-run reads:
// connection.nodes, connection.nodesTLS, export.parallelism and export.selectors. The
// fields a real export or an import consumes land with those steps, and until then a file
// that carries them is rejected by LoadPlanFile.
type Plan struct {
	Export     ExportConfig     `json:"export"`
	Connection ConnectionConfig `json:"connection"`
}

// ConnectionConfig describes how to reach the liaison gRPC endpoints.
type ConnectionConfig struct {
	Nodes    []string  `json:"nodes"`
	NodesTLS TLSConfig `json:"nodesTLS"`
}

// TLSConfig is the connection.nodesTLS section: whether TLS is on, whether the server
// certificate is verified, and the CA certificate file to verify it with.
type TLSConfig struct {
	Cert     string `json:"cert"`
	Enable   bool   `json:"enable"`
	Insecure bool   `json:"insecure"`
}

// ExportConfig is the export: section of plan.yaml: the scope and the node parallelism.
type ExportConfig struct {
	Parallelism Parallelism      `json:"parallelism"`
	Selectors   []SelectorConfig `json:"selectors"`
}

// SelectorConfig is one selectors[] entry: a catalog name and optional group names.
type SelectorConfig struct {
	Catalog string   `json:"catalog"`
	Groups  []string `json:"groups"`
}

// Parallelism keeps the raw plan.yaml value ("max" or an integer) so the CLI and the file
// go through the same ParseParallelism.
type Parallelism string

// UnmarshalJSON accepts a bare integer or a string.
func (p *Parallelism) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*p = Parallelism(s)
		return nil
	}
	var n json.Number
	if err := json.Unmarshal(data, &n); err == nil {
		*p = Parallelism(n.String())
		return nil
	}
	return fmt.Errorf("parallelism must be \"max\" or a positive integer, got %s", string(data))
}

func (p Parallelism) String() string { return string(p) }

// LoadPlanFile reads and decodes plan.yaml. Unknown fields are rejected so a typo in the
// file never silently falls back to a default.
func LoadPlanFile(path string) (*Plan, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read plan file: %w", err)
	}
	var plan Plan
	if err := yaml.UnmarshalStrict(raw, &plan); err != nil {
		return nil, fmt.Errorf("parse plan file %s: %w", path, err)
	}
	return &plan, nil
}
