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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package qlgen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc/codes"
)

// Feature declares a capability's generated success expectations, rejection
// expectations, or pending implementation/fixture dependency. RPC tests verify it.
type Feature struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// Suite is the reproducible QL-only integration artifact.
type Suite struct {
	FixtureDigest  string          `json:"fixture_digest"`
	ReuseScenarios []ReuseScenario `json:"reuse_scenarios"`
	ReuseReadiness Case            `json:"reuse_readiness"`
	Features       []Feature       `json:"features"`
	Cases          []Case          `json:"cases"`
	Pairs          PairCoverage    `json:"pairs"`
	Version        int             `json:"version"`
	Seed           int64           `json:"seed"`
}

// Load checks the generated contract before integration tests consume it.
func Load(content []byte) (Suite, error) {
	var suite Suite
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&suite); err != nil {
		return suite, fmt.Errorf("decode generated QL suite: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return suite, fmt.Errorf("generated QL suite contains trailing data")
	}
	return suite, suite.Validate()
}

// Validate rejects stale fixtures, duplicate cases and incomplete declared coverage.
func (s Suite) Validate() error {
	if err := ValidateCoverageContract(s.Features); err != nil {
		return err
	}
	if s.Version != 1 || s.Seed != 1412 || s.FixtureDigest != FixtureDigest() {
		return fmt.Errorf("generated QL suite is stale; run make generate-ql-test-cases")
	}
	if len(s.Cases) == 0 || s.Cases[0].Name != "generated_scan_all" {
		return fmt.Errorf("generated QL suite is missing its visibility witness")
	}
	features := make(map[string]Feature, len(s.Features))
	for _, feature := range s.Features {
		if _, exists := features[feature.ID]; exists || feature.ID == "" {
			return fmt.Errorf("duplicate or empty QL feature %q", feature.ID)
		}
		switch feature.Status {
		case "supported", "rejection":
		case "pending":
			if feature.Reason == "" {
				return fmt.Errorf("pending QL feature %s has no reason", feature.ID)
			}
		default:
			return fmt.Errorf("unknown QL feature status %q", feature.Status)
		}
		features[feature.ID] = feature
	}
	witnesses := make(map[string]int)
	names := make(map[string]bool, len(s.Cases))
	for _, c := range s.Cases {
		if c.Name == "" || names[c.Name] || strings.TrimSpace(c.Query) == "" || len(c.Features) == 0 {
			return fmt.Errorf("invalid or duplicate generated QL case %q", c.Name)
		}
		names[c.Name] = true
		if c.WantCode == codes.OK && !json.Valid(c.WantJSON) {
			return fmt.Errorf("case %s has no independent expected result", c.Name)
		}
		for _, id := range c.Features {
			feature, exists := features[id]
			if !exists || feature.Status == "pending" {
				return fmt.Errorf("case %s claims undeclared or pending feature %s", c.Name, id)
			}
			if (feature.Status == "supported") != (c.WantCode == codes.OK) {
				return fmt.Errorf("case %s claims the wrong witness kind for %s", c.Name, id)
			}
			witnesses[id]++
		}
	}
	for _, feature := range s.Features {
		if feature.Status != "pending" && witnesses[feature.ID] == 0 {
			return fmt.Errorf("QL feature %s has no generated witness", feature.ID)
		}
	}
	if err := validateReuseReadiness(s.ReuseReadiness); err != nil {
		return err
	}
	if err := validateReuseScenarios(s.ReuseScenarios); err != nil {
		return err
	}
	return validatePairWitnesses(s.Pairs, s.Cases)
}

// Write generates the suite without reading a running server or capturing output.
func Write(directory string) error {
	suite, err := Generate()
	if err != nil {
		return err
	}
	content, err := json.MarshalIndent(suite, "", "  ")
	if err != nil {
		return fmt.Errorf("encode generated QL suite: %w", err)
	}
	if err = os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create generated QL directory: %w", err)
	}
	if err = os.WriteFile(filepath.Join(directory, "suite.json"), append(content, '\n'), 0o600); err != nil {
		return fmt.Errorf("write generated QL suite: %w", err)
	}
	return nil
}

// Check verifies that the committed artifact matches deterministic regeneration.
func Check(directory string) error {
	suite, err := Generate()
	if err != nil {
		return err
	}
	want, err := json.MarshalIndent(suite, "", "  ")
	if err != nil {
		return fmt.Errorf("encode generated QL suite: %w", err)
	}
	got, err := os.ReadFile(filepath.Join(directory, "suite.json"))
	if err != nil {
		return fmt.Errorf("read generated QL suite: %w", err)
	}
	if !bytes.Equal(got, append(want, '\n')) {
		return fmt.Errorf("generated QL suite is stale; run make generate-ql-test-cases")
	}
	return nil
}
