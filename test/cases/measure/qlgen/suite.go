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
	GroupingReadiness            []AggregateFixture            `json:"grouping_readiness"`
	GroupingScenarios            []GroupingScenario            `json:"grouping_scenarios"`
	GroupingRejections           []Case                        `json:"grouping_rejections"`
	DistinctScenarios            []DistinctScenario            `json:"distinct_scenarios"`
	DistinctReadiness            []Case                        `json:"distinct_readiness"`
	DistinctRejections           []Case                        `json:"distinct_rejections"`
	HavingReviewScenarios        []HavingScenario              `json:"having_review_scenarios"`
	HavingLegacyScenarios        []HavingScenario              `json:"having_legacy_scenarios"`
	HavingReadiness              []AggregateFixture            `json:"having_readiness"`
	HavingScenarios              []HavingScenario              `json:"having_scenarios"`
	HavingRejections             []Case                        `json:"having_rejections"`
	LegacyCompatibilityReadiness []Case                        `json:"legacy_compatibility_readiness"`
	LegacyCompatibilityScenarios []LegacyCompatibilityScenario `json:"legacy_compatibility_scenarios"`
	FilterReadiness              []Case                        `json:"filter_readiness"`
	FilterScenarios              []FilterScenario              `json:"filter_scenarios"`
	FilterRejections             []Case                        `json:"filter_rejections"`
	ExpressionReadiness          []Case                        `json:"expression_readiness"`
	ExpressionRejections         []Case                        `json:"expression_rejections"`
	ExpressionScenarios          []ExpressionScenario          `json:"expression_scenarios"`
	NativeAggregateReadiness     []AggregateFixture            `json:"native_aggregate_readiness"`
	NativeAggregateRejections    []Case                        `json:"native_aggregate_rejections"`
	NativeAggregateScenarios     []NativeAggregateScenario     `json:"native_aggregate_scenarios"`
	AggregateReadiness           []AggregateFixture            `json:"aggregate_readiness"`
	AggregateRejections          []Case                        `json:"aggregate_rejections"`
	AggregateScenarios           []AggregateScenario           `json:"aggregate_scenarios"`
	FixtureDigest                string                        `json:"fixture_digest"`
	ReuseScenarios               []ReuseScenario               `json:"reuse_scenarios"`
	ReuseReadiness               Case                          `json:"reuse_readiness"`
	Features                     []Feature                     `json:"features"`
	Cases                        []Case                        `json:"cases"`
	Pairs                        PairCoverage                  `json:"pairs"`
	Version                      int                           `json:"version"`
	Seed                         int64                         `json:"seed"`
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
	if err := validateGroupingScenarios(s); err != nil {
		return err
	}
	if err := validateDistinctScenarios(s); err != nil {
		return err
	}
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
	for _, c := range s.witnessCases() {
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
	if err := s.validateScenarios(); err != nil {
		return err
	}
	return validatePairWitnesses(s.Pairs, s.Cases)
}

func (s Suite) validateScenarios() error {
	if err := validateT14ReviewScenarios(s.HavingReviewScenarios); err != nil {
		return err
	}
	if err := validateHavingScenarios(s.HavingReadiness, s.HavingScenarios); err != nil {
		return err
	}
	if len(s.HavingLegacyScenarios) == 0 {
		return fmt.Errorf("T14 legacy membership witnesses missing")
	}
	for _, scenario := range s.HavingLegacyScenarios {
		if len(scenario.Cases) != 3 {
			return fmt.Errorf("T14 legacy membership binding missing")
		}
	}
	if err := validateT12Legacy(s.LegacyCompatibilityReadiness, s.LegacyCompatibilityScenarios); err != nil {
		return err
	}
	if err := validateFilterScenarios(s.FilterReadiness, s.FilterScenarios); err != nil {
		return err
	}
	if err := validateReuseReadiness(s.ReuseReadiness); err != nil {
		return err
	}
	if err := validateReuseScenarios(s.ReuseScenarios); err != nil {
		return err
	}
	if err := validateAggregateScenarios(s.AggregateReadiness, s.AggregateScenarios); err != nil {
		return err
	}
	if err := validateExpressionScenarios(s.ExpressionReadiness, s.ExpressionScenarios); err != nil {
		return err
	}
	return validateNativeAggregateScenarios(s.NativeAggregateReadiness, s.NativeAggregateScenarios)
}

func (s Suite) witnessCases() []Case {
	cases := append([]Case(nil), s.Cases...)
	for _, fixture := range s.GroupingReadiness {
		cases = append(cases, fixture.Cases...)
	}
	cases = append(cases, s.GroupingRejections...)
	for _, scenario := range s.GroupingScenarios {
		cases = append(cases, scenario.Cases...)
	}
	cases = append(cases, s.DistinctReadiness...)
	cases = append(cases, s.DistinctRejections...)
	for _, scenario := range s.DistinctScenarios {
		cases = append(cases, scenario.Cases...)
	}
	for _, scenario := range s.HavingReviewScenarios {
		cases = append(cases, scenario.Cases...)
	}
	for _, scenario := range s.HavingLegacyScenarios {
		cases = append(cases, scenario.Cases...)
	}
	for _, fixture := range s.HavingReadiness {
		cases = append(cases, fixture.Cases...)
	}
	cases = append(cases, s.HavingRejections...)
	for _, scenario := range s.HavingScenarios {
		cases = append(cases, scenario.Cases...)
	}
	cases = append(cases, s.LegacyCompatibilityReadiness...)
	for _, scenario := range s.LegacyCompatibilityScenarios {
		cases = append(cases, scenario.Cases...)
	}
	cases = append(cases, s.FilterReadiness...)
	cases = append(cases, s.FilterRejections...)
	for _, scenario := range s.FilterScenarios {
		cases = append(cases, scenario.Cases...)
	}
	cases = append(cases, s.ExpressionReadiness...)
	cases = append(cases, s.ExpressionRejections...)
	for _, scenario := range s.ExpressionScenarios {
		cases = append(cases, scenario.Cases...)
	}
	for _, fixture := range s.AggregateReadiness {
		cases = append(cases, fixture.Cases...)
	}
	cases = append(cases, s.AggregateRejections...)
	for _, scenario := range s.AggregateScenarios {
		cases = append(cases, scenario.Cases...)
	}
	for _, fixture := range s.NativeAggregateReadiness {
		cases = append(cases, fixture.Cases...)
	}
	cases = append(cases, s.NativeAggregateRejections...)
	for _, scenario := range s.NativeAggregateScenarios {
		cases = append(cases, scenario.Cases...)
	}
	return cases
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
