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

// Package gmatcher provides custom Gomega matchers.
package gmatcher

import (
	"fmt"

	"github.com/onsi/gomega"
)

// HaveZeroRef returns a matcher that checks if all pools have 0 references.
func HaveZeroRef() gomega.OmegaMatcher {
	return &ZeroRefMatcher{}
}

var _ gomega.OmegaMatcher = &ZeroRefMatcher{}

// ZeroRefMatcher is a matcher that checks if all pools have 0 references.
type ZeroRefMatcher struct{}

// FailureMessage implements types.GomegaMatcher.
func (p *ZeroRefMatcher) FailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("expected all pools to have 0 references, got %v", actual)
}

// Match implements types.GomegaMatcher.
func (p *ZeroRefMatcher) Match(actual interface{}) (success bool, err error) {
	data, ok := actual.(map[string]int)
	if !ok {
		return false, fmt.Errorf("expected map[string]int, got %T", actual)
	}
	for pooName, refers := range data {
		if refers > 0 {
			return false, fmt.Errorf("pool %s has %d references", pooName, refers)
		}
	}
	return true, nil
}

// NegatedFailureMessage implements types.GomegaMatcher.
func (p *ZeroRefMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("expected at least one pool to have references, got %v", actual)
}
