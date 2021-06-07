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

package physical_test

import (
	"github.com/hashicorp/go-multierror"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
)

var _ gomega.OmegaMatcher = (*multiErrorMatcher)(nil)

type multiErrorMatcher struct {
	delegate gomega.OmegaMatcher
	expected error
}

func (m *multiErrorMatcher) Match(actual interface{}) (bool, error) {
	// first delegate to gomega.MatchError
	success, err := m.delegate.Match(actual)
	if success {
		return success, nil
	}

	hashiCorpMultiError, ok := actual.(*multierror.Error)
	if !ok {
		return success, err
	}

	innerErrors := hashiCorpMultiError.WrappedErrors()
	for _, innerErr := range innerErrors {
		innerSuc, err := m.delegate.Match(innerErr)
		// Early exit if it matches
		if err == nil && innerSuc {
			return innerSuc, err
		}
	}

	// Otherwise just return most recent result
	return success, err
}

func (m *multiErrorMatcher) FailureMessage(actual interface{}) (message string) {
	return format.Message(actual, "to contain matched error", m.expected)
}

func (m *multiErrorMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	return format.Message(actual, "not to contain matched error", m.expected)
}

func ContainError(anyError error) gomega.OmegaMatcher {
	return &multiErrorMatcher{
		delegate: gomega.MatchError(anyError),
		expected: anyError,
	}
}
