// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership. The ASF
// licenses this file to You under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package query_test

import (
	"testing"

	"github.com/onsi/ginkgo/v2/types"
	"github.com/stretchr/testify/assert"
)

func TestHasSpecFilter(t *testing.T) {
	testCases := []struct {
		name        string
		suiteConfig types.SuiteConfig
		want        bool
	}{
		{name: "unfiltered suite", want: false},
		{name: "focused specs", suiteConfig: types.SuiteConfig{FocusStrings: []string{"TopN"}}, want: true},
		{name: "skipped specs", suiteConfig: types.SuiteConfig{SkipStrings: []string{"TopN"}}, want: true},
		{name: "focused files", suiteConfig: types.SuiteConfig{FocusFiles: []string{"vectorized_test.go"}}, want: true},
		{name: "skipped files", suiteConfig: types.SuiteConfig{SkipFiles: []string{"vectorized_test.go"}}, want: true},
		{name: "label filter", suiteConfig: types.SuiteConfig{LabelFilter: "vectorized"}, want: true},
		{name: "semantic version filter", suiteConfig: types.SuiteConfig{SemVerFilter: ">=1.0.0"}, want: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.want, hasSpecFilter(testCase.suiteConfig))
		})
	}
}
