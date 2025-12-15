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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_listPrefixesForEntity(t *testing.T) {
	tests := []struct {
		name         string
		group        string
		entityPrefix string
		expected     string
	}{
		{
			name:         "records group with streams prefix",
			group:        "records",
			entityPrefix: "/streams/",
			expected:     "/streams/records/",
		},
		{
			name:         "recordsTrace group with streams prefix",
			group:        "recordsTrace",
			entityPrefix: "/streams/",
			expected:     "/streams/recordsTrace/",
		},
		{
			name:         "recordsLog group with streams prefix",
			group:        "recordsLog",
			entityPrefix: "/streams/",
			expected:     "/streams/recordsLog/",
		},
		{
			name:         "metrics group with measures prefix",
			group:        "metrics",
			entityPrefix: "/measures/",
			expected:     "/measures/metrics/",
		},
		{
			name:         "single character group",
			group:        "a",
			entityPrefix: "/streams/",
			expected:     "/streams/a/",
		},
		{
			name:         "empty group",
			group:        "",
			entityPrefix: "/streams/",
			expected:     "/streams/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := listPrefixesForEntity(tt.group, tt.entityPrefix)
			assert.Equal(t, tt.expected, result, "listPrefixesForEntity(%q, %q) should return %q", tt.group, tt.entityPrefix, tt.expected)
		})
	}
}

func Test_listPrefixesForEntity_FixValidation(t *testing.T) {
	// This test validates that the fix prevents the original GitHub issue
	// where group "records" would match groups "recordsTrace" and "recordsLog"

	recordsPrefix := listPrefixesForEntity("records", "/streams/")
	recordsTracePrefix := listPrefixesForEntity("recordsTrace", "/streams/")
	recordsLogPrefix := listPrefixesForEntity("recordsLog", "/streams/")

	// After the fix, each prefix should be unique and not be a prefix of another
	assert.Equal(t, "/streams/records/", recordsPrefix)
	assert.Equal(t, "/streams/recordsTrace/", recordsTracePrefix)
	assert.Equal(t, "/streams/recordsLog/", recordsLogPrefix)

	// Verify that "records" prefix doesn't match the other two
	// (This would fail before the fix)
	assert.False(t, hasPrefix(recordsTracePrefix, recordsPrefix), "recordsTrace prefix should not start with records prefix")
	assert.False(t, hasPrefix(recordsLogPrefix, recordsPrefix), "recordsLog prefix should not start with records prefix")

	// Verify that they are all distinct
	assert.NotEqual(t, recordsPrefix, recordsTracePrefix)
	assert.NotEqual(t, recordsPrefix, recordsLogPrefix)
	assert.NotEqual(t, recordsTracePrefix, recordsLogPrefix)
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
