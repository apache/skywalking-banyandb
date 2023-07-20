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

package convert_test

import (
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

func TestParseSize(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"0B", 0},
		{"1KB", 1000},
		{"1MB", 1000 * 1000},
		{"1GB", 1000 * 1000 * 1000},
		{"1KiB", 1 << 10},
		{"1MiB", 1 << 20},
		{"1GiB", 1 << 30},
		{"1.5Gi", int64(1.5 * float64(1<<30))},
		{"1024 M", 1024 * 1000 * 1000},
		{"42 Ki", 42 * (1 << 10)},
	}

	for _, test := range tests {
		result, err := convert.ParseSize(test.input)
		if err != nil {
			t.Errorf("Failed to parse size for input %q: %v", test.input, err)
		} else if result != test.expected {
			t.Errorf("Incorrect result for input %q: expected %d, got %d", test.input, test.expected, result)
		}
	}
}
