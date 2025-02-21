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

package convert

import (
	"bytes"
	"fmt"
	"testing"
)

func TestInt64ToBytes(t *testing.T) {
	testCases := []struct {
		expected []byte
		input    int64
	}{
		{[]byte{127, 255, 255, 255, 255, 255, 255, 156}, -100},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 254}, -2},
		{[]byte{127, 255, 255, 255, 255, 255, 255, 255}, -1},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 0}, 0},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 1}, 1},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 2}, 2},
		{[]byte{128, 0, 0, 0, 0, 0, 0, 100}, 100},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Int64ToBytes(%d)", tc.input), func(t *testing.T) {
			result := Int64ToBytes(tc.input)
			if !bytes.Equal(result, tc.expected) {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}
