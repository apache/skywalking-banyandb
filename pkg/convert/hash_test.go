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
	"testing"

	"github.com/cespare/xxhash/v2"
)

func TestHash(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
	}{
		{"EmptyKey", []byte{}},
		{"NonEmptyKey", []byte("hello")},
		{"SpecialCharsKey", []byte("!@#$%^&*()_+{}")},
		{"CustomKey", []byte("test123")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Hash(tt.key)
			want := xxhash.Sum64(tt.key)
			if got != want {
				t.Errorf("Hash() = %v, want %v", got, want)
			}
		})
	}
}

func TestHashStr(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{"EmptyKey", ""},
		{"NonEmptyKey", "hello"},
		{"SpecialCharsKey", "!@#$%^&*()_+{}"},
		{"CustomKey", "test123"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HashStr(tt.key)
			want := xxhash.Sum64String(tt.key)
			if got != want {
				t.Errorf("HashStr() = %v, want %v", got, want)
			}
		})
	}
}
