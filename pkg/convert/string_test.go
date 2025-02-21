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
	"reflect"
	"testing"
)

func TestStringToBytes(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want []byte
	}{
		{"EmptyString", "", nil},
		{"NonEmptyString", "hello", []byte("hello")},
		{"SpecialChars", "!@#$%^&*()_+{}", []byte("!@#$%^&*()_+{}")},
		{"CustomString", "test123", []byte("test123")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StringToBytes(tt.s); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StringToBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBytesToString(t *testing.T) {
	tests := []struct {
		name string
		want string
		b    []byte
	}{
		{"EmptyString", "", nil},
		{"NonEmptyString", "hello", []byte("hello")},
		{"SpecialChars", "!@#$%^&*()_+{}", []byte("!@#$%^&*()_+{}")},
		{"CustomString", "test123", []byte("test123")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BytesToString(tt.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BytesToString() = %v, want %v", got, tt.want)
			}
		})
	}
}
