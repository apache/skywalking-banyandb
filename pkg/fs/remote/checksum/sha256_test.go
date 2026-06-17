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

package checksum

import (
	"io"
	"strings"
	"testing"
)

func TestSum(t *testing.T) {
	v, err := DefaultSHA256Verifier()
	if err != nil {
		t.Fatal(err)
	}
	// Well-known SHA-256 of "hello".
	const want = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	got, err := v.Sum(strings.NewReader("hello"))
	if err != nil {
		t.Fatalf("Sum: %v", err)
	}
	if got != want {
		t.Errorf("Sum = %s, want %s", got, want)
	}

	// Sum must agree with the streaming ComputeAndWrap path on the same input.
	wrapped, getHash := v.ComputeAndWrap(strings.NewReader("hello"))
	if _, err = io.Copy(io.Discard, wrapped); err != nil {
		t.Fatal(err)
	}
	streamed, err := getHash()
	if err != nil {
		t.Fatal(err)
	}
	if streamed != got {
		t.Errorf("Sum (%s) disagrees with ComputeAndWrap (%s)", got, streamed)
	}
}
