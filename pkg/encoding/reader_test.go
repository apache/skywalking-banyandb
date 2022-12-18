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

package encoding

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReader(t *testing.T) {
	data := []byte{3, 255, 0xcc, 0x1a, 0xbc, 0xde, 0x80}

	r := NewReader(bytes.NewBuffer(data))
	a := assert.New(t)

	eq(a, byte(3))(r.ReadByte())
	eq(a, uint64(255))(r.ReadBits(8))

	eq(a, uint64(0xc))(r.ReadBits(4))

	eq(a, uint64(0xc1))(r.ReadBits(8))

	eq(a, uint64(0xabcde))(r.ReadBits(20))

	eq(a, true)(r.ReadBool())
	eq(a, false)(r.ReadBool())
}

func eq(a *assert.Assertions, expected interface{}) func(interface{}, error) {
	return func(actual interface{}, err error) {
		a.NoError(err)
		a.Equal(expected, actual)
	}
}
