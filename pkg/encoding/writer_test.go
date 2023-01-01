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

func TestWriter(t *testing.T) {
	out := &bytes.Buffer{}
	w := NewWriter()
	w.Reset(out)

	a := assert.New(t)

	w.WriteByte(0xc1)
	w.WriteBool(false)
	w.WriteBits(0x3f, 6)
	w.WriteBool(true)
	w.WriteByte(0xac)
	w.WriteBits(0x01, 1)
	w.WriteBits(0x1248f, 20)
	w.Flush()

	w.WriteByte(0x01)
	w.WriteByte(0x02)

	w.WriteBits(0x0f, 4)

	w.WriteByte(0x80)
	w.WriteByte(0x8f)
	w.Flush()

	w.WriteBits(0x01, 1)
	w.WriteByte(0xff)
	w.Flush()
	a.Equal([]byte{0xc1, 0x7f, 0xac, 0x89, 0x24, 0x78, 0x01, 0x02, 0xf8, 0x08, 0xf0, 0xff, 0x80}, out.Bytes())
}
