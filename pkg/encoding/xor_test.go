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
	"io"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	myBytes "github.com/apache/skywalking-banyandb/pkg/bytes"
)

func TestXOR(t *testing.T) {
	var buf bytes.Buffer
	bitWriter := NewWriter()
	bitWriter.Reset(&buf)
	e := NewXOREncoder(bitWriter)
	e.Write(uint64(76))
	e.Write(uint64(50))
	e.Write(uint64(50))
	e.Write(uint64(999999999))
	e.Write(uint64(100))

	bitWriter.Flush()
	data := buf.Bytes()

	reader := NewReader(bytes.NewReader(data))
	d := NewXORDecoder(reader)
	a := assert.New(t)
	verify(d, a, uint64(76))
	verify(d, a, uint64(50))
	verify(d, a, uint64(50))
	verify(d, a, uint64(999999999))
	verify(d, a, uint64(100))
}

func TestXOR1(t *testing.T) {
	var buf myBytes.Buffer
	bitWriter := NewWriter()
	bitWriter.Reset(&buf)
	e := NewXOREncoder(bitWriter)
	e.Write(uint64(76))
	e.Write(uint64(50))
	e.Write(uint64(50))
	e.Write(uint64(999999999))
	e.Write(uint64(100))

	bitWriter.Flush()
	data := buf.Bytes()

	reader := NewReader(bytes.NewReader(data))
	d := NewXORDecoder(reader)
	a := assert.New(t)
	verify(d, a, uint64(76))
	verify(d, a, uint64(50))
	verify(d, a, uint64(50))
	verify(d, a, uint64(999999999))
	verify(d, a, uint64(100))
}

func verify(d *XORDecoder, a *assert.Assertions, except uint64) {
	a.True(d.Next())
	if d.Err() != nil && !errors.Is(d.Err(), io.EOF) {
		a.Fail("error: %v", d.Err())
	}
	a.Equal(except, d.Value())
}
