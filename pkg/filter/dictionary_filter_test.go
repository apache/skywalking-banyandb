// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package filter

import (
	stdbytes "bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

const (
	entityDelimiter = '|'
	escape          = '\\'
)

func marshalVarArray(dest, src []byte) []byte {
	if stdbytes.IndexByte(src, entityDelimiter) < 0 && stdbytes.IndexByte(src, escape) < 0 {
		dest = append(dest, src...)
		dest = append(dest, entityDelimiter)
		return dest
	}
	for _, b := range src {
		if b == entityDelimiter || b == escape {
			dest = append(dest, escape)
		}
		dest = append(dest, b)
	}
	dest = append(dest, entityDelimiter)
	return dest
}

func TestDictionaryFilter(t *testing.T) {
	assert := assert.New(t)

	items := [][]byte{
		[]byte("skywalking"),
		[]byte("banyandb"),
		[]byte(""),
		[]byte("hello"),
		[]byte("world"),
	}

	df := NewDictionaryFilter(items[:3])
	assert.NotNil(df)

	for i := 0; i < 3; i++ {
		mightContain := df.MightContain(items[i])
		assert.True(mightContain)
	}

	for i := 3; i < 5; i++ {
		mightContain := df.MightContain(items[i])
		assert.False(mightContain)
	}
}

func TestDictionaryFilterResetClearsValues(t *testing.T) {
	assert := assert.New(t)

	key := []byte("reuse-key")

	df := NewDictionaryFilter([][]byte{key})
	assert.True(df.MightContain(key))

	df.Reset()
	assert.False(df.MightContain(key))
}

func TestDictionaryFilterInt64Arr(t *testing.T) {
	assert := assert.New(t)

	items := []int64{1, 2, 3, 4, 5}
	dst := make([]byte, 0, 24)
	for i := 0; i < 3; i++ {
		dst = append(dst, convert.Int64ToBytes(items[i])...)
	}

	df := NewDictionaryFilter([][]byte{dst})
	df.SetValueType(pbv1.ValueTypeInt64Arr)

	for i := 0; i < 3; i++ {
		assert.True(df.MightContain(convert.Int64ToBytes(items[i])))
	}
	for i := 3; i < 5; i++ {
		assert.False(df.MightContain(convert.Int64ToBytes(items[i])))
	}
}

func TestDictionaryFilterStrArr(t *testing.T) {
	assert := assert.New(t)

	items := [][]byte{
		[]byte("skywalking"),
		[]byte("banyandb"),
		[]byte(""),
		[]byte("hello"),
		[]byte("world"),
	}
	dst := make([]byte, 0)
	for i := 0; i < 3; i++ {
		dst = marshalVarArray(dst, items[i])
	}

	df := NewDictionaryFilter([][]byte{dst})
	df.SetValueType(pbv1.ValueTypeStrArr)

	for i := 0; i < 3; i++ {
		assert.True(df.MightContain(items[i]))
	}
	for i := 3; i < 5; i++ {
		assert.False(df.MightContain(items[i]))
	}
}
