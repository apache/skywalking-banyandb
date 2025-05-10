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

package encoding_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/filter"
)

func TestEncodeAndDecodeBloomFilter(t *testing.T) {
	assert := assert.New(t)

	bf, err := filter.NewBloomFilter(3, 0.01)
	assert.NoError(err)

	items := [][]byte{
		[]byte("skywalking"),
		[]byte("banyandb"),
		[]byte(""),
		[]byte("hello"),
		[]byte("world"),
	}

	for i := 0; i < 3; i++ {
		err = bf.Add(items[i])
		assert.Nil(err)
	}

	data, err := encoding.BloomFilterToBytes(bf)
	assert.NoError(err)
	bf2, err := encoding.BytesToBloomFilter(data)
	assert.NoError(err)

	for i := 0; i < 3; i++ {
		mightContain, err := bf2.MightContain(items[i])
		assert.Nil(err)
		assert.True(mightContain, "Should contain item %d", i)
	}

	for i := 3; i < 5; i++ {
		mightContain, err := bf2.MightContain(items[i])
		assert.Nil(err)
		assert.False(mightContain, "Should probably not contain item %d", i)
	}
}
