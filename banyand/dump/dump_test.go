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

package dump_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/banyand/dump"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestDecodeTagValue(t *testing.T) {
	assert.Equal(t, "hello", dump.DecodeTagValue(pbv1.ValueTypeStr, []byte("hello"), nil).GetStr().GetValue())
	assert.Equal(t, int64(42), dump.DecodeTagValue(pbv1.ValueTypeInt64, convert.Int64ToBytes(42), nil).GetInt().GetValue())

	ts := dump.DecodeTagValue(pbv1.ValueTypeTimestamp, convert.Int64ToBytes(1_000_000_000), nil)
	assert.Equal(t, int64(1), ts.GetTimestamp().GetSeconds())

	arr := dump.DecodeTagValue(pbv1.ValueTypeStrArr, nil, [][]byte{[]byte("a"), []byte("b")})
	assert.Equal(t, []string{"a", "b"}, arr.GetStrArray().GetValue())

	// A nil value and a nil array both decode to the null tag value.
	assert.Equal(t, pbv1.NullTagValue, dump.DecodeTagValue(pbv1.ValueTypeStr, nil, nil))
}

// TestDecodeTagValueBinaryDeepCopy: binary data must be copied so the decoded
// proto survives mutation/reuse of the source buffer.
func TestDecodeTagValueBinaryDeepCopy(t *testing.T) {
	raw := []byte{1, 2, 3, 4}
	tv := dump.DecodeTagValue(pbv1.ValueTypeBinaryData, raw, nil)
	for i := range raw {
		raw[i] = 0xFF
	}
	assert.Equal(t, []byte{1, 2, 3, 4}, tv.GetBinaryData())
}

func TestIteratorWalksAllRows(t *testing.T) {
	blocks := [][]int{{1, 2}, {}, {3}}
	it := dump.NewIterator(len(blocks), func(blockIdx int) ([]int, error) {
		return blocks[blockIdx], nil
	})

	assert.Equal(t, 0, it.Row(), "zero value before first Next")

	var got []int
	for it.Next() {
		got = append(got, it.Row())
	}
	assert.NoError(t, it.Err())
	assert.Equal(t, []int{1, 2, 3}, got)

	assert.NoError(t, it.Close())
	assert.NoError(t, it.Close(), "Close must be idempotent")
	assert.False(t, it.Next(), "Next after Close stays false")
}

func TestIteratorDecodeError(t *testing.T) {
	boom := errors.New("boom")
	it := dump.NewIterator(2, func(blockIdx int) ([]int, error) {
		if blockIdx == 1 {
			return nil, boom
		}
		return []int{blockIdx}, nil
	})

	assert.True(t, it.Next())
	assert.Equal(t, 0, it.Row())
	assert.False(t, it.Next(), "stops at the failing block")
	assert.ErrorIs(t, it.Err(), boom)
	assert.False(t, it.Next(), "terminal error state is sticky")
}

func TestErrIterator(t *testing.T) {
	boom := errors.New("open failed")
	it := dump.NewErrIterator[int](boom)
	assert.False(t, it.Next())
	assert.ErrorIs(t, it.Err(), boom)
	assert.Equal(t, dump.Position{BlockIdx: 0, RowIdx: -1}, it.Position())
}
