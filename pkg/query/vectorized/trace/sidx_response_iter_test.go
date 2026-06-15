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

package trace

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSidxResponseIteratorAdaptsRows(t *testing.T) {
	iter := NewSidxResponseIterator([]*SidxRowBatch{
		{
			Keys:    []int64{1, 2},
			Data:    [][]byte{traceIDPayload("a"), traceIDPayload("b")},
			SIDs:    []int64{10, 20},
			PartIDs: []int64{100, 200},
		},
	})

	require.True(t, iter.Next())
	first := iter.Val()
	require.Equal(t, int64(1), first.Key)
	require.Equal(t, int64(10), first.SeriesID)
	require.Equal(t, int64(100), first.PartID)
	require.Equal(t, traceIDPayload("a"), first.Payload)

	require.True(t, iter.Next())
	second := iter.Val()
	require.Equal(t, int64(2), second.Key)
	require.Equal(t, int64(20), second.SeriesID)
	require.Equal(t, int64(200), second.PartID)
	require.Equal(t, traceIDPayload("b"), second.Payload)

	require.False(t, iter.Next())
	require.NoError(t, iter.Error())
	require.NoError(t, iter.Close())
}

func TestSidxResponseIteratorCapturesResponseError(t *testing.T) {
	wantErr := errors.New("boom")
	iter := NewSidxResponseIterator([]*SidxRowBatch{{Error: wantErr}})
	require.False(t, iter.Next())
	require.ErrorIs(t, iter.Error(), wantErr)
}

func TestSidxResponseIteratorRejectsMalformedPayload(t *testing.T) {
	iter := NewSidxResponseIterator([]*SidxRowBatch{
		{
			Keys:    []int64{1},
			Data:    [][]byte{{0xff, 'x'}},
			SIDs:    []int64{10},
			PartIDs: []int64{100},
		},
	})
	require.False(t, iter.Next())
	require.Error(t, iter.Error())
}

func traceIDPayload(traceID string) []byte {
	return append([]byte{idFormatV1}, []byte(traceID)...)
}
