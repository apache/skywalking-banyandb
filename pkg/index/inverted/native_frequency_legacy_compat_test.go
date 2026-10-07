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

package inverted

import (
	"testing"

	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

// The retired ICE reader is the independent grammar oracle for the native
// writer's frequency stream, not the native reader under test.
func TestNativeFrequencyStreamLoadsInLegacyICE(t *testing.T) {
	payload, encodeErr := nativeice.EncodeSegment(nativeice.Generation{Documents: []nativeice.EncodeDocument{
		{Identifier: []byte("id-0"), Fields: []nativeice.EncodeField{{Name: "keyword", Index: true, Terms: []nativeice.EncodeTerm{{Value: []byte("term"), Frequency: 3}}}}},
		{Identifier: []byte("id-1"), Fields: []nativeice.EncodeField{{Name: "keyword", Index: true, Terms: []nativeice.EncodeTerm{{Value: []byte("term"), Frequency: 1}}}}},
	}})
	require.NoError(t, encodeErr)
	loaded, loadErr := loadLegacySegment(segment.NewDataBytes(payload))
	require.NoError(t, loadErr)
	dictionary, dictionaryErr := loaded.Dictionary("keyword")
	require.NoError(t, dictionaryErr)
	postings, postingsErr := dictionary.PostingsList([]byte("term"), nil, nil)
	require.NoError(t, postingsErr)
	iterator, iteratorErr := postings.Iterator(true, true, false, nil)
	require.NoError(t, iteratorErr)
	frequencies := make([]int, 0, 2)
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				t.Fatalf("legacy ICE iterator panicked on missing frequency stream: %v", recovered)
			}
		}()
		for {
			posting, nextErr := iterator.Next()
			require.NoError(t, nextErr)
			if posting == nil {
				break
			}
			frequencies = append(frequencies, posting.Frequency())
		}
	}()
	require.Equal(t, []int{3, 1}, frequencies)
}

func TestNativeAllOneFrequencyPostingLoadsInLegacyICE(t *testing.T) {
	payload, encodeErr := nativeice.EncodeSegment(nativeice.Generation{Documents: []nativeice.EncodeDocument{
		{Identifier: []byte("id-0"), Fields: []nativeice.EncodeField{{Name: "keyword", Index: true, Terms: []nativeice.EncodeTerm{{Value: []byte("term"), Frequency: 1}}}}},
		{Identifier: []byte("id-1"), Fields: []nativeice.EncodeField{{Name: "keyword", Index: true, Terms: []nativeice.EncodeTerm{{Value: []byte("term"), Frequency: 1}}}}},
	}})
	require.NoError(t, encodeErr)
	loaded, loadErr := loadLegacySegment(segment.NewDataBytes(payload))
	require.NoError(t, loadErr)
	dictionary, dictionaryErr := loaded.Dictionary("keyword")
	require.NoError(t, dictionaryErr)
	postings, postingsErr := dictionary.PostingsList([]byte("term"), nil, nil)
	require.NoError(t, postingsErr)
	iterator, iteratorErr := postings.Iterator(true, true, false, nil)
	require.NoError(t, iteratorErr)
	frequencies := make([]int, 0, 2)
	require.NotPanics(t, func() {
		for {
			posting, nextErr := iterator.Next()
			require.NoError(t, nextErr)
			if posting == nil {
				break
			}
			frequencies = append(frequencies, posting.Frequency())
		}
	})
	require.Equal(t, []int{1, 1}, frequencies)
}
