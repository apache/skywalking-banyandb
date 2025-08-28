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

package trace

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestTimestampTagValueEncodingDecoding(t *testing.T) {
	// Test timestamp encoding
	t.Run("test timestamp tag encoding", func(t *testing.T) {
		// Create a test timestamp
		testTime := time.Date(2023, 1, 1, 12, 0, 0, 123456789, time.UTC)
		timestampProto := timestamppb.New(testTime)

		tagValue := &modelv1.TagValue{
			Value: &modelv1.TagValue_Timestamp{
				Timestamp: timestampProto,
			},
		}

		// Encode the timestamp
		encoded := encodeTagValue("test_timestamp", databasev1.TagType_TAG_TYPE_TIMESTAMP, tagValue)

		// Verify encoding
		assert.Equal(t, pbv1.ValueTypeTimestamp, encoded.valueType)
		assert.Equal(t, "test_timestamp", encoded.tag)
		assert.NotNil(t, encoded.value)

		// The encoded value should be 8 bytes (int64 nanoseconds)
		assert.Equal(t, 8, len(encoded.value))

		// Verify the encoded value represents the correct nanoseconds
		expectedNanos := testTime.UnixNano()
		// Use the same conversion function to decode
		decodedNanos := convert.BytesToInt64(encoded.value)
		assert.Equal(t, expectedNanos, decodedNanos)
	})

	// Test timestamp decoding
	t.Run("test timestamp tag decoding", func(t *testing.T) {
		// Create test timestamp
		testTime := time.Date(2023, 1, 1, 12, 0, 0, 123456789, time.UTC)
		expectedNanos := testTime.UnixNano()

		// Create encoded bytes using the same conversion function
		encodedBytes := convert.Int64ToBytes(expectedNanos)

		// Decode the timestamp
		decoded := mustDecodeTagValue(pbv1.ValueTypeTimestamp, encodedBytes)

		// Verify decoding
		assert.NotNil(t, decoded)
		assert.NotNil(t, decoded.GetTimestamp())

		decodedTime := decoded.GetTimestamp()
		assert.Equal(t, testTime.Unix(), decodedTime.Seconds)
		assert.Equal(t, int32(123456789), decodedTime.Nanos)
	})

	// Test round-trip encoding and decoding
	t.Run("test timestamp round-trip", func(t *testing.T) {
		// Create a test timestamp
		testTime := time.Date(2023, 1, 1, 12, 0, 0, 123456789, time.UTC)
		timestampProto := timestamppb.New(testTime)

		tagValue := &modelv1.TagValue{
			Value: &modelv1.TagValue_Timestamp{
				Timestamp: timestampProto,
			},
		}

		// Encode
		encoded := encodeTagValue("test_timestamp", databasev1.TagType_TAG_TYPE_TIMESTAMP, tagValue)

		// Decode
		decoded := mustDecodeTagValue(pbv1.ValueTypeTimestamp, encoded.value)

		// Verify round-trip
		assert.NotNil(t, decoded)
		assert.NotNil(t, decoded.GetTimestamp())

		decodedTime := decoded.GetTimestamp()
		assert.Equal(t, testTime.Unix(), decodedTime.Seconds)
		assert.Equal(t, int32(123456789), decodedTime.Nanos)
	})

	// Test nil timestamp handling
	t.Run("test nil timestamp handling", func(t *testing.T) {
		// Test encoding nil timestamp
		encoded := encodeTagValue("test_timestamp", databasev1.TagType_TAG_TYPE_TIMESTAMP, nil)
		assert.Equal(t, pbv1.ValueTypeTimestamp, encoded.valueType)
		assert.Nil(t, encoded.value)

		// Test decoding nil value
		decoded := mustDecodeTagValue(pbv1.ValueTypeTimestamp, nil)
		assert.Equal(t, pbv1.NullTagValue, decoded)
	})
}
