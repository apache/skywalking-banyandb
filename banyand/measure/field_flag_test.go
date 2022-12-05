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

package measure

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestEncodeFieldFlag(t *testing.T) {
	flag := pbv1.EncoderFieldFlag(&databasev1.FieldSpec{
		EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
		CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
	}, time.Minute)
	fieldSpec, interval, err := pbv1.DecodeFieldFlag(flag)
	assert.NoError(t, err)
	assert.Equal(t, databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, fieldSpec.EncodingMethod)
	assert.Equal(t, databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD, fieldSpec.CompressionMethod)
	assert.Equal(t, time.Minute, interval)
}
