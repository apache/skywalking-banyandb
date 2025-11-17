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

package model

import (
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// TagValueDecoder decodes a byte slice to a TagValue based on the value type.
type TagValueDecoder func(valueType pbv1.ValueType, value []byte, valueArr [][]byte) *modelv1.TagValue

// TagValueDecoderProvider provides a decoder for tag values.
type TagValueDecoderProvider interface {
	GetTagValueDecoder() TagValueDecoder
}

// TagFilterMatcher is a simple interface for matching tags without schema dependency.
// This avoids cycle imports between logical and sidx packages.
type TagFilterMatcher interface {
	// Match returns true if the given tags match the filter criteria.
	// tags is a slice of tags for a single element/row.
	Match(tags []*modelv1.Tag) (bool, error)
	// GetDecoder returns the decoder function for tag values.
	GetDecoder() TagValueDecoder
}
