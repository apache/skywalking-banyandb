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

package tsdb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

func TestNewPath(t *testing.T) {
	tester := assert.New(t)
	tests := []struct {
		name   string
		entity Entity
		want   *Path
	}{
		{
			name: "general path",
			entity: Entity{
				Entry("productpage"),
				Entry("10.0.0.1"),
				Entry(convert.Uint64ToBytes(0)),
			},
			want: &Path{
				isFull: true,
				prefix: bytes.Join([][]byte{
					hash([]byte("productpage")),
					hash([]byte("10.0.0.1")),
					hash(convert.Uint64ToBytes(0)),
				}, nil),
				template: bytes.Join([][]byte{
					hash([]byte("productpage")),
					hash([]byte("10.0.0.1")),
					hash(convert.Uint64ToBytes(0)),
				}, nil),
				mask: bytes.Join([][]byte{
					maxIntBytes,
					maxIntBytes,
					maxIntBytes,
				}, nil),
			},
		},
		{
			name: "the first is anyone",
			entity: Entity{
				AnyEntry,
				Entry("10.0.0.1"),
				Entry(convert.Uint64ToBytes(0)),
			},
			want: &Path{
				prefix: []byte{},
				template: bytes.Join([][]byte{
					zeroIntBytes,
					hash([]byte("10.0.0.1")),
					hash(convert.Uint64ToBytes(0)),
				}, nil),
				mask: bytes.Join([][]byte{
					zeroIntBytes,
					maxIntBytes,
					maxIntBytes,
				}, nil),
			},
		},
		{
			name: "the second is anyone",
			entity: Entity{
				Entry("productpage"),
				AnyEntry,
				Entry(convert.Uint64ToBytes(0)),
			},
			want: &Path{
				prefix: bytes.Join([][]byte{
					hash([]byte("productpage")),
				}, nil),
				template: bytes.Join([][]byte{
					hash([]byte("productpage")),
					zeroIntBytes,
					hash(convert.Uint64ToBytes(0)),
				}, nil),
				mask: bytes.Join([][]byte{
					maxIntBytes,
					zeroIntBytes,
					maxIntBytes,
				}, nil),
			},
		},
		{
			name: "the last is anyone",
			entity: Entity{
				Entry("productpage"),
				Entry("10.0.0.1"),
				AnyEntry,
			},
			want: &Path{
				prefix: bytes.Join([][]byte{
					hash([]byte("productpage")),
					hash([]byte("10.0.0.1")),
				}, nil),
				template: bytes.Join([][]byte{
					hash([]byte("productpage")),
					hash([]byte("10.0.0.1")),
					zeroIntBytes,
				}, nil),
				mask: bytes.Join([][]byte{
					maxIntBytes,
					maxIntBytes,
					zeroIntBytes,
				}, nil),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewPath(tt.entity)
			tester.Equal(tt.want, got)
		})
	}
}
