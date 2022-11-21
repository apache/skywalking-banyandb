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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

func TestNewIntEncoderAndDecoder(t *testing.T) {
	type tsData struct {
		ts    []uint64
		data  []int64
		start uint64
		end   uint64
	}
	tests := []struct {
		name string
		args tsData
		want tsData
	}{
		{
			name: "golden path",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data: []int64{7, 8, 7, 9},
			},
			want: tsData{
				ts:    []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data:  []int64{7, 8, 7, 9},
				start: uint64(time.Minute),
				end:   uint64(4 * time.Minute),
			},
		},
		{
			name: "more than the size",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute), uint64(1 * time.Minute)},
				data: []int64{7, 8, 7, 9, 6},
			},
			want: tsData{
				ts:    []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data:  []int64{7, 8, 7, 9},
				start: uint64(time.Minute),
				end:   uint64(4 * time.Minute),
			},
		},
		{
			name: "less than the size",
			args: tsData{
				ts:   []uint64{uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(time.Minute)},
				data: []int64{7, 8, 7},
			},
			want: tsData{
				ts:    []uint64{uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(time.Minute)},
				data:  []int64{7, 8, 7},
				start: uint64(time.Minute),
				end:   uint64(3 * time.Minute),
			},
		},
		{
			name: "empty slot in the middle",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(time.Minute)},
				data: []int64{7, 9},
			},
			want: tsData{
				ts:    []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data:  []int64{7, 0, 0, 9},
				start: uint64(time.Minute),
				end:   uint64(4 * time.Minute),
			},
		},
	}
	key := []byte("foo")
	fn := func(k []byte) time.Duration {
		assert.Equal(t, key, k)
		return 1 * time.Minute
	}
	encoderPool := NewIntEncoderPool("minute", 4, fn)
	decoderPool := NewIntDecoderPool("minute", 4, fn)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			at := assert.New(t)
			encoder := encoderPool.Get(key)
			defer encoderPool.Put(encoder)
			decoder := decoderPool.Get(key)
			defer decoderPool.Put(decoder)
			encoder.Reset(key)
			isFull := false
			for i, v := range tt.args.ts {
				encoder.Append(v, convert.Int64ToBytes(tt.args.data[i]))
				if encoder.IsFull() {
					isFull = true
					break
				}
			}
			bb, err := encoder.Encode()
			at.NoError(err)

			at.Equal(tt.want.start, encoder.StartTime())
			at.NoError(decoder.Decode(key, bb))
			start, end := decoder.Range()
			at.Equal(tt.want.start, start)
			at.Equal(tt.want.end, end)
			if isFull {
				at.True(decoder.IsFull())
			}
			i := 0
			for iter := decoder.Iterator(); iter.Next(); i++ {
				at.NoError(iter.Error())
				at.Equal(tt.want.ts[i], iter.Time())
				at.Equal(tt.want.data[i], convert.BytesToInt64(iter.Val()))
				v, err := decoder.Get(iter.Time())
				at.NoError(err)
				at.Equal(tt.want.data[i], convert.BytesToInt64(v))
			}
			at.Equal(len(tt.want.ts), i)
		})
	}
}

func TestNewIntDecoderGet(t *testing.T) {
	type tsData struct {
		ts    []uint64
		data  []int64
		start uint64
		end   uint64
	}
	tests := []struct {
		name string
		args tsData
		want tsData
	}{
		{
			name: "golden path",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data: []int64{7, 8, 7, 9},
			},
			want: tsData{
				ts:    []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data:  []int64{7, 8, 7, 9},
				start: uint64(time.Minute),
				end:   uint64(4 * time.Minute),
			},
		},
		{
			name: "more than the size",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute), uint64(1 * time.Minute)},
				data: []int64{7, 8, 7, 9, 6},
			},
			want: tsData{
				ts:    []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute), 0},
				data:  []int64{7, 8, 7, 9, 0},
				start: uint64(time.Minute),
				end:   uint64(4 * time.Minute),
			},
		},
		{
			name: "less than the size",
			args: tsData{
				ts:   []uint64{uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(time.Minute)},
				data: []int64{7, 8, 7},
			},
			want: tsData{
				ts:    []uint64{uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(time.Minute)},
				data:  []int64{7, 8, 7},
				start: uint64(time.Minute),
				end:   uint64(3 * time.Minute),
			},
		},
		{
			name: "empty slot in the middle",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(time.Minute)},
				data: []int64{7, 9},
			},
			want: tsData{
				ts:    []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data:  []int64{7, 0, 0, 9},
				start: uint64(time.Minute),
				end:   uint64(4 * time.Minute),
			},
		},
	}
	key := []byte("foo")
	fn := func(k []byte) time.Duration {
		assert.Equal(t, key, k)
		return 1 * time.Minute
	}
	encoderPool := NewIntEncoderPool("minute", 4, fn)
	decoderPool := NewIntDecoderPool("minute", 4, fn)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			at := assert.New(t)
			encoder := encoderPool.Get(key)
			defer encoderPool.Put(encoder)
			decoder := decoderPool.Get(key)
			defer decoderPool.Put(decoder)
			encoder.Reset(key)
			isFull := false
			for i, v := range tt.args.ts {
				encoder.Append(v, convert.Int64ToBytes(tt.args.data[i]))
				if encoder.IsFull() {
					isFull = true
					break
				}
			}
			bb, err := encoder.Encode()
			at.NoError(err)

			at.Equal(tt.want.start, encoder.StartTime())
			at.NoError(decoder.Decode(key, bb))
			start, end := decoder.Range()
			at.Equal(tt.want.start, start)
			at.Equal(tt.want.end, end)
			if isFull {
				at.True(decoder.IsFull())
			}
			for i, t := range tt.want.ts {
				v, err := decoder.Get(t)
				at.NoError(err)
				at.Equal(tt.want.data[i], convert.BytesToInt64(v))
			}
		})
	}
}
