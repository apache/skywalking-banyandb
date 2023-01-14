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
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

func TestNewEncoderAndDecoder(t *testing.T) {
	type tsData struct {
		ts    []uint64
		data  []any
		start uint64
		end   uint64
	}
	tests := []struct {
		name string
		args tsData
		want tsData
	}{
		{
			name: "int golden path",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data: []any{7, 8, 7, 9},
			},
			want: tsData{
				ts:    []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data:  []any{7, 8, 7, 9},
				start: uint64(time.Minute),
				end:   uint64(4 * time.Minute),
			},
		},
		{
			name: "int more than the size",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute), uint64(1 * time.Minute)},
				data: []any{7, 8, 7, 9, 6},
			},
			want: tsData{
				ts:    []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data:  []any{7, 8, 7, 9},
				start: uint64(time.Minute),
				end:   uint64(4 * time.Minute),
			},
		},
		{
			name: "int less than the size",
			args: tsData{
				ts:   []uint64{uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(time.Minute)},
				data: []any{7, 8, 7},
			},
			want: tsData{
				ts:    []uint64{uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(time.Minute)},
				data:  []any{7, 8, 7},
				start: uint64(time.Minute),
				end:   uint64(3 * time.Minute),
			},
		},
		{
			name: "int empty slot in the middle",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(time.Minute)},
				data: []any{7, 9},
			},
			want: tsData{
				ts:    []uint64{uint64(4 * time.Minute), uint64(1 * time.Minute)},
				data:  []any{7, 9},
				start: uint64(time.Minute),
				end:   uint64(4 * time.Minute),
			},
		},
		{
			name: "float64 golden path",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data: []any{7.0, 8.0, 7.0, 9.0},
			},
			want: tsData{
				ts:    []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data:  []any{7.0, 8.0, 7.0, 9.0},
				start: uint64(time.Minute),
				end:   uint64(4 * time.Minute),
			},
		},
		{
			name: "float64 more than the size",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute), uint64(1 * time.Minute)},
				data: []any{0.7, 0.8, 0.7, 0.9, 0.6},
			},
			want: tsData{
				ts:    []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data:  []any{0.7, 0.8, 0.7, 0.9},
				start: uint64(time.Minute),
				end:   uint64(4 * time.Minute),
			},
		},
		{
			name: "float64 less than the size",
			args: tsData{
				ts:   []uint64{uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(time.Minute)},
				data: []any{1.7, 1.8, 1.7},
			},
			want: tsData{
				ts:    []uint64{uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(time.Minute)},
				data:  []any{1.7, 1.8, 1.7},
				start: uint64(time.Minute),
				end:   uint64(3 * time.Minute),
			},
		},
		{
			name: "float64 empty slot in the middle",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(time.Minute)},
				data: []any{0.700033, 0.988822},
			},
			want: tsData{
				ts:    []uint64{uint64(4 * time.Minute), uint64(1 * time.Minute)},
				data:  []any{0.700033, 0.988822},
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
	encoderPool := NewEncoderPool("minute", 4, fn)
	decoderPool := NewDecoderPool("minute", 4, fn)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			at := assert.New(t)
			var buffer bytes.Buffer
			encoder := encoderPool.Get(key, &buffer)
			defer encoderPool.Put(encoder)
			decoder := decoderPool.Get(key)
			defer decoderPool.Put(decoder)
			isFull := false
			for i, v := range tt.args.ts {
				encoder.Append(v, ToBytes(tt.args.data[i]))
				if encoder.IsFull() {
					isFull = true
					break
				}
			}
			err := encoder.Encode()
			at.NoError(err)

			at.Equal(tt.want.start, encoder.StartTime())
			at.NoError(decoder.Decode(key, buffer.Bytes()))
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
				at.Equal(tt.want.data[i], BytesTo(tt.want.data[i], iter.Val()))
				v, err := decoder.Get(iter.Time())
				at.NoError(err)
				at.Equal(tt.want.data[i], BytesTo(tt.want.data[i], v))
			}
			at.Equal(len(tt.want.ts), i)
		})
	}
}

func ToBytes(v any) []byte {
	switch d := v.(type) {
	case int:
		return convert.Int64ToBytes(int64(d))
	case float64:
		return convert.Float64ToBytes(d)
	}
	return nil
}

func BytesTo(t any, b []byte) any {
	switch t.(type) {
	case int:
		return int(convert.BytesToInt64(b))
	case float64:
		return convert.BytesToFloat64(b)
	}
	return nil
}

func TestNewDecoderGet(t *testing.T) {
	type tsData struct {
		ts   []uint64
		data []any
	}
	type wantData struct {
		ts      []uint64
		data    []any
		wantErr []bool
		start   uint64
		end     uint64
	}
	tests := []struct {
		name string
		args tsData
		want wantData
	}{
		{
			name: "int golden path",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data: []any{7, 8, 7, 9},
			},
			want: wantData{
				ts:    []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data:  []any{7, 8, 7, 9},
				start: uint64(time.Minute),
				end:   uint64(4 * time.Minute),
			},
		},
		{
			name: "int more than the size",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute), uint64(1 * time.Minute)},
				data: []any{7, 8, 7, 9, 6},
			},
			want: wantData{
				ts:      []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute), 0},
				data:    []any{7, 8, 7, 9, nil},
				wantErr: []bool{false, false, false, false, true},
				start:   uint64(time.Minute),
				end:     uint64(4 * time.Minute),
			},
		},
		{
			name: "int less than the size",
			args: tsData{
				ts:   []uint64{uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(time.Minute)},
				data: []any{7, 8, 7},
			},
			want: wantData{
				ts:    []uint64{uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(time.Minute)},
				data:  []any{7, 8, 7},
				start: uint64(time.Minute),
				end:   uint64(3 * time.Minute),
			},
		},
		{
			name: "int empty slot in the middle",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(time.Minute)},
				data: []any{7, 9},
			},
			want: wantData{
				ts:      []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data:    []any{7, nil, nil, 9},
				wantErr: []bool{false, true, true, false},
				start:   uint64(time.Minute),
				end:     uint64(4 * time.Minute),
			},
		},
		{
			name: "float golden path",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data: []any{7.0, 8.0, 7.0, 9.0},
			},
			want: wantData{
				ts:    []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data:  []any{7.0, 8.0, 7.0, 9.0},
				start: uint64(time.Minute),
				end:   uint64(4 * time.Minute),
			},
		},
		{
			name: "float more than the size",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute), uint64(1 * time.Minute)},
				data: []any{1.7, 1.8, 1.7, 1.9, 1.6},
			},
			want: wantData{
				ts:      []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute), 0},
				data:    []any{1.7, 1.8, 1.7, 1.9, nil},
				wantErr: []bool{false, false, false, false, true},
				start:   uint64(time.Minute),
				end:     uint64(4 * time.Minute),
			},
		},
		{
			name: "float less than the size",
			args: tsData{
				ts:   []uint64{uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(time.Minute)},
				data: []any{0.71, 0.833, 0.709},
			},
			want: wantData{
				ts:    []uint64{uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(time.Minute)},
				data:  []any{0.71, 0.833, 0.709},
				start: uint64(time.Minute),
				end:   uint64(3 * time.Minute),
			},
		},
		{
			name: "float empty slot in the middle",
			args: tsData{
				ts:   []uint64{uint64(4 * time.Minute), uint64(time.Minute)},
				data: []any{1.7, 1.9},
			},
			want: wantData{
				ts:      []uint64{uint64(4 * time.Minute), uint64(3 * time.Minute), uint64(2 * time.Minute), uint64(1 * time.Minute)},
				data:    []any{1.7, nil, nil, 1.9},
				wantErr: []bool{false, true, true, false},
				start:   uint64(time.Minute),
				end:     uint64(4 * time.Minute),
			},
		},
	}
	key := []byte("foo")
	fn := func(k []byte) time.Duration {
		assert.Equal(t, key, k)
		return 1 * time.Minute
	}
	encoderPool := NewEncoderPool("minute", 4, fn)
	decoderPool := NewDecoderPool("minute", 4, fn)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			at := assert.New(t)
			var buffer bytes.Buffer
			encoder := encoderPool.Get(key, &buffer)
			defer encoderPool.Put(encoder)
			decoder := decoderPool.Get(key)
			defer decoderPool.Put(decoder)
			isFull := false
			for i, v := range tt.args.ts {
				encoder.Append(v, ToBytes(tt.args.data[i]))
				if encoder.IsFull() {
					isFull = true
					break
				}
			}
			err := encoder.Encode()
			at.NoError(err)

			at.Equal(tt.want.start, encoder.StartTime())
			at.NoError(decoder.Decode(key, buffer.Bytes()))
			start, end := decoder.Range()
			at.Equal(tt.want.start, start)
			at.Equal(tt.want.end, end)
			if isFull {
				at.True(decoder.IsFull())
			}
			for i, t := range tt.want.ts {
				wantErr := false
				if tt.want.wantErr != nil {
					wantErr = tt.want.wantErr[i]
				}
				v, err := decoder.Get(t)
				if wantErr {
					at.ErrorIs(err, errNoData)
				} else {
					at.NoError(err)
					at.Equal(tt.want.data[i], BytesTo(tt.want.data[i], v))
				}
			}
		})
	}
}
