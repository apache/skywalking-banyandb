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
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func Test_block_reset(t *testing.T) {
	type fields struct {
		timestamps  []int64
		tagFamilies []ColumnFamily
		field       ColumnFamily
	}
	tests := []struct {
		name   string
		fields fields
		want   block
	}{
		{
			name: "Test reset",
			fields: fields{
				timestamps:  []int64{1, 2, 3},
				tagFamilies: []ColumnFamily{{}, {}, {}},
				field:       ColumnFamily{Columns: []Column{{}, {}}},
			},
			want: block{
				timestamps:  []int64{},
				tagFamilies: []ColumnFamily{},
				field:       ColumnFamily{Columns: []Column{}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &block{
				timestamps:  tt.fields.timestamps,
				tagFamilies: tt.fields.tagFamilies,
				field:       tt.fields.field,
			}
			b.reset()
			if !reflect.DeepEqual(*b, tt.want) {
				t.Errorf("block.reset() = %+v, want %+v", *b, tt.want)
			}
		})
	}
}

func Test_block_mustInitFromDataPoints(t *testing.T) {
	type args struct {
		timestamps  []int64
		tagFamilies [][]nameValues
		fields      []nameValues
	}
	tests := []struct {
		name string
		args args
		want block
	}{
		{
			name: "Test mustInitFromDataPoints",
			args: args{
				timestamps: []int64{1, 2},
				tagFamilies: [][]nameValues{{
					{
						"singleTag", []*nameValue{
							{"strTag", pbv1.ValueTypeStr, []byte("value1"), nil},
							{"intTag", pbv1.ValueTypeInt64, convert.Int64ToBytes(10), nil},
							{"floatTag", pbv1.ValueTypeFloat64, convert.Float64ToBytes(12233.343), nil},
						},
					}, {
						"arrTag", []*nameValue{
							{"strArrTag", pbv1.ValueTypeStrArr, nil, [][]byte{[]byte("value1"), []byte("value2")}},
							{"intArrTag", pbv1.ValueTypeInt64Arr, nil, [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
						},
					}, {
						"binaryTag", []*nameValue{
							{"binaryTag", pbv1.ValueTypeBinaryData, []byte(longText), nil},
						},
					},
				}, {
					{
						"singleTag", []*nameValue{
							{"strTag", pbv1.ValueTypeStr, []byte("value2"), nil},
							{"intTag", pbv1.ValueTypeInt64, convert.Int64ToBytes(20), nil},
							{"floatTag", pbv1.ValueTypeFloat64, convert.Float64ToBytes(24466.686), nil},
						},
					}, {
						"arrTag", []*nameValue{
							{"strArrTag", pbv1.ValueTypeStrArr, nil, [][]byte{[]byte("value3"), []byte("value4")}},
							{"intArrTag", pbv1.ValueTypeInt64Arr, nil, [][]byte{convert.Int64ToBytes(50), convert.Int64ToBytes(60)}},
						},
					}, {
						"binaryTag", []*nameValue{
							{"binaryTag", pbv1.ValueTypeBinaryData, []byte(longText), nil},
						},
					},
				}},
				fields: []nameValues{{
					"skipped", []*nameValue{
						{"strField", pbv1.ValueTypeStr, []byte("field1"), nil},
						{"intField", pbv1.ValueTypeInt64, convert.Int64ToBytes(1110), nil},
						{"floatField", pbv1.ValueTypeFloat64, convert.Float64ToBytes(1221233.343), nil},
						{"binaryField", pbv1.ValueTypeBinaryData, []byte(longText), nil},
					},
				}, {
					"skipped", []*nameValue{
						{"strField", pbv1.ValueTypeStr, []byte("field2"), nil},
						{"intField", pbv1.ValueTypeInt64, convert.Int64ToBytes(2220), nil},
						{"floatField", pbv1.ValueTypeFloat64, convert.Float64ToBytes(2442466.686), nil},
						{"binaryField", pbv1.ValueTypeBinaryData, []byte(longText), nil},
					},
				}},
			},
			want: block{
				timestamps: []int64{1, 2},
				tagFamilies: []ColumnFamily{
					{"singleTag", []Column{
						{"strTag", pbv1.ValueTypeStr, [][]byte{[]byte("value1"), []byte("value2")}},
						{"intTag", pbv1.ValueTypeInt64, [][]byte{convert.Int64ToBytes(10), convert.Int64ToBytes(20)}},
						{"floatTag", pbv1.ValueTypeFloat64, [][]byte{convert.Float64ToBytes(12233.343), convert.Float64ToBytes(24466.686)}},
					}},
					{"arrTag", []Column{
						{"strArrTag", pbv1.ValueTypeStrArr, [][]byte{marshalStrArr([][]byte{[]byte("value1"), []byte("value2")}), marshalStrArr([][]byte{[]byte("value3"), []byte("value4")})}},
						{"intArrTag", pbv1.ValueTypeInt64Arr, [][]byte{marshalIntArr([][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}), marshalIntArr([][]byte{convert.Int64ToBytes(50), convert.Int64ToBytes(60)})}},
					}},
					{"binaryTag", []Column{
						{Name: "binaryTag", ValueType: pbv1.ValueTypeBinaryData, Values: [][]byte{[]byte(longText), []byte(longText)}},
					}},
				},
				field: ColumnFamily{Columns: []Column{
					{"strField", pbv1.ValueTypeStr, [][]byte{[]byte("field1"), []byte("field2")}},
					{"intField", pbv1.ValueTypeInt64, [][]byte{convert.Int64ToBytes(1110), convert.Int64ToBytes(2220)}},
					{"floatField", pbv1.ValueTypeFloat64, [][]byte{convert.Float64ToBytes(1221233.343), convert.Float64ToBytes(2442466.686)}},
					{Name: "binaryField", ValueType: pbv1.ValueTypeBinaryData, Values: [][]byte{[]byte(longText), []byte(longText)}},
				}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &block{}
			b.mustInitFromDataPoints(tt.args.timestamps, tt.args.tagFamilies, tt.args.fields)
			if !reflect.DeepEqual(*b, tt.want) {
				t.Errorf("block.mustInitFromDataPoints() = %+v, want %+v", *b, tt.want)
			}
		})
	}
}

var longText = []byte(`
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a diam lectus. Sed sit amet ipsum mauris.
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a diam lectus. Sed sit amet ipsum mauris.
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a diam lectus. Sed sit amet ipsum mauris.
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a diam lectus. Sed sit amet ipsum mauris.
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a diam lectus. Sed sit amet ipsum mauris.
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a diam lectus. Sed sit amet ipsum mauris.
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a diam lectus. Sed sit amet ipsum mauris.
`)

func marshalStrArr(arr [][]byte) []byte {
	nv := &nameValue{valueType: pbv1.ValueTypeStrArr, valueArr: arr}
	return nv.marshal()
}

func marshalIntArr(arr [][]byte) []byte {
	nv := &nameValue{valueType: pbv1.ValueTypeInt64Arr, valueArr: arr}
	return nv.marshal()
}

func Test_mustWriteAndReadTimestamps(t *testing.T) {
	tests := []struct {
		name      string
		args      []int64
		wantPanic bool
		wantTM    timestampsMetadata
	}{
		{
			name:      "Test mustWriteAndReadTimestamps",
			args:      []int64{1, 2, 3, 4, 5},
			wantPanic: false,
		},
		{
			name:      "Test mustWriteAndReadTimestamps with panic",
			args:      getBitInt64Arr(),
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if (r != nil) != tt.wantPanic {
					t.Errorf("mustWriteTimestampsTo() recover = %v, wantPanic = %v", r, tt.wantPanic)
				}
			}()
			tm := &timestampsMetadata{}
			b := &bytes.Buffer{}
			mustWriteTimestampsTo(tm, tt.args, writer{w: b})
			timestamps := mustReadTimestampsFrom(nil, tm, len(tt.args), b)
			if !reflect.DeepEqual(timestamps, tt.args) {
				t.Errorf("mustReadTimestampsFrom() = %v, want %v", timestamps, tt.args)
			}
		})
	}
}

func getBitInt64Arr() []int64 {
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	size := maxTimestampsBlockSize + 1
	randSlice := make([]int64, size)
	for i := range randSlice {
		randSlice[i] = r.Int63()
	}
	return randSlice
}
