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

	"github.com/google/go-cmp/cmp"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
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

func toTagProjection(b block) map[string][]string {
	result := make(map[string][]string, len(b.tagFamilies))
	for i := range b.tagFamilies {
		names := make([]string, len(b.tagFamilies[i].Columns))
		for i2 := range b.tagFamilies[i].Columns {
			names[i2] = b.tagFamilies[i].Columns[i2].Name
		}
		result[b.tagFamilies[i].Name] = names
	}
	return result
}

var conventionalBlock = block{
	timestamps: []int64{1, 2},
	tagFamilies: []ColumnFamily{
		{
			Name: "arrTag",
			Columns: []Column{
				{Name: "strArrTag", ValueType: pbv1.ValueTypeStrArr, Values: [][]byte{marshalStrArr([][]byte{[]byte("value1"), []byte("value2")}), marshalStrArr([][]byte{[]byte("value3"), []byte("value4")})}},
				{Name: "intArrTag", ValueType: pbv1.ValueTypeInt64Arr, Values: [][]byte{marshalIntArr([][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}), marshalIntArr([][]byte{convert.Int64ToBytes(50), convert.Int64ToBytes(60)})}},
			},
		},
		{
			Name: "binaryTag",
			Columns: []Column{
				{Name: "binaryTag", ValueType: pbv1.ValueTypeBinaryData, Values: [][]byte{[]byte(longText), []byte(longText)}},
			},
		},
		{
			Name: "singleTag",
			Columns: []Column{
				{Name: "strTag", ValueType: pbv1.ValueTypeStr, Values: [][]byte{[]byte("value1"), []byte("value2")}},
				{Name: "intTag", ValueType: pbv1.ValueTypeInt64, Values: [][]byte{convert.Int64ToBytes(10), convert.Int64ToBytes(20)}},
			},
		},
	},
	field: ColumnFamily{
		Columns: []Column{
			{Name: "strField", ValueType: pbv1.ValueTypeStr, Values: [][]byte{[]byte("field1"), []byte("field2")}},
			{Name: "intField", ValueType: pbv1.ValueTypeInt64, Values: [][]byte{convert.Int64ToBytes(1110), convert.Int64ToBytes(2220)}},
			{Name: "floatField", ValueType: pbv1.ValueTypeFloat64, Values: [][]byte{convert.Float64ToBytes(1221233.343), convert.Float64ToBytes(2442466.686)}},
			{Name: "binaryField", ValueType: pbv1.ValueTypeBinaryData, Values: [][]byte{[]byte(longText), []byte(longText)}},
		},
	},
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
				tagFamilies: [][]nameValues{
					{
						{
							"arrTag", []*nameValue{
								{name: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
								{name: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
							},
						},
						{
							"binaryTag", []*nameValue{
								{name: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: []byte(longText), valueArr: nil},
							},
						},
						{
							"singleTag", []*nameValue{
								{name: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value1"), valueArr: nil},
								{name: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(10), valueArr: nil},
							},
						},
					},
					{
						{
							"arrTag", []*nameValue{
								{name: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value3"), []byte("value4")}},
								{name: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(50), convert.Int64ToBytes(60)}},
							},
						},
						{
							"binaryTag", []*nameValue{
								{name: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: []byte(longText), valueArr: nil},
							},
						},
						{
							"singleTag", []*nameValue{
								{name: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value2"), valueArr: nil},
								{name: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(20), valueArr: nil},
							},
						},
					},
				},
				fields: []nameValues{
					{
						"skipped", []*nameValue{
							{name: "strField", valueType: pbv1.ValueTypeStr, value: []byte("field1"), valueArr: nil},
							{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(1110), valueArr: nil},
							{name: "floatField", valueType: pbv1.ValueTypeFloat64, value: convert.Float64ToBytes(1221233.343), valueArr: nil},
							{name: "binaryField", valueType: pbv1.ValueTypeBinaryData, value: []byte(longText), valueArr: nil},
						},
					},
					{
						"skipped", []*nameValue{
							{name: "strField", valueType: pbv1.ValueTypeStr, value: []byte("field2"), valueArr: nil},
							{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(2220), valueArr: nil},
							{name: "floatField", valueType: pbv1.ValueTypeFloat64, value: convert.Float64ToBytes(2442466.686), valueArr: nil},
							{name: "binaryField", valueType: pbv1.ValueTypeBinaryData, value: []byte(longText), valueArr: nil},
						},
					},
				},
			},
			want: conventionalBlock,
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

func Test_marshalAndUnmarshalTagFamily(t *testing.T) {
	metaBuffer, dataBuffer := &bytes.Buffer{}, &bytes.Buffer{}
	ww := &writers{
		mustCreateTagFamilyWriters: func(name string) (fs.Writer, fs.Writer) {
			return metaBuffer, dataBuffer
		},
		tagFamilyMetadataWriters: make(map[string]*writer),
		tagFamilyWriters:         make(map[string]*writer),
	}
	b := &conventionalBlock
	tagProjection := toTagProjection(*b)
	tfIndex := 0
	name := "arrTag"
	decoder := &encoding.BytesBlockDecoder{}
	bm := &blockMetadata{}

	b.marshalTagFamily(b.tagFamilies[tfIndex], bm, ww)

	metaWriter, ok1 := ww.tagFamilyMetadataWriters[name]
	valueWriter, ok2 := ww.tagFamilyWriters[name]
	if !ok1 || !ok2 {
		t.Fatalf("Writers not correctly added to maps")
	}
	if metaWriter.w != metaBuffer || valueWriter.w != dataBuffer {
		t.Fatalf("Writers not correctly added to maps")
	}

	unmarshaled := generateBlock()
	defer releaseBlock(unmarshaled)
	// set the timestamps to the same length as the original block
	// the data size in a block depends on the timestamps length
	unmarshaled.timestamps = make([]int64, len(b.timestamps))
	unmarshaled.resizeTagFamilies(1)

	unmarshaled.unmarshalTagFamily(decoder, tfIndex, name, bm.getTagFamilyMetadata(name), tagProjection[name], metaBuffer, dataBuffer)

	if diff := cmp.Diff(unmarshaled.tagFamilies[0], b.tagFamilies[0]); diff != "" {
		t.Errorf("block.unmarshalTagFamily() (-got +want):\n%s", diff)
	}
}

func Test_marshalAndUnmarshalBlock(t *testing.T) {
	timestampBuffer, fieldBuffer := &bytes.Buffer{}, &bytes.Buffer{}
	ww := &writers{
		mustCreateTagFamilyWriters: func(name string) (fs.Writer, fs.Writer) {
			return &bytes.Buffer{}, &bytes.Buffer{}
		},
		tagFamilyMetadataWriters: make(map[string]*writer),
		tagFamilyWriters:         make(map[string]*writer),
		timestampsWriter:         writer{w: timestampBuffer},
		fieldValuesWriter:        writer{w: fieldBuffer},
	}
	p := &part{
		timestamps:  timestampBuffer,
		fieldValues: fieldBuffer,
	}
	b := &conventionalBlock
	tagProjection := toTagProjection(*b)
	decoder := &encoding.BytesBlockDecoder{}
	sid := common.SeriesID(1)
	bm := blockMetadata{}

	b.mustWriteTo(sid, &bm, ww)

	tagFamilyMetadataReaders := make(map[string]fs.Reader)
	tagFamilyReaders := make(map[string]fs.Reader)

	for k, w := range ww.tagFamilyMetadataWriters {
		tagFamilyMetadataReaders[k] = w.w.(*bytes.Buffer)
		tagFamilyReaders[k] = ww.tagFamilyWriters[k].w.(*bytes.Buffer)
	}
	p.tagFamilyMetadata = tagFamilyMetadataReaders
	p.tagFamilies = tagFamilyReaders

	unmarshaled := generateBlock()
	defer releaseBlock(unmarshaled)

	var tp []pbv1.TagProjection
	for family, names := range tagProjection {
		tp = append(tp, pbv1.TagProjection{
			Family: family,
			Names:  names,
		})
	}
	bm.tagProjection = tp
	unmarshaled.mustReadFrom(decoder, p, bm)
	// blockMetadata is using a map, so the order of tag families is not guaranteed
	unmarshaled.sortTagFamilies()

	if !reflect.DeepEqual(b, unmarshaled) {
		t.Errorf("block.mustReadFrom() = %+v, want %+v", unmarshaled, b)
	}
}

func Test_findRange(t *testing.T) {
	type args struct {
		timestamps []int64
		min        int64
		max        int64
	}
	tests := []struct {
		name      string
		args      args
		wantStart int
		wantEnd   int
		wantExist bool
	}{
		{
			name: "Test with empty timestamps",
			args: args{
				timestamps: []int64{},
				min:        1,
				max:        10,
			},
			wantStart: 0,
			wantEnd:   0,
			wantExist: false,
		},
		{
			name: "Test with single timestamp",
			args: args{
				timestamps: []int64{1},
				min:        1,
				max:        1,
			},
			wantStart: 0,
			wantEnd:   1,
			wantExist: true,
		},
		{
			name: "Test with range not in timestamps",
			args: args{
				timestamps: []int64{1, 2, 3, 4, 5},
				min:        6,
				max:        10,
			},
			wantStart: 0,
			wantEnd:   0,
			wantExist: false,
		},
		{
			name: "Test with range in timestamps",
			args: args{
				timestamps: []int64{1, 2, 3, 4, 5},
				min:        2,
				max:        4,
			},
			wantStart: 1,
			wantEnd:   4,
			wantExist: true,
		},
		{
			name: "Test with range as timestamps",
			args: args{
				timestamps: []int64{1, 2, 3, 4, 5},
				min:        1,
				max:        5,
			},
			wantStart: 0,
			wantEnd:   5,
			wantExist: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStart, gotEnd, gotExist := findRange(tt.args.timestamps, tt.args.min, tt.args.max)
			if gotStart != tt.wantStart {
				t.Errorf("findRange() gotStart = %v, want %v", gotStart, tt.wantStart)
			}
			if gotEnd != tt.wantEnd {
				t.Errorf("findRange() gotEnd = %v, want %v", gotEnd, tt.wantEnd)
			}
			if gotExist != tt.wantExist {
				t.Errorf("findRange() gotExist = %v, want %v", gotExist, tt.wantExist)
			}
		})
	}
}
