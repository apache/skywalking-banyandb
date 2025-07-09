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

package stream

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

func Test_block_reset(t *testing.T) {
	type fields struct {
		timestamps  []int64
		elementIDs  []uint64
		tagFamilies []tagFamily
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
				elementIDs:  []uint64{0, 1, 2},
				tagFamilies: []tagFamily{{}, {}, {}},
			},
			want: block{
				timestamps:  []int64{},
				elementIDs:  []uint64{},
				tagFamilies: []tagFamily{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &block{
				timestamps:  tt.fields.timestamps,
				elementIDs:  tt.fields.elementIDs,
				tagFamilies: tt.fields.tagFamilies,
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
		names := make([]string, len(b.tagFamilies[i].tags))
		for i2 := range b.tagFamilies[i].tags {
			names[i2] = b.tagFamilies[i].tags[i2].name
		}
		result[b.tagFamilies[i].name] = names
	}
	return result
}

var conventionalBlock = block{
	timestamps: []int64{1, 2},
	elementIDs: []uint64{0, 1},
	tagFamilies: []tagFamily{
		{
			name: "arrTag",
			tags: []tag{
				{
					name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
					values: [][]byte{marshalStrArr([][]byte{[]byte("value1"), []byte("value2")}), marshalStrArr([][]byte{[]byte("value3"), []byte("value4")})},
				},
				{
					name: "intArrTag", valueType: pbv1.ValueTypeInt64Arr,
					values: [][]byte{
						marshalIntArr([][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}),
						marshalIntArr([][]byte{convert.Int64ToBytes(50), convert.Int64ToBytes(60)}),
					},
				},
			},
		},
		{
			name: "binaryTag",
			tags: []tag{
				{name: "binaryTag", valueType: pbv1.ValueTypeBinaryData, values: [][]byte{longText, longText}},
			},
		},
		{
			name: "singleTag",
			tags: []tag{
				{name: "strTag", valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte("value1"), []byte("value2")}},
				{name: "intTag", valueType: pbv1.ValueTypeInt64, values: [][]byte{convert.Int64ToBytes(10), convert.Int64ToBytes(20)}},
				{name: "floatTag", valueType: pbv1.ValueTypeFloat64, values: [][]byte{convert.Float64ToBytes(0.1), convert.Float64ToBytes(0.2)}},
			},
		},
	},
}

func Test_block_mustInitFromElements(t *testing.T) {
	type args struct {
		timestamps  []int64
		elementIDs  []uint64
		tagFamilies [][]tagValues
	}
	tests := []struct {
		name string
		args args
		want block
	}{
		{
			name: "Test mustInitFromElements",
			args: args{
				timestamps: []int64{1, 2},
				elementIDs: []uint64{0, 1},
				tagFamilies: [][]tagValues{
					{
						{
							"arrTag", []*tagValue{
								{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
								{tag: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
							},
						},
						{
							"binaryTag", []*tagValue{
								{tag: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
							},
						},
						{
							"singleTag", []*tagValue{
								{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value1"), valueArr: nil},
								{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(10), valueArr: nil},
								{tag: "floatTag", valueType: pbv1.ValueTypeFloat64, value: convert.Float64ToBytes(0.1), valueArr: nil},
							},
						},
					},
					{
						{
							"arrTag", []*tagValue{
								{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value3"), []byte("value4")}},
								{tag: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(50), convert.Int64ToBytes(60)}},
							},
						},
						{
							"binaryTag", []*tagValue{
								{tag: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
							},
						},
						{
							"singleTag", []*tagValue{
								{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value2"), valueArr: nil},
								{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(20), valueArr: nil},
								{tag: "floatTag", valueType: pbv1.ValueTypeFloat64, value: convert.Float64ToBytes(0.2), valueArr: nil},
							},
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
			b.mustInitFromElements(tt.args.timestamps, tt.args.elementIDs, tt.args.tagFamilies)
			if !reflect.DeepEqual(*b, tt.want) {
				t.Errorf("block.mustInitFromElements() = %+v, want %+v", *b, tt.want)
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
	nv := &tagValue{valueType: pbv1.ValueTypeStrArr, valueArr: arr}
	return nv.marshal()
}

func marshalIntArr(arr [][]byte) []byte {
	nv := &tagValue{valueType: pbv1.ValueTypeInt64Arr, valueArr: arr}
	return nv.marshal()
}

func Test_mustWriteAndReadTimestamps(t *testing.T) {
	tests := []struct {
		name       string
		timestamps []int64
		elementIDs []uint64
		wantPanic  bool
		wantTM     timestampsMetadata
	}{
		{
			name:       "Test mustWriteAndReadTimestamps",
			timestamps: []int64{1, 2, 3, 4, 5},
			elementIDs: []uint64{0, 1, 2, 3, 4},
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
			w := new(writer)
			w.init(b)
			mustWriteTimestampsTo(tm, tt.timestamps, tt.elementIDs, w)
			timestamps, elementIDs := mustReadTimestampsFrom(nil, nil, tm, len(tt.timestamps), b)
			if !reflect.DeepEqual(timestamps, tt.timestamps) {
				t.Errorf("mustReadTimestampsFrom() timestamps = %v, want %v", timestamps, tt.timestamps)
			}
			if !reflect.DeepEqual(elementIDs, tt.elementIDs) {
				t.Errorf("mustReadTimestampsFrom() elementIDs = %v, want %v", elementIDs, tt.elementIDs)
			}
		})
	}
}

func Test_marshalAndUnmarshalTagFamily(t *testing.T) {
	metaBuffer, dataBuffer, filterBuffer := &bytes.Buffer{}, &bytes.Buffer{}, &bytes.Buffer{}
	ww := &writers{
		mustCreateTagFamilyWriters: func(_ string) (fs.Writer, fs.Writer, fs.Writer) {
			return metaBuffer, dataBuffer, filterBuffer
		},
		tagFamilyMetadataWriters: make(map[string]*writer),
		tagFamilyWriters:         make(map[string]*writer),
		tagFamilyFilterWriters:   make(map[string]*writer),
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

	unmarshaled.unmarshalTagFamily(decoder, tfIndex, name, bm.getTagFamilyMetadata(name), tagProjection[name], metaBuffer, dataBuffer, 1)

	if diff := cmp.Diff(unmarshaled.tagFamilies[0], b.tagFamilies[0],
		cmp.AllowUnexported(tagFamily{}, tag{}, tagFilter{}),
	); diff != "" {
		t.Errorf("block.unmarshalTagFamily() (-got +want):\n%s", diff)
	}

	unmarshaled2 := generateBlock()
	defer releaseBlock(unmarshaled2)
	unmarshaled2.timestamps = make([]int64, len(b.timestamps))
	unmarshaled2.resizeTagFamilies(1)

	metaReader := generateSeqReader()
	defer releaseSeqReader(metaReader)
	metaReader.init(metaBuffer)
	valueReader := generateSeqReader()
	defer releaseSeqReader(valueReader)
	valueReader.init(dataBuffer)

	unmarshaled2.unmarshalTagFamilyFromSeqReaders(decoder, tfIndex, name, bm.getTagFamilyMetadata(name), metaReader, valueReader)

	if diff := cmp.Diff(unmarshaled2.tagFamilies[0], b.tagFamilies[0],
		cmp.AllowUnexported(tagFamily{}, tag{}, tagFilter{}),
	); diff != "" {
		t.Errorf("block.unmarshalTagFamilyFromSeqReaders() (-got +want):\n%s", diff)
	}
}

func Test_marshalAndUnmarshalBlock(t *testing.T) {
	timestampBuffer := &bytes.Buffer{}
	timestampWriter := &writer{}
	timestampWriter.init(timestampBuffer)
	ww := &writers{
		mustCreateTagFamilyWriters: func(_ string) (fs.Writer, fs.Writer, fs.Writer) {
			return &bytes.Buffer{}, &bytes.Buffer{}, &bytes.Buffer{}
		},
		tagFamilyMetadataWriters: make(map[string]*writer),
		tagFamilyWriters:         make(map[string]*writer),
		tagFamilyFilterWriters:   make(map[string]*writer),
		timestampsWriter:         *timestampWriter,
	}
	p := &part{
		primary:    &bytes.Buffer{},
		timestamps: timestampBuffer,
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

	var tp []model.TagProjection
	for family, names := range tagProjection {
		tp = append(tp, model.TagProjection{
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

	unmarshaled2 := generateBlock()
	defer releaseBlock(unmarshaled2)
	var sr seqReaders
	sr.init(p)
	defer sr.reset()

	unmarshaled2.mustSeqReadFrom(decoder, &sr, bm)
	if !reflect.DeepEqual(b, unmarshaled2) {
		t.Errorf("block.mustSeqReadFrom() = %+v, want %+v", unmarshaled, b)
	}
}

func Test_blockPointer_append(t *testing.T) {
	type fields struct {
		timestamps  []int64
		elementIDs  []uint64
		tagFamilies []tagFamily
	}
	type args struct {
		b      *blockPointer
		offset int
	}
	tests := []struct {
		want      *blockPointer
		name      string
		args      args
		fields    fields
		wantPanic bool
	}{
		{
			name: "Test append with empty block",
			fields: fields{
				timestamps: []int64{1, 2},
				elementIDs: []uint64{0, 1},
				tagFamilies: []tagFamily{
					{
						name: "arrTag",
						tags: []tag{
							{
								name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
								values: [][]byte{marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}), marshalStrArr([][]byte{[]byte("value7"), []byte("value8")})},
							},
						},
					},
				},
			},
			args: args{
				b: &blockPointer{
					block: block{
						timestamps:  []int64{},
						elementIDs:  []uint64{},
						tagFamilies: []tagFamily{},
					},
					idx: 0,
				},
				offset: 0,
			},
			want: &blockPointer{
				block: block{
					timestamps: []int64{1, 2},
					elementIDs: []uint64{0, 1},
					tagFamilies: []tagFamily{
						{
							name: "arrTag",
							tags: []tag{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}), marshalStrArr([][]byte{[]byte("value7"), []byte("value8")})},
								},
							},
						},
					},
				},
				idx: 0,
			},
		},
		{
			name: "Test append to a empty block",
			fields: fields{
				timestamps:  nil,
				elementIDs:  nil,
				tagFamilies: nil,
			},
			args: args{
				b: &blockPointer{
					block: block{
						timestamps: []int64{4, 5},
						elementIDs: []uint64{3, 4},
						tagFamilies: []tagFamily{
							{
								name: "arrTag",
								tags: []tag{
									{
										name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
										values: [][]byte{marshalStrArr([][]byte{[]byte("value9"), []byte("value10")}), marshalStrArr([][]byte{[]byte("value11"), []byte("value12")})},
									},
								},
							},
						},
					},
					idx: 0,
				},
				offset: 2,
			},
			want: &blockPointer{
				block: block{
					timestamps: []int64{4, 5},
					elementIDs: []uint64{3, 4},
					tagFamilies: []tagFamily{
						{
							name: "arrTag",
							tags: []tag{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{marshalStrArr([][]byte{[]byte("value9"), []byte("value10")}), marshalStrArr([][]byte{[]byte("value11"), []byte("value12")})},
								},
							},
						},
					},
				},
				idx: 0,
			},
		},
		{
			name: "Test append with offset equals to the data size. All data",
			fields: fields{
				timestamps: []int64{1, 2},
				elementIDs: []uint64{0, 1},
				tagFamilies: []tagFamily{
					{
						name: "arrTag",
						tags: []tag{
							{
								name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
								values: [][]byte{marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}), marshalStrArr([][]byte{[]byte("value7"), []byte("value8")})},
							},
						},
					},
				},
			},
			args: args{
				b: &blockPointer{
					block: block{
						timestamps: []int64{4, 5},
						elementIDs: []uint64{3, 4},
						tagFamilies: []tagFamily{
							{
								name: "arrTag",
								tags: []tag{
									{
										name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
										values: [][]byte{marshalStrArr([][]byte{[]byte("value9"), []byte("value10")}), marshalStrArr([][]byte{[]byte("value11"), []byte("value12")})},
									},
								},
							},
						},
					},
					idx: 0,
				},
				offset: 2,
			},
			want: &blockPointer{
				block: block{
					timestamps: []int64{1, 2, 4, 5},
					elementIDs: []uint64{0, 1, 3, 4},
					tagFamilies: []tagFamily{
						{
							name: "arrTag",
							tags: []tag{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{
										marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}),
										marshalStrArr([][]byte{[]byte("value7"), []byte("value8")}), marshalStrArr([][]byte{[]byte("value9"), []byte("value10")}),
										marshalStrArr([][]byte{[]byte("value11"), []byte("value12")}),
									},
								},
							},
						},
					},
				},
				idx: 0,
			},
		},
		{
			name: "Test append with non-empty block and offset less than timestamps",
			fields: fields{
				timestamps: []int64{1, 2},
				elementIDs: []uint64{0, 1},
				tagFamilies: []tagFamily{
					{
						name: "arrTag",
						tags: []tag{
							{
								name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
								values: [][]byte{marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}), marshalStrArr([][]byte{[]byte("value7"), []byte("value8")})},
							},
						},
					},
				},
			},
			args: args{
				b: &blockPointer{
					block: block{
						timestamps: []int64{4, 5},
						elementIDs: []uint64{3, 4},
						tagFamilies: []tagFamily{
							{
								name: "arrTag",
								tags: []tag{
									{
										name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
										values: [][]byte{marshalStrArr([][]byte{[]byte("value9"), []byte("value10")}), marshalStrArr([][]byte{[]byte("value11"), []byte("value12")})},
									},
								},
							},
						},
					},
					idx: 0,
				},
				offset: 1,
			},
			want: &blockPointer{
				block: block{
					timestamps: []int64{1, 2, 4},
					elementIDs: []uint64{0, 1, 3},
					tagFamilies: []tagFamily{
						{
							name: "arrTag",
							tags: []tag{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}), marshalStrArr([][]byte{
										[]byte("value7"),
										[]byte("value8"),
									}), marshalStrArr([][]byte{[]byte("value9"), []byte("value10")})},
								},
							},
						},
					},
				},
				idx: 0,
			},
		},
		{
			name: "Test append with missing tag family",
			fields: fields{
				timestamps: []int64{1, 2},
				elementIDs: []uint64{0, 1},
				tagFamilies: []tagFamily{
					{
						name: "arrTag",
						tags: []tag{
							{
								name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
								values: [][]byte{marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}), marshalStrArr([][]byte{[]byte("value7"), []byte("value8")})},
							},
						},
					},
				},
			},
			args: args{
				b: &blockPointer{
					block: block{
						timestamps:  []int64{4, 5},
						elementIDs:  []uint64{2, 3},
						tagFamilies: []tagFamily{},
					},
					idx: 0,
				},
				offset: 2,
			},
			want: &blockPointer{
				block: block{
					timestamps: []int64{1, 2, 4, 5},
					elementIDs: []uint64{0, 1, 2, 3},
					tagFamilies: []tagFamily{
						{
							name: "arrTag",
							tags: []tag{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{
										marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}),
										marshalStrArr([][]byte{[]byte("value7"), []byte("value8")}),
										nil, nil,
									},
								},
							},
						},
					},
				},
				idx: 0,
			},
		},
		{
			name: "Test append with additional tag family",
			fields: fields{
				timestamps:  []int64{1, 2},
				elementIDs:  []uint64{0, 1},
				tagFamilies: []tagFamily{},
			},
			args: args{
				b: &blockPointer{
					block: block{
						timestamps: []int64{4, 5},
						elementIDs: []uint64{2, 3},
						tagFamilies: []tagFamily{
							{
								name: "arrTag",
								tags: []tag{
									{
										name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
										values: [][]byte{marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}), marshalStrArr([][]byte{[]byte("value7"), []byte("value8")})},
									},
								},
							},
						},
					},
					idx: 0,
				},
				offset: 2,
			},
			want: &blockPointer{
				block: block{
					timestamps: []int64{1, 2, 4, 5},
					elementIDs: []uint64{0, 1, 2, 3},
					tagFamilies: []tagFamily{
						{
							name: "arrTag",
							tags: []tag{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{
										nil, nil,
										marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}),
										marshalStrArr([][]byte{[]byte("value7"), []byte("value8")}),
									},
								},
							},
						},
					},
				},
				idx: 0,
			},
		},
		{
			name: "Test append with missing tag",
			fields: fields{
				timestamps: []int64{1, 2},
				elementIDs: []uint64{0, 1},
				tagFamilies: []tagFamily{
					{
						name: "arrTag",
						tags: []tag{
							{
								name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
								values: [][]byte{marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}), marshalStrArr([][]byte{[]byte("value7"), []byte("value8")})},
							},
						},
					},
				},
			},
			args: args{
				b: &blockPointer{
					block: block{
						timestamps: []int64{4, 5},
						elementIDs: []uint64{2, 3},
						tagFamilies: []tagFamily{
							{
								name: "arrTag",
								tags: []tag{},
							},
						},
					},
					idx: 0,
				},
				offset: 2,
			},
			want: &blockPointer{
				block: block{
					timestamps: []int64{1, 2, 4, 5},
					elementIDs: []uint64{0, 1, 2, 3},
					tagFamilies: []tagFamily{
						{
							name: "arrTag",
							tags: []tag{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{
										marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}),
										marshalStrArr([][]byte{[]byte("value7"), []byte("value8")}),
										nil, nil,
									},
								},
							},
						},
					},
				},
				idx: 0,
			},
		},
		{
			name: "Test append with additional tag",
			fields: fields{
				timestamps: []int64{1, 2},
				elementIDs: []uint64{0, 1},
				tagFamilies: []tagFamily{
					{
						name: "arrTag",
						tags: []tag{},
					},
				},
			},
			args: args{
				b: &blockPointer{
					block: block{
						timestamps: []int64{4, 5},
						elementIDs: []uint64{2, 3},
						tagFamilies: []tagFamily{
							{
								name: "arrTag",
								tags: []tag{
									{
										name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
										values: [][]byte{marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}), marshalStrArr([][]byte{[]byte("value7"), []byte("value8")})},
									},
								},
							},
						},
					},
					idx: 0,
				},
				offset: 2,
			},
			want: &blockPointer{
				block: block{
					timestamps: []int64{1, 2, 4, 5},
					elementIDs: []uint64{0, 1, 2, 3},
					tagFamilies: []tagFamily{
						{
							name: "arrTag",
							tags: []tag{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{
										nil, nil,
										marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}),
										marshalStrArr([][]byte{[]byte("value7"), []byte("value8")}),
									},
								},
							},
						},
					},
				},
				idx: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if (r != nil) != tt.wantPanic {
					t.Errorf("blockPointer.append() recover = %v, wantPanic = %v", r, tt.wantPanic)
				}
			}()
			bi := &blockPointer{
				block: block{
					timestamps:  tt.fields.timestamps,
					tagFamilies: tt.fields.tagFamilies,
					elementIDs:  tt.fields.elementIDs,
				},
			}
			bi.append(tt.args.b, tt.args.offset)
			if !reflect.DeepEqual(bi, tt.want) {
				t.Errorf("blockPointer.append() = %+v, want %+v", bi, tt.want)
			}
		})
	}
}

func Test_blockPointer_copyFrom(t *testing.T) {
	type fields struct {
		bm  blockMetadata
		idx int
	}
	type args struct {
		src *blockPointer
	}
	tests := []struct {
		args   args
		want   *blockPointer
		name   string
		fields fields
	}{
		{
			name: "Test copyFrom",
			fields: fields{
				bm:  blockMetadata{},
				idx: 0,
			},
			args: args{
				src: &blockPointer{
					bm:    blockMetadata{count: 1},
					idx:   0,
					block: conventionalBlock,
				},
			},
			want: &blockPointer{
				bm:    blockMetadata{count: 1},
				idx:   0,
				block: conventionalBlock,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bi := &blockPointer{
				bm:  tt.fields.bm,
				idx: tt.fields.idx,
			}
			bi.copyFrom(tt.args.src)
			if !reflect.DeepEqual(bi, tt.want) {
				t.Errorf("blockPointer.copyFrom() = %+v, want %+v", bi, tt.want)
			}
		})
	}
}
