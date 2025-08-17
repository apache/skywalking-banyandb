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
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

func Test_block_reset(t *testing.T) {
	type fields struct {
		spans [][]byte
		tags  []tag
		minTS int64
		maxTS int64
	}
	tests := []struct {
		name   string
		fields fields
		want   block
	}{
		{
			name: "Test reset",
			fields: fields{
				spans: [][]byte{[]byte("span1"), []byte("span2"), []byte("span3")},
				tags:  []tag{{}, {}, {}},
				minTS: 1,
				maxTS: 3,
			},
			want: block{
				spans: [][]byte{},
				tags:  []tag{},
				minTS: 0,
				maxTS: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &block{
				spans: tt.fields.spans,
				tags:  tt.fields.tags,
				minTS: tt.fields.minTS,
				maxTS: tt.fields.maxTS,
			}
			b.reset()
			if !reflect.DeepEqual(*b, tt.want) {
				t.Errorf("block.reset() = %+v, want %+v", *b, tt.want)
			}
		})
	}
}

func toTagProjection(b block) []string {
	result := make([]string, len(b.tags))
	for i := range b.tags {
		result[i] = b.tags[i].name
	}
	return result
}

var conventionalBlockWithTS = block{
	spans: [][]byte{[]byte("span1"), []byte("span2")},
	tags: []tag{
		{
			name: "binaryTag", valueType: pbv1.ValueTypeBinaryData,
			values: [][]byte{longText, longText},
		},
		{
			name: "floatTag", valueType: pbv1.ValueTypeFloat64, values: [][]byte{convert.Float64ToBytes(0.1), convert.Float64ToBytes(0.2)},
		},
		{
			name: "intArrTag", valueType: pbv1.ValueTypeInt64Arr,
			values: [][]byte{
				marshalIntArr([][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}),
				marshalIntArr([][]byte{convert.Int64ToBytes(50), convert.Int64ToBytes(60)}),
			},
		},
		{
			name: "intTag", valueType: pbv1.ValueTypeInt64, values: [][]byte{convert.Int64ToBytes(10), convert.Int64ToBytes(20)},
		},
		{
			name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
			values: [][]byte{marshalStrArr([][]byte{[]byte("value1"), []byte("value2")}), marshalStrArr([][]byte{[]byte("value3"), []byte("value4")})},
		},
		{
			name: "strTag", valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte("value1"), []byte("value2")},
		},
	},
	minTS: 1,
	maxTS: 2,
}

var conventionalBlock = block{
	spans: [][]byte{[]byte("span1"), []byte("span2")},
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
		{
			name: "binaryTag", valueType: pbv1.ValueTypeBinaryData,
			values: [][]byte{longText, longText},
		},
		{
			name: "strTag", valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte("value1"), []byte("value2")},
		},
		{
			name: "intTag", valueType: pbv1.ValueTypeInt64, values: [][]byte{convert.Int64ToBytes(10), convert.Int64ToBytes(20)},
		},
		{
			name: "floatTag", valueType: pbv1.ValueTypeFloat64, values: [][]byte{convert.Float64ToBytes(0.1), convert.Float64ToBytes(0.2)},
		},
	},
}

func Test_block_mustInitFromTrace(t *testing.T) {
	type args struct {
		timestamps []int64
		spans      [][]byte
		tags       [][]*tagValue
	}
	tests := []struct {
		name string
		args args
		want block
	}{
		{
			name: "Test mustInitFromTrace",
			args: args{
				timestamps: []int64{1, 2},
				spans:      [][]byte{[]byte("span1"), []byte("span2")},
				tags: [][]*tagValue{
					{
						{tag: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
						{tag: "floatTag", valueType: pbv1.ValueTypeFloat64, value: convert.Float64ToBytes(0.1), valueArr: nil},
						{tag: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(25), convert.Int64ToBytes(30)}},
						{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(10), valueArr: nil},
						{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
						{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value1"), valueArr: nil},
					},
					{
						{tag: "binaryTag", valueType: pbv1.ValueTypeBinaryData, value: longText, valueArr: nil},
						{tag: "floatTag", valueType: pbv1.ValueTypeFloat64, value: convert.Float64ToBytes(0.2), valueArr: nil},
						{tag: "intArrTag", valueType: pbv1.ValueTypeInt64Arr, value: nil, valueArr: [][]byte{convert.Int64ToBytes(50), convert.Int64ToBytes(60)}},
						{tag: "intTag", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(20), valueArr: nil},
						{tag: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value3"), []byte("value4")}},
						{tag: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("value2"), valueArr: nil},
					},
				},
			},
			want: conventionalBlockWithTS,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &block{}
			b.mustInitFromTrace(tt.args.spans, tt.args.tags, tt.args.timestamps)
			if !reflect.DeepEqual(*b, tt.want) {
				t.Errorf("block.mustInitFromTrace() = %+v, want %+v", *b, tt.want)
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

func Test_mustWriteAndReadSpans(t *testing.T) {
	tests := []struct {
		name      string
		spans     [][]byte
		wantPanic bool
	}{
		{
			name:  "Test mustWriteAndReadSpans",
			spans: [][]byte{[]byte("span1"), []byte("span2"), []byte("span3")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if (r != nil) != tt.wantPanic {
					t.Errorf("mustWriteSpansTo() recover = %v, wantPanic = %v", r, tt.wantPanic)
				}
			}()
			sm := &dataBlock{}
			b := &bytes.Buffer{}
			w := new(writer)
			w.init(b)
			mustWriteSpansTo(sm, tt.spans, w)
			spans := mustReadSpansFrom(nil, sm, len(tt.spans), b)
			if !reflect.DeepEqual(spans, tt.spans) {
				t.Errorf("mustReadSpansFrom() spans = %v, want %v", spans, tt.spans)
			}
		})
	}
}

func Test_marshalAndUnmarshalTag(t *testing.T) {
	metaBuffer, dataBuffer, filterBuffer := &bytes.Buffer{}, &bytes.Buffer{}, &bytes.Buffer{}
	ww := &writers{
		mustCreateTagWriters: func(_ string) (fs.Writer, fs.Writer, fs.Writer) {
			return metaBuffer, dataBuffer, filterBuffer
		},
		tagMetadataWriters: make(map[string]*writer),
		tagWriters:         make(map[string]*writer),
		tagFilterWriters:   make(map[string]*writer),
	}
	b := &conventionalBlock
	tagProjection := toTagProjection(*b)
	tagIndex := 0
	name := b.tags[tagIndex].name
	decoder := &encoding.BytesBlockDecoder{}
	bm := &blockMetadata{}
	bm.tagType = make(map[string]pbv1.ValueType)

	b.marshalTag(b.tags[tagIndex], bm, ww)

	metaWriter, ok1 := ww.tagMetadataWriters[name]
	valueWriter, ok2 := ww.tagWriters[name]
	if !ok1 || !ok2 {
		t.Fatalf("Writers not correctly added to maps")
	}
	if metaWriter.w != metaBuffer || valueWriter.w != dataBuffer {
		t.Fatalf("Writers not correctly added to maps")
	}

	unmarshaled := generateBlock()
	defer releaseBlock(unmarshaled)
	// set the spans to the same length as the original block
	// the data size in a block depends on the spans length
	unmarshaled.spans = make([][]byte, len(b.spans))
	unmarshaled.resizeTags(1)

	unmarshaled.unmarshalTag(decoder, tagIndex, bm.getTagMetadata(name), tagProjection[tagIndex], bm.tagType, metaBuffer, dataBuffer)

	if diff := cmp.Diff(unmarshaled.tags[0], b.tags[0],
		cmp.AllowUnexported(tag{}),
	); diff != "" {
		t.Errorf("block.unmarshalTag() (-got +want):\n%s", diff)
	}

	unmarshaled2 := generateBlock()
	defer releaseBlock(unmarshaled2)
	unmarshaled2.spans = make([][]byte, len(b.spans))
	unmarshaled2.resizeTags(1)

	metaReader := generateSeqReader()
	defer releaseSeqReader(metaReader)
	metaReader.init(metaBuffer)
	valueReader := generateSeqReader()
	defer releaseSeqReader(valueReader)
	valueReader.init(dataBuffer)

	unmarshaled2.unmarshalTagFromSeqReaders(decoder, tagIndex, bm.getTagMetadata(name), bm.tagType, metaReader, valueReader)

	if diff := cmp.Diff(unmarshaled2.tags[0], b.tags[0],
		cmp.AllowUnexported(tag{}),
	); diff != "" {
		t.Errorf("block.unmarshalTagFromSeqReaders() (-got +want):\n%s", diff)
	}
}

func Test_marshalAndUnmarshalBlock(t *testing.T) {
	spanBuffer := &bytes.Buffer{}
	spanWriter := &writer{}
	spanWriter.init(spanBuffer)
	ww := &writers{
		mustCreateTagWriters: func(_ string) (fs.Writer, fs.Writer, fs.Writer) {
			return &bytes.Buffer{}, &bytes.Buffer{}, &bytes.Buffer{}
		},
		tagMetadataWriters: make(map[string]*writer),
		tagWriters:         make(map[string]*writer),
		tagFilterWriters:   make(map[string]*writer),
		spanWriter:         *spanWriter,
	}
	p := &part{
		primary: &bytes.Buffer{},
		spans:   spanBuffer,
	}
	b := &conventionalBlock
	tagProjection := toTagProjection(*b)
	decoder := &encoding.BytesBlockDecoder{}
	tid := "traceid1"
	bm := blockMetadata{}

	b.mustWriteTo(tid, &bm, ww)

	tagMetadataReaders := make(map[string]fs.Reader)
	tagReaders := make(map[string]fs.Reader)

	for k, w := range ww.tagMetadataWriters {
		tagMetadataReaders[k] = w.w.(*bytes.Buffer)
		tagReaders[k] = ww.tagWriters[k].w.(*bytes.Buffer)
	}
	p.tagMetadata = tagMetadataReaders
	p.tags = tagReaders

	unmarshaled := generateBlock()
	defer releaseBlock(unmarshaled)

	bm.tagProjection = &model.TagProjection{
		Names: tagProjection,
	}
	unmarshaled.mustReadFrom(decoder, p, bm)

	if !reflect.DeepEqual(b, unmarshaled) {
		t.Errorf("block.mustReadFrom() = %+v, want %+v", unmarshaled, b)
	}

	unmarshaled2 := generateBlock()
	defer releaseBlock(unmarshaled2)
	var sr seqReaders
	sr.init(p)
	defer sr.reset()

	unmarshaled2.mustSeqReadFrom(decoder, &sr, bm)
	b.sortTags()
	if !reflect.DeepEqual(b, unmarshaled2) {
		t.Errorf("block.mustSeqReadFrom() = %+v, want %+v", unmarshaled2, b)
	}
}
