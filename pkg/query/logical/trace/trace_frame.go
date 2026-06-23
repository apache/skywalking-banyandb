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

package trace

import (
	"encoding/binary"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// traceFrameVersion is the on-wire version of the columnar trace result frame.
const traceFrameVersion byte = 1

// Tag-value type markers inside the columnar trace frame. Common scalar variants
// are encoded directly (no protobuf reflection / oneof dispatch); anything else
// (arrays, future variants) falls back to a per-cell proto.Marshal under
// traceTagProto.
const (
	traceTagNull   byte = 0
	traceTagStr    byte = 1
	traceTagInt    byte = 2
	traceTagBinary byte = 3
	traceTagProto  byte = 4
)

// EncodeTraceResultFrame serializes a columnar slice of model.TraceResult into a
// magic+version-prefixed frame: length-delimited traces, each with its spans and
// columnar tags (one TagValue per span). It avoids the protobuf
// message-slice + oneof machinery that dominates the data↔liaison hop.
func EncodeTraceResultFrame(results []model.TraceResult) []byte {
	// Pre-size to the exact encoded length so the buffer is allocated once with no
	// doubling/realloc churn (the realloc churn was the source of the native path's
	// excess bytes/RSS vs the proto path).
	buf := make([]byte, 0, traceFrameSize(results))
	buf = append(buf, data.RawFrameMagicLeadingByte, traceFrameVersion)
	buf = binary.AppendUvarint(buf, uint64(len(results)))
	for resultIdx := range results {
		result := &results[resultIdx]
		buf = appendLenString(buf, result.TID)
		buf = appendFixed64(buf, uint64(result.Key))
		buf = binary.AppendUvarint(buf, uint64(len(result.Spans)))
		for spanIdx, spanBytes := range result.Spans {
			buf = appendLenBytes(buf, spanBytes)
			spanID := ""
			if spanIdx < len(result.SpanIDs) {
				spanID = result.SpanIDs[spanIdx]
			}
			buf = appendLenString(buf, spanID)
		}
		buf = binary.AppendUvarint(buf, uint64(len(result.Tags)))
		for tagIdx := range result.Tags {
			tag := &result.Tags[tagIdx]
			buf = appendLenString(buf, tag.Name)
			buf = binary.AppendUvarint(buf, uint64(len(tag.Values)))
			for _, tagValue := range tag.Values {
				buf = appendTraceTagValue(buf, tagValue)
			}
		}
	}
	return buf
}

// traceFrameSize computes the exact encoded byte length of results, mirroring
// EncodeTraceResultFrame field-for-field so the encode buffer is pre-sized exactly.
func traceFrameSize(results []model.TraceResult) int {
	size := 2 + uvarintLen(uint64(len(results)))
	for resultIdx := range results {
		result := &results[resultIdx]
		size += lenStringSize(result.TID) + 8 + uvarintLen(uint64(len(result.Spans)))
		for spanIdx, spanBytes := range result.Spans {
			size += uvarintLen(uint64(len(spanBytes))) + len(spanBytes)
			spanIDLen := 0
			if spanIdx < len(result.SpanIDs) {
				spanIDLen = len(result.SpanIDs[spanIdx])
			}
			size += uvarintLen(uint64(spanIDLen)) + spanIDLen
		}
		size += uvarintLen(uint64(len(result.Tags)))
		for tagIdx := range result.Tags {
			tag := &result.Tags[tagIdx]
			size += lenStringSize(tag.Name) + uvarintLen(uint64(len(tag.Values)))
			for _, tagValue := range tag.Values {
				size += 1 + traceTagValueSize(tagValue)
			}
		}
	}
	return size
}

// traceTagValueSize returns the encoded byte length of one tag value, matching
// appendTraceTagValue (1 type byte + payload).
func traceTagValueSize(tv *modelv1.TagValue) int {
	switch v := tv.GetValue().(type) {
	case nil, *modelv1.TagValue_Null:
		return 0
	case *modelv1.TagValue_Str:
		return lenStringSize(v.Str.GetValue())
	case *modelv1.TagValue_Int:
		return 8
	case *modelv1.TagValue_BinaryData:
		return uvarintLen(uint64(len(v.BinaryData))) + len(v.BinaryData)
	default:
		marshaledSize := proto.Size(tv)
		return uvarintLen(uint64(marshaledSize)) + marshaledSize
	}
}

func lenStringSize(s string) int {
	return uvarintLen(uint64(len(s))) + len(s)
}

func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}

// DecodeTraceResultFrame reconstructs the columnar slice of model.TraceResult
// from a frame body. All bytes/strings are copied because the body buffer is
// reused after this call returns.
func DecodeTraceResultFrame(body []byte) ([]model.TraceResult, error) {
	if len(body) < 2 || body[0] != data.RawFrameMagicLeadingByte {
		return nil, fmt.Errorf("trace result frame: invalid magic")
	}
	if body[1] != traceFrameVersion {
		return nil, fmt.Errorf("trace result frame: unsupported version %d", body[1])
	}
	cur := &frameCursor{b: body, pos: 2}
	nTraces := cur.uvarint()
	results := make([]model.TraceResult, 0, nTraces)
	for traceIdx := 0; traceIdx < nTraces && cur.err == nil; traceIdx++ {
		tid := cur.str()
		key := int64(cur.fixed64())
		nSpans := cur.uvarint()
		spans := make([][]byte, 0, nSpans)
		spanIDs := make([]string, 0, nSpans)
		for spanIdx := 0; spanIdx < nSpans && cur.err == nil; spanIdx++ {
			spans = append(spans, cur.bytesCopy())
			spanIDs = append(spanIDs, cur.str())
		}
		nTags := cur.uvarint()
		var tags []model.Tag
		if nTags > 0 {
			tags = make([]model.Tag, 0, nTags)
		}
		for tagIdx := 0; tagIdx < nTags && cur.err == nil; tagIdx++ {
			tagName := cur.str()
			nValues := cur.uvarint()
			values := make([]*modelv1.TagValue, 0, nValues)
			for valueIdx := 0; valueIdx < nValues && cur.err == nil; valueIdx++ {
				values = append(values, cur.tagValue())
			}
			tags = append(tags, model.Tag{Name: tagName, Values: values})
		}
		results = append(results, model.TraceResult{TID: tid, Key: key, Spans: spans, SpanIDs: spanIDs, Tags: tags})
	}
	if cur.err != nil {
		return nil, cur.err
	}
	return results, nil
}

func appendLenString(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

func appendLenBytes(buf, b []byte) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(b)))
	return append(buf, b...)
}

func appendFixed64(buf []byte, v uint64) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	return append(buf, tmp[:]...)
}

func appendTraceTagValue(buf []byte, tv *modelv1.TagValue) []byte {
	switch v := tv.GetValue().(type) {
	case nil, *modelv1.TagValue_Null:
		return append(buf, traceTagNull)
	case *modelv1.TagValue_Str:
		buf = append(buf, traceTagStr)
		return appendLenString(buf, v.Str.GetValue())
	case *modelv1.TagValue_Int:
		buf = append(buf, traceTagInt)
		return appendFixed64(buf, uint64(v.Int.GetValue()))
	case *modelv1.TagValue_BinaryData:
		buf = append(buf, traceTagBinary)
		return appendLenBytes(buf, v.BinaryData)
	default:
		marshaled, err := proto.Marshal(tv)
		if err != nil {
			// proto.Marshal of a well-formed TagValue cannot fail; encode an empty
			// proto-fallback payload so decode reconstructs a zero TagValue rather
			// than corrupting the frame.
			marshaled = nil
		}
		buf = append(buf, traceTagProto)
		return appendLenBytes(buf, marshaled)
	}
}

// frameCursor is a bounds-checked cursor over a frame body; the first error is
// sticky so callers can read straight-line and check err once at the end.
type frameCursor struct {
	err error
	b   []byte
	pos int
}

func (c *frameCursor) uvarint() int {
	if c.err != nil {
		return 0
	}
	v, n := binary.Uvarint(c.b[c.pos:])
	if n <= 0 {
		c.err = fmt.Errorf("trace result frame: bad uvarint at offset %d", c.pos)
		return 0
	}
	c.pos += n
	return int(v)
}

func (c *frameCursor) fixed64() uint64 {
	if c.err != nil {
		return 0
	}
	if c.pos+8 > len(c.b) {
		c.err = fmt.Errorf("trace result frame: truncated fixed64 at offset %d", c.pos)
		return 0
	}
	v := binary.LittleEndian.Uint64(c.b[c.pos:])
	c.pos += 8
	return v
}

func (c *frameCursor) str() string {
	n := c.uvarint()
	if c.err != nil {
		return ""
	}
	if n < 0 || c.pos+n > len(c.b) {
		c.err = fmt.Errorf("trace result frame: truncated string len %d at offset %d", n, c.pos)
		return ""
	}
	s := string(c.b[c.pos : c.pos+n])
	c.pos += n
	return s
}

func (c *frameCursor) bytesCopy() []byte {
	n := c.uvarint()
	if c.err != nil {
		return nil
	}
	if n < 0 || c.pos+n > len(c.b) {
		c.err = fmt.Errorf("trace result frame: truncated bytes len %d at offset %d", n, c.pos)
		return nil
	}
	out := make([]byte, n)
	copy(out, c.b[c.pos:c.pos+n])
	c.pos += n
	return out
}

func (c *frameCursor) tagValue() *modelv1.TagValue {
	if c.err != nil {
		return nil
	}
	if c.pos >= len(c.b) {
		c.err = fmt.Errorf("trace result frame: truncated tag value at offset %d", c.pos)
		return nil
	}
	typ := c.b[c.pos]
	c.pos++
	switch typ {
	case traceTagNull:
		return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
	case traceTagStr:
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: c.str()}}}
	case traceTagInt:
		return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(c.fixed64())}}}
	case traceTagBinary:
		return &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: c.bytesCopy()}}
	case traceTagProto:
		raw := c.bytesCopy()
		if c.err != nil {
			return nil
		}
		tv := &modelv1.TagValue{}
		if err := proto.Unmarshal(raw, tv); err != nil {
			c.err = fmt.Errorf("trace result frame: tag value proto fallback: %w", err)
			return nil
		}
		return tv
	default:
		c.err = fmt.Errorf("trace result frame: unknown tag value type %d", typ)
		return nil
	}
}
