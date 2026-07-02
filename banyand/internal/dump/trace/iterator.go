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
	"fmt"

	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// Iterator returns a new iterator over the part. All block metadata is parsed
// eagerly (cheap); span/tag data is decoded lazily one block at a time.
func (p *PartReader) Iterator() *dump.Iterator[Row] {
	var blocks []*blockMetadata
	for i := range p.primaryBlockMetadata {
		pbm := p.primaryBlockMetadata[i]
		primaryData := make([]byte, pbm.size)
		if readErr := dump.ReadData(p.primary, int64(pbm.offset), primaryData); readErr != nil {
			return dump.NewErrIterator[Row](fmt.Errorf("cannot read primary block: %w", readErr))
		}
		decompressed, err := zstd.Decompress(nil, primaryData)
		if err != nil {
			return dump.NewErrIterator[Row](fmt.Errorf("cannot decompress primary block: %w", err))
		}
		bms, err := parseAllBlockMetadata(decompressed, p.tagType)
		if err != nil {
			return dump.NewErrIterator[Row](fmt.Errorf("cannot parse block metadata: %w", err))
		}
		blocks = append(blocks, bms...)
	}
	var decoder encoding.BytesBlockDecoder
	return dump.NewIterator(len(blocks), func(blockIdx int) ([]Row, error) {
		return p.decodeBlock(&decoder, blocks[blockIdx])
	})
}

func (p *PartReader) decodeBlock(decoder *encoding.BytesBlockDecoder, bm *blockMetadata) ([]Row, error) {
	// The decoder's internal buffer accumulates every decoded span/string block
	// and is never reset on its own, so without this it grows to hold the whole
	// part's data. Decoded values alias it but the proto build copies them before
	// the next block, so bounding it to one block is safe.
	decoder.Reset()
	count := int(bm.count)
	spans, spanIDs, err := readSpans(decoder, bm.spans, count, p.spans)
	if err != nil {
		return nil, fmt.Errorf("cannot read spans for trace %s: %w", bm.traceID, err)
	}

	tagsBySpan := make(map[string][][]byte, len(bm.tags))
	for tagName, tagBlock := range bm.tags {
		metaReader := p.tagMetadata[tagName]
		valueReader := p.tags[tagName]
		// A tag file may have failed to open (missing/corrupt); skip it rather
		// than dereferencing a nil reader.
		if metaReader == nil || valueReader == nil {
			continue
		}
		values, tagErr := readTagValues(decoder, tagBlock, tagName, count, metaReader, valueReader, p.tagType[tagName])
		if tagErr != nil {
			return nil, fmt.Errorf("cannot read tag %s for trace %s: %w", tagName, bm.traceID, tagErr)
		}
		tagsBySpan[tagName] = values
	}

	rows := make([]Row, 0, len(spans))
	for i := range spans {
		spanTags := make(map[string][]byte, len(tagsBySpan))
		tagTypesForRow := make(map[string]pbv1.ValueType, len(tagsBySpan))
		for tagName, tagValues := range tagsBySpan {
			if i < len(tagValues) {
				spanTags[tagName] = tagValues[i]
				tagTypesForRow[tagName] = bm.tagType[tagName]
			}
		}
		seriesID := calculateSeriesIDFromTags(spanTags)
		rows = append(rows, Row{
			TraceID:   bm.traceID,
			SpanID:    spanIDs[i],
			Span:      spans[i],
			Tags:      spanTags,
			TagTypes:  tagTypesForRow,
			SeriesID:  seriesID,
			Timestamp: timestampFromTags(spanTags, tagTypesForRow),
		})
	}
	return rows, nil
}

// timestampFromTags returns the int64 nanos of the ValueType=Timestamp tag, or 0
// if none is present. If several exist, the lexicographically-first tag name
// wins (deterministic). This is how trace stores per-row time.
func timestampFromTags(tags map[string][]byte, tagTypes map[string]pbv1.ValueType) int64 {
	best := ""
	for name, vt := range tagTypes {
		if vt == pbv1.ValueTypeTimestamp && (best == "" || name < best) {
			best = name
		}
	}
	if best == "" {
		return 0
	}
	if v := tags[best]; len(v) >= 8 {
		return convert.BytesToInt64(v)
	}
	return 0
}
