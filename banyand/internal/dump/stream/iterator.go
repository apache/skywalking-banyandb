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
	"fmt"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// Iterator returns a new iterator over the part. All block metadata is parsed
// eagerly (cheap); row data is decoded lazily one block at a time.
func (p *PartReader) Iterator() *dump.Iterator[Row] {
	var blocks []*blockMetadata
	for i := range p.primaryBlockMetadata {
		decompressed, err := dump.ReadPrimaryBlock(p.primary, p.primaryBlockMetadata[i])
		if err != nil {
			return dump.NewErrIterator[Row](err)
		}
		bms, err := parseBlockMetadata(decompressed)
		if err != nil {
			return dump.NewErrIterator[Row](fmt.Errorf("cannot parse block metadata: %w", err))
		}
		blocks = append(blocks, bms...)
	}
	// When the part has no part-level series metadata (the standalone write path
	// writes none), recover EntityValues for just this part's series by scanning
	// the segment-level series index scoped to the part.
	if p.seriesMap == nil && p.indexResolver != nil {
		seriesIDs := make(map[common.SeriesID]struct{})
		for _, bm := range blocks {
			seriesIDs[bm.seriesID] = struct{}{}
		}
		seriesMap, err := p.indexResolver.PartSeriesMap(seriesIDs)
		if err != nil {
			return dump.NewErrIterator[Row](fmt.Errorf("cannot build part series map: %w", err))
		}
		p.seriesMap = seriesMap
	}
	var decoder encoding.BytesBlockDecoder
	return dump.NewIterator(len(blocks), func(blockIdx int) ([]Row, error) {
		return p.decodeBlock(&decoder, blocks[blockIdx])
	})
}

func (p *PartReader) decodeBlock(decoder *encoding.BytesBlockDecoder, bm *blockMetadata) ([]Row, error) {
	// The decoder's internal buffer accumulates every decoded string block and is
	// never reset on its own. Stream tags are all strings, so without this it
	// grows to hold the entire part's tag data (the dominant resident cost).
	// Decoded values alias it but the proto build copies them before the next
	// block, so bounding it to one block is safe.
	decoder.Reset()
	count := int(bm.count)
	timestamps, elementIDs, err := readTimestamps(bm.timestamps, count, p.timestamps, p.reuseScratch)
	if err != nil {
		return nil, fmt.Errorf("cannot read timestamps/elementIDs for series %d: %w", bm.seriesID, err)
	}

	tagsByElement := make(map[string][][]byte)
	tagTypes := make(map[string]pbv1.ValueType)
	for tagFamilyName, tagFamilyBlock := range bm.tagFamilies {
		metaReader := p.tagFamilyMetadata[tagFamilyName]
		valueReader := p.tagFamilies[tagFamilyName]
		// A tag family file may have failed to open (missing/corrupt); skip it
		// rather than dereferencing a nil reader.
		if metaReader == nil || valueReader == nil {
			continue
		}
		metaData := dump.ReadBuf(p.reuseScratch, (*dump.ReadScratch).MetaBuf, int(tagFamilyBlock.size))
		if readErr := dump.ReadData(metaReader, int64(tagFamilyBlock.offset), metaData); readErr != nil {
			return nil, fmt.Errorf("cannot read tag family %s for series %d: %w", tagFamilyName, bm.seriesID, readErr)
		}
		tagMetadatas, parseErr := parseTagFamilyMetadata(metaData)
		if parseErr != nil {
			return nil, fmt.Errorf("cannot parse tag family %s for series %d: %w", tagFamilyName, bm.seriesID, parseErr)
		}
		for _, tagMeta := range tagMetadatas {
			fullTagName := tagFamilyName + "." + tagMeta.name
			values, tagErr := dump.ReadTagValues(decoder, tagMeta.dataBlock.offset, tagMeta.dataBlock.size, count, valueReader, tagMeta.valueType, p.reuseScratch)
			if tagErr != nil {
				return nil, fmt.Errorf("cannot read tag %s for series %d: %w", fullTagName, bm.seriesID, tagErr)
			}
			tagsByElement[fullTagName] = values
			tagTypes[fullTagName] = tagMeta.valueType
		}
	}

	var entityValues []byte
	if p.seriesMap != nil {
		entityValues = p.seriesMap[bm.seriesID]
	}

	rows := make([]Row, 0, len(timestamps))
	for i := range timestamps {
		tags := make(map[string][]byte, len(tagsByElement))
		tagTypesForRow := make(map[string]pbv1.ValueType, len(tagsByElement))
		for tagName, tagValues := range tagsByElement {
			if i < len(tagValues) {
				tags[tagName] = tagValues[i]
				tagTypesForRow[tagName] = tagTypes[tagName]
			}
		}
		rows = append(rows, Row{
			Timestamp:    timestamps[i],
			ElementID:    elementIDs[i],
			SeriesID:     bm.seriesID,
			EntityValues: entityValues,
			Tags:         tags,
			TagTypes:     tagTypesForRow,
		})
	}
	return rows, nil
}
