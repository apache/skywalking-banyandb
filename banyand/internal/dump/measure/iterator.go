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
	"fmt"

	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// Iterator returns a new iterator over the part. All block metadata is parsed
// eagerly (cheap); row data is decoded lazily one block at a time.
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
		bms, err := parseBlockMetadata(decompressed)
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
	count := int(bm.count)
	timestamps, versions, err := readTimestamps(bm.timestamps, count, p.timestamps)
	if err != nil {
		return nil, fmt.Errorf("cannot read timestamps for series %d: %w", bm.seriesID, err)
	}

	fieldsByDataPoint := make(map[string][][]byte, len(bm.field.columns))
	fieldTypes := make(map[string]pbv1.ValueType, len(bm.field.columns))
	for _, colMeta := range bm.field.columns {
		values, fieldErr := readFieldValues(decoder, colMeta.dataBlock, colMeta.name, count, p.fieldValues, colMeta.valueType)
		if fieldErr != nil {
			return nil, fmt.Errorf("cannot read field %s for series %d: %w", colMeta.name, bm.seriesID, fieldErr)
		}
		fieldsByDataPoint[colMeta.name] = values
		fieldTypes[colMeta.name] = colMeta.valueType
	}

	tagsByDataPoint := make(map[string][][]byte)
	tagTypes := make(map[string]pbv1.ValueType)
	for tagFamilyName, tagFamilyBlock := range bm.tagFamilies {
		metaReader := p.tagFamilyMetadata[tagFamilyName]
		valueReader := p.tagFamilies[tagFamilyName]
		// A tag family file may have failed to open (missing/corrupt); skip it
		// rather than dereferencing a nil reader.
		if metaReader == nil || valueReader == nil {
			continue
		}
		metaData := make([]byte, tagFamilyBlock.size)
		if readErr := dump.ReadData(metaReader, int64(tagFamilyBlock.offset), metaData); readErr != nil {
			return nil, fmt.Errorf("cannot read tag family %s for series %d: %w", tagFamilyName, bm.seriesID, readErr)
		}
		var cfm columnFamilyMetadata
		if _, unmarshalErr := cfm.unmarshal(metaData); unmarshalErr != nil {
			return nil, fmt.Errorf("cannot unmarshal tag family %s for series %d: %w", tagFamilyName, bm.seriesID, unmarshalErr)
		}
		for _, colMeta := range cfm.columns {
			fullTagName := tagFamilyName + "." + colMeta.name
			values, tagErr := readTagValues(decoder, colMeta.dataBlock, fullTagName, count, valueReader, colMeta.valueType)
			if tagErr != nil {
				return nil, fmt.Errorf("cannot read tag %s for series %d: %w", fullTagName, bm.seriesID, tagErr)
			}
			tagsByDataPoint[fullTagName] = values
			tagTypes[fullTagName] = colMeta.valueType
		}
	}

	var entityValues []byte
	if p.seriesMap != nil {
		entityValues = p.seriesMap[bm.seriesID]
	}

	rows := make([]Row, 0, len(timestamps))
	for i := range timestamps {
		tags := make(map[string][]byte, len(tagsByDataPoint))
		tagTypesForRow := make(map[string]pbv1.ValueType, len(tagsByDataPoint))
		for tagName, tagValues := range tagsByDataPoint {
			if i < len(tagValues) {
				tags[tagName] = tagValues[i]
				tagTypesForRow[tagName] = tagTypes[tagName]
			}
		}
		fields := make(map[string][]byte, len(fieldsByDataPoint))
		fieldTypesForRow := make(map[string]pbv1.ValueType, len(fieldsByDataPoint))
		for fieldName, fieldValues := range fieldsByDataPoint {
			if i < len(fieldValues) {
				fields[fieldName] = fieldValues[i]
				fieldTypesForRow[fieldName] = fieldTypes[fieldName]
			}
		}
		rows = append(rows, Row{
			Timestamp:    timestamps[i],
			Version:      versions[i],
			SeriesID:     bm.seriesID,
			EntityValues: entityValues,
			Tags:         tags,
			Fields:       fields,
			TagTypes:     tagTypesForRow,
			FieldTypes:   fieldTypesForRow,
		})
	}
	return rows, nil
}
