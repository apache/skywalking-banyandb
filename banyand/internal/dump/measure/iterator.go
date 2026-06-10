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

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// Iterator returns a new iterator over the part. All block metadata is parsed
// eagerly (cheap); row data is decoded lazily one block at a time.
func (p *PartReader) Iterator() *dump.Iterator[Row] {
	blocks, err := p.parseBlocks()
	if err != nil {
		return dump.NewErrIterator[Row](err)
	}
	var decoder encoding.BytesBlockDecoder
	return dump.NewIterator(len(blocks), func(blockIdx int) ([]Row, error) {
		return p.decodeBlock(&decoder, blocks[blockIdx])
	})
}

// parseBlocks eagerly parses all block metadata and, when the part has no
// part-level series metadata, recovers the per-part series map. Shared by
// Iterator and IterateColumnar.
func (p *PartReader) parseBlocks() ([]*blockMetadata, error) {
	var blocks []*blockMetadata
	for i := range p.primaryBlockMetadata {
		decompressed, err := dump.ReadPrimaryBlock(p.primary, p.primaryBlockMetadata[i])
		if err != nil {
			return nil, err
		}
		bms, err := parseBlockMetadata(decompressed)
		if err != nil {
			return nil, fmt.Errorf("cannot parse block metadata: %w", err)
		}
		blocks = append(blocks, bms...)
	}
	// When the part has no part-level series metadata (the standalone write path
	// writes none), recover EntityValues for just this part's series by scanning
	// the series index scoped to the part — keeping peak memory bounded by the
	// part rather than loading a segment-wide map.
	if p.seriesMap == nil && p.indexResolver != nil {
		seriesIDs := make(map[common.SeriesID]struct{})
		for _, bm := range blocks {
			seriesIDs[bm.seriesID] = struct{}{}
		}
		seriesMap, err := p.indexResolver.PartSeriesMap(seriesIDs)
		if err != nil {
			return nil, fmt.Errorf("cannot build part series map: %w", err)
		}
		p.seriesMap = seriesMap
	}
	return blocks, nil
}

// ColumnarBlock is a read-only column view of one block (one series), shared by
// all of the block's rows. It avoids the per-row maps that decodeBlock builds:
// the caller reads each row by (column, index). Byte slices alias the decode
// buffer and are valid only until the next block is decoded.
type ColumnarBlock struct {
	EntityValues []byte
	IndexedTags  map[uint32][][]byte
	TagCols      map[string][][]byte // "family.tag" -> per-row values
	TagTypes     map[string]pbv1.ValueType
	FieldCols    map[string][][]byte // field name -> per-row values
	FieldTypes   map[string]pbv1.ValueType
	timestamps   []int64
	versions     []int64
	SeriesID     common.SeriesID
	Count        int
}

// Timestamp returns the timestamp of row i.
func (c *ColumnarBlock) Timestamp(i int) int64 { return c.timestamps[i] }

// Version returns the version of row i.
func (c *ColumnarBlock) Version(i int) int64 { return c.versions[i] }

// decodeColumns decodes one block into its column arrays (shared across rows).
// It is the per-block work that both decodeBlock and IterateColumnar share.
func (p *PartReader) decodeColumns(decoder *encoding.BytesBlockDecoder, bm *blockMetadata) (*ColumnarBlock, error) {
	// The decoder's internal buffer accumulates every decoded string block and is
	// never reset on its own, so without this it would grow to hold the whole
	// part's string data. Decoded values alias it but the proto build copies them
	// (string(raw)) before the next block, so bounding it to one block is safe.
	decoder.Reset()
	count := int(bm.count)
	timestamps, versions, err := readTimestamps(bm.timestamps, count, p.timestamps, p.reuseScratch)
	if err != nil {
		return nil, fmt.Errorf("cannot read timestamps for series %d: %w", bm.seriesID, err)
	}

	fieldsByDataPoint := make(map[string][][]byte, len(bm.field.columns))
	fieldTypes := make(map[string]pbv1.ValueType, len(bm.field.columns))
	for _, colMeta := range bm.field.columns {
		values, fieldErr := readFieldValues(decoder, colMeta.dataBlock, colMeta.name, count, p.fieldValues, colMeta.valueType, p.reuseScratch)
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
		metaData := dump.ReadBuf(p.reuseScratch, (*dump.ReadScratch).MetaBuf, int(tagFamilyBlock.size))
		if readErr := dump.ReadData(metaReader, int64(tagFamilyBlock.offset), metaData); readErr != nil {
			return nil, fmt.Errorf("cannot read tag family %s for series %d: %w", tagFamilyName, bm.seriesID, readErr)
		}
		var cfm columnFamilyMetadata
		if _, unmarshalErr := cfm.unmarshal(metaData); unmarshalErr != nil {
			return nil, fmt.Errorf("cannot unmarshal tag family %s for series %d: %w", tagFamilyName, bm.seriesID, unmarshalErr)
		}
		for _, colMeta := range cfm.columns {
			fullTagName := tagFamilyName + "." + colMeta.name
			values, tagErr := dump.ReadTagValues(decoder, colMeta.dataBlock.offset, colMeta.dataBlock.size, count, valueReader, colMeta.valueType, p.reuseScratch)
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

	// Indexed (non-column) tags live only in the series index; resolve them once
	// per block (one series) and share across the block's rows.
	var indexedTags map[uint32][][]byte
	if p.indexResolver != nil {
		var resolveErr error
		indexedTags, resolveErr = p.indexResolver.Resolve(bm.seriesID, entityValues)
		if resolveErr != nil {
			return nil, fmt.Errorf("cannot resolve indexed tags for series %d: %w", bm.seriesID, resolveErr)
		}
	}

	return &ColumnarBlock{
		timestamps:   timestamps,
		versions:     versions,
		SeriesID:     bm.seriesID,
		EntityValues: entityValues,
		IndexedTags:  indexedTags,
		TagCols:      tagsByDataPoint,
		TagTypes:     tagTypes,
		FieldCols:    fieldsByDataPoint,
		FieldTypes:   fieldTypes,
		Count:        count,
	}, nil
}

func (p *PartReader) decodeBlock(decoder *encoding.BytesBlockDecoder, bm *blockMetadata) ([]Row, error) {
	c, err := p.decodeColumns(decoder, bm)
	if err != nil {
		return nil, err
	}
	rows := make([]Row, 0, c.Count)
	for i := 0; i < c.Count; i++ {
		tags := make(map[string][]byte, len(c.TagCols))
		tagTypesForRow := make(map[string]pbv1.ValueType, len(c.TagCols))
		for tagName, tagValues := range c.TagCols {
			if i < len(tagValues) {
				tags[tagName] = tagValues[i]
				tagTypesForRow[tagName] = c.TagTypes[tagName]
			}
		}
		fields := make(map[string][]byte, len(c.FieldCols))
		fieldTypesForRow := make(map[string]pbv1.ValueType, len(c.FieldCols))
		for fieldName, fieldValues := range c.FieldCols {
			if i < len(fieldValues) {
				fields[fieldName] = fieldValues[i]
				fieldTypesForRow[fieldName] = c.FieldTypes[fieldName]
			}
		}
		rows = append(rows, Row{
			Timestamp:    c.timestamps[i],
			Version:      c.versions[i],
			SeriesID:     c.SeriesID,
			EntityValues: c.EntityValues,
			Tags:         tags,
			Fields:       fields,
			TagTypes:     tagTypesForRow,
			FieldTypes:   fieldTypesForRow,
			IndexedTags:  c.IndexedTags,
		})
	}
	return rows, nil
}

// IterateColumnar walks the part one block at a time, handing the caller a
// shared column view (no per-row maps). The view is only valid inside fn (its
// byte slices alias the decode buffer reused per block). This is the A3 path:
// the caller builds output by (column, row index) directly.
func (p *PartReader) IterateColumnar(fn func(*ColumnarBlock) error) error {
	blocks, err := p.parseBlocks()
	if err != nil {
		return err
	}
	var decoder encoding.BytesBlockDecoder
	for _, bm := range blocks {
		c, decErr := p.decodeColumns(&decoder, bm)
		if decErr != nil {
			return decErr
		}
		if fnErr := fn(c); fnErr != nil {
			return fnErr
		}
	}
	return nil
}

// ColumnarCursor is a pull-based columnar iterator (the A3 path): it advances
// one row at a time like Iterator but exposes the shared ColumnarBlock + row
// index instead of building a per-row Row map. Block constants (entity, indexed
// tags) are decoded once per block by the consumer. Not safe for concurrent use.
// Block byte slices alias the decode buffer and are valid only until the next
// Next() crosses a block boundary or Close().
//
// Block metadata is parsed one primary block at a time (lazily) rather than all
// up front, so the cursor never holds the whole part's block metadata (tens of
// MB for a large part); only the current primary block's blocks are resident.
type ColumnarCursor struct {
	p              *PartReader
	cur            *ColumnarBlock
	err            error
	curBlocks      []*blockMetadata // the current primary block's parsed blocks
	primaryRead    []byte           // reused read buffer for primary block data
	primaryDecomp  []byte           // reused zstd-decompress buffer for primary block
	decoder        encoding.BytesBlockDecoder
	primaryIdx     int // index into p.primaryBlockMetadata, -1 before first
	blockInPrimary int // index into curBlocks, -1 before first
	globalBlockIdx int // 0-based global block index for Position
	rowIdx         int
	closed         bool
}

// ColumnarIterator returns a pull-based columnar cursor over the part. Block
// metadata is parsed lazily one primary block at a time; columns are decoded one
// block at a time, reusing a single decoder across blocks.
func (p *PartReader) ColumnarIterator() *ColumnarCursor {
	if err := p.ensureSeriesMap(); err != nil {
		return &ColumnarCursor{err: err, primaryIdx: -1, blockInPrimary: -1, globalBlockIdx: -1, rowIdx: -1}
	}
	return &ColumnarCursor{p: p, primaryIdx: -1, blockInPrimary: -1, globalBlockIdx: -1, rowIdx: -1}
}

// ensureSeriesMap builds the part's series map (seriesID -> EntityValues) when
// the part carries no smeta.bin. It scans every block's seriesID (a primary
// block can span multiple series, so the primary block metadata's seriesID is
// not sufficient) but discards the parsed block metadata afterward, so the cursor
// can still parse block metadata lazily during iteration without holding it all.
func (p *PartReader) ensureSeriesMap() error {
	if p.seriesMap != nil || p.indexResolver == nil {
		return nil
	}
	seriesIDs := make(map[common.SeriesID]struct{})
	var readScratch, decompScratch []byte
	for i := range p.primaryBlockMetadata {
		bms, err := p.readPrimaryBlock(i, &readScratch, &decompScratch)
		if err != nil {
			return err
		}
		for _, bm := range bms {
			seriesIDs[bm.seriesID] = struct{}{}
		}
	}
	seriesMap, err := p.indexResolver.PartSeriesMap(seriesIDs)
	if err != nil {
		return fmt.Errorf("cannot build part series map: %w", err)
	}
	p.seriesMap = seriesMap
	return nil
}

// parsePrimaryBlock reads, decompresses and parses the i-th primary block's
// block metadata, reusing the cursor's scratch buffers.
func (c *ColumnarCursor) parsePrimaryBlock(i int) ([]*blockMetadata, error) {
	return c.p.readPrimaryBlock(i, &c.primaryRead, &c.primaryDecomp)
}

// readPrimaryBlock reads, decompresses and parses the i-th primary block's block
// metadata, growing and reusing the caller's read/decomp scratch buffers in
// place. The returned block metadata aliases *decompScratch, so a caller that
// retains blocks across iterations must pass fresh buffers per block (see
// parseBlocks); the series-map scan and the cursor consume each block before the
// next and so reuse one pair.
func (p *PartReader) readPrimaryBlock(i int, readScratch, decompScratch *[]byte) ([]*blockMetadata, error) {
	pbm := p.primaryBlockMetadata[i]
	*readScratch = bytes.ResizeOver(*readScratch, int(pbm.Size))
	if readErr := dump.ReadData(p.primary, int64(pbm.Offset), *readScratch); readErr != nil {
		return nil, fmt.Errorf("cannot read primary block: %w", readErr)
	}
	decompressed, err := zstd.Decompress((*decompScratch)[:0], *readScratch)
	if err != nil {
		return nil, fmt.Errorf("cannot decompress primary block: %w", err)
	}
	*decompScratch = decompressed
	bms, err := parseBlockMetadata(decompressed)
	if err != nil {
		return nil, fmt.Errorf("cannot parse block metadata: %w", err)
	}
	return bms, nil
}

// Next advances to the next row, decoding the next block — and parsing the next
// primary block — on demand. It returns false at end-of-part or on error; check
// Err() afterwards. The terminal state is sticky.
func (c *ColumnarCursor) Next() bool {
	if c.closed || c.err != nil {
		return false
	}
	for {
		if c.cur != nil && c.rowIdx+1 < c.cur.Count {
			c.rowIdx++
			return true
		}
		// Decode the next block within the current primary block.
		if c.blockInPrimary+1 < len(c.curBlocks) {
			c.blockInPrimary++
			c.globalBlockIdx++
			cb, err := c.p.decodeColumns(&c.decoder, c.curBlocks[c.blockInPrimary])
			if err != nil {
				c.err = err
				return false
			}
			c.cur = cb
			c.rowIdx = -1
			continue
		}
		// Parse the next primary block.
		if c.primaryIdx+1 >= len(c.p.primaryBlockMetadata) {
			return false
		}
		c.primaryIdx++
		bms, err := c.parsePrimaryBlock(c.primaryIdx)
		if err != nil {
			c.err = err
			return false
		}
		c.curBlocks = bms
		c.blockInPrimary = -1
	}
}

// Err returns the first non-EOF error encountered during iteration.
func (c *ColumnarCursor) Err() error { return c.err }

// Position returns the current (block, row) position, matching Iterator's
// numbering so resume bookkeeping is identical across the Row and columnar paths.
func (c *ColumnarCursor) Position() dump.Position {
	blockIdx := c.globalBlockIdx
	if blockIdx < 0 {
		blockIdx = 0
	}
	return dump.Position{BlockIdx: blockIdx, RowIdx: c.rowIdx}
}

// Block returns the current block's shared column view, or nil before the first
// successful Next().
func (c *ColumnarCursor) Block() *ColumnarBlock { return c.cur }

// Index returns the current row's index within Block().
func (c *ColumnarCursor) Index() int { return c.rowIdx }

// Close releases the cursor's buffers. Safe to call multiple times.
func (c *ColumnarCursor) Close() error {
	c.closed = true
	c.cur = nil
	c.curBlocks = nil
	c.primaryRead = nil
	c.primaryDecomp = nil
	return nil
}
