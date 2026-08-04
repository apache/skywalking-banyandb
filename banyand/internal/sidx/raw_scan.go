// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package sidx

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// RawRow is one physical row from a persisted secondary-index part. Its byte
// slices are valid only for the duration of the ScanRaw callback.
type RawRow struct {
	Data     []byte
	Tags     []Tag
	SeriesID common.SeriesID
	PartID   uint64
	BlockID  uint64
	Key      int64
}

// ScanRaw visits every physical row without query-result deduplication.
func ScanRaw(ctx context.Context, instance SIDX, visit func(RawRow) error) error {
	storage, ok := instance.(*sidx)
	if !ok {
		return fmt.Errorf("raw scan requires the native SIDX implementation, got %T", instance)
	}
	if visit == nil {
		return fmt.Errorf("raw scan visitor cannot be nil")
	}
	snapshot := storage.currentSnapshot()
	if snapshot == nil {
		return nil
	}
	defer snapshot.decRef() //nolint:contextcheck // reference counting cleanup does not require context

	parts := append([]*partWrapper(nil), snapshot.parts...)
	sort.Slice(parts, func(leftIdx, rightIdx int) bool {
		return parts[leftIdx].ID() < parts[rightIdx].ID()
	})
	for _, partData := range parts {
		if contextErr := ctx.Err(); contextErr != nil {
			return fmt.Errorf("raw secondary-index scan canceled before part %016x: %w", partData.ID(), contextErr)
		}
		if partData == nil || partData.p == nil || partData.isMemPart() {
			return fmt.Errorf("raw scan requires a persisted part")
		}
		loader := queryResult{pm: storage.pm, l: storage.l}
		if scanErr := scanRawPart(ctx, loader, partData.p, partData.ID(), visit); scanErr != nil {
			return fmt.Errorf("cannot raw-scan secondary-index part %016x: %w", partData.ID(), scanErr)
		}
	}
	return nil
}

// ScanRawParts visits persisted parts without opening a mutable SIDX instance.
func ScanRawParts(ctx context.Context, fileSystem fs.FileSystem, root string, partIDs []uint64, visit func(RawRow) error) error {
	if visit == nil {
		return fmt.Errorf("raw scan visitor cannot be nil")
	}
	orderedPartIDs := append([]uint64(nil), partIDs...)
	sort.Slice(orderedPartIDs, func(leftIdx, rightIdx int) bool {
		return orderedPartIDs[leftIdx] < orderedPartIDs[rightIdx]
	})
	loader := queryResult{pm: protector.Nop{}, l: logger.GetLogger().Named("sidx-raw-scan")}
	for _, partID := range orderedPartIDs {
		if contextErr := ctx.Err(); contextErr != nil {
			return fmt.Errorf("raw secondary-index scan canceled before part %016x: %w", partID, contextErr)
		}
		partPath := partPath(root, partID)
		partData := mustOpenPartReadOnly(partID, partPath, fileSystem)
		scanErr := scanRawPart(ctx, loader, partData, partID, visit)
		partData.close()
		if scanErr != nil {
			return fmt.Errorf("cannot raw-scan secondary-index part %016x: %w", partID, scanErr)
		}
	}
	return nil
}

func scanRawPart(ctx context.Context, loader queryResult, partData *part, partID uint64, visit func(RawRow) error) error {
	metadataArray := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(metadataArray)
	partIterator := &partIter{}
	partIterator.init(metadataArray, partData, math.MinInt64, math.MaxInt64)
	decodedBlock := generateBlock()
	defer releaseBlock(decodedBlock)
	var blockID uint64
	for partIterator.nextBlock() {
		if contextErr := ctx.Err(); contextErr != nil {
			return fmt.Errorf("raw secondary-index scan canceled in part %016x: %w", partID, contextErr)
		}
		if !loader.loadBlockData(decodedBlock, partData, partIterator.curBlock) {
			blockID++
			continue
		}
		tagNames := make([]string, 0, len(decodedBlock.tags))
		for tagName := range decodedBlock.tags {
			tagNames = append(tagNames, tagName)
		}
		sort.Strings(tagNames)
		for rowIdx, key := range decodedBlock.userKeys {
			tags := make([]Tag, 0, len(tagNames))
			for _, tagName := range tagNames {
				tagData := decodedBlock.tags[tagName]
				if rowIdx >= len(tagData.values) {
					return fmt.Errorf("tag %q has %d rows, expected at least %d", tagName, len(tagData.values), rowIdx+1)
				}
				tagValue := tagData.values[rowIdx]
				tags = append(tags, Tag{
					Name:      tagName,
					Value:     tagValue.value,
					ValueArr:  tagValue.valueArr,
					ValueType: tagData.valueType,
				})
			}
			row := RawRow{
				Data:     decodedBlock.data[rowIdx],
				Tags:     tags,
				SeriesID: partIterator.curBlock.seriesID,
				PartID:   partID,
				BlockID:  blockID,
				Key:      key,
			}
			if visitErr := visit(row); visitErr != nil {
				return fmt.Errorf("raw scan visitor failed for part %016x row %d: %w", partID, rowIdx, visitErr)
			}
		}
		blockID++
	}
	if iteratorErr := partIterator.error(); iteratorErr != nil {
		return fmt.Errorf("cannot scan raw part %016x: %w", partID, iteratorErr)
	}
	return nil
}
