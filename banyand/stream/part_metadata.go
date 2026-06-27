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
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

type partMetadata struct {
	CompressedSizeBytes   uint64 `json:"compressedSizeBytes"`
	UncompressedSizeBytes uint64 `json:"uncompressedSizeBytes"`
	TotalCount            uint64 `json:"totalCount"`
	BlocksCount           uint64 `json:"blocksCount"`
	MinTimestamp          int64  `json:"minTimestamp"`
	MaxTimestamp          int64  `json:"maxTimestamp"`
	ID                    uint64 `json:"-"`
}

func (pm *partMetadata) reset() {
	pm.CompressedSizeBytes = 0
	pm.UncompressedSizeBytes = 0
	pm.TotalCount = 0
	pm.BlocksCount = 0
	pm.MinTimestamp = 0
	pm.MaxTimestamp = 0
	pm.ID = 0
}

func (pm *partMetadata) fillFromSyncContext(ctx *queue.ChunkedSyncPartContext) {
	pm.CompressedSizeBytes = ctx.CompressedSizeBytes
	pm.UncompressedSizeBytes = ctx.UncompressedSizeBytes
	pm.TotalCount = ctx.TotalCount
	pm.BlocksCount = ctx.BlocksCount
	pm.MinTimestamp = ctx.MinTimestamp
	pm.MaxTimestamp = ctx.MaxTimestamp
}

func validatePartMetadata(fileSystem fs.FileSystem, partPath string) error {
	metadataPath := filepath.Join(partPath, metadataFilename)
	metadata, err := fileSystem.Read(metadataPath)
	if err != nil {
		return errors.WithMessage(err, "cannot read metadata.json")
	}
	var pm partMetadata
	if err := json.Unmarshal(metadata, &pm); err != nil {
		return errors.WithMessage(err, "cannot parse metadata.json")
	}
	return nil
}

func (pm *partMetadata) mustReadMetadata(fileSystem fs.FileSystem, partPath string) {
	pm.reset()

	metadataPath := filepath.Join(partPath, metadataFilename)
	metadata, err := fileSystem.Read(metadataPath)
	if err != nil {
		logger.Panicf("cannot read %s", err)
		return
	}
	if err := json.Unmarshal(metadata, pm); err != nil {
		logger.Panicf("cannot parse %q: %s", metadataPath, err)
		return
	}

	if pm.MinTimestamp > pm.MaxTimestamp {
		logger.Panicf("MinTimestamp cannot exceed MaxTimestamp; got %d vs %d", pm.MinTimestamp, pm.MaxTimestamp)
	}
}

func (pm *partMetadata) mustWriteMetadata(fileSystem fs.FileSystem, partPath string) {
	metadata, err := json.Marshal(pm)
	if err != nil {
		logger.Panicf("cannot marshal metadata: %s", err)
		return
	}
	metadataPath := filepath.Join(partPath, metadataFilename)
	n, err := fileSystem.WriteAtomic(metadata, metadataPath, storage.FilePerm)
	if err != nil {
		logger.Panicf("cannot write metadata: %s", err)
		return
	}
	if n != len(metadata) {
		logger.Panicf("unexpected number of bytes written to %s; got %d; want %d", metadataPath, n, len(metadata))
	}
}

type tagType map[string]map[string]pbv1.ValueType

func (tt tagType) reset() {
	clear(tt)
}

func (tt tagType) copyFrom(source tagType) {
	for familyName, sourceTags := range source {
		tags := tt[familyName]
		if tags == nil {
			tags = make(map[string]pbv1.ValueType, len(sourceTags))
			tt[familyName] = tags
		}
		for tagName, valueType := range sourceTags {
			tags[tagName] = valueType
		}
	}
}

func (tt tagType) marshal() []byte {
	var dst []byte
	dst = encoding.VarUint64ToBytes(dst, uint64(len(tt)))
	familyNames := make([]string, 0, len(tt))
	for familyName := range tt {
		familyNames = append(familyNames, familyName)
	}
	sort.Strings(familyNames)
	for _, familyName := range familyNames {
		dst = encoding.EncodeBytes(dst, convert.StringToBytes(familyName))
		tags := tt[familyName]
		dst = encoding.VarUint64ToBytes(dst, uint64(len(tags)))
		tagNames := make([]string, 0, len(tags))
		for tagName := range tags {
			tagNames = append(tagNames, tagName)
		}
		sort.Strings(tagNames)
		for _, tagName := range tagNames {
			dst = encoding.EncodeBytes(dst, convert.StringToBytes(tagName))
			dst = append(dst, byte(tags[tagName]))
		}
	}
	return dst
}

func (tt tagType) unmarshal(src []byte) error {
	tt.reset()
	remaining, familyCount := encoding.BytesToVarUint64(src)
	for i := uint64(0); i < familyCount; i++ {
		var familyNameBytes []byte
		var decodeErr error
		remaining, familyNameBytes, decodeErr = encoding.DecodeBytes(remaining)
		if decodeErr != nil {
			return fmt.Errorf("cannot decode tag family name: %w", decodeErr)
		}
		var tagCount uint64
		remaining, tagCount = encoding.BytesToVarUint64(remaining)
		tags := make(map[string]pbv1.ValueType, tagCount)
		for j := uint64(0); j < tagCount; j++ {
			var tagNameBytes []byte
			remaining, tagNameBytes, decodeErr = encoding.DecodeBytes(remaining)
			if decodeErr != nil {
				return fmt.Errorf("cannot decode tag name: %w", decodeErr)
			}
			if len(remaining) < 1 {
				return errors.New("insufficient data for tag value type")
			}
			tags[string(tagNameBytes)] = pbv1.ValueType(remaining[0])
			remaining = remaining[1:]
		}
		tt[string(familyNameBytes)] = tags
	}
	return nil
}

func (tt tagType) mustWriteTagType(fileSystem fs.FileSystem, partPath string) {
	if len(tt) == 0 {
		return
	}
	data := tt.marshal()
	tagTypePath := filepath.Join(partPath, tagTypeFilename)
	written, writeErr := fileSystem.WriteAtomic(data, tagTypePath, storage.FilePerm)
	if writeErr != nil {
		logger.Panicf("cannot write tag type: %s", writeErr)
		return
	}
	if written != len(data) {
		logger.Panicf("unexpected number of bytes written to %s; got %d; want %d", tagTypePath, written, len(data))
	}
}

// ParsePartMetadata parses the part metadata from the metadata.json file.
func ParsePartMetadata(fileSystem fs.FileSystem, partPath string) (queue.StreamingPartData, error) {
	metadataPath := filepath.Join(partPath, metadataFilename)
	metadata, err := fileSystem.Read(metadataPath)
	if err != nil {
		return queue.StreamingPartData{}, errors.WithMessage(err, "cannot read metadata.json")
	}
	var pm partMetadata
	if err := json.Unmarshal(metadata, &pm); err != nil {
		return queue.StreamingPartData{}, errors.WithMessage(err, "cannot parse metadata.json")
	}

	return queue.StreamingPartData{
		ID:                    pm.ID,
		CompressedSizeBytes:   pm.CompressedSizeBytes,
		UncompressedSizeBytes: pm.UncompressedSizeBytes,
		TotalCount:            pm.TotalCount,
		BlocksCount:           pm.BlocksCount,
		MinTimestamp:          pm.MinTimestamp,
		MaxTimestamp:          pm.MaxTimestamp,
	}, nil
}
