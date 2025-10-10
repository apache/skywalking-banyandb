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
	"encoding/json"
	"path/filepath"
	"sort"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/filter"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

type partMetadata struct {
	CompressedSizeBytes       uint64 `json:"compressedSizeBytes"`
	UncompressedSpanSizeBytes uint64 `json:"uncompressedSpanSizeBytes"`
	TotalCount                uint64 `json:"totalCount"`
	BlocksCount               uint64 `json:"blocksCount"`
	MinTimestamp              int64  `json:"minTimestamp"`
	MaxTimestamp              int64  `json:"maxTimestamp"`
	ID                        uint64 `json:"-"`
}

func (pm *partMetadata) reset() {
	pm.CompressedSizeBytes = 0
	pm.UncompressedSpanSizeBytes = 0
	pm.TotalCount = 0
	pm.BlocksCount = 0
	pm.MinTimestamp = 0
	pm.MaxTimestamp = 0
	pm.ID = 0
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
	n, err := fileSystem.Write(metadata, metadataPath, storage.FilePerm)
	if err != nil {
		logger.Panicf("cannot write metadata: %s", err)
		return
	}
	if n != len(metadata) {
		logger.Panicf("unexpected number of bytes written to %s; got %d; want %d", metadataPath, n, len(metadata))
	}
}

type tagType map[string]pbv1.ValueType

func (tt tagType) reset() {
	clear(tt)
}

func (tt tagType) copyFrom(src tagType) {
	for k, vt := range src {
		tt[k] = vt
	}
}

func (tt tagType) marshal(dst []byte) []byte {
	dst = encoding.VarUint64ToBytes(dst, uint64(len(tt)))

	keys := make([]string, 0, len(tt))
	for k := range tt {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, name := range keys {
		valueType := tt[name]
		dst = encoding.EncodeBytes(dst, convert.StringToBytes(name))
		dst = append(dst, byte(valueType))
	}
	return dst
}

func (tt tagType) unmarshal(src []byte) error {
	tt.reset()

	src, count := encoding.BytesToVarUint64(src)

	for i := uint64(0); i < count; i++ {
		var nameBytes []byte
		var err error
		src, nameBytes, err = encoding.DecodeBytes(src)
		if err != nil {
			return err
		}
		name := string(nameBytes)

		if len(src) < 1 {
			return errors.New("insufficient data for valueType")
		}
		valueType := pbv1.ValueType(src[0])
		src = src[1:]
		tt[name] = valueType
	}
	return nil
}

func (tt tagType) mustReadTagType(fileSystem fs.FileSystem, partPath string) {
	tt.reset()

	tagTypePath := filepath.Join(partPath, tagTypeFilename)
	data, err := fileSystem.Read(tagTypePath)
	if err != nil {
		var fsErr *fs.FileSystemError
		if errors.As(err, &fsErr) && fsErr.Code == fs.IsNotExistError {
			return
		}
		logger.Panicf("cannot read %s: %s", tagTypePath, err)
		return
	}

	if len(data) == 0 {
		return
	}

	err = tt.unmarshal(data)
	if err != nil {
		logger.Panicf("cannot parse %q: %s", tagTypePath, err)
		return
	}
}

func (tt tagType) mustWriteTagType(fileSystem fs.FileSystem, partPath string) {
	if len(tt) == 0 {
		return
	}

	var data []byte
	data = tt.marshal(data)

	tagTypePath := filepath.Join(partPath, tagTypeFilename)
	n, err := fileSystem.Write(data, tagTypePath, storage.FilePerm)
	if err != nil {
		logger.Panicf("cannot write tagType: %s", err)
		return
	}
	if n != len(data) {
		logger.Panicf("unexpected number of bytes written to %s; got %d; want %d", tagTypePath, n, len(data))
	}
}

type traceIDFilter struct {
	filter *filter.BloomFilter
}

func (tf *traceIDFilter) reset() {
	releaseBloomFilter(tf.filter)
	tf.filter = nil
}

func (tf *traceIDFilter) mustReadTraceIDFilter(fileSystem fs.FileSystem, partPath string) {
	traceIDFilterPath := filepath.Join(partPath, traceIDFilterFilename)
	data, err := fileSystem.Read(traceIDFilterPath)
	if err != nil {
		var fsErr *fs.FileSystemError
		if errors.As(err, &fsErr) && fsErr.Code == fs.IsNotExistError {
			tf.filter = nil
			return
		}
		logger.Panicf("cannot read %s: %s", traceIDFilterPath, err)
		return
	}

	if len(data) == 0 {
		tf.filter = nil
		return
	}

	bf := generateBloomFilter()
	tf.filter = decodeBloomFilter(data, bf)
}

func (tf *traceIDFilter) mustWriteTraceIDFilter(fileSystem fs.FileSystem, partPath string) {
	if tf.filter == nil {
		return
	}

	var data []byte
	data = encodeBloomFilter(data, tf.filter)

	traceIDFilterPath := filepath.Join(partPath, traceIDFilterFilename)
	n, err := fileSystem.Write(data, traceIDFilterPath, storage.FilePerm)
	if err != nil {
		logger.Panicf("cannot write traceIDFilter: %s", err)
		return
	}
	if n != len(data) {
		logger.Panicf("unexpected number of bytes written to %s; got %d; want %d", traceIDFilterPath, n, len(data))
	}
}
