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

package dump

import (
	"fmt"
	"io"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// PrimaryBlockMetadata is one entry of a measure or stream part's meta.bin: the
// series id, time range, and the (offset, size) of its primary block within
// primary.bin. Trace parts use a different on-disk layout and do not share it.
type PrimaryBlockMetadata struct {
	SeriesID     common.SeriesID
	MinTimestamp int64
	MaxTimestamp int64
	Offset       uint64
	Size         uint64
}

// ReadPrimaryBlockMetadata reads and zstd-decompresses a part's meta.bin reader
// and decodes it into the part's list of primary-block metadata entries.
func ReadPrimaryBlockMetadata(r fs.Reader) ([]PrimaryBlockMetadata, error) {
	sr := r.SequentialRead()
	data, err := io.ReadAll(sr)
	fs.MustClose(sr)
	if err != nil {
		return nil, fmt.Errorf("cannot read: %w", err)
	}

	decompressed, err := zstd.Decompress(nil, data)
	if err != nil {
		return nil, fmt.Errorf("cannot decompress: %w", err)
	}

	var result []PrimaryBlockMetadata
	src := decompressed
	for len(src) > 0 {
		var pbm PrimaryBlockMetadata
		src, err = unmarshalPrimaryBlockMetadata(&pbm, src)
		if err != nil {
			return nil, err
		}
		result = append(result, pbm)
	}
	return result, nil
}

// ReadPrimaryBlock reads pbm's primary block from primary.bin and returns its
// zstd-decompressed bytes (a fresh buffer). Callers that retain the parsed result
// across blocks use this; callers iterating one block at a time can reuse scratch
// buffers instead (see the measure iterator's readPrimaryBlock).
func ReadPrimaryBlock(primary fs.Reader, pbm PrimaryBlockMetadata) ([]byte, error) {
	primaryData := make([]byte, pbm.Size)
	if err := ReadData(primary, int64(pbm.Offset), primaryData); err != nil {
		return nil, fmt.Errorf("cannot read primary block: %w", err)
	}
	decompressed, err := zstd.Decompress(nil, primaryData)
	if err != nil {
		return nil, fmt.Errorf("cannot decompress primary block: %w", err)
	}
	return decompressed, nil
}

func unmarshalPrimaryBlockMetadata(pbm *PrimaryBlockMetadata, src []byte) ([]byte, error) {
	if len(src) < 40 {
		return nil, fmt.Errorf("insufficient data")
	}
	pbm.SeriesID = common.SeriesID(encoding.BytesToUint64(src))
	src = src[8:]
	pbm.MinTimestamp = int64(encoding.BytesToUint64(src))
	src = src[8:]
	pbm.MaxTimestamp = int64(encoding.BytesToUint64(src))
	src = src[8:]
	pbm.Offset = encoding.BytesToUint64(src)
	src = src[8:]
	pbm.Size = encoding.BytesToUint64(src)
	return src[8:], nil
}
