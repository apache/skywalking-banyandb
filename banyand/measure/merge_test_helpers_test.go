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

package measure

import (
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

type mergeTestBlock struct {
	data     block
	seriesID common.SeriesID
}

func writeMergeTestPart(fileSystem fs.FileSystem, root string, partID uint64, blocks ...mergeTestBlock) *partWrapper {
	path := partPath(root, partID)
	writer := generateBlockWriter()
	defer releaseBlockWriter(writer)
	writer.mustInitForFilePart(fileSystem, path, true)
	for _, item := range blocks {
		writer.mustWriteBlock(item.seriesID, &item.data)
	}
	metadata := &partMetadata{}
	types := make(tagType)
	writer.Flush(metadata, &types)
	types.mustWriteTagType(fileSystem, path)
	metadata.mustWriteMetadata(fileSystem, path)
	return newPartWrapper(nil, mustOpenFilePart(partID, root, fileSystem))
}

// walkMergeTestPart uses the byte-backed persisted reader to check compatibility
// independently of the typed numeric representation used during merging.
// The callback must not retain values after it returns.
func walkMergeTestPart(t testing.TB, output *part, visit func(*blockPointer)) {
	t.Helper()
	iterator := generatePartMergeIter()
	defer releasePartMergeIter(iterator)
	iterator.mustInitFromPart(output)
	decoder := generateColumnValuesDecoder()
	defer releaseColumnValuesDecoder(decoder)
	for iterator.nextBlockMetadata() {
		iterator.mustLoadBlockData(decoder, &iterator.block)
		visit(&iterator.block)
		decoder.Reset()
	}
	if err := iterator.error(); err != nil {
		t.Fatal(err)
	}
}
