// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package native

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

const (
	nativeStoredChunkWalkDocumentCount = 129
	nativeStoredChunkWalkSegmentID     = uint64(1)
	nativeStoredChunkWalkSnapshotID    = uint64(1)
)

// TestNativeStoredDocumentWalksMultipleStoredChunks proves that a native walk
// retains every document identity and stored value when a generation spans
// more than one stored chunk.
func TestNativeStoredDocumentWalksMultipleStoredChunks(t *testing.T) {
	tester := require.New(t)
	indexDir := t.TempDir()

	fieldKey := index.FieldKey{
		Analyzer:    index.AnalyzerKeyword,
		SeriesID:    common.SeriesID(1),
		IndexRuleID: 1,
	}
	fieldName := fieldKey.Marshal()
	seriesIdentity := hex.EncodeToString(convert.Uint64ToBytes(1))
	expected := make(map[string]map[string][]string, nativeStoredChunkWalkDocumentCount)
	documents := make([]nativeice.EncodeDocument, 0, nativeStoredChunkWalkDocumentCount)
	for documentNumber := uint64(1); documentNumber <= nativeStoredChunkWalkDocumentCount; documentNumber++ {
		storedValue := []byte(fmt.Sprintf("stored-value-%03d", documentNumber))
		identity := convert.Uint64ToBytes(documentNumber)
		identityHex := hex.EncodeToString(identity)
		expected[identityHex] = map[string][]string{
			identifierField: {identityHex},
			seriesIDField:   {seriesIdentity},
			fieldName:       {hex.EncodeToString(storedValue)},
		}
		documents = append(documents, nativeice.EncodeDocument{
			Identifier: identity,
			Fields: []nativeice.EncodeField{
				{Name: fieldName, Value: storedValue, Store: true, Index: true},
				{Name: seriesIDField, Value: convert.Uint64ToBytes(1), Store: true, Index: true},
			},
		})
	}
	require.NoError(t, nativeice.Encode(indexDir, nativeice.Generation{
		SegmentID:  nativeStoredChunkWalkSegmentID,
		SnapshotID: nativeStoredChunkWalkSnapshotID,
		Documents:  documents,
	}))

	actual := make(map[string]map[string][]string, nativeStoredChunkWalkDocumentCount)
	walkErr := ReadOnlyWalkDocuments(context.Background(), indexDir, func(document StoredDocument) error {
		fields := make(map[string][]string)
		identityHex := ""
		if visitErr := document.VisitStoredFields(func(name string, value []byte) bool {
			encodedValue := hex.EncodeToString(value)
			fields[name] = append(fields[name], encodedValue)
			if name == identifierField {
				identityHex = encodedValue
			}
			return true
		}); visitErr != nil {
			return visitErr
		}
		if identityHex == "" {
			return fmt.Errorf("walked document has no identity field")
		}
		actual[identityHex] = fields
		return nil
	})

	tester.NoError(walkErr)
	tester.Equal(expected, actual)
}
