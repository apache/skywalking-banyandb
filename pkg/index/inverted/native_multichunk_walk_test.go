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

package inverted

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

const nativeStoredChunkWalkDocumentCount = 129

// TestNativeStoredDocumentWalksMultipleStoredChunks proves that a native walk
// retains every document identity and stored value when a compatibility-writer
// segment spans more than one stored chunk.
func TestNativeStoredDocumentWalksMultipleStoredChunks(t *testing.T) {
	tester := require.New(t)
	indexDir := t.TempDir()
	writer, newStoreErr := NewStore(StoreOpts{Path: indexDir})
	tester.NoError(newStoreErr)
	writerClosed := false
	defer func() {
		if !writerClosed {
			tester.NoError(writer.Close())
		}
	}()

	fieldKey := index.FieldKey{
		Analyzer:    index.AnalyzerKeyword,
		SeriesID:    common.SeriesID(1),
		IndexRuleID: 1,
	}
	seriesIdentity := hex.EncodeToString(convert.Uint64ToBytes(1))
	expected := make(map[string]map[string][]string, nativeStoredChunkWalkDocumentCount)
	documents := make(index.Documents, 0, nativeStoredChunkWalkDocumentCount)
	for documentNumber := uint64(1); documentNumber <= nativeStoredChunkWalkDocumentCount; documentNumber++ {
		storedValue := []byte(fmt.Sprintf("stored-value-%03d", documentNumber))
		field := index.NewBytesField(fieldKey, storedValue)
		field.Store = true
		field.Index = true
		identity := convert.Uint64ToBytes(documentNumber)
		identityHex := hex.EncodeToString(identity)
		expected[identityHex] = map[string][]string{
			docIDField:         {identityHex},
			seriesIDField:      {seriesIdentity},
			fieldKey.Marshal(): {hex.EncodeToString(storedValue)},
		}
		documents = append(documents, index.Document{DocID: documentNumber, Fields: []index.Field{field}})
	}
	tester.NoError(writer.Batch(index.Batch{Documents: documents}))
	tester.NoError(writer.Close())
	writerClosed = true

	actual := make(map[string]map[string][]string, nativeStoredChunkWalkDocumentCount)
	walkErr := ReadOnlyWalkDocuments(context.Background(), indexDir, func(document StoredDocument) error {
		fields := make(map[string][]string)
		identityHex := ""
		if visitErr := document.VisitStoredFields(func(name string, value []byte) bool {
			encodedValue := hex.EncodeToString(value)
			fields[name] = append(fields[name], encodedValue)
			if name == docIDField {
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
