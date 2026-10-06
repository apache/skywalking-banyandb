// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package native

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
)

func BenchmarkNativeStoredFields(b *testing.B) {
	nativePath := filepath.Join(b.TempDir(), "native")
	legacyPath := filepath.Join(b.TempDir(), "legacy")
	identifier := []byte("series-a")
	fieldKey := index.FieldKey{IndexRuleID: 7}
	if err := nativeice.Encode(nativePath, nativeice.Generation{Documents: []nativeice.EncodeDocument{{
		Identifier: identifier,
		Fields: []nativeice.EncodeField{
			{Name: string(convert.Uint32ToBytes(7)), Value: []byte("one"), Store: true},
			{Name: string(convert.Uint32ToBytes(7)), Value: []byte("two"), Store: true},
		},
	}}}); err != nil {
		b.Fatal(err)
	}
	nativeReader, err := OpenReadOnlyGeneration(nativePath)
	if err != nil {
		b.Fatal(err)
	}
	defer nativeReader.Close()
	oracle, err := inverted.NewStore(inverted.StoreOpts{Path: legacyPath})
	if err != nil {
		b.Fatal(err)
	}
	defer oracle.Close()
	legacyField := index.NewBytesField(fieldKey, []byte("one"))
	legacyField.Store = true
	legacyField.Index = true
	legacyFieldTwo := index.NewBytesField(fieldKey, []byte("two"))
	legacyFieldTwo.Store = true
	legacyFieldTwo.Index = true
	if err := oracle.InsertSeriesBatch(index.Batch{Documents: index.Documents{{EntityValues: identifier, Fields: []index.Field{legacyField, legacyFieldTwo}}}}); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.Run("native", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := nativeReader.StoredFields(ctx, identifier); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("oracle", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := oracle.StoredFields(ctx, identifier); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkNativePartSeriesMap(b *testing.B) {
	nativePath := filepath.Join(b.TempDir(), "native")
	legacyPath := filepath.Join(b.TempDir(), "legacy")
	const documentCount = 128
	documents := make([]nativeice.EncodeDocument, documentCount)
	legacyDocuments := make(index.Documents, documentCount)
	for documentNumber := range documentCount {
		identifier := []byte("series-" + string(rune('a'+documentNumber/26)) + string(rune('a'+documentNumber%26)))
		documents[documentNumber] = nativeice.EncodeDocument{Identifier: identifier}
		legacyDocuments[documentNumber] = index.Document{EntityValues: identifier}
	}
	if err := nativeice.Encode(nativePath, nativeice.Generation{Documents: documents}); err != nil {
		b.Fatal(err)
	}
	nativeReader, err := OpenReadOnlyGeneration(nativePath)
	if err != nil {
		b.Fatal(err)
	}
	defer nativeReader.Close()
	oracle, err := inverted.NewStore(inverted.StoreOpts{Path: legacyPath})
	if err != nil {
		b.Fatal(err)
	}
	defer oracle.Close()
	if err := oracle.InsertSeriesBatch(index.Batch{Documents: legacyDocuments}); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.Run("native", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			count := 0
			if err := nativeReader.VisitIdentifiers(ctx, func([]byte) bool {
				count++
				return true
			}); err != nil {
				b.Fatal(err)
			}
			if count != documentCount {
				b.Fatalf("visited %d identifiers, want %d", count, documentCount)
			}
		}
	})
	b.Run("oracle", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			iterator, iteratorErr := oracle.SeriesIterator(ctx)
			if iteratorErr != nil {
				b.Fatal(iteratorErr)
			}
			count := 0
			for iterator.Next() {
				count++
			}
			if closeErr := iterator.Close(); closeErr != nil {
				b.Fatal(closeErr)
			}
			if count != documentCount {
				b.Fatalf("iterated %d identifiers, want %d", count, documentCount)
			}
		}
	})
}
