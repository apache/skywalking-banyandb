// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to You under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package inverted

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index"
)

func TestRepairTuplePageOrdersEqualTuplesByLocalDocumentNumber(t *testing.T) {
	tester := require.New(t)
	shardPath := t.TempDir()
	writer, writerErr := NewStore(StoreOpts{Path: shardPath})
	tester.NoError(writerErr)
	documents := index.Documents{
		repairTupleTieDocument("document-1", "sha-first"),
		repairTupleTieDocument("document-2", "sha-second"),
	}
	tester.NoError(writer.UpdateSeriesBatch(index.Batch{Documents: documents}))
	tester.NoError(writer.Close())
	generation, generationErr := OpenReadOnlyGeneration(shardPath)
	tester.NoError(generationErr)
	t.Cleanup(func() {
		tester.NoError(generation.Close())
	})
	page, pageErr := generation.RepairTuplePage(context.Background(), RepairPageRequest{
		PageSize: 2,
	})
	tester.NoError(pageErr)
	tester.Equal([]RepairRow{
		{SortValues: [][]byte{[]byte("g-tie"), []byte("n-tie"), []byte("e-tie"), nidx01eEncodedTimestamps[10]}, Value: []byte("sha-first")},
		{SortValues: [][]byte{[]byte("g-tie"), []byte("n-tie"), []byte("e-tie"), nidx01eEncodedTimestamps[10]}, Value: []byte("sha-second")},
	}, nidx01eWithoutCursors(page))
}

func repairTupleTieDocument(identifier, shaValue string) index.Document {
	return index.Document{
		EntityValues: []byte(identifier),
		Timestamp:    10,
		Fields: []index.Field{
			nidx01eSortableField(nidx01eEntityField, []byte("e-tie")),
			nidx01eSortableField(nidx01eGroupField, []byte("g-tie")),
			nidx01eSortableField(nidx01eNameField, []byte("n-tie")),
			nidx01eStoredField(nidx01eSHAField, []byte(shaValue)),
		},
	}
}

// TestRepairTuplePageRejectsFieldsWithoutDocValueLocations verifies that stored
// fields cannot substitute for the four required sortable doc values.
func TestRepairTuplePageRejectsFieldsWithoutDocValueLocations(t *testing.T) {
	tester := require.New(t)
	shardPath := t.TempDir()
	writer, writerErr := NewStore(StoreOpts{Path: shardPath})
	tester.NoError(writerErr)
	tester.NoError(writer.UpdateSeriesBatch(index.Batch{Documents: index.Documents{
		repairTupleNoSortDocument("document-1", "sha-first"),
	}}))
	tester.NoError(writer.Close())
	generation := nidx01eOpen(t, shardPath)
	page, pageErr := generation.RepairTuplePage(context.Background(), RepairPageRequest{PageSize: 1})
	tester.ErrorIs(pageErr, ErrCorruptIndex)
	tester.Empty(page)
}

// TestRepairTuplePageResumesEqualTuples verifies that continuation includes the
// physical row identity instead of skipping rows with the same visible tuple.
func TestRepairTuplePageResumesEqualTuples(t *testing.T) {
	tester := require.New(t)
	shardPath := t.TempDir()
	writer, writerErr := NewStore(StoreOpts{Path: shardPath})
	tester.NoError(writerErr)
	tester.NoError(writer.UpdateSeriesBatch(index.Batch{Documents: index.Documents{
		repairTupleTieDocument("document-1", "sha-first"),
		repairTupleTieDocument("document-2", "sha-second"),
	}}))
	tester.NoError(writer.Close())
	generation := nidx01eOpen(t, shardPath)
	first, firstErr := generation.RepairTuplePage(context.Background(), RepairPageRequest{PageSize: 1})
	tester.NoError(firstErr)
	tester.Len(first, 1)
	tester.NotNil(first[0].Cursor)
	second, secondErr := generation.RepairTuplePage(context.Background(), RepairPageRequest{PageSize: 1, After: first[0].Cursor})
	tester.NoError(secondErr)
	tester.Len(second, 1)
	tester.Equal(first[0].SortValues, second[0].SortValues)
	tester.Equal("sha-first", string(first[0].Value))
	tester.Equal("sha-second", string(second[0].Value))
	last, lastErr := generation.RepairTuplePage(context.Background(), RepairPageRequest{PageSize: 1, After: second[0].Cursor})
	tester.NoError(lastErr)
	tester.Empty(last)

	otherGeneration := nidx01eOpen(t, shardPath)
	foreign, foreignErr := otherGeneration.RepairTuplePage(context.Background(), RepairPageRequest{PageSize: 1, After: first[0].Cursor})
	tester.ErrorIs(foreignErr, ErrInvalidRepairPage)
	tester.Empty(foreign)
}

func repairTupleNoSortDocument(identifier, shaValue string) index.Document {
	return index.Document{
		EntityValues: []byte(identifier),
		Timestamp:    0,
		Fields: []index.Field{
			repairTupleNoSortStoredField(nidx01eGroupField, []byte("g-no-sort")),
			repairTupleNoSortStoredField(nidx01eNameField, []byte("n-no-sort")),
			repairTupleNoSortStoredField(nidx01eEntityField, []byte("e-no-sort")),
			repairTupleNoSortStoredField(nidx01eSHAField, []byte(shaValue)),
		},
	}
}

func repairTupleNoSortStoredField(tagName string, value []byte) index.Field {
	field := index.NewBytesField(index.FieldKey{TagName: tagName}, value)
	field.Store = true
	field.NoSort = true
	field.Index = false
	return field
}
