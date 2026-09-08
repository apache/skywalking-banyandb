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
		SortFields:   [RepairSortFieldCount]string{nidx01eGroupField, nidx01eNameField, nidx01eEntityField, timestampField},
		ProjectField: nidx01eSHAField,
		PageSize:     2,
	})
	tester.NoError(pageErr)
	tester.Equal([]RepairRow{
		{SortValues: [][]byte{[]byte("g-tie"), []byte("n-tie"), []byte("e-tie"), nidx01eEncodedTimestamps[10]}, Value: []byte("sha-first")},
		{SortValues: [][]byte{[]byte("g-tie"), []byte("n-tie"), []byte("e-tie"), nidx01eEncodedTimestamps[10]}, Value: []byte("sha-second")},
	}, page)
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

// TestRepairTuplePagePagesAllRowsWithNilSortValuesWhenRequestedFieldsHaveNoDocValueLocations verifies that a generation recording no doc-value
// locations for any of the four requested sort fields pages every live row with all four SortValues nil.
//
// All four sort components are absent, so every row ties. The page size covers
// every live row deliberately: the four-component cursor cannot resume past a
// tie without an identity component, and the contract deliberately exposes no
// identity component.
//
// No fixture reaches the whole-section branch at nativeice.go:1319 or the
// footer relaxations at nativeice.go:548-550 and nativeice.go:927-929. A
// store written through NewStore always emits a doc-value section, and forging
// a footer with docValueOffset == math.MaxUint64 requires ICE types outside
// this issue's seam.
func TestRepairTuplePagePagesAllRowsWithNilSortValuesWhenRequestedFieldsHaveNoDocValueLocations(t *testing.T) {
	tester := require.New(t)
	shardPath := t.TempDir()
	writer, writerErr := NewStore(StoreOpts{Path: shardPath})
	tester.NoError(writerErr)
	documents := index.Documents{
		repairTupleNoSortDocument("document-1", "sha-first"),
		repairTupleNoSortDocument("document-2", "sha-second"),
	}
	tester.NoError(writer.UpdateSeriesBatch(index.Batch{Documents: documents}))
	tester.NoError(writer.Close())
	generation, generationErr := OpenReadOnlyGeneration(shardPath)
	tester.NoError(generationErr)
	tester.NotErrorIs(generationErr, ErrCorruptIndex)
	t.Cleanup(func() {
		tester.NoError(generation.Close())
	})
	page, pageErr := generation.RepairTuplePage(context.Background(), RepairPageRequest{
		SortFields:   [RepairSortFieldCount]string{nidx01eGroupField, nidx01eNameField, nidx01eEntityField, timestampField},
		ProjectField: nidx01eSHAField,
		PageSize:     len(documents),
	})
	tester.NoError(pageErr)
	expected := []RepairRow{
		{SortValues: [][]byte{nil, nil, nil, nil}, Value: []byte("sha-first")},
		{SortValues: [][]byte{nil, nil, nil, nil}, Value: []byte("sha-second")},
	}
	tester.Equal(expected, page)
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
