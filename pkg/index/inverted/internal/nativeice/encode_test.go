// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package nativeice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestEncodeRejectsInvalidGenerationBeforeWriting(t *testing.T) {
	target := filepath.Join(t.TempDir(), "generation")
	generation := Generation{
		Documents: []EncodeDocument{{
			Fields: []EncodeField{{Name: "field", Value: []byte("value"), Store: true}},
		}},
	}

	encodeErr := Encode(target, generation)
	if !errors.Is(encodeErr, ErrInvalidGeneration) {
		t.Fatalf("Encode() error = %v, want an error wrapping ErrInvalidGeneration", encodeErr)
	}
	if _, statErr := os.Stat(target); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("target state after rejected generation = %v, want no directory", statErr)
	}
}

func TestEncodePublishesReadableGeneration(t *testing.T) {
	directory := t.TempDir()
	generation := Generation{
		SegmentID:  2,
		SnapshotID: 3,
		Documents: []EncodeDocument{{
			Identifier: []byte("doc-1"),
			Fields:     []EncodeField{{Name: "title", Value: []byte("one"), Store: true}},
		}},
	}

	if encodeErr := Encode(directory, generation); encodeErr != nil {
		t.Fatal(encodeErr)
	}
	reader, openErr := Open(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() {
		if closeErr := reader.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	if reader.SnapshotID() != generation.SnapshotID {
		t.Fatalf("SnapshotID() = %d, want %d", reader.SnapshotID(), generation.SnapshotID)
	}
	count, countErr := reader.VisibleDocCount()
	if countErr != nil {
		t.Fatal(countErr)
	}
	if count != 1 {
		t.Fatalf("VisibleDocCount() = %d, want 1", count)
	}
	var stored []string
	walkErr := reader.VisitLiveDocuments(context.Background(), func(document StoredDocument) error {
		return document.VisitStoredFields(func(name string, value []byte) bool {
			stored = append(stored, name+"="+string(value))
			return true
		})
	})
	if walkErr != nil {
		t.Fatal(walkErr)
	}
	want := []string{"_id=doc-1", "title=one"}
	if len(stored) != len(want) {
		t.Fatalf("stored values = %v, want %v", stored, want)
	}
	for index := range want {
		if stored[index] != want[index] {
			t.Fatalf("stored values = %v, want %v", stored, want)
		}
	}
}

func TestEncodeSelectsIndexedDocuments(t *testing.T) {
	directory := t.TempDir()
	generation := Generation{
		SegmentID:  2,
		SnapshotID: 3,
		Documents: []EncodeDocument{
			{Identifier: []byte("doc-1"), Fields: []EncodeField{{Name: "tag", Value: []byte("red"), Index: true}}},
			{Identifier: []byte("doc-2"), Fields: []EncodeField{{Name: "tag", Value: []byte("blue"), Index: true}}},
		},
	}
	if encodeErr := Encode(directory, generation); encodeErr != nil {
		t.Fatal(encodeErr)
	}
	reader, openErr := Open(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() {
		if closeErr := reader.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	var selected []string
	selectionErr := reader.VisitSelectedDocuments(context.Background(), "tag", [][]byte{[]byte("red")}, func(document StoredDocument) error {
		return document.VisitStoredFields(func(name string, value []byte) bool {
			if name == identifierField {
				selected = append(selected, string(value))
			}
			return true
		})
	})
	if selectionErr != nil {
		t.Fatal(selectionErr)
	}
	if len(selected) != 1 || selected[0] != "doc-1" {
		t.Fatalf("selected documents = %v, want [doc-1]", selected)
	}
}

func TestEncodeServesSortableDocument(t *testing.T) {
	directory := t.TempDir()
	generation := Generation{
		SegmentID:  2,
		SnapshotID: 3,
		Documents: []EncodeDocument{{
			Identifier: []byte("doc-1"),
			Fields: []EncodeField{
				{Name: "group", Value: []byte("g-a"), Sort: true},
				{Name: "name", Value: []byte("n-a"), Sort: true},
				{Name: "entity", Value: []byte("e-1"), Sort: true},
				{Name: "timestamp", Value: []byte{0x20, 0x01}, Sort: true},
				{Name: "sha", Value: []byte("sha-1"), Store: true},
			},
		}},
	}
	if encodeErr := Encode(directory, generation); encodeErr != nil {
		t.Fatal(encodeErr)
	}
	reader, openErr := Open(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() {
		if closeErr := reader.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	page, pageErr := reader.RepairTuplePage(context.Background(), RepairPageRequest{
		SortFields:   [repairSortFieldCount]string{"group", "name", "entity", "timestamp"},
		ProjectField: "sha",
		PageSize:     1,
	})
	if pageErr != nil {
		t.Fatal(pageErr)
	}
	if len(page) != 1 {
		t.Fatalf("page length = %d, want 1", len(page))
	}
	if string(page[0].Value) != "sha-1" {
		t.Fatalf("projected value = %q, want sha-1", page[0].Value)
	}
	wantSortValues := []string{"g-a", "n-a", "e-1", string([]byte{0x20, 0x01})}
	for valueIndex, want := range wantSortValues {
		if string(page[0].SortValues[valueIndex]) != want {
			t.Fatalf("sort value %d = %x, want %x", valueIndex, page[0].SortValues[valueIndex], want)
		}
	}
}

func TestNativeEncodePreservesEscapedSortValue(t *testing.T) {
	directory := t.TempDir()
	sortValue := []byte{0x01, 0xff, 0x5c, 0x7f, 0x5c}
	generation := Generation{
		SegmentID:  2,
		SnapshotID: 3,
		Documents: []EncodeDocument{{
			Identifier: []byte("doc-1"),
			Fields: []EncodeField{
				{Name: "group", Value: sortValue, Sort: true},
				{Name: "name", Value: []byte("n-a"), Sort: true},
				{Name: "entity", Value: []byte("e-1"), Sort: true},
				{Name: "timestamp", Value: []byte("t-1"), Sort: true},
				{Name: "sha", Value: []byte("sha-1"), Store: true},
			},
		}},
	}

	if encodeErr := Encode(directory, generation); encodeErr != nil {
		t.Fatal(encodeErr)
	}
	reader, openErr := Open(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() {
		if closeErr := reader.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	page, pageErr := reader.RepairTuplePage(context.Background(), RepairPageRequest{
		SortFields:   [repairSortFieldCount]string{"group", "name", "entity", "timestamp"},
		ProjectField: "sha",
		PageSize:     1,
	})
	if pageErr != nil {
		t.Fatal(pageErr)
	}
	if len(page) != 1 {
		t.Fatalf("page length = %d, want 1", len(page))
	}
	if len(page[0].SortValues) != repairSortFieldCount {
		t.Fatalf("sort value count = %d, want %d", len(page[0].SortValues), repairSortFieldCount)
	}
	if !bytes.Equal(page[0].SortValues[0], sortValue) {
		t.Fatalf("escaped sort value = %x, want %x", page[0].SortValues[0], sortValue)
	}
}

func TestNativeEncodeRoundTripsMultipleStoredAndDocValueChunks(t *testing.T) {
	const (
		documentCount    = 2100
		selectedDocument = 1024
		pageSize         = 173
	)
	documents := make([]EncodeDocument, 0, documentCount)
	for documentIndex := 0; documentIndex < documentCount; documentIndex++ {
		tagValue := []byte("other")
		if documentIndex == selectedDocument {
			tagValue = []byte("needle")
		}
		documents = append(documents, EncodeDocument{
			Identifier: []byte(fmt.Sprintf("doc-%04d", documentIndex)),
			Fields: []EncodeField{
				{Name: "payload", Value: []byte(fmt.Sprintf("payload-%04d", documentIndex)), Store: true},
				{Name: "tag", Value: tagValue, Index: true},
				{Name: "group", Value: []byte("g"), Sort: true},
				{Name: "name", Value: []byte(fmt.Sprintf("n-%04d", documentIndex)), Sort: true},
				{Name: "entity", Value: []byte("e"), Sort: true},
				{Name: "timestamp", Value: []byte("t"), Sort: true},
			},
		})
	}

	directory := t.TempDir()
	generation := Generation{SegmentID: 2, SnapshotID: 3, Documents: documents}
	if encodeErr := Encode(directory, generation); encodeErr != nil {
		t.Fatal(encodeErr)
	}
	reader, openErr := Open(directory)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() {
		if closeErr := reader.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})

	var storedFields []string
	walkErr := reader.VisitLiveDocuments(context.Background(), func(document StoredDocument) error {
		return document.VisitStoredFields(func(name string, value []byte) bool {
			storedFields = append(storedFields, name+"="+string(value))
			return true
		})
	})
	if walkErr != nil {
		t.Fatal(walkErr)
	}
	if len(storedFields) != documentCount*2 {
		t.Fatalf("stored field count = %d, want %d", len(storedFields), documentCount*2)
	}
	for documentIndex := 0; documentIndex < documentCount; documentIndex++ {
		wantIdentifier := fmt.Sprintf("_id=doc-%04d", documentIndex)
		if storedFields[documentIndex*2] != wantIdentifier {
			t.Fatalf("stored identifier at document %d = %q, want %q", documentIndex, storedFields[documentIndex*2], wantIdentifier)
		}
		wantPayload := fmt.Sprintf("payload=payload-%04d", documentIndex)
		if storedFields[documentIndex*2+1] != wantPayload {
			t.Fatalf("stored payload at document %d = %q, want %q", documentIndex, storedFields[documentIndex*2+1], wantPayload)
		}
	}

	var selected []string
	selectionErr := reader.VisitSelectedDocuments(context.Background(), "tag", [][]byte{[]byte("needle")}, func(document StoredDocument) error {
		return document.VisitStoredFields(func(name string, value []byte) bool {
			if name == identifierField {
				selected = append(selected, string(value))
			}
			return true
		})
	})
	if selectionErr != nil {
		t.Fatal(selectionErr)
	}
	wantSelected := fmt.Sprintf("doc-%04d", selectedDocument)
	if len(selected) != 1 || selected[0] != wantSelected {
		t.Fatalf("selected documents = %v, want [%s]", selected, wantSelected)
	}

	var after *RepairCursor
	repaired := make([]string, 0, documentCount)
	for {
		page, pageErr := reader.RepairTuplePage(context.Background(), RepairPageRequest{
			After:        after,
			SortFields:   [repairSortFieldCount]string{"group", "name", "entity", "timestamp"},
			ProjectField: "payload",
			PageSize:     pageSize,
		})
		if pageErr != nil {
			t.Fatal(pageErr)
		}
		if len(page) == 0 {
			break
		}
		for _, row := range page {
			repaired = append(repaired, string(row.Value))
		}
		if len(repaired) > documentCount {
			t.Fatalf("repair returned %d rows, want at most %d", len(repaired), documentCount)
		}
		nextCursor := page[len(page)-1].Cursor
		after = &nextCursor
	}
	if len(repaired) != documentCount {
		t.Fatalf("repair row count = %d, want %d", len(repaired), documentCount)
	}
	for documentIndex, payload := range repaired {
		wantPayload := fmt.Sprintf("payload-%04d", documentIndex)
		if payload != wantPayload {
			t.Fatalf("repair payload at row %d = %q, want %q", documentIndex, payload, wantPayload)
		}
	}
}
