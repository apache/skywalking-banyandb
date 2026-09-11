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
	"errors"
	"fmt"
)

// identifierField is the name ICE v3 reserves for the document identifier, the
// field every segment records first. Encode writes each document's Identifier
// under this name, so a walk yields it ahead of the document's other stored
// values and a selection resolves it like any other exact term.
const identifierField = "_id"

// ErrInvalidGeneration is the sentinel reported when a caller asks Encode to
// write a generation the ICE v3 grammar has no representation for: a document
// carrying no identifier, or a field carrying no name. It is deliberately
// distinct from ErrCorrupt -- nothing on disk is damaged, and nothing on disk
// is touched, because the request is rejected before the first byte is written.
// Callers classify with errors.Is.
var ErrInvalidGeneration = errors.New("nativeice: invalid generation")

// errEncodeIncomplete reports that Encode produced no bytes for the requested
// generation. It is the scaffolding the contract's tests run against and the
// first complete Encode replaces it.
var errEncodeIncomplete = errors.New("nativeice: generation encoder wrote no bytes")

// EncodeField is one value a document contributes to a generation. The same
// name may appear on several fields of one document; each contributes its own
// value, and Encode preserves them in the order the document lists them.
//
// Value is an opaque byte sequence. Encode applies no analysis, no
// normalization and no numeric coding to it, so a term selects, sorts and
// reads back as exactly the bytes the caller supplied -- which is the same
// contract Reader.VisitSelectedDocuments resolves terms under.
type EncodeField struct {
	// Name is the segment field name the value is recorded under.
	Name string
	// Value is the field's raw bytes.
	Value []byte
	// Index records Value as a term in Name's dictionary, so a selection on
	// Name and Value reaches the document.
	Index bool
	// Store records Value as a stored value, so a document walk yields it.
	Store bool
	// Sort records Value as a doc value, so a repair page may sort on Name.
	Sort bool
}

// EncodeDocument is one physical document of a generation.
type EncodeDocument struct {
	// Fields are the document's values, in the order the document records them.
	Fields []EncodeField
	// Identifier is the document's identity. Encode records it as an indexed,
	// stored value under the reserved identifier field.
	Identifier []byte
	// Deleted marks the document as covered by the generation's deletion masks.
	// It still occupies a document number in the segment, and it is absent from
	// every count, walk, selection and repair page the generation serves.
	Deleted bool
}

// Generation is one committed ICE v3 generation: the documents it holds and the
// identifiers its segment and snapshot manifest are numbered by.
type Generation struct {
	// Documents are the generation's physical documents, in the order they take
	// document numbers in the segment.
	Documents []EncodeDocument
	// SegmentID numbers the segment file the generation writes.
	SegmentID uint64
	// SnapshotID numbers the snapshot manifest that publishes the generation,
	// and is the identifier Reader.SnapshotID reports once it is opened.
	SnapshotID uint64
}

// Encode writes generation into the index directory at path as one committed
// ICE v3 generation -- a segment holding every document, and a snapshot
// manifest referencing that segment and carrying its deletion masks -- and
// leaves the directory's existing generations untouched. Open selects the
// written generation when its snapshot identifier is the directory's newest.
//
// The generation is published by its snapshot manifest, so the segment is
// complete on disk before any manifest names it. A write interrupted before
// publication therefore leaves the directory's committed state exactly as it
// was: Open still selects the newest generation that was published, or reports
// ErrNoSnapshot when none ever was.
//
// Every field a document marks Index contributes its value as a term to that
// field's dictionary, every field it marks Store contributes a stored value,
// and every field it marks Sort contributes a doc value; one field may do all
// three. Within a document, a walk yields the identifier's stored value first
// and the remaining names in ascending byte order, with the values of a
// repeated name consecutive and in the order the document lists them.
//
// The snapshot manifest carries a calculated CRC32, because the pinned
// compatibility reader validates it: that reader's default configuration turns
// snapshot CRC validation on, and it rejects a manifest whose trailing four
// bytes are not the IEEE CRC32 of everything before them. The segment footer's
// CRC32 field is a different case and stays reserved -- no reader validates it,
// so Encode writes the field without calculating it.
//
// A generation whose documents the ICE v3 grammar cannot represent is rejected
// with an error wrapping ErrInvalidGeneration before the directory is touched.
func Encode(path string, generation Generation) error {
	return fmt.Errorf("encode %d documents as generation %d into %q: %w",
		len(generation.Documents), generation.SnapshotID, path, errEncodeIncomplete)
}
