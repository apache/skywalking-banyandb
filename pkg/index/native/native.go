// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package native provides bounded, committed-generation read-only operations.
package native

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

const (
	identifierField = "_id"
	seriesIDField   = "_series_id"
	timestampField  = "_timestamp"
	versionField    = "_version"
)

var errStopVisit = errors.New("native: stop visit")

// ErrNoSnapshot reports an index directory with no committed generation.
var ErrNoSnapshot = nativeice.ErrNoSnapshot

// ErrCorrupt reports malformed committed-generation bytes.
var ErrCorrupt = nativeice.ErrCorrupt

// ErrInvalidSelection reports that a TermSelection names no field, or that its
// term count or one of its term lengths exceeds the bound the read-only reader
// serves. It is distinct from ErrCorrupt: nothing on disk is damaged, the
// request itself is outside the reader's bounds, so no dictionary is opened and
// no posting is decoded.
var ErrInvalidSelection = nativeice.ErrInvalidSelection

// ReadOnlyGeneration owns one immutable native ICE generation. The selected
// snapshot is fixed when it is opened; later files published in the directory
// are invisible until another generation is opened.
type ReadOnlyGeneration struct {
	reader *nativeice.Reader
}

// OpenReadOnlyGeneration opens the newest structurally complete committed
// generation without creating or modifying files. ErrNoSnapshot is returned
// when the directory has no committed generation, and ErrCorrupt is returned
// when committed candidates are malformed.
func OpenReadOnlyGeneration(path string) (*ReadOnlyGeneration, error) {
	reader, err := nativeice.Open(path)
	if err != nil {
		return nil, err
	}
	return &ReadOnlyGeneration{reader: reader}, nil
}

// OpenReadOnlyGenerationStrict opens only the newest committed snapshot in
// path, like a writer reopening the directory at startup, and never falls
// back to an older generation when that newest manifest or one of its
// segments is damaged: it returns ErrCorrupt (or another decode error)
// instead. Use this -- instead of the lenient OpenReadOnlyGeneration, which
// deliberately recovers the newest structurally complete generation for
// best-effort offline tools -- wherever a silent rollback to stale data
// would be a correctness bug, for example reading a closed segment's
// acknowledged generation for a backup or a doc-count report: a corrupt
// newest manifest must fail loudly there rather than make the backup (or
// the live owner, which already opens strict) disagree about which
// generation is current.
func OpenReadOnlyGenerationStrict(path string) (*ReadOnlyGeneration, error) {
	reader, err := nativeice.OpenStrict(path)
	if err != nil {
		return nil, err
	}
	return &ReadOnlyGeneration{reader: reader}, nil
}

// SnapshotID returns the identifier of the generation pinned at open time.
func (g *ReadOnlyGeneration) SnapshotID() uint64 {
	if g == nil || g.reader == nil {
		return 0
	}
	return g.reader.SnapshotID()
}

// ReferencedFiles returns the on-disk file names (relative to the index
// directory, not full paths) the pinned generation's manifest references:
// its own snapshot manifest file and every segment file it lists, in the
// same "%012x.snp" / "%012x.seg" naming Encode and the owner's persister
// write. A caller that must copy or hard-link only the current generation --
// for example snapshotting a closed (cold) index directory -- uses this to
// exclude superseded snapshots and segments a live owner has not yet
// garbage collected, instead of copying the whole directory.
func (g *ReadOnlyGeneration) ReferencedFiles() []string {
	if g == nil || g.reader == nil {
		return nil
	}
	metadata := g.reader.SnapshotMetadata()
	files := make([]string, 0, len(metadata.Segments)+1)
	files = append(files, nativeICEFileName(metadata.ID, ".snp"))
	for _, segment := range metadata.Segments {
		files = append(files, nativeICEFileName(segment.ID, ".seg"))
	}
	return files
}

// nativeICEFileName reproduces the on-disk name Encode/PublishSnapshot gives
// one manifest or segment file. It is duplicated here (rather than exported
// from pkg/index/internal/nativeice, an internal package pkg/index/native
// already depends on) because owner.go's own TakeFileSnapshot fallback path
// already relies on this exact "%012x" format for a segment's source path.
func nativeICEFileName(identifier uint64, extension string) string {
	return fmt.Sprintf("%012x%s", identifier, extension)
}

// StoredFields returns the first live physical document whose identifier is
// docID. Stored values are copied before the borrowed native document is
// released. Internal bookkeeping fields are always omitted; projection names
// are applied to the remaining fields. A missing identifier returns nil, nil.
func (g *ReadOnlyGeneration) StoredFields(ctx context.Context, docID []byte, projection ...string) (map[string][][]byte, error) {
	if g == nil || g.reader == nil {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	wanted := projectionSet(projection)
	var result map[string][][]byte
	var callbackErr error
	err := g.reader.VisitSelectedDocuments(ctx, identifierField, [][]byte{bytes.Clone(docID)}, func(document nativeice.StoredDocument) error {
		if callbackErr = ctx.Err(); callbackErr != nil {
			return callbackErr
		}
		result = make(map[string][][]byte)
		visitErr := document.VisitStoredFields(func(name string, value []byte) bool {
			if callbackErr = ctx.Err(); callbackErr != nil {
				return false
			}
			if internalField(name) || (wanted != nil && !wanted[name]) {
				return true
			}
			result[name] = append(result[name], bytes.Clone(value))
			return true
		})
		if visitErr != nil {
			return visitErr
		}
		if callbackErr != nil {
			return callbackErr
		}
		return errStopVisit
	})
	if errors.Is(err, errStopVisit) {
		return result, nil
	}
	if callbackErr != nil {
		return nil, callbackErr
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// VisibleDocCount returns the number of live (non-deleted) documents the
// pinned generation holds. It is the read-only counterpart to Owner.Stats
// for a closed/cold index directory: callers that need a document count
// without reopening a writable owner (for example, reporting a segment's
// series-index size while it is idle-closed) open a ReadOnlyGeneration and
// call this instead of reopening the owner.
func (g *ReadOnlyGeneration) VisibleDocCount() (int64, error) {
	if g == nil || g.reader == nil {
		return 0, nil
	}
	return g.reader.VisibleDocCount()
}

// DecodeTimestamp decodes one series/property document's stored "_timestamp"
// field value, in the same prefix-coded int64 layout the owner's encoder
// writes (newMemorySegment) and ProjectHit already decodes internally. It is
// exported for offline tools that walk stored documents directly (via
// VisitLiveDocuments/StoredFields) instead of going through ProjectHit, so
// they never need their own encoding of this reserved field.
func DecodeTimestamp(value []byte) (int64, error) {
	return nativeice.DecodePrefixCodedInt64(value)
}

// StoredDocument is one live physical document of a committed generation,
// borrowed for the duration of a single VisitLiveDocuments callback. It
// mirrors nativeice.StoredDocument's shape without exposing that internal
// package to callers outside pkg/index.
type StoredDocument interface {
	// VisitStoredFields calls visit once for every stored value the document
	// records, passing the field's name and its raw value bytes, in the
	// order the document recorded them. The name and value handed to visit
	// are borrowed and stay valid only until visit returns.
	VisitStoredFields(visit func(name string, value []byte) bool) error
}

// VisitLiveDocuments calls visit once for every live document the pinned
// generation holds, streaming one document at a time. Documents the
// generation's deletion masks cover are skipped. Canceling ctx stops the
// walk between two documents and returns ctx.Err(); an error visit returns
// stops the walk and is returned as-is.
//
// This is the read-only counterpart to a writable owner's full contents that
// offline tools use instead of opening one: the union-sidx builder and the
// index-mode Measure copy both need to re-emit every live series document
// through EncodeSeriesDocument without acquiring the exclusive directory
// ownership a native.Owner requires.
func (g *ReadOnlyGeneration) VisitLiveDocuments(ctx context.Context, visit func(doc StoredDocument) error) error {
	if g == nil || g.reader == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return g.reader.VisitLiveDocuments(ctx, func(doc nativeice.StoredDocument) error {
		return visit(doc)
	})
}

// TermSelection is the one bounded document filter a read-only walk accepts:
// the documents whose Field records any of the literal byte sequences in Terms.
//
// It is deliberately not a query language. There is exactly one field, the
// terms are matched as raw bytes with no analysis or normalization, and they
// are unioned. There is no range, prefix, wildcard, negation, conjunction,
// existence test, scoring, projection or ordering, and the term dictionary
// and postings that resolve a selection stay private to the reader.
type TermSelection struct {
	// Field is the name of the indexed field whose term dictionary the
	// selection resolves against.
	Field string
	// Terms are the literal term byte sequences to select. The documents
	// selected are the union of these terms' postings; an empty Terms selects
	// no document.
	Terms [][]byte
}

// VisitSelectedDocuments calls visit once for every live document of the
// pinned generation the selection holds, streaming one document at a time.
//
// The selection resolves exact terms against one field's dictionary, unions
// their postings and removes the pinned generation's deletion masks, so a
// deleted document is never handed to visit however many terms selected it,
// and a document several terms select is handed to visit once. A term the
// dictionary does not hold, and an empty term set, select nothing rather than
// failing. Selection precedes stored-field decoding, so a document the
// selection excludes has its stored bytes left unread.
//
// A selection naming no field, or exceeding the reader's term-count or
// term-length bounds, reports an error wrapping ErrInvalidSelection before any
// document is visited. Canceling ctx stops posting decode, posting union or
// the read between two documents and returns ctx.Err(); an error from visit
// stops the read and is returned as-is.
func (g *ReadOnlyGeneration) VisitSelectedDocuments(ctx context.Context, selection TermSelection, visit func(doc StoredDocument) error) error {
	if g == nil || g.reader == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return g.reader.VisitSelectedDocuments(ctx, selection.Field, selection.Terms, func(doc nativeice.StoredDocument) error {
		return visit(doc)
	})
}

// ReadOnlyDocCount opens the index directory at path read-only and returns the
// number of indexed (live, non-deleted) documents. Unlike a writable owner it
// never acquires exclusive directory ownership, so it can inspect a closed (or
// even concurrently open) index without reopening its writer. A missing or
// unflushed index (no usable committed generation) returns a count of 0
// together with the open error, which callers may treat as an empty index.
func ReadOnlyDocCount(path string) (int64, error) {
	generation, err := OpenReadOnlyGeneration(path)
	if err != nil {
		return 0, fmt.Errorf("open read-only index %q: %w", path, err)
	}
	defer func() { _ = generation.Close() }()
	return generation.VisibleDocCount()
}

// ReadOnlyWalkDocuments opens the index directory at path read-only, pins its
// newest structurally complete committed generation and calls visit once for
// every live document that generation holds, streaming one document at a time.
// Documents the pinned generation's deletion masks cover are skipped.
//
// It never acquires exclusive directory ownership and writes no bytes, so a
// directory a live owner holds can be walked while it is being written, and
// the walk leaves file contents, modification times and directory entries
// unchanged.
//
// A directory holding no committed generation reports an error wrapping
// ErrNoSnapshot, which callers that treat a cold or unflushed index as empty
// match on. Committed bytes that violate the on-disk grammar report an error
// wrapping ErrCorrupt. Canceling ctx stops the walk between two documents and
// returns ctx.Err(); an error from visit stops the walk and is returned as-is.
func ReadOnlyWalkDocuments(ctx context.Context, path string, visit func(doc StoredDocument) error) error {
	generation, err := OpenReadOnlyGeneration(path)
	if err != nil {
		return fmt.Errorf("open read-only index %q: %w", path, err)
	}
	defer func() { _ = generation.Close() }()
	return generation.VisitLiveDocuments(ctx, visit)
}

// ReadOnlySelectDocuments opens the index directory at path read-only, pins its
// newest structurally complete committed generation and calls visit once for
// every live document of that generation the selection holds, streaming one
// document at a time. See ReadOnlyGeneration.VisitSelectedDocuments for the
// selection semantics, and ReadOnlyWalkDocuments for the open/error contract.
func ReadOnlySelectDocuments(ctx context.Context, path string, selection TermSelection, visit func(doc StoredDocument) error) error {
	generation, err := OpenReadOnlyGeneration(path)
	if err != nil {
		return fmt.Errorf("open read-only index %q: %w", path, err)
	}
	defer func() { _ = generation.Close() }()
	return generation.VisitSelectedDocuments(ctx, selection, visit)
}

// VisitIdentifiers visits each identifier term in committed segment order.
// Terms from deleted documents are intentionally included because this seam is
// metadata enumeration rather than a live query hit walk. The identifier
// passed to visit is copied and remains valid after the callback. Returning
// false stops the walk without error; global lexical ordering across segments
// is not promised.
func (g *ReadOnlyGeneration) VisitIdentifiers(ctx context.Context, visit func([]byte) bool) error {
	if g == nil || g.reader == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var callbackErr error
	err := g.reader.VisitTerms(ctx, identifierField, func(identifier []byte) bool {
		if callbackErr = ctx.Err(); callbackErr != nil {
			return false
		}
		return visit(identifier)
	})
	if callbackErr != nil {
		return callbackErr
	}
	return err
}

// Close releases the pinned generation. It is idempotent and must not be
// called concurrently with another operation on the same generation.
func (g *ReadOnlyGeneration) Close() error {
	if g == nil || g.reader == nil {
		return nil
	}
	return g.reader.Close()
}

func projectionSet(projection []string) map[string]bool {
	if len(projection) == 0 {
		return nil
	}
	wanted := make(map[string]bool, len(projection))
	for _, field := range projection {
		wanted[field] = true
	}
	return wanted
}

func internalField(name string) bool {
	switch name {
	case identifierField, seriesIDField, timestampField, versionField:
		return true
	default:
		return false
	}
}
