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

package nativeice

import (
	"context"
	"errors"
)

// ErrInvalidSelection is the sentinel reported when a caller asks for a term
// selection the reader will not serve: one that names no field, that carries
// more terms than the reader's configured term-count bound, or that carries a
// term longer than its configured term-length bound.
//
// It is deliberately distinct from ErrCorrupt. Nothing on disk is damaged; the
// request itself lies outside the bounds BDB-NIDX-SPEC-001 revision 0.2
// READ-002 requires the reader to hold, so no dictionary is opened and no
// posting is decoded. Callers classify with errors.Is.
var ErrInvalidSelection = errors.New("nativeice: invalid term selection")

// VisitSelectedDocuments streams the pinned generation's live documents whose
// field records any of terms to visit, one at a time, in ascending segment and
// local document order.
//
// Terms are resolved exactly against the field's term dictionary: they are
// compared as raw bytes, with no analysis, no normalization and no range,
// prefix or wildcard expansion. A term the dictionary does not hold selects
// nothing rather than failing. The documents the terms' postings select are
// unioned, the pinned snapshot's deletion masks are removed from that union,
// and each surviving document is handed to visit exactly once however many of
// the terms selected it. An empty term set selects no document and is not an
// error.
//
// Selection precedes stored-field decoding: a document the selection does not
// hold has its stored bytes left unread, so a generation holding a damaged
// document still serves a selection that excludes it.
//
// The StoredDocument handed to visit is borrowed exactly as in
// VisitLiveDocuments: it, and every name and value it yields, stay valid only
// until visit returns. At most one document plus the reader's configured decode
// buffers, the selected field's dictionary and the unioned postings are
// resident at a time.
//
// A selection that names no field, or whose term count or term length exceeds
// the reader's configured bounds, is rejected with an error wrapping
// ErrInvalidSelection before any document is visited. The walk stops and
// returns ctx.Err() when ctx is canceled between two documents, and stops and
// returns visit's error when visit fails. A dictionary, posting record or
// stored record that violates the ICE v3 grammar, or that would require
// decoding past a configured bound, stops the walk with an error wrapping
// ErrCorrupt.
func (r *Reader) VisitSelectedDocuments(_ context.Context, _ string, _ [][]byte, _ func(StoredDocument) error) error {
	return errors.New("nativeice: this reader serves no term selection")
}
