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

// SnapshotID returns the identifier of the generation pinned at open time.
func (g *ReadOnlyGeneration) SnapshotID() uint64 {
	if g == nil || g.reader == nil {
		return 0
	}
	return g.reader.SnapshotID()
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
