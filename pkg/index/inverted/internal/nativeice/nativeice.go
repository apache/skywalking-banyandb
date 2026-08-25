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

// Package nativeice reads the ICE v3 segment and snapshot v3 manifest grammar
// defined by BDB-NIDX-SPEC-001 revision 0.2 sections 08 and 09, using only
// BanyanDB code. It is the bounded read-only container reader that the
// read-only production paths in pkg/index/inverted open committed index
// directories through, and it never depends on Bluge or ICE.
//
// The package is deliberately reachable only from pkg/index/inverted. Footer,
// offset, mapping, and section decoder types are private to it; the contract
// other packages observe is the behavior of the exported functions in
// pkg/index/inverted that call it.
package nativeice

import (
	"errors"
	"fmt"
)

// ErrCorrupt is the sentinel that every structural rejection wraps: a footer,
// record framing, count, length, or section offset that violates the ICE v3 or
// snapshot v3 grammar, or that would require reading or allocating past a
// configured bound. Callers classify with errors.Is.
var ErrCorrupt = errors.New("nativeice: corrupt index")

// ErrNoSnapshot is the sentinel reported when a directory holds no committed
// generation at all: it is absent, empty, or was never flushed. It is distinct
// from ErrCorrupt because nothing is damaged -- there is simply nothing
// committed to read.
var ErrNoSnapshot = errors.New("nativeice: no committed snapshot")

// errUnwired is scaffolding with an enforced expiry: a read path returns it
// only while it has no decoder behind it, and giving that path a decoder
// removes its reference. The unused linter fails the build on the last
// reference going away, so this variable cannot outlive the scaffolding.
var errUnwired = errors.New("native reader has no decoder installed")

// Reader is a bounded read-only handle on exactly one generation of an index
// directory. The generation is chosen at Open and fixed for the Reader's
// lifetime, so generations committed afterwards stay invisible to it.
type Reader struct{}

// Open selects the newest committed generation in the index directory at path,
// validates its snapshot manifest and every segment that manifest references
// against the grammar and the reader's configured bounds, and returns a Reader
// pinned to that generation.
//
// Open takes no exclusive directory lock and creates, removes, or modifies no
// file, so a directory a live writer owns can be inspected while it is being
// written. Directory entries outside the grammar, the writer lock file among
// them, are ignored. Snapshot and segment identifiers are numbered
// independently, so the newest generation is chosen by decoding the manifest
// rather than by pairing file names.
//
// A directory holding no committed generation reports an error wrapping
// ErrNoSnapshot. Bytes that violate the grammar, including a manifest whose
// referenced segment fails validation, report an error wrapping ErrCorrupt;
// Open reports that rather than falling back to an older generation.
func Open(path string) (*Reader, error) {
	return nil, fmt.Errorf("nativeice: open %q: %w", path, errUnwired)
}

// VisibleDocCount returns the number of live documents in the pinned
// generation: the document counts the manifest records for the segments it
// references, less the documents those segments' deletion masks mark.
func (r *Reader) VisibleDocCount() (int64, error) {
	return 0, fmt.Errorf("nativeice: visible doc count: %w", errUnwired)
}

// Close releases the file handles and mappings the Reader pinned at Open.
func (r *Reader) Close() error {
	return nil
}
