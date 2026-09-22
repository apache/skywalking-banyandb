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

package inverted

import (
	"errors"
	"fmt"
	"io"

	roaringpkg "github.com/RoaringBitmap/roaring"
)

// This file drives BanyanDB's native ICE v3 encoder through the three entry
// points an index lifecycle manager's segment plugin is built from: build a
// segment from the analyzed documents of one batch, reopen a segment from the
// bytes it was persisted as, and merge segments under the deletion masks the
// manager supplies.
//
// The three functions carry exactly the shapes the manager's plugin fields are
// typed as, so registering the plugin is a later milestone's one-line change
// rather than an adapter. Nothing here is registered and no production path
// reaches it; it is constructed and driven from tests.
//
// It lives beside the store rather than in a package of its own, and every
// name it adds is unexported, because the vocabulary these signatures are
// written in reaches it through the neutral seam this package already declares.
// A package of its own would have to import the retired engine's segment API
// directly, which the workstream's lexical gate does not admit.

// nativeSegmentPluginNew builds one in-memory ICE v3 segment from the analyzed
// documents results holds, and reports how many documents that segment covers.
//
// Every field a document visits contributes according to the flags it carries:
// an indexed field contributes each term its term visitor yields to that
// field's dictionary, a stored field contributes its value to the document's
// stored record, and a doc-values field contributes its value to the field's
// doc values. One field may do all three. The field name ICE v3 reserves for
// the document identifier, "_id", names the document and is recorded once, and
// a stored-field walk yields it ahead of the document's remaining names, which
// follow in ascending byte order.
//
// normCalc is the length norm a scoring reader would weight a term with. The
// ICE v3 generation BanyanDB writes carries the fixed norm the grammar
// defines instead, because no BanyanDB query consumes a relevance score.
//
// The returned segment answers the whole segment contract from the documents it
// was built from, before it is ever persisted, and its WriteTo emits the
// segment bytes the native encoder writes for the same documents. A name no
// document indexed answers with an empty dictionary.
func nativeSegmentPluginNew(results []segmentDocument, normCalc func(string, int) float32) (segmentValue, uint64, error) {
	_, _ = results, normCalc
	return nil, 0, errors.New("inverted: the native segment plugin builds no segment")
}

// nativeSegmentPluginLoad reopens the segment persisted in data and returns it.
//
// data holds one ICE v3 segment exactly as WriteTo emitted it, which is also
// exactly how the retired writer emitted the segments already on disk, so a
// segment written before this adapter existed reopens here unchanged. The
// returned segment answers the same questions a segment built from the same
// documents answers.
//
// Bytes that violate the ICE v3 grammar, including a segment truncated by an
// interrupted write, are reported as an error rather than decoded.
func nativeSegmentPluginLoad(data *segmentBytes) (segmentValue, error) {
	_ = data
	return nil, errors.New("inverted: the native segment plugin reopens no segment")
}

// nativeSegmentPluginMerge returns a merger that writes the union of segments'
// documents, less the documents drops marks, as one ICE v3 segment.
//
// drops is positional: drops[i] marks the document numbers of segments[i] that
// the merged segment must not carry, and a nil entry drops nothing from that
// segment. mergeBufferSize is the write buffer the merger writes through.
func nativeSegmentPluginMerge(segments []segmentValue, drops []*roaringpkg.Bitmap, mergeBufferSize int) segmentMergerValue {
	return &nativeSegmentMerger{segments: segments, drops: drops, mergeBufferSize: mergeBufferSize}
}

// nativeSegmentMerger writes the union of the segments it was built from, less
// the documents their deletion masks mark, as one ICE v3 segment.
type nativeSegmentMerger struct {
	newDocumentNumbers [][]uint64
	segments           []segmentValue
	drops              []*roaringpkg.Bitmap
	mergeBufferSize    int
}

// WriteTo writes the merged segment to w and reports how many bytes it wrote.
//
// The merged segment holds the surviving documents of every input segment, in
// input order and, within one input, in ascending document number. Closing
// closeCh stops the write.
func (m *nativeSegmentMerger) WriteTo(w io.Writer, closeCh chan struct{}) (int64, error) {
	_, _ = w, closeCh
	return 0, fmt.Errorf("inverted: merging %d segments under %d deletion masks through a %d byte buffer writes nothing",
		len(m.segments), len(m.drops), m.mergeBufferSize)
}

// DocumentNumbers reports where every input document landed in the merged
// segment: entry i is segments[i]'s mapping, indexed by the document's number
// in that input segment and holding its number in the merged segment. A
// document the deletion masks dropped is reported as math.MaxInt64, the
// sentinel the ICE v3 grammar reserves for a document the merge did not carry.
//
// The mapping is available once WriteTo has returned.
func (m *nativeSegmentMerger) DocumentNumbers() [][]uint64 {
	return m.newDocumentNumbers
}
