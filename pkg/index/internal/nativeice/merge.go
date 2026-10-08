// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	roaringpkg "github.com/RoaringBitmap/roaring"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

// DroppedDocumentNumber marks an input physical document removed by its
// snapshot deletion mask or an external merge drop mask.
const DroppedDocumentNumber = math.MaxUint64

// MergeInput is one single-segment reader participating in a native merge.
// IndexedFields carries modality information that ICE deliberately does not
// encode when an indexed field has an empty term set; callers should provide
// those names from the segment owner. Reader deletion masks are always applied
// in addition to Drop.
type MergeInput struct {
	Reader        *Reader
	Drop          *roaringpkg.Bitmap
	IndexedFields []string
}

// MergeResult contains an in-memory encoded merged segment. Input readers
// remain owned by the caller and are not closed.
type MergeResult struct {
	Payload []byte
}

// MergeStats describes a segment written by MergeSegmentsTo or
// MergeSegmentsToFile.
type MergeStats struct {
	// DocumentCount is the merged segment's physical document count: every
	// input document that neither its snapshot deletion mask nor its Drop
	// mask removed, numbered in input order.
	DocumentCount uint64
	// Size is the number of segment bytes written.
	Size uint64
}

// MergeSegments merges native single-segment readers into an in-memory
// segment. It is the buffered form of MergeSegmentsTo and holds the whole
// output in memory, so it is meant for small merges and owners without a
// directory; MergeSegmentsToFile keeps merge memory independent of the
// merged segment's size.
func MergeSegments(ctx context.Context, inputs []MergeInput) (MergeResult, error) {
	var output bytes.Buffer
	if _, mergeErr := MergeSegmentsTo(ctx, inputs, &output, ""); mergeErr != nil {
		return MergeResult{}, mergeErr
	}
	return MergeResult{Payload: output.Bytes()}, nil
}

// MergeSegmentsToFile streams a merge of inputs into a new segment file at
// path, fsyncs it and closes it. Temporary spill files are created beside it
// under path's name with a ".spill-" suffix. On any failure, including a
// panic, the partial output and every spill file are removed, so a failed
// merge never leaves a file behind.
func MergeSegmentsToFile(ctx context.Context, inputs []MergeInput, path string) (stats MergeStats, err error) {
	file, createErr := segmentFileSystem.CreateFile(path, 0o600)
	if createErr != nil {
		return MergeStats{}, fmt.Errorf("create merged segment %q: %w", path, createErr)
	}
	// The merged segment is read back as soon as it is published, so keep its
	// pages cached instead of dropping them after the final fsync.
	fs.SetCached(file, true)
	completed := false
	defer func() {
		if completed {
			return
		}
		if recovered := recover(); recovered != nil {
			_ = file.Close()
			_ = segmentFileSystem.DeleteFile(path)
			panic(recovered)
		}
		err = errors.Join(err, file.Close(), segmentFileSystem.DeleteFile(path))
	}()
	writer := file.SequentialWrite()
	stats, err = MergeSegmentsTo(ctx, inputs, writer, path)
	// Closing the sequential writer flushes and fsyncs the file.
	if closeErr := writer.Close(); err == nil && closeErr != nil {
		err = fmt.Errorf("flush merged segment %q: %w", path, closeErr)
	}
	if err != nil {
		return MergeStats{}, err
	}
	if closeErr := file.Close(); closeErr != nil {
		_ = segmentFileSystem.DeleteFile(path)
		completed = true
		return MergeStats{}, fmt.Errorf("close merged segment %q: %w", path, closeErr)
	}
	completed = true
	return stats, nil
}

// MergeSegmentsTo merges native single-segment readers without involving a
// query engine and streams the merged segment to output in one forward pass.
// Stored repeated values, analyzed terms and frequencies, and repeated doc
// values are retained. Documents are emitted in input order, and context
// cancellation is checked between documents, chunks and terms.
//
// The ICE v3 layout is strictly sequential, so every section is written as
// soon as it is complete: stored chunks as each fills, doc values one chunk at
// a time, and each field's postings in term order while the inputs' term
// dictionaries are k-way merged. Only sections whose length must precede
// them -- the per-document stored offset index and each field's term
// dictionary -- are staged, in buffers that spill to files named
// spillPrefix+".spill-N" once they outgrow a fixed bound. An empty
// spillPrefix keeps them in memory. Merge memory is therefore bounded by
// chunk buffers, one term's posting bitmap, the dictionary builder, and
// per-field and per-input metadata, not by the merged segment's size.
//
// The output is byte-identical to encoding the merged documents with
// EncodeSegment. Input readers remain owned by the caller and are not closed.
func MergeSegmentsTo(ctx context.Context, inputs []MergeInput, output io.Writer, spillPrefix string) (MergeStats, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return MergeStats{}, ctxErr
	}
	// Admit the merge on every input: an input closed while the merge runs
	// keeps its files open until the merge ends, and is released then. Its
	// release error is the input's (see Reader.WhenReleased), never the
	// merge's.
	admitted := make([]*Reader, 0, len(inputs))
	defer func() {
		for _, reader := range admitted {
			reader.endUse()
		}
	}()
	for _, input := range inputs {
		if input.Reader == nil {
			continue
		}
		if useErr := input.Reader.use(); useErr != nil {
			return MergeStats{}, useErr
		}
		admitted = append(admitted, input.Reader)
	}
	merger := acquireStreamMerger(ctx, output, spillPrefix)
	defer releaseStreamMerger(merger)
	//nolint:contextcheck // posting decodes are bounded; cancellation is checked between terms and documents.
	if prepareErr := merger.prepare(inputs); prepareErr != nil {
		return MergeStats{}, prepareErr
	}
	//nolint:contextcheck // posting decodes are bounded; cancellation is checked between terms and documents.
	if writeErr := merger.write(); writeErr != nil {
		return MergeStats{}, writeErr
	}
	return MergeStats{DocumentCount: merger.documentCount, Size: merger.output.written}, nil
}

// countingWriter tracks the running segment offset every section records.
type countingWriter struct {
	writer  io.Writer
	scratch [8]byte
	written uint64
}

func (w *countingWriter) Write(data []byte) (int, error) {
	written, writeErr := w.writer.Write(data)
	w.written += uint64(written)
	if writeErr == nil && written != len(data) {
		writeErr = io.ErrShortWrite
	}
	return written, writeErr
}

func (w *countingWriter) writeUvarint(value uint64) error {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(encoded[:], value)
	_, writeErr := w.Write(encoded[:length])
	return writeErr
}

func (w *countingWriter) writeUint64(value uint64) error {
	_, writeErr := w.Write(appendNativeUint64(w.scratch[:0], value))
	return writeErr
}

// streamMergerPool keeps merge working sets between merges, so a steady
// stream of merges reuses one set of section buffers instead of regrowing
// them each time.
var streamMergerPool = pool.Register[*streamMerger]("nativeice-stream-merger")

// maxPooledMergeBuffer bounds the capacity a pooled merger keeps per buffer.
const maxPooledMergeBuffer = 1 << 20

func acquireStreamMerger(ctx context.Context, output io.Writer, spillPrefix string) *streamMerger {
	merger := streamMergerPool.Get()
	if merger == nil {
		merger = &streamMerger{}
	}
	merger.ctx = ctx
	merger.output = &countingWriter{writer: output}
	merger.spill = spillFactory{prefix: spillPrefix}
	merger.inputs, merger.fields, merger.fieldIDs = nil, nil, nil
	merger.documentCount, merger.storedIndex = 0, 0
	return merger
}

// releaseStreamMerger removes every spill file of the merge and returns the
// merger, minus inputs and oversized buffers, to the pool.
func releaseStreamMerger(merger *streamMerger) {
	for _, buffer := range merger.staged {
		_ = buffer.reset()
		if cap(buffer.memory) > maxPooledMergeBuffer {
			buffer.memory = nil
		}
	}
	trim := func(buffer []byte) []byte {
		if cap(buffer) > maxPooledMergeBuffer {
			return nil
		}
		return buffer[:0]
	}
	merger.decodedChunk, merger.encodedChunk = trim(merger.decodedChunk), trim(merger.encodedChunk)
	merger.docValueBuf, merger.docValueOut = trim(merger.docValueBuf), trim(merger.docValueOut)
	merger.frequencyRaw, merger.frequencyOut = trim(merger.frequencyRaw), trim(merger.frequencyOut)
	merger.storedMeta = trim(merger.storedMeta)
	if merger.dictionary != nil {
		merger.dictionary.arena = trim(merger.dictionary.arena)
		merger.dictionary.builder = nil
	}
	merger.frequencies = frequencyCursor{offsets: merger.frequencies.offsets[:0]}
	if merger.postings != nil {
		merger.postings.Clear()
	}
	if merger.covered != nil {
		merger.covered.Clear()
	}
	clear(merger.storedValues[:cap(merger.storedValues)])
	clear(merger.storedFields[:cap(merger.storedFields)])
	clear(merger.termDocuments[:cap(merger.termDocuments)])
	merger.ctx, merger.output, merger.inputs, merger.fields, merger.fieldIDs = nil, nil, nil, nil, nil
	streamMergerPool.Put(merger)
}
