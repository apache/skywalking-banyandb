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

package csvfmt

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
)

// SinkWriteSize is the maximum length of one Sink.Write call; on-disk bytes are buffered up
// to this size before being handed to the sink.
const SinkWriteSize = 256 << 10

var errWriterClosed = errors.New("csv writer is closed")

// Sink receives the files produced by a Writer.
type Sink interface {
	// StartFile announces a new file; seq starts at 1 and has no gaps.
	StartFile(relativePath string, seq uint32) error
	// Write receives bytes exactly as they land on disk (after gzip), at most SinkWriteSize
	// bytes per call. p is reused after Write returns and must not be retained.
	Write(p []byte) error
	// FinishFile seals the current file. size and crc (IEEE CRC-32) cover every byte passed
	// to Write for this file; rowCounts holds the per-resource row count of the file.
	FinishFile(size uint64, crc uint32, rowCounts map[string]uint64) error
}

// WriterOptions configures a Writer.
type WriterOptions struct {
	Layout *Layout
	// PathPrefix is e.g. "stream/sw_record/seg-20260907/shard-0"; the writer appends
	// "-%05d.csv" or "-%05d.csv.gz".
	PathPrefix string
	// MaxFileSize is the soft on-disk size limit of one file; 0 never rolls.
	MaxFileSize uint64
	Gzip        bool
	// Annotations writes a "#datatype" line before the header of every file.
	Annotations bool
}

// Writer writes one CSV stream, rolling to the next file after a complete row once the
// on-disk bytes of the current file reach MaxFileSize. Every file repeats the header.
// A Writer is not safe for concurrent use.
type Writer struct {
	sink      Sink
	err       error
	out       *diskWriter
	csvw      *csv.Writer
	fileRows  map[string]uint64
	gz        *gzip.Writer
	prelude   []byte
	opts      WriterOptions
	rowBuf    bytes.Buffer
	rows      uint64
	fileCount uint64
	// oversized counts the rows that got a file of their own for being larger than
	// MaxFileSize; largest is the encoded length of the largest of them.
	oversized uint64
	largest   uint64
	seq       uint32
	open      bool
	closed    bool
}

// NewWriter creates a Writer that emits files to sink.
func NewWriter(sink Sink, opts WriterOptions) *Writer {
	w := &Writer{sink: sink, opts: opts}
	w.csvw = csv.NewWriter(&w.rowBuf)
	return w
}

// WriteRow writes one row. cells must come from Layout.NewRow; cells[0] is overwritten
// with resource. Rows are never split across files: the file rolls after a row once its
// on-disk bytes reach MaxFileSize, and a row whose encoded length alone reaches
// MaxFileSize is written to a file of its own.
func (w *Writer) WriteRow(resource string, cells []string) error {
	if w.closed {
		return errWriterClosed
	}
	if w.err != nil {
		return w.err
	}
	if len(cells) != len(w.opts.Layout.Columns) {
		return fmt.Errorf("row has %d cells, layout has %d columns", len(cells), len(w.opts.Layout.Columns))
	}
	if !w.opts.Layout.hasResource(resource) {
		return fmt.Errorf("resource %q is not in the layout", resource)
	}
	cells[0] = resource
	row, err := w.encode(cells)
	if err != nil {
		return err
	}
	// An oversized row gets a file of its own. Its encoded length is checked because with
	// gzip most of it may still sit in the deflate buffer after it is written.
	oversized := w.opts.MaxFileSize > 0 && uint64(len(row)) >= w.opts.MaxFileSize
	if oversized && w.fileCount > 0 {
		if err = w.finishFile(); err != nil {
			return w.fail(err)
		}
	}
	if !w.open {
		if err = w.startFile(); err != nil {
			return w.fail(err)
		}
	}
	if err = w.writeFile(row); err != nil {
		return w.fail(err)
	}
	w.track(resource)
	if oversized {
		w.oversized++
		w.largest = max(w.largest, uint64(len(row)))
	}
	if oversized || w.opts.MaxFileSize > 0 && w.out.size >= w.opts.MaxFileSize {
		if err = w.finishFile(); err != nil {
			return w.fail(err)
		}
	}
	return nil
}

// Close finishes the open file. No file is produced when no row was ever written.
func (w *Writer) Close() error {
	if w.closed {
		return w.err
	}
	w.closed = true
	if w.err != nil {
		return w.err
	}
	if w.open {
		if err := w.finishFile(); err != nil {
			return w.fail(err)
		}
	}
	return nil
}

// Flush hands every byte of the open file written so far to the sink, including what the
// compressor still holds, so that a reader of the stream sees each row already written. A
// gzip flush ends the deflate block early, which costs a little compression. Flush does
// nothing without an open file.
func (w *Writer) Flush() error {
	if w.closed || w.err != nil || !w.open {
		return w.err
	}
	if w.opts.Gzip {
		if err := w.gz.Flush(); err != nil {
			return w.fail(fmt.Errorf("flush gzip of csv file %d: %w", w.seq, err))
		}
	}
	if err := w.out.flush(); err != nil {
		return w.fail(fmt.Errorf("write csv file %d: %w", w.seq, err))
	}
	return nil
}

// Rows returns the number of rows written so far.
func (w *Writer) Rows() uint64 {
	return w.rows
}

// Oversized returns how many rows got a file of their own for being larger than
// MaxFileSize, and the encoded length of the largest of them.
func (w *Writer) Oversized() (rows, largest uint64) {
	return w.oversized, w.largest
}

func (w *Writer) fail(err error) error {
	w.err = err
	return err
}

func (w *Writer) encode(record []string) ([]byte, error) {
	w.rowBuf.Reset()
	if err := w.csvw.Write(record); err != nil {
		return nil, fmt.Errorf("encode csv record: %w", err)
	}
	w.csvw.Flush()
	if err := w.csvw.Error(); err != nil {
		return nil, fmt.Errorf("encode csv record: %w", err)
	}
	return w.rowBuf.Bytes(), nil
}

func (w *Writer) startFile() error {
	w.seq++
	ext := ".csv"
	if w.opts.Gzip {
		ext = ".csv.gz"
	}
	if err := w.sink.StartFile(fmt.Sprintf("%s-%05d%s", w.opts.PathPrefix, w.seq, ext), w.seq); err != nil {
		return fmt.Errorf("start csv file %d: %w", w.seq, err)
	}
	w.open = true
	w.fileRows = make(map[string]uint64)
	if w.out == nil {
		w.out = &diskWriter{sink: w.sink, crc: crc32.NewIEEE()}
	}
	w.out.reset()
	if w.opts.Gzip {
		if w.gz == nil {
			w.gz = gzip.NewWriter(w.out)
		} else {
			w.gz.Reset(w.out)
		}
	}
	if w.prelude == nil {
		prelude, err := encodePrelude(w.opts)
		if err != nil {
			return err
		}
		w.prelude = prelude
	}
	return w.writeFile(w.prelude)
}

// encodePrelude encodes the lines that start every file: the optional #datatype line and
// the header.
func encodePrelude(opts WriterOptions) ([]byte, error) {
	var buf bytes.Buffer
	cw := csv.NewWriter(&buf)
	if opts.Annotations {
		if err := cw.Write(opts.Layout.DatatypeLine()); err != nil {
			return nil, fmt.Errorf("encode csv annotations: %w", err)
		}
	}
	if err := cw.Write(opts.Layout.Header()); err != nil {
		return nil, fmt.Errorf("encode csv header: %w", err)
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return nil, fmt.Errorf("encode csv header: %w", err)
	}
	return buf.Bytes(), nil
}

func (w *Writer) writeFile(p []byte) error {
	var dst io.Writer = w.out
	if w.opts.Gzip {
		dst = w.gz
	}
	if _, err := dst.Write(p); err != nil {
		return fmt.Errorf("write csv file %d: %w", w.seq, err)
	}
	return nil
}

func (w *Writer) finishFile() error {
	if w.opts.Gzip {
		if err := w.gz.Close(); err != nil {
			return fmt.Errorf("close gzip of csv file %d: %w", w.seq, err)
		}
	}
	if err := w.out.flush(); err != nil {
		return fmt.Errorf("write csv file %d: %w", w.seq, err)
	}
	w.open = false
	w.fileCount = 0
	if err := w.sink.FinishFile(w.out.size, w.out.crc.Sum32(), w.fileRows); err != nil {
		return fmt.Errorf("finish csv file %d: %w", w.seq, err)
	}
	return nil
}

func (w *Writer) track(resource string) {
	w.rows++
	w.fileCount++
	w.fileRows[resource]++
}

// diskWriter counts and checksums on-disk bytes and forwards them to the sink in batches.
type diskWriter struct {
	sink Sink
	crc  hash.Hash32
	buf  []byte
	size uint64
}

func (d *diskWriter) reset() {
	d.crc.Reset()
	d.buf = d.buf[:0]
	d.size = 0
}

func (d *diskWriter) Write(p []byte) (int, error) {
	d.size += uint64(len(p))
	_, _ = d.crc.Write(p)
	d.buf = append(d.buf, p...)
	if len(d.buf) >= SinkWriteSize {
		if err := d.flush(); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

func (d *diskWriter) flush() error {
	for start := 0; start < len(d.buf); start += SinkWriteSize {
		end := min(start+SinkWriteSize, len(d.buf))
		if err := d.sink.Write(d.buf[start:end]); err != nil {
			d.buf = d.buf[:0]
			return err
		}
	}
	d.buf = d.buf[:0]
	return nil
}
