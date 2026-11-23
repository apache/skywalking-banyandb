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
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

var errInvalidUTF8 = errors.New("string is not valid UTF-8")

// ReaderOptions configures NewReader.
type ReaderOptions struct {
	// Gzip tells that the input is gzip-compressed.
	Gzip bool
}

// Value is one decoded cell. Null is set for NullCell; otherwise the field matching the
// column type is set: Str, Int (int and timestamp columns), Float, Bytes, Strs or Ints.
// An empty binary cell decodes to a non-nil empty Bytes and an empty int array to a
// non-nil empty Ints, so both stay distinct from null.
type Value struct {
	Str   string
	Bytes []byte
	Strs  []string
	Ints  []int64
	Int   int64
	Float float64
	Null  bool
}

// Reader reads the records of one CSV file written by Writer for a layout. It checks the
// header, and the optional #datatype line before it, against the layout.
// A Reader is not safe for concurrent use.
type Reader struct {
	src      *fieldReader
	gz       *gzip.Reader
	layout   *Layout
	err      error
	row      []Value
	recordNo uint64
}

// NewReader reads and validates the prelude of a CSV file: an optional #datatype line,
// whose tokens must match the column types, followed by the header, whose cells must
// equal Layout.Header().
func NewReader(r io.Reader, layout *Layout, opts ReaderOptions) (*Reader, error) {
	rd := &Reader{layout: layout, row: make([]Value, len(layout.Columns))}
	in := r
	if opts.Gzip {
		gz, err := gzip.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("open gzip stream: %w", err)
		}
		rd.gz = gz
		in = gz
	}
	rd.src = newFieldReader(in)
	if err := readPrelude(rd.src.rawRecord, layout); err != nil {
		_ = rd.Close()
		return nil, err
	}
	return rd, nil
}

// readPrelude reads the optional #datatype line and the header with next, which returns
// the next raw record, and checks them against layout.
func readPrelude(next func() ([]string, error), layout *Layout) error {
	first, err := next()
	if errors.Is(err, io.EOF) {
		return errors.New("csv file has no header")
	}
	if err != nil {
		return fmt.Errorf("read csv header: %w", err)
	}
	datatype := layout.DatatypeLine()
	if len(first) > 0 && first[0] == datatype[0] {
		if err = compareCells("#datatype line", first, datatype); err != nil {
			return err
		}
		if first, err = next(); err != nil {
			if errors.Is(err, io.EOF) {
				return errors.New("csv file has no header after the #datatype line")
			}
			return fmt.Errorf("read csv header: %w", err)
		}
	}
	return compareCells("header", first, layout.Header())
}

func compareCells(what string, got, want []string) error {
	if len(got) != len(want) {
		return fmt.Errorf("csv %s has %d cells, the layout expects %d", what, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			return fmt.Errorf("csv %s cell %d is %q, the layout expects %q", what, i, got[i], want[i])
		}
	}
	return nil
}

// Next returns the next record, or io.EOF after the last one. The returned slice and its
// Values are overwritten by the next call, but the slices and strings the Values hold are
// never reused. Errors carry the record number and are sticky.
func (r *Reader) Next() ([]Value, error) {
	if r.err != nil {
		return nil, r.err
	}
	err := r.src.record(r.row, r.layout.Columns)
	if errors.Is(err, io.EOF) {
		r.err = io.EOF
		return nil, io.EOF
	}
	if err != nil {
		r.err = fmt.Errorf("record %d: %w", r.recordNo+1, err)
		return nil, r.err
	}
	r.recordNo++
	return r.row, nil
}

// RecordNo returns the 1-based number of the last data record returned by Next; records
// are counted, not physical lines, and the prelude is not counted.
func (r *Reader) RecordNo() uint64 {
	return r.recordNo
}

// Close releases the reader; Next fails afterwards. It does not close the underlying
// io.Reader. Read errors are reported by Next, never by Close.
func (r *Reader) Close() error {
	if r.err == nil || errors.Is(r.err, io.EOF) {
		r.err = errors.New("csv reader is closed")
	}
	if r.gz != nil {
		// The gzip reader only repeats the error Next already returned.
		_ = r.gz.Close()
		r.gz = nil
	}
	return nil
}

// decodeCell decodes cell as type t into v with the semantics of DecodeCell, and rejects
// strings that are not valid UTF-8.
func decodeCell(cell string, t ValueType, v *Value) error {
	*v = Value{}
	x, isNull, err := DecodeCell(cell, t)
	if err != nil {
		return err
	}
	if isNull {
		v.Null = true
		return nil
	}
	switch t {
	case ValueString:
		s := x.(string)
		if !utf8.ValidString(s) {
			return errInvalidUTF8
		}
		v.Str = s
	case ValueInt, ValueTimestamp:
		v.Int = x.(int64)
	case ValueFloat:
		v.Float = x.(float64)
	case ValueBinary:
		v.Bytes = x.([]byte)
	case ValueStringArray:
		v.Strs = x.([]string)
	case ValueIntArray:
		v.Ints = x.([]int64)
	}
	return nil
}

func columnError(c *Column, err error) error {
	return fmt.Errorf("column %q: %w", c.Header, err)
}
