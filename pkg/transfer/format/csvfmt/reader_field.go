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
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

const (
	// csvReadBuffer is the size of the chunks a long base64 cell is decoded into.
	csvReadBuffer = 1 << 20
	// fieldSourceBuffer is the input window of the field reader. A bare cell that does not
	// fit in it is long: binary and `\B` cells are then decoded while streaming.
	fieldSourceBuffer = 64 << 10
	// maxRetainedScratch bounds the scratch buffer kept between cells.
	maxRetainedScratch = 4 << 20
)

var (
	errBareQuote = errors.New(`bare " in non-quoted field`)
	errQuote     = errors.New(`extraneous or missing " in quoted field`)
)

type fieldTerm uint8

const (
	termComma fieldTerm = iota
	termEOL
	termEOF
)

// fieldReader is a byte state machine that accepts exactly what encoding/csv accepts with
// its default settings: quoted fields with commas, doubled quotes and line breaks, "\r\n"
// line endings (also inside quoted fields, where they become "\n"), empty fields and
// skipped empty lines. Unlike encoding/csv it does not materialize long base64 cells:
// binary and `\B` string cells are decoded straight from the input window.
type fieldReader struct {
	rd      io.Reader
	err     error // sticky read error, io.EOF at the end of the input
	buf     []byte
	scratch []byte
	raw     []string
	r, w    int
}

func newFieldReader(r io.Reader) *fieldReader {
	return &fieldReader{rd: r, buf: make([]byte, fieldSourceBuffer)}
}

// fill compacts the window and reads more input. It reports whether bytes were added.
func (f *fieldReader) fill() bool {
	if f.err != nil {
		return false
	}
	if f.r > 0 {
		copy(f.buf, f.buf[f.r:f.w])
		f.w -= f.r
		f.r = 0
	}
	if f.w == len(f.buf) {
		return false
	}
	for range 100 {
		n, err := f.rd.Read(f.buf[f.w:])
		f.w += n
		if err != nil {
			f.err = err
			return n > 0
		}
		if n > 0 {
			return true
		}
	}
	f.err = io.ErrNoProgress
	return false
}

// readErr returns the sticky read error unless it is io.EOF.
func (f *fieldReader) readErr() error {
	if errors.Is(f.err, io.EOF) {
		return nil
	}
	return f.err
}

// peekAt returns the byte k positions ahead without consuming it.
func (f *fieldReader) peekAt(k int) (byte, bool) {
	for f.w-f.r <= k {
		if !f.fill() {
			return 0, false
		}
	}
	return f.buf[f.r+k], true
}

// startRecord skips empty lines like encoding/csv does. It returns io.EOF at the end of
// the input.
func (f *fieldReader) startRecord() error {
	for {
		c, ok := f.peekAt(0)
		if !ok {
			if err := f.readErr(); err != nil {
				return err
			}
			return io.EOF
		}
		switch c {
		case '\n':
			f.r++
			continue
		case '\r':
			// "\r\n" is an empty line, and so is a lone "\r" right before the end of input.
			n, ok := f.peekAt(1)
			if ok && n == '\n' {
				f.r += 2
				continue
			}
			if !ok && f.readErr() == nil {
				f.r++
				continue
			}
		}
		return nil
	}
}

func (f *fieldReader) rawRecord() ([]string, error) {
	if err := f.startRecord(); err != nil {
		return nil, err
	}
	f.raw = f.raw[:0]
	for {
		cell, term, _, err := f.scanField(false)
		if err != nil {
			return nil, err
		}
		f.raw = append(f.raw, string(cell))
		if term != termComma {
			return f.raw, nil
		}
	}
}

func (f *fieldReader) record(dst []Value, cols []Column) error {
	if err := f.startRecord(); err != nil {
		return err
	}
	defer f.trimScratch()
	for i := 0; ; i++ {
		if i == len(cols) {
			return fmt.Errorf("record has more columns than the %d of the layout", len(cols))
		}
		term, err := f.readCell(cols[i].Type, &dst[i])
		if err != nil {
			return columnError(&cols[i], err)
		}
		if term != termComma {
			if i+1 != len(cols) {
				return fmt.Errorf("record has %d columns, the layout has %d", i+1, len(cols))
			}
			return nil
		}
	}
}

func (f *fieldReader) trimScratch() {
	if cap(f.scratch) > maxRetainedScratch {
		f.scratch = nil
	}
}

// readCell reads one field and decodes it as type t into v.
func (f *fieldReader) readCell(t ValueType, v *Value) (fieldTerm, error) {
	cell, term, long, err := f.scanField(t == ValueBinary || t == ValueString)
	if err != nil {
		return 0, err
	}
	if long {
		return f.readLongCell(t, v)
	}
	switch {
	case string(cell) == NullCell:
		*v = Value{Null: true}
		return term, nil
	case t == ValueInt || t == ValueTimestamp || t == ValueFloat:
		return term, decodeNumber(cell, t, v)
	case t != ValueBinary:
		return term, decodeCell(string(cell), t, v)
	}
	// Same as DecodeString, without copying the cell into a string first.
	*v = Value{Bytes: make([]byte, base64.StdEncoding.DecodedLen(len(cell)))}
	n, err := base64.StdEncoding.Decode(v.Bytes, cell)
	if err != nil {
		return 0, fmt.Errorf("decode binary cell: %w", err)
	}
	v.Bytes = v.Bytes[:n]
	return term, nil
}

// decodeNumber decodes a non-null int, timestamp or float cell like DecodeCell, but
// straight from the bytes. strconv copies the input into the errors it returns, so the
// cell may alias the input window.
func decodeNumber(cell []byte, t ValueType, v *Value) error {
	*v = Value{}
	s := convert.BytesToString(cell)
	if t == ValueFloat {
		x, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return fmt.Errorf("decode double cell: %w", err)
		}
		v.Float = x
		return nil
	}
	x, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("decode %s cell: %w", t, err)
	}
	v.Int = x
	return nil
}

// readLongCell decodes a binary or string cell that does not fit in the input window, so
// it cannot be NullCell. A binary cell is base64 throughout; a string cell is base64 only
// after the `\B` prefix, and is otherwise collected and decoded as usual.
func (f *fieldReader) readLongCell(t ValueType, v *Value) (fieldTerm, error) {
	*v = Value{}
	if t == ValueBinary {
		chunks, total, term, err := f.streamBase64(0)
		if err != nil {
			return 0, fmt.Errorf("decode binary cell: %w", err)
		}
		v.Bytes = joinBytes(chunks, total)
		return term, nil
	}
	if c0, _ := f.peekAt(0); c0 == binaryPrefix[0] {
		if c1, _ := f.peekAt(1); c1 == binaryPrefix[1] {
			chunks, total, term, err := f.streamBase64(len(binaryPrefix))
			if err != nil {
				return 0, fmt.Errorf("decode \\B string cell: %w", err)
			}
			s := joinString(chunks, total)
			if !utf8.ValidString(s) {
				return 0, errInvalidUTF8
			}
			v.Str = s
			return term, nil
		}
	}
	cell, term, _, err := f.scanField(false)
	if err != nil {
		return 0, err
	}
	return term, decodeCell(string(cell), t, v)
}

// scanField reads one field. The returned cell is valid until the next read. When
// allowLong is set and a bare field does not fit in the window, scanField consumes
// nothing and reports long so that the caller can stream it.
func (f *fieldReader) scanField(allowLong bool) (cell []byte, term fieldTerm, long bool, err error) {
	c, ok := f.peekAt(0)
	if !ok {
		if err = f.readErr(); err != nil {
			return nil, 0, false, err
		}
		return nil, termEOF, false, nil
	}
	if c == '"' {
		f.r++
		cell, term, err = f.scanQuoted()
		return cell, term, false, err
	}
	return f.scanBare(allowLong)
}

func (f *fieldReader) scanQuoted() ([]byte, fieldTerm, error) {
	f.scratch = f.scratch[:0]
	for {
		if f.r == f.w && !f.fill() {
			if err := f.readErr(); err != nil {
				return nil, 0, err
			}
			return nil, 0, errQuote
		}
		seg := f.buf[f.r:f.w]
		i := indexQuoteOrCR(seg)
		if i < 0 {
			f.scratch = append(f.scratch, seg...)
			f.r = f.w
			continue
		}
		f.scratch = append(f.scratch, seg[:i]...)
		f.r += i
		if f.buf[f.r] == '\r' {
			// encoding/csv turns every "\r\n" into "\n", also inside quoted fields.
			if n, ok := f.peekAt(1); ok && n == '\n' {
				f.scratch = append(f.scratch, '\n')
				f.r += 2
			} else {
				f.scratch = append(f.scratch, '\r')
				f.r++
			}
			continue
		}
		f.r++
		n, ok := f.peekAt(0)
		if ok && n == '"' {
			f.scratch = append(f.scratch, '"')
			f.r++
			continue
		}
		// The closing quote must end the field.
		if !ok {
			if err := f.readErr(); err != nil {
				return nil, 0, err
			}
			return f.scratch, termEOF, nil
		}
		switch n {
		case ',':
			f.r++
			return f.scratch, termComma, nil
		case '\n':
			f.r++
			return f.scratch, termEOL, nil
		case '\r':
			n2, ok2 := f.peekAt(1)
			if ok2 && n2 == '\n' {
				f.r += 2
				return f.scratch, termEOL, nil
			}
			if !ok2 && f.readErr() == nil {
				f.r++
				return f.scratch, termEOF, nil
			}
		}
		if err := f.readErr(); err != nil {
			return nil, 0, err
		}
		return nil, 0, errQuote
	}
}

func indexQuoteOrCR(b []byte) int {
	for i, c := range b {
		if c == '"' || c == '\r' {
			return i
		}
	}
	return -1
}

func (f *fieldReader) scanBare(allowLong bool) ([]byte, fieldTerm, bool, error) {
	collecting := false
	scanned := 0
	for {
		for i := f.r + scanned; i < f.w; i++ {
			switch f.buf[i] {
			case ',', '\n':
				term := termComma
				if f.buf[i] == '\n' {
					term = termEOL
				}
				cell := f.buf[f.r:i]
				f.r = i + 1
				if collecting {
					f.scratch = append(f.scratch, cell...)
					cell = f.scratch
				}
				if term == termEOL {
					cell = trimCR(cell)
				}
				return cell, term, false, nil
			case '"':
				return nil, 0, false, errBareQuote
			}
		}
		scanned = f.w - f.r
		if scanned == len(f.buf) {
			if !collecting {
				if allowLong {
					return nil, 0, true, nil
				}
				collecting = true
				f.scratch = f.scratch[:0]
			}
			f.scratch = append(f.scratch, f.buf[f.r:f.w]...)
			f.r = f.w
			scanned = 0
		}
		if !f.fill() {
			if err := f.readErr(); err != nil {
				return nil, 0, false, err
			}
			cell := f.buf[f.r:f.w]
			f.r = f.w
			if collecting {
				f.scratch = append(f.scratch, cell...)
				cell = f.scratch
			}
			// encoding/csv drops a "\r" right before the end of input.
			return trimCR(cell), termEOF, false, nil
		}
	}
}

func trimCR(b []byte) []byte {
	if n := len(b); n > 0 && b[n-1] == '\r' {
		return b[:n-1]
	}
	return b
}

// streamBase64 decodes the bare field at the read position, after skipping skip bytes,
// into chunks of csvReadBuffer bytes. The base64 text is never held as a whole. Callers join
// the chunks into one value, so peak memory is about twice the decoded value (the chunks plus
// the joined copy).
func (f *fieldReader) streamBase64(skip int) ([][]byte, int, fieldTerm, error) {
	f.r += skip
	src := &bareFieldReader{f: f}
	dec := base64.NewDecoder(base64.StdEncoding, src)
	var chunks [][]byte
	var chunk []byte
	total := 0
	for {
		if len(chunk) == cap(chunk) {
			chunk = make([]byte, 0, csvReadBuffer)
			chunks = append(chunks, nil)
		}
		n, err := dec.Read(chunk[len(chunk):cap(chunk)])
		chunk = chunk[:len(chunk)+n]
		chunks[len(chunks)-1] = chunk
		total += n
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, 0, 0, err
		}
	}
	// The stream decoder accepts padded quanta followed by more data, which DecodeString
	// rejects, so the decoded length is checked against the text.
	if !src.done || total != src.text/4*3-src.pads {
		return nil, 0, 0, errors.New("illegal base64 data: data after padding")
	}
	return chunks, total, src.term, nil
}

func joinBytes(chunks [][]byte, total int) []byte {
	out := make([]byte, 0, total)
	for _, c := range chunks {
		out = append(out, c...)
	}
	return out
}

func joinString(chunks [][]byte, total int) string {
	var b strings.Builder
	b.Grow(total)
	for _, c := range chunks {
		b.Write(c)
	}
	return b.String()
}

// bareFieldReader yields the bytes of the bare field at the read position and consumes
// its terminator. It does not need to drop the "\r" of a "\r\n" ending: base64 decoding
// ignores it.
type bareFieldReader struct {
	f *fieldReader
	// text counts the base64 characters read, pads the trailing '=' among them.
	text int
	pads int
	done bool
	term fieldTerm
}

func (b *bareFieldReader) Read(p []byte) (int, error) {
	if b.done {
		return 0, io.EOF
	}
	f := b.f
	if f.r == f.w && !f.fill() {
		if err := f.readErr(); err != nil {
			return 0, err
		}
		b.done, b.term = true, termEOF
		return 0, io.EOF
	}
	seg := f.buf[f.r:min(f.w, f.r+len(p))]
	for i, c := range seg {
		switch c {
		case ',', '\n':
			n := copy(p, seg[:i])
			f.r += i + 1
			b.done, b.term = true, termComma
			if c == '\n' {
				b.term = termEOL
			}
			return n, nil
		case '"':
			return 0, errBareQuote
		case '\r':
		case '=':
			b.text++
			b.pads++
		default:
			b.text++
			b.pads = 0
		}
	}
	n := copy(p, seg)
	f.r += n
	return n, nil
}
