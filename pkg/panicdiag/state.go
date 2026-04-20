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

package panicdiag

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/davecgh/go-spew/spew"
)

const (
	defaultStateLimitBytes = 5 * 1024 * 1024 // 5 MiB
	deepDumpFileName       = "deep-dump.json"
	deepDumpSpewFileName   = "deep-dump.spew"
	spewMaxDepth           = 8 // limits recursion depth on cyclic structures
)

// errStateLimitExceeded is returned by limitedWriter when a write would push the buffer past max.
var errStateLimitExceeded = errors.New("state dump size limit exceeded")

// limitedWriter rejects writes that would push the total past max bytes,
// so the output buffer never grows beyond the configured limit.
type limitedWriter struct {
	buf bytes.Buffer
	max int64
}

func (lw *limitedWriter) Write(p []byte) (int, error) {
	if int64(lw.buf.Len())+int64(len(p)) > lw.max {
		return 0, errStateLimitExceeded
	}
	return lw.buf.Write(p)
}

// BoundedStateWriter writes JSON snapshots under a fixed size limit.
type BoundedStateWriter interface {
	WriteJSON(path string, value any, limitBytes int64) (truncated bool, err error)
}

type jsonStateWriter struct{}

// NewBoundedStateWriter returns the default bounded JSON state writer.
func NewBoundedStateWriter() BoundedStateWriter {
	return jsonStateWriter{}
}

// BoundedSpewWriter writes reflection-based state snapshots under a fixed size limit.
// Unlike BoundedStateWriter, it serialises all struct fields including unexported ones
// via go-spew, making it suitable for dumping complex internal database state.
type BoundedSpewWriter interface {
	WriteSpew(path string, value any, limitBytes int64) (truncated bool, err error)
}

type spewStateWriter struct{}

// NewBoundedSpewWriter returns the default bounded reflection-based state writer.
func NewBoundedSpewWriter() BoundedSpewWriter {
	return spewStateWriter{}
}

// WriteSpew dumps value using go-spew reflection and writes it to path.
// The dump is truncated at limitBytes if the full output exceeds the cap.
// Unlike WriteJSON, a partial spew dump is still written when truncated since
// partial text remains useful, whereas partial JSON would be invalid.
// Sdump is used instead of Fdump because go-spew does not propagate io.Writer
// errors internally, so a limitedWriter cannot intercept the boundary mid-write.
func (spewStateWriter) WriteSpew(path string, value any, limitBytes int64) (bool, error) {
	if limitBytes <= 0 {
		limitBytes = defaultStateLimitBytes
	}
	conf := &spew.ConfigState{
		Indent:                  "  ",
		MaxDepth:                spewMaxDepth,
		DisableMethods:          false,
		DisablePointerAddresses: true,
		DisableCapacities:       true,
		SortKeys:                true,
	}
	dump := conf.Sdump(value)
	truncated := false
	if int64(len(dump)) > limitBytes {
		dump = dump[:limitBytes]
		truncated = true
	}
	if writeErr := os.WriteFile(path, []byte(dump), 0o644); writeErr != nil {
		return truncated, fmt.Errorf("write spew dump: %w", writeErr)
	}
	return truncated, nil
}

// WriteJSON encodes value as indented JSON and writes it to path if the result
// fits within limitBytes. Encoding is streamed through a limitedWriter so the
// output buffer is bounded to limitBytes, avoiding a full-size allocation when
// the state exceeds the cap.
func (jsonStateWriter) WriteJSON(path string, value any, limitBytes int64) (bool, error) {
	if limitBytes <= 0 {
		limitBytes = defaultStateLimitBytes
	}
	lw := &limitedWriter{max: limitBytes}
	enc := json.NewEncoder(lw)
	enc.SetIndent("", "  ")
	if encErr := enc.Encode(value); encErr != nil {
		if errors.Is(encErr, errStateLimitExceeded) {
			return true, fmt.Errorf("state dump exceeds %d-byte limit", limitBytes)
		}
		return false, fmt.Errorf("marshal state dump: %w", encErr)
	}
	if writeErr := os.WriteFile(path, lw.buf.Bytes(), 0o644); writeErr != nil {
		return false, fmt.Errorf("write state dump: %w", writeErr)
	}
	return false, nil
}
