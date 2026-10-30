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

package test

import (
	"bytes"
	"io"
	"os"
	"sync"
)

// CapturedStderr holds what the process wrote to os.Stderr while it was
// replaced by CaptureStderr.
type CapturedStderr struct {
	previous *os.File
	buf      bytes.Buffer
	mux      sync.Mutex
}

// CaptureStderr replaces os.Stderr with a pipe and collects what is written to
// it. Logging reads os.Stderr when it is initialized, so a test that asserts on
// what a server printed calls this before it initializes logging.
//
// The pipe is drained for the life of the process, so a writer that still
// holds it after Restore never blocks.
func CaptureStderr() (*CapturedStderr, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	c := &CapturedStderr{previous: os.Stderr}
	os.Stderr = w
	go func() { _, _ = io.Copy(c, r) }()
	return c, nil
}

// Write appends p to the captured output.
func (c *CapturedStderr) Write(p []byte) (int, error) {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.buf.Write(p)
}

// String returns what has been captured so far.
func (c *CapturedStderr) String() string {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.buf.String()
}

// Restore puts back the os.Stderr that CaptureStderr replaced. Logging
// initialized during the capture keeps writing to the pipe until it is
// initialized again.
func (c *CapturedStderr) Restore() {
	os.Stderr = c.previous
}
