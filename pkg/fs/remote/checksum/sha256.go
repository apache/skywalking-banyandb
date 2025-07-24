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

// Package checksum provides functions for computing checksums algorithms and verifying.
package checksum

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"sync"
)

type sha256Verifier struct{}

type hashingReader struct {
	reader io.Reader
	hash   hash.Hash
	err    error
	mu     sync.RWMutex
	done   bool
}

func (hr *hashingReader) Read(p []byte) (n int, err error) {
	n, err = hr.reader.Read(p)
	if n > 0 {
		hr.hash.Write(p[:n])
	}

	if err != nil {
		hr.mu.Lock()
		if err == io.EOF {
			hr.done = true
		} else {
			hr.err = err
		}
		hr.mu.Unlock()
	}

	return n, err
}

func (hr *hashingReader) getHash() (string, error) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if hr.err != nil {
		return "", fmt.Errorf("read error: %w", hr.err)
	}

	if !hr.done {
		return "", fmt.Errorf("stream not fully read")
	}

	return hex.EncodeToString(hr.hash.Sum(nil)), nil
}

func (v *sha256Verifier) ComputeAndWrap(r io.Reader) (io.Reader, func() (string, error)) {
	hr := &hashingReader{
		reader: r,
		hash:   sha256.New(),
	}
	return hr, hr.getHash
}

func (v *sha256Verifier) Wrap(rc io.ReadCloser, expected string) io.ReadCloser {
	h := sha256.New()
	return &verifyingReadCloser{
		Reader:   io.TeeReader(rc, h),
		closer:   rc,
		hasher:   h,
		expected: expected,
	}
}

type verifyingReadCloser struct {
	io.Reader
	closer   io.Closer
	hasher   hash.Hash
	expected string
}

func (v *verifyingReadCloser) Close() error {
	if err := v.closer.Close(); err != nil {
		return err
	}
	actual := hex.EncodeToString(v.hasher.Sum(nil))
	if actual != v.expected {
		return fmt.Errorf("sha256 mismatch: expected %s, got %s", v.expected, actual)
	}
	return nil
}

// DefaultSHA256Verifier returns a default SHA256 Verifier.
func DefaultSHA256Verifier() (Verifier, error) {
	return &sha256Verifier{}, nil
}
