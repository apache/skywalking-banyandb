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
)

type sha256Verifier struct{}

// ComputeStreaming implements Verifier.ComputeStreaming by returning a reader that
// computes SHA-256 while streaming. When the reader is fully consumed, the callback
// is called with the computed hash.
func (v *sha256Verifier) ComputeStreaming(r io.Reader, callback func(string) error) (io.Reader, error) {
	hasher := sha256.New()
	teeReader := io.TeeReader(r, hasher)
	
	// Return a reader that will call the callback when it's done
	return &callbackReader{
		Reader: teeReader,
		onEOF: func() error {
			hash := hex.EncodeToString(hasher.Sum(nil))
			return callback(hash)
		},
	}, nil
}

func (v *sha256Verifier) Verify(r io.Reader, expected string) error {
	hasher := sha256.New()
	_, err := io.Copy(hasher, r)
	if err != nil {
		return err
	}
	actual := hex.EncodeToString(hasher.Sum(nil))
	if actual != expected {
		return fmt.Errorf("sha256 mismatch: expected %s, got %s", expected, actual)
	}
	return nil
}

// Wrap implements Verifier.Wrap by returning a ReadCloser that computes SHA-256
// while streaming and verifies it on Close().
func (v *sha256Verifier) Wrap(rc io.ReadCloser, expected string) io.ReadCloser {
	h := sha256.New()
	return &sha256ReadCloser{
		Reader:   io.TeeReader(rc, h),
		closer:   rc,
		hasher:   h,
		expected: expected,
	}
}

// DefaultSHA256Verifier returns a reusable SHA256 verifier
func DefaultSHA256Verifier() (Verifier, error) {
	return &sha256Verifier{}, nil
}

// --- Streaming helpers ---

// sha256ReadCloser wraps an io.ReadCloser and verifies the SHA-256 checksum
// after the stream is fully consumed and Close() is invoked.
type sha256ReadCloser struct {
	io.Reader
	closer   io.Closer
	hasher   hash.Hash
	expected string
}

func (s *sha256ReadCloser) Read(p []byte) (int, error) {
	return s.Reader.Read(p)
}

func (s *sha256ReadCloser) Close() error {
	if err := s.closer.Close(); err != nil {
		return err
	}
	actual := hex.EncodeToString(s.hasher.Sum(nil))
	if actual != s.expected {
		return fmt.Errorf("sha256 mismatch: expected %s, got %s", s.expected, actual)
	}
	return nil
}

// SHA256VerifyingReadCloser returns a ReadCloser that transparently computes
// SHA-256 while the caller reads from rc. When the caller closes the returned
// reader, the computed hash is compared with expected. If they differ, Close()
// returns an error.
//
// Deprecated: Use DefaultSHA256Verifier().Wrap() instead.
func SHA256VerifyingReadCloser(rc io.ReadCloser, expected string) io.ReadCloser {
	v := &sha256Verifier{}
	return v.Wrap(rc, expected)
}

// callbackReader is a reader that calls a function when it reaches EOF
type callbackReader struct {
	io.Reader
	onEOF    func() error
	eofCalled bool
}

func (r *callbackReader) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	if err == io.EOF && !r.eofCalled {
		r.eofCalled = true
		callbackErr := r.onEOF()
		if callbackErr != nil {
			return n, callbackErr
		}
	}
	return n, err
}
