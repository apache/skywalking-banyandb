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

package checksum

import "io"

// Verifier defines the interface for computing and verifying checksums
type Verifier interface {
	// ComputeStreaming returns a reader that computes the checksum while reading.
	// When the returned reader is fully consumed, the callback function is called
	// with the computed checksum. This allows for streaming operations without
	// buffering the entire content in memory.
	ComputeStreaming(io.Reader, func(string) error) (io.Reader, error)

	// Verify checks if the data from the reader matches the expected checksum.
	// Returns an error if verification fails or reading fails.
	Verify(io.Reader, string) error
	
	// Wrap returns an io.ReadCloser that transparently verifies the checksum
	// when the returned reader is closed. This enables streaming verification
	// without buffering the entire content in memory.
	Wrap(io.ReadCloser, string) io.ReadCloser
}
