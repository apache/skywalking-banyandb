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

// Package remote provides an interface for interacting with various remote file systems such as S3, GCS, and distributed file systems for backup/restore operations.
package remote

import (
	"context"
	"io"
)

// FS defines the interface for interacting with various remote file systems
// such as S3, GCS, and distributed file systems for backup/restore operations.
type FS interface {
	// Upload writes a file to the remote file system at the specified path.
	// The path should include the snapshot directory (e.g. "snapshot123/file.data").
	// Implementations must handle both new file creation and overwrite scenarios.
	Upload(ctx context.Context, path string, data io.Reader) error

	// Download retrieves a file from the remote file system at the specified path.
	// Returns a ReadCloser that must be closed by the caller after consumption.
	Download(ctx context.Context, path string) (io.ReadCloser, error)

	// List returns all file paths under a given prefix (typically a snapshot directory).
	// Used to check existing files during backup/restore operations.
	List(ctx context.Context, prefix string) ([]string, error)

	// Delete removes a specific file from the remote file system.
	// Path must include the snapshot directory and file name.
	Delete(ctx context.Context, path string) error

	// Close releases any resources or connections associated with the remote FS.
	// Must be called when the client is no longer needed.
	Close() error
}
