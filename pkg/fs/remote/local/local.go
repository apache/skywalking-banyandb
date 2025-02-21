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

// Package local provides a local file system implementation.
package local

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/apache/skywalking-banyandb/pkg/fs/remote"
)

const dirPerm = 0o755

var _ remote.FS = (*fs)(nil)

type fs struct {
	baseDir string
}

// NewFS creates a new local file system.
func NewFS(baseDir string) (remote.FS, error) {
	if err := os.MkdirAll(baseDir, dirPerm); err != nil {
		return nil, err
	}
	return &fs{baseDir: baseDir}, nil
}

func (l *fs) Upload(_ context.Context, path string, data io.Reader) error {
	fullPath := filepath.Join(l.baseDir, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), dirPerm); err != nil {
		return err
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, data)
	return err
}

func (l *fs) Download(_ context.Context, path string) (io.ReadCloser, error) {
	fullPath := filepath.Join(l.baseDir, path)
	return os.Open(fullPath)
}

func (l *fs) List(_ context.Context, prefix string) ([]string, error) {
	var files []string
	fullPath := filepath.Join(l.baseDir, prefix)

	err := filepath.Walk(fullPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			relPath, err := filepath.Rel(l.baseDir, path)
			if err != nil {
				return err
			}
			files = append(files, filepath.ToSlash(relPath))
		}
		return nil
	})

	if os.IsNotExist(err) {
		return nil, nil
	}
	return files, err
}

func (l *fs) Delete(_ context.Context, path string) error {
	fullPath := filepath.Join(l.baseDir, path)
	return os.Remove(fullPath)
}

func (l *fs) Close() error {
	return nil
}
