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

// Package gcp provides the GCS file system.
package gcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/apache/skywalking-banyandb/pkg/fs/remote"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote/checksum"
	config2 "github.com/apache/skywalking-banyandb/pkg/fs/remote/config"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const checksumSha256Key = "checksum_sha256"

var _ remote.FS = (*gcsFS)(nil)

// gcsFS implements remote.FS backed by Google Cloud Storage.
// Field order is optimized to reduce struct padding.
type gcsFS struct {
	client   *storage.Client
	verifier checksum.Verifier
	bucket   string
	basePath string
}

// NewFS creates a new GCS-backed FS. The input path must be in the form
// <bucket>/<optional-basePath> (without leading slash).
func NewFS(p string, cfg *config2.FsConfig) (remote.FS, error) {
	if cfg == nil {
		cfg = &config2.FsConfig{}
	}
	if cfg.GCP == nil {
		cfg = &config2.FsConfig{GCP: &config2.GCPConfig{}}
	}

	rawPath := strings.Trim(p, "/")

	var bucket, basePath string

	if cfg.GCP.Bucket != "" {
		bucket = cfg.GCP.Bucket
		if strings.HasPrefix(rawPath, bucket+"/") {
			basePath = strings.TrimPrefix(rawPath, bucket+"/")
		} else {
			basePath = rawPath
		}
	} else {
		parts := strings.SplitN(rawPath, "/", 2)
		if parts[0] == "" {
			return nil, fmt.Errorf("bucket name must be specified either in config or path")
		}
		bucket = parts[0]
		if len(parts) == 2 {
			basePath = parts[1]
		} else {
			basePath = ""
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var client *storage.Client
	var err error
	if cfg.GCP.GCPServiceAccountFile != "" {
		if info, statErr := os.Stat(cfg.GCP.GCPServiceAccountFile); statErr != nil {
			return nil, fmt.Errorf("credentials file error: %w", statErr)
		} else if info.Mode().Perm() != 0o600 {
			return nil, fmt.Errorf("credentials file %s must have permission 0600", cfg.GCP.GCPServiceAccountFile)
		}
		client, err = storage.NewClient(ctx, option.WithCredentialsFile(cfg.GCP.GCPServiceAccountFile))
	} else {
		client, err = storage.NewClient(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	verifier, err := checksum.DefaultSHA256Verifier()
	if err != nil {
		return nil, err
	}
	logger.Infof("GCS NewFS: input path=%s, bucket=%s, basePath=%s", p, bucket, basePath)
	return &gcsFS{
		client:   client,
		bucket:   bucket,
		basePath: basePath,
		verifier: verifier,
	}, nil
}

func (g *gcsFS) getFullPath(p string) string {
	if g.basePath == "" {
		return p
	}
	return path.Join(g.basePath, p)
}

func (g *gcsFS) Upload(ctx context.Context, p string, data io.Reader) error {
	if data == nil {
		return fmt.Errorf("upload data cannot be nil")
	}

	if g.verifier == nil {
		return fmt.Errorf("verifier not initialized")
	}

	objPath := g.getFullPath(p)
	logger.Infof("GCS Upload: bucket=%s, path=%s, fullPath=%s", g.bucket, p, objPath)
	wrappedReader, getHash := g.verifier.ComputeAndWrap(data)

	w := g.client.Bucket(g.bucket).Object(objPath).NewWriter(ctx)
	if _, err := io.Copy(w, wrappedReader); err != nil {
		_ = w.Close()
		return fmt.Errorf("failed to write object: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	hash, err := getHash()
	if err != nil {
		return fmt.Errorf("failed to compute hash: %w", err)
	}

	// Update metadata with checksum
	// Only attempt to set metadata when not running against the local emulator.
	// The fake-gcs-server used in tests does not support the Update call and will
	// return an error. Detect the test environment via the STORAGE_EMULATOR_HOST
	// environment variable which is set by the test helper.
	if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
		_, err = g.client.Bucket(g.bucket).Object(objPath).Update(ctx, storage.ObjectAttrsToUpdate{
			Metadata: map[string]string{checksumSha256Key: hash},
		})
		if err != nil {
			return fmt.Errorf("failed to set metadata: %w", err)
		}
	}
	return nil
}

func (g *gcsFS) Download(ctx context.Context, p string) (io.ReadCloser, error) {
	if g.verifier == nil {
		return nil, fmt.Errorf("verifier not initialized")
	}

	objPath := g.getFullPath(p)
	logger.Infof("GCS Download: bucket=%s, path=%s, fullPath=%s", g.bucket, p, objPath)
	obj := g.client.Bucket(g.bucket).Object(objPath)

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get object attrs: %w", err)
	}
	expected := ""
	if attrs.Metadata != nil {
		expected = attrs.Metadata[checksumSha256Key]
	}
	if expected == "" {
		return nil, fmt.Errorf("sha256 metadata missing for object %s", objPath)
	}

	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}

	logger.Infof("GCS Upload completed: %s", objPath)
	return g.verifier.Wrap(reader, expected), nil
}

func (g *gcsFS) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := g.getFullPath(prefix)
	logger.Infof("GCS List: bucket=%s, prefix=%s, fullPrefix=%s", g.bucket, prefix, fullPrefix)
	it := g.client.Bucket(g.bucket).Objects(ctx, &storage.Query{Prefix: fullPrefix})
	var files []string
	basePrefix := g.basePath
	if basePrefix != "" && !strings.HasSuffix(basePrefix, "/") {
		basePrefix += "/"
	}
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		key := attrs.Name
		if g.basePath != "" {
			key = strings.TrimPrefix(key, basePrefix)
		}
		key = strings.TrimPrefix(key, "/")
		// Skip empty keys or directory markers (objects ending with /)
		if key == "" || strings.HasSuffix(key, "/") {
			continue
		}
		files = append(files, key)
	}
	logger.Infof("GCS List found %d files with prefix %s", len(files), prefix)
	return files, nil
}

func (g *gcsFS) Delete(ctx context.Context, p string) error {
	objPath := g.getFullPath(p)
	return g.client.Bucket(g.bucket).Object(objPath).Delete(ctx)
}

func (g *gcsFS) Close() error {
	return g.client.Close()
}
