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

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/apache/skywalking-banyandb/pkg/fs/remote"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote/checksum"
	config2 "github.com/apache/skywalking-banyandb/pkg/fs/remote/config"
)

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
		cfg.GCP = &config2.GCPConfig{}
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
		if len(parts) == 0 || parts[0] == "" {
			return nil, fmt.Errorf("bucket name must be specified either in config or path")
		}
		bucket = parts[0]
		cfg.GCP.Bucket = bucket
		if len(parts) == 2 {
			basePath = parts[1]
		} else {
			basePath = ""
		}
	}

	ctx := context.Background()
	var client *storage.Client
	var err error
	if cfg.GCP.GCPServiceAccountFile != "" {
		client, err = storage.NewClient(ctx, option.WithCredentialsFile(cfg.GCP.GCPServiceAccountFile))
	} else {
		if os.Getenv("STORAGE_EMULATOR_HOST") != "" {
			client, err = storage.NewClient(ctx, option.WithoutAuthentication())
		} else {
			client, err = storage.NewClient(ctx)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	verifier, err := checksum.DefaultSHA256Verifier()
	if err != nil {
		return nil, err
	}

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
	_, err = g.client.Bucket(g.bucket).Object(objPath).Update(ctx, storage.ObjectAttrsToUpdate{
		Metadata: map[string]string{"checksum_sha256": hash},
	})
	if err != nil {
		return fmt.Errorf("failed to set metadata: %w", err)
	}
	return nil
}

func (g *gcsFS) Download(ctx context.Context, p string) (io.ReadCloser, error) {
	if g.verifier == nil {
		return nil, fmt.Errorf("verifier not initialized")
	}

	objPath := g.getFullPath(p)
	obj := g.client.Bucket(g.bucket).Object(objPath)

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get object attrs: %w", err)
	}
	expected := ""
	const checksumKey = "checksum_sha256"
	if attrs.Metadata != nil {
		expected = attrs.Metadata[checksumKey]
	}
	if expected == "" {
		return nil, fmt.Errorf("sha256 metadata missing for object %s", objPath)
	}

	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}

	return g.verifier.Wrap(r, expected), nil
}

func (g *gcsFS) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := g.getFullPath(prefix)
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
		files = append(files, key)
	}
	return files, nil
}

func (g *gcsFS) Delete(ctx context.Context, p string) error {
	objPath := g.getFullPath(p)
	return g.client.Bucket(g.bucket).Object(objPath).Delete(ctx)
}

func (g *gcsFS) Close() error {
	return g.client.Close()
}
