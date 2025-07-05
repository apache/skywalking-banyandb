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

// Package azure provides an Azure Blob Storage implementation of the remote.FS interface.
package azure

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote/config"
	"io"
	"path"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/apache/skywalking-banyandb/pkg/fs/remote"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote/checksum"
)

var _ remote.FS = (*blobFS)(nil)

type blobFS struct {
	client    *azblob.Client
	container string
	basePath  string
	verifier  checksum.Verifier
}

// NewFS creates a new Azure Blob Storage backed FS.
// The input path must be in the form <container>/<optional-basePath> (without leading slash).
func NewFS(p string, cfg *config.FsConfig) (remote.FS, error) {
	containerName, basePath := extractContainerAndBase(p)
	if containerName == "" {
		return nil, fmt.Errorf("container name not provided")
	}
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}

	client, err := buildClient(cfg)
	if err != nil {
		return nil, err
	}

	verifier, err := checksum.DefaultSHA256Verifier()
	if err != nil {
		return nil, err
	}

	// ensure container exists (create if not)
	if err := ensureContainer(context.Background(), client, containerName); err != nil {
		return nil, err
	}

	return &blobFS{
		client:    client,
		container: containerName,
		basePath:  basePath,
		verifier:  verifier,
	}, nil
}

func buildClient(cfg *config.FsConfig) (*azblob.Client, error) {
	endpoint := cfg.AzureEndpoint
	if endpoint == "" {
		if cfg.AzureAccountName == "" {
			return nil, fmt.Errorf("AzureAccountName is required when AzureEndpoint is empty")
		}
		endpoint = fmt.Sprintf("https://%s.blob.core.windows.net/", cfg.AzureAccountName)
	}

	// Prefer SAS token if supplied
	if cfg.AzureSASToken != "" {
		// Ensure the token starts without leading "?"
		sas := strings.TrimPrefix(cfg.AzureSASToken, "?")
		var serviceURL string
		if strings.Contains(endpoint, "?") {
			serviceURL = endpoint + "&" + sas
		} else {
			serviceURL = endpoint + "?" + sas
		}
		return azblob.NewClientWithNoCredential(serviceURL, nil)
	}

	if cfg.AzureAccountName == "" || cfg.AzureAccountKey == "" {
		return nil, fmt.Errorf("either SAS token or account name/key must be provided")
	}

	cred, err := azblob.NewSharedKeyCredential(cfg.AzureAccountName, cfg.AzureAccountKey)
	if err != nil {
		return nil, fmt.Errorf("create shared key credential: %w", err)
	}
	return azblob.NewClientWithSharedKeyCredential(endpoint, cred, nil)
}

func ensureContainer(ctx context.Context, client *azblob.Client, containerName string) error {
	_, err := client.CreateContainer(ctx, containerName, nil)
	if err == nil {
		return nil
	}
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		if respErr.ErrorCode == "ContainerAlreadyExists" {
			return nil
		}
	}
	return fmt.Errorf("failed to ensure container %s exists: %w", containerName, err)
}

func extractContainerAndBase(p string) (string, string) {
	trimmed := strings.Trim(p, "/")
	if trimmed == "" {
		return "", ""
	}
	parts := strings.SplitN(trimmed, "/", 2)
	container := parts[0]
	var base string
	if len(parts) > 1 {
		base = parts[1]
	}
	return container, base
}

func (b *blobFS) getFullPath(p string) string {
	if b.basePath == "" {
		return p
	}
	return path.Join(b.basePath, p)
}

func (b *blobFS) Upload(ctx context.Context, p string, data io.Reader) error {
	if b.verifier == nil {
		return fmt.Errorf("verifier not initialized")
	}

	blobName := b.getFullPath(p)

	// Use a channel to get the hash from the callback
	hashCh := make(chan string, 1)
	errCh := make(chan error, 1)

	// Create a streaming reader that computes the hash while uploading
	streamReader, err := b.verifier.ComputeStreaming(data, func(hash string) error {
		hashCh <- hash
		return nil
	})

	if err != nil {
		return err
	}

	// Upload the data with streaming
	_, err = b.client.UploadStream(ctx, b.container, blobName, streamReader, nil)
	if err != nil {
		return err
	}

	// Get the computed hash
	var hash string
	select {
	case hash = <-hashCh:
		// Got the hash
	case err = <-errCh:
		return fmt.Errorf("error computing hash: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	}

	// Set the metadata with the hash
	_, err = b.client.SetBlobMetadata(ctx, b.container, blobName, map[string]*string{
		"sha256": ptr(hash),
	}, nil)

	return err
}

func (b *blobFS) Download(ctx context.Context, p string) (io.ReadCloser, error) {
	if b.verifier == nil {
		return nil, fmt.Errorf("verifier not initialized")
	}

	blobName := b.getFullPath(p)
	resp, err := b.client.DownloadStream(ctx, b.container, blobName, nil)
	if err != nil {
		return nil, err
	}

	expected := ""
	if resp.Metadata != nil && resp.Metadata["sha256"] != nil {
		expected = *resp.Metadata["sha256"]
	}
	if expected == "" {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("sha256 metadata missing for blob: %s", blobName)
	}

	// Use verifier to wrap the response body with streaming verification
	return b.verifier.Wrap(resp.Body, expected), nil
}

func (b *blobFS) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := b.getFullPath(prefix)
	pager := b.client.NewListBlobsFlatPager(b.container, &azblob.ListBlobsFlatOptions{Prefix: &fullPrefix})
	var files []string
	// prepare base prefix once to avoid variable shadowing
	basePrefix := b.basePath
	if basePrefix != "" && !strings.HasSuffix(basePrefix, "/") {
		basePrefix += "/"
	}
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Segment.BlobItems {
			key := *item.Name
			if b.basePath != "" {
				key = strings.TrimPrefix(key, basePrefix)
			}
			files = append(files, key)
		}
	}
	return files, nil
}

func (b *blobFS) Delete(ctx context.Context, p string) error {
	blobName := b.getFullPath(p)
	_, err := b.client.DeleteBlob(ctx, b.container, blobName, nil)
	return err
}

func (b *blobFS) Close() error {
	// azblob.Client is stateless, nothing to close
	return nil
}

// helper function for pointer to string
func ptr(s string) *string {
	return &s
}
