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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/apache/skywalking-banyandb/pkg/fs/remote"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote/checksum"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote/config"
)

var _ remote.FS = (*blobFS)(nil)

type blobFS struct {
	client    *azblob.Client
	verifier  checksum.Verifier
	container string
	basePath  string
}

// NewFS creates a new Azure Blob Storage backed FS.
// The input path must be in the form <container>/<optional-basePath> (without leading slash).
func NewFS(p string, cfg *config.FsConfig) (remote.FS, error) {
	// Ensure cfg and its Azure section are non-nil so that callers are not
	// forced to always prepare them in advance. This makes the function more
	// ergonomic when the destination URL already contains enough information
	// (which is the case for the backup CLI & tests).
	if cfg == nil {
		cfg = &config.FsConfig{}
	}
	if cfg.Azure == nil {
		cfg.Azure = &config.AzureConfig{}
	}

	// Normalise the supplied path and try to infer the container name if it
	// hasn't been provided via cfg.Azure.Container.
	rawPath := strings.Trim(p, "/")

	containerName := cfg.Azure.Container
	if containerName == "" {
		// Extract the first segment of the path as the container name.
		parts := strings.SplitN(rawPath, "/", 2)
		if len(parts) == 0 || parts[0] == "" {
			return nil, fmt.Errorf("container name must be specified either in config or path")
		}
		containerName = parts[0]
		cfg.Azure.Container = containerName
		if len(parts) == 2 {
			rawPath = parts[1]
		} else {
			rawPath = ""
		}
	} else {
		// Remove duplicated container prefix if caller already passed it via the path.
		prefix := containerName + "/"
		if strings.HasPrefix(rawPath, prefix) {
			rawPath = strings.TrimPrefix(rawPath, prefix)
		}
	}

	// Ensure basePath does not start with a leading slash to avoid blobs having
	// an extra leading segment and to make listing logic consistent.
	basePath := strings.TrimPrefix(rawPath, "/")

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
	azureCfg := cfg.Azure
	endpoint := azureCfg.AzureEndpoint
	if connStr := os.Getenv("AZURE_STORAGE_CONNECTION_STRING"); connStr != "" {
		return azblob.NewClientFromConnectionString(connStr, nil)
	}

	// Prefer SAS token if supplied
	if azureCfg.AzureSASToken != "" {
		// Ensure the token starts without leading "?"
		sas := strings.TrimPrefix(azureCfg.AzureSASToken, "?")
		var serviceURL string
		if strings.Contains(endpoint, "?") {
			serviceURL = endpoint + "&" + sas
		} else {
			serviceURL = endpoint + "?" + sas
		}
		return azblob.NewClientWithNoCredential(serviceURL, nil)
	}

	if azureCfg.AzureAccountName != "" && azureCfg.AzureAccountKey != "" {
		endpoint := azureCfg.AzureEndpoint
		if endpoint == "" {
			endpoint = fmt.Sprintf("https://%s.blob.core.windows.net/", azureCfg.AzureAccountName)
		}
		cred, err := azblob.NewSharedKeyCredential(
			azureCfg.AzureAccountName,
			azureCfg.AzureAccountKey,
		)
		if err != nil {
			return nil, fmt.Errorf("invalid shared key credential: %w", err)
		}
		return azblob.NewClientWithSharedKeyCredential(endpoint, cred, nil)
	}

	return nil, errors.New("no Azure credentials found: set AZURE_STORAGE_CONNECTION_STRING, sas_token, or account_name+account_key")
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

	wrappedReader, getHash := b.verifier.ComputeAndWrap(data)

	// upload
	_, err := b.client.UploadStream(ctx, b.container, blobName, wrappedReader, nil)
	if err != nil {
		return fmt.Errorf("failed to upload blob: %w", err)
	}

	// get hash
	hash, err := getHash()
	if err != nil {
		return fmt.Errorf("failed to compute hash: %w", err)
	}

	// set metadata
	blobClient := b.client.ServiceClient().NewContainerClient(b.container).NewBlobClient(blobName)
	metadata := map[string]*string{
		"checksum_sha256": ptr(hash),
	}
	_, err = blobClient.SetMetadata(ctx, metadata, nil)
	if err != nil {
		return fmt.Errorf("failed to set metadata: %w", err)
	}

	return nil
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

	const checksumKey = "checksum_sha256"
	expected := ""
	if resp.Metadata != nil {
		for k, v := range resp.Metadata {
			if strings.EqualFold(k, checksumKey) && v != nil {
				expected = *v
				break
			}
		}
	}
	if expected == "" {
		_ = resp.Body.Close()
		availableKeys := make([]string, 0, len(resp.Metadata))
		for k := range resp.Metadata {
			availableKeys = append(availableKeys, k)
		}
		return nil, fmt.Errorf("sha256 metadata missing for blob %s, available metadata: %v",
			blobName, availableKeys)
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

// helper function for pointer to string.
func ptr(s string) *string {
	return &s
}
